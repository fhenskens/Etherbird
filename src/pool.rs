use crate::{
    AttributeSetter, ConfigError, CreatedHook, Error, FifoQueue, Lifecycle, OperationQueue,
    PriorityQueue, ResourceHandle, ResourceState, RetryPolicy, Supervisor,
};
use std::{
    any::Any,
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Notify, oneshot, watch},
    task::JoinSet,
    time::{Instant, sleep},
};

#[derive(Clone, Debug)]
pub struct PoolConfig {
    pub min_size: usize,
    pub max_size: usize,
    pub idle_timeout: Duration,
    /// Default queue selection. Ignored when an explicit queue factory is supplied.
    pub priority_queue: bool,
}
impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_size: 1,
            max_size: 1,
            idle_timeout: Duration::from_secs(60),
            priority_queue: false,
        }
    }
}
impl PoolConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_size == 0 {
            return Err(ConfigError {
                field: "max_size",
                reason: "must be positive",
            });
        }
        if self.min_size > self.max_size {
            return Err(ConfigError {
                field: "min_size",
                reason: "must not exceed max_size",
            });
        }
        if self.idle_timeout.is_zero() {
            return Err(ConfigError {
                field: "idle_timeout",
                reason: "must be positive",
            });
        }
        Ok(())
    }
}
struct Entry<L: Lifecycle> {
    supervisor: Supervisor<L>,
    leased: AtomicBool,
    idle_since: Mutex<Instant>,
    relay: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
type Admission<L> = Result<ResourceLease<L>, Error<<L as Lifecycle>::Error>>;
type Job<L> = Box<dyn FnOnce(Admission<L>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;
/// Opaque queued work with scheduling metadata. Only the pool can execute it.
pub struct QueuedOperation<L: Lifecycle> {
    priority: i32,
    sequence: u64,
    job: Job<L>,
    cancelled: Arc<AtomicBool>,
}
impl<L: Lifecycle> QueuedOperation<L> {
    pub fn priority(&self) -> i32 {
        self.priority
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}
type Operation<L> = QueuedOperation<L>;
impl<L: Lifecycle> PartialEq for Operation<L> {
    fn eq(&self, other: &Self) -> bool {
        (self.priority, self.sequence) == (other.priority, other.sequence)
    }
}
impl<L: Lifecycle> Eq for Operation<L> {}
impl<L: Lifecycle> PartialOrd for Operation<L> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<L: Lifecycle> Ord for Operation<L> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (other.priority, other.sequence).cmp(&(self.priority, self.sequence))
    }
}
struct Contents<L: Lifecycle> {
    entries: Vec<Arc<Entry<L>>>,
    queue: Box<dyn OperationQueue<QueuedOperation<L>>>,
    sequence: u64,
    attributes: BTreeMap<String, AttributeSetter<L::Resource>>,
    values: BTreeMap<String, Arc<dyn Any + Send + Sync>>,
    retiring: usize,
}
struct Shared<L: Lifecycle> {
    config: PoolConfig,
    factory: Box<dyn Fn() -> Supervisor<L> + Send + Sync>,
    contents: Mutex<Contents<L>>,
    operations: Mutex<JoinSet<()>>,
    fast_admission: bool,
    changed: watch::Sender<u64>,
    dispatcher: Arc<Notify>,
    stopping: watch::Sender<bool>,
    done: watch::Sender<bool>,
    waiters: AtomicUsize,
    last_resource: Mutex<Option<Arc<L::Resource>>>,
    on_created: Mutex<Option<CreatedHook<L::Resource, L::Error>>>,
}
impl<L: Lifecycle> Shared<L> {
    fn apply_attribute(
        &self,
        contents: &Contents<L>,
        name: &str,
        setter: &AttributeSetter<L::Resource>,
    ) {
        let last_resource = self.last_resource.lock().unwrap().clone();
        let mut last_updated = false;
        for entry in &contents.entries {
            let mut attributes = entry.supervisor.attributes.lock().unwrap();
            if let Some(resource) = &attributes.last {
                setter(resource);
                last_updated |= last_resource
                    .as_ref()
                    .is_some_and(|last| Arc::ptr_eq(last, resource));
            }
            attributes.setters.insert(name.to_owned(), setter.clone());
        }
        if !last_updated && let Some(resource) = last_resource {
            setter(&resource);
        }
    }
    fn notify(&self) {
        self.dispatcher.notify_one();
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }
    fn grow(self: &Arc<Self>, contents: &mut Contents<L>) {
        let supervisor = (self.factory)();
        for (name, setter) in &contents.attributes {
            supervisor.set_attribute(name.clone(), setter.clone());
        }
        let weak = Arc::downgrade(self);
        // Track creation before connect; callers can read attributes during connection attempts.
        supervisor.attributes.lock().unwrap().after_created = Some(Arc::new(move |resource| {
            let shared = weak.upgrade();
            Box::pin(async move {
                if let Some(shared) = shared {
                    {
                        let mut last = shared.last_resource.lock().unwrap();
                        // Timed-out created callbacks may finish after pool stop.
                        if *shared.stopping.borrow() {
                            return Ok(());
                        }
                        *last = Some(resource.clone());
                    }
                    let hook = shared.on_created.lock().unwrap().clone();
                    if let Some(hook) = hook {
                        hook(resource).await?;
                    }
                }
                Ok(())
            })
        }));
        supervisor.begin();
        let entry = Arc::new(Entry {
            supervisor,
            leased: AtomicBool::new(false),
            idle_since: Mutex::new(Instant::now()),
            relay: Mutex::new(None),
        });
        let state = entry.supervisor.subscribe();
        let changed = self.changed.clone();
        let dispatcher = self.dispatcher.clone();
        let relay = tokio::spawn(relay_state(state, changed, dispatcher));
        *entry.relay.lock().unwrap() = Some(relay);
        contents.entries.push(entry);
        self.notify();
    }
}
struct PoolOwner {
    stopping: watch::Sender<bool>,
    started: AtomicBool,
}
async fn relay_state<R: Send + Sync + 'static>(
    mut state: watch::Receiver<crate::ResourceStatus<R>>,
    changed: watch::Sender<u64>,
    dispatcher: Arc<Notify>,
) {
    loop {
        // Observe before notifying. Ready may have arrived before our first
        // poll; forwarding only subsequent changes would strand pool waiters.
        let stopped = state.borrow_and_update().state == ResourceState::Stopped;
        changed.send_modify(|revision| *revision = revision.wrapping_add(1));
        dispatcher.notify_one();
        if stopped || state.changed().await.is_err() {
            break;
        }
    }
}
impl Drop for PoolOwner {
    fn drop(&mut self) {
        self.stopping.send_replace(true);
    }
}

/// Demand-grown pool of independently supervised resources. Minimum counts all entries.
/// Start inside a Tokio runtime. Lower priority numbers run first when priority_queue is enabled.
pub struct Pool<L: Lifecycle> {
    shared: Arc<Shared<L>>,
    owner: Arc<PoolOwner>,
}
impl<L: Lifecycle> Clone for Pool<L> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            owner: self.owner.clone(),
        }
    }
}
impl<L: Lifecycle> Pool<L> {
    /// Validate before constructing channels, queues or invoking the factory.
    /// Each supervisor factory remains responsible for its lifecycle Config.
    pub fn try_new(
        factory: impl Fn() -> Supervisor<L> + Send + Sync + 'static,
        config: PoolConfig,
    ) -> Result<Self, ConfigError> {
        config.validate()?;
        Ok(Self::new(factory, config))
    }
    pub fn try_start(
        factory: impl Fn() -> Supervisor<L> + Send + Sync + 'static,
        config: PoolConfig,
    ) -> Result<Self, ConfigError> {
        let pool = Self::try_new(factory, config)?;
        pool.begin();
        Ok(pool)
    }
    pub fn try_new_with_queue_factory<Q>(
        factory: impl Fn() -> Supervisor<L> + Send + Sync + 'static,
        config: PoolConfig,
        queue_factory: impl FnOnce() -> Q,
    ) -> Result<Self, ConfigError>
    where
        Q: OperationQueue<QueuedOperation<L>> + 'static,
    {
        config.validate()?;
        let queue = queue_factory();
        if !queue.is_empty() {
            return Err(ConfigError {
                field: "queue_factory",
                reason: "must return an empty queue",
            });
        }
        Ok(Self::with_queue(factory, config, move || queue, false))
    }
    pub fn try_start_with_queue_factory<Q>(
        factory: impl Fn() -> Supervisor<L> + Send + Sync + 'static,
        config: PoolConfig,
        queue_factory: impl FnOnce() -> Q,
    ) -> Result<Self, ConfigError>
    where
        Q: OperationQueue<QueuedOperation<L>> + 'static,
    {
        let pool = Self::try_new_with_queue_factory(factory, config, queue_factory)?;
        pool.begin();
        Ok(pool)
    }
    pub fn start(
        factory: impl Fn() -> Supervisor<L> + Send + Sync + 'static,
        config: PoolConfig,
    ) -> Self {
        let pool = Self::new(factory, config);
        pool.begin();
        pool
    }
    /// Construct a lazy pool. Borrowing or executing work starts it automatically.
    pub fn new(
        factory: impl Fn() -> Supervisor<L> + Send + Sync + 'static,
        config: PoolConfig,
    ) -> Self {
        if config.priority_queue {
            Self::with_queue(factory, config, PriorityQueue::default, true)
        } else {
            Self::with_queue(factory, config, FifoQueue::default, true)
        }
    }
    /// Create one custom operation queue for this pool.
    /// The returned queue must be empty. Queue methods run under the pool lock.
    /// The explicit queue controls ordering regardless of `config.priority_queue`.
    pub fn start_with_queue_factory<Q>(
        factory: impl Fn() -> Supervisor<L> + Send + Sync + 'static,
        config: PoolConfig,
        queue_factory: impl FnOnce() -> Q,
    ) -> Self
    where
        Q: OperationQueue<QueuedOperation<L>> + 'static,
    {
        let pool = Self::new_with_queue_factory(factory, config, queue_factory);
        pool.begin();
        pool
    }
    pub fn new_with_queue_factory<Q>(
        factory: impl Fn() -> Supervisor<L> + Send + Sync + 'static,
        config: PoolConfig,
        queue_factory: impl FnOnce() -> Q,
    ) -> Self
    where
        Q: OperationQueue<QueuedOperation<L>> + 'static,
    {
        Self::with_queue(factory, config, queue_factory, false)
    }
    fn with_queue<Q>(
        factory: impl Fn() -> Supervisor<L> + Send + Sync + 'static,
        config: PoolConfig,
        queue_factory: impl FnOnce() -> Q,
        fast_admission: bool,
    ) -> Self
    where
        Q: OperationQueue<QueuedOperation<L>> + 'static,
    {
        assert!(
            config.max_size > 0 && config.min_size <= config.max_size,
            "invalid pool capacity"
        );
        assert!(
            !config.idle_timeout.is_zero(),
            "idle timeout must be positive"
        );
        let queue = queue_factory();
        assert!(queue.is_empty(), "queue factory must return an empty queue");
        let (changed, _) = watch::channel(0);
        let (stopping, _) = watch::channel(false);
        let (done, _) = watch::channel(false);
        let shared = Arc::new(Shared {
            config,
            factory: Box::new(factory),
            contents: Mutex::new(Contents {
                entries: Vec::new(),
                queue: Box::new(queue),
                sequence: 0,
                attributes: BTreeMap::new(),
                values: BTreeMap::new(),
                retiring: 0,
            }),
            operations: Mutex::new(JoinSet::new()),
            fast_admission,
            changed,
            dispatcher: Arc::new(Notify::new()),
            stopping,
            done,
            waiters: AtomicUsize::new(0),
            last_resource: Mutex::new(None),
            on_created: Mutex::new(None),
        });
        Self {
            owner: Arc::new(PoolOwner {
                stopping: shared.stopping.clone(),
                started: AtomicBool::new(false),
            }),
            shared,
        }
    }
    /// Start once and eagerly establish the configured minimum. Stopped pools stay stopped.
    pub fn begin(&self) {
        if self.owner.started.load(Ordering::Acquire) || *self.shared.stopping.borrow() {
            return;
        }
        {
            let mut contents = self.shared.contents.lock().unwrap();
            // Serialize initial startup with the lazy-stop completion check.
            // Stop must not finish before a racing begin registers its worker.
            if *self.shared.stopping.borrow() || self.owner.started.swap(true, Ordering::AcqRel) {
                return;
            }
            while contents.entries.len() < self.shared.config.min_size {
                self.shared.grow(&mut contents);
            }
        }
        tokio::spawn(run(self.shared.clone()));
    }
    pub fn size(&self) -> usize {
        self.shared.contents.lock().unwrap().entries.len()
    }
    pub fn supervisors(&self) -> Vec<Supervisor<L>> {
        self.shared
            .contents
            .lock()
            .unwrap()
            .entries
            .iter()
            .map(|entry| entry.supervisor.clone())
            .collect()
    }
    pub fn resources(&self) -> Vec<ResourceHandle<L::Resource>> {
        self.supervisors()
            .iter()
            .filter_map(Supervisor::current)
            .collect()
    }
    /// Read the latest resource during outages or idle retirement. Returns None
    /// after awaited stop; this read neither reserves capacity nor implies readiness.
    pub fn with_latest<T>(&self, read: impl FnOnce(&L::Resource) -> T) -> Option<T> {
        let resource = self
            .resources()
            .first()
            .map(|handle| handle.resource().clone())
            .or_else(|| self.shared.last_resource.lock().unwrap().clone());
        resource.as_deref().map(read)
    }
    /// Observe resource state and capacity changes. Queue insertion/cancellation
    /// wakes the dispatcher directly rather than broadcasting to observers.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.shared.changed.subscribe()
    }
    pub fn state(&self) -> ResourceState {
        if *self.shared.stopping.borrow() {
            return if *self.shared.done.borrow() {
                ResourceState::Stopped
            } else {
                ResourceState::Stopping
            };
        }
        let states: Vec<_> = self.supervisors().iter().map(Supervisor::state).collect();
        for candidate in [
            ResourceState::Connected,
            ResourceState::Connecting,
            ResourceState::Recovering,
            ResourceState::Failed,
        ] {
            if states.contains(&candidate) {
                return candidate;
            }
        }
        ResourceState::Disconnected
    }
    pub async fn connected(&self) -> Result<(), Error<L::Error>> {
        self.begin();
        let mut changed = self.subscribe();
        let mut stopping = self.shared.stopping.subscribe();
        loop {
            if *stopping.borrow() {
                return Err(Error::Stopped);
            }
            if let Some(cause) = self.failure() {
                return Err(Error::Lifecycle(cause));
            }
            if self.is_connected() {
                return Ok(());
            }
            let mut connections = JoinSet::new();
            for supervisor in self.supervisors() {
                if let Some(resource) = supervisor.current() {
                    let lifecycle = supervisor.lifecycle.clone();
                    connections.spawn(async move {
                        lifecycle.wait_connected(&resource.resource).await;
                    });
                }
            }
            if self.is_connected() {
                connections.abort_all();
                while connections.join_next().await.is_some() {}
                return Ok(());
            }
            tokio::select! { _ = crate::shutdown(&mut stopping) => {}, _ = changed.changed() => {}, _ = connections.join_next(), if !connections.is_empty() => {} }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        }
    }
    pub fn is_connected(&self) -> bool {
        self.supervisors().iter().any(Supervisor::is_connected)
    }
    pub fn stopped(&self) -> watch::Receiver<bool> {
        self.shared.stopping.subscribe()
    }
    /// Return the first slot's cause only when every slot is failed. Healthy,
    /// leased or recovering slots remain eligible and prevent aggregate failure.
    pub fn failure(&self) -> Option<Arc<L::Error>> {
        let contents = self.shared.contents.lock().unwrap();
        all_failed(&contents)
    }
    /// Reset suspended slots explicitly. Failed slots remain counted toward
    /// capacity and are never idle-retired or automatically replaced.
    pub fn reset_failed(&self) -> usize {
        if *self.shared.stopping.borrow() {
            return 0;
        }
        let reset = self
            .supervisors()
            .iter()
            .filter(|s| s.reset_failure())
            .count();
        if reset > 0 {
            self.shared.notify();
        }
        reset
    }
    pub async fn borrow(&self) -> Result<ResourceLease<L>, Error<L::Error>> {
        self.begin();
        let mut lease = borrow(self.shared.clone()).await?;
        lease.owner = Some(self.owner.clone());
        Ok(lease)
    }
    pub async fn recover(
        &self,
        expected: Option<&ResourceHandle<L::Resource>>,
        terminal: bool,
    ) -> Result<(), Error<L::Error>> {
        for supervisor in self.supervisors() {
            if let Some(handle) = expected {
                supervisor.recover(handle, terminal).await?;
            } else if let Some(handle) = supervisor.current() {
                supervisor.recover(&handle, terminal).await?;
            }
        }
        Ok(())
    }
    /// Setters apply immediately to existing resources and before setup on future resources.
    pub fn set_attribute(&self, name: impl Into<String>, setter: AttributeSetter<L::Resource>) {
        let name = name.into();
        let mut contents = self.shared.contents.lock().unwrap();
        self.shared.apply_attribute(&contents, &name, &setter);
        contents.attributes.insert(name, setter);
    }
    pub fn set_value<T: Clone + Send + Sync + 'static>(
        &self,
        name: impl Into<String>,
        value: T,
        apply: impl Fn(&L::Resource, &T) + Send + Sync + 'static,
    ) {
        let name = name.into();
        let value = Arc::new(value);
        let stored = value.clone();
        let setter: AttributeSetter<L::Resource> =
            Arc::new(move |resource| apply(resource, &value));
        let mut contents = self.shared.contents.lock().unwrap();
        self.shared.apply_attribute(&contents, &name, &setter);
        contents.values.insert(name.clone(), stored);
        contents.attributes.insert(name, setter);
    }
    pub fn value<T: Clone + Send + Sync + 'static>(&self, name: &str) -> Option<T> {
        self.shared
            .contents
            .lock()
            .unwrap()
            .values
            .get(name)?
            .downcast_ref::<T>()
            .cloned()
    }
    pub fn set_on_resource_created(&self, hook: CreatedHook<L::Resource, L::Error>) {
        *self.shared.on_created.lock().unwrap() = Some(hook);
    }
    pub async fn execute<F, Fut, T>(&self, operation: F) -> Result<T, Error<L::Error>>
    where
        F: FnOnce(Arc<L::Resource>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, L::Error>> + Send + 'static,
        T: Send + 'static,
    {
        self.execute_with_priority(0, operation).await
    }
    pub async fn execute_with_priority<F, Fut, T>(
        &self,
        priority: i32,
        operation: F,
    ) -> Result<T, Error<L::Error>>
    where
        F: FnOnce(Arc<L::Resource>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, L::Error>> + Send + 'static,
        T: Send + 'static,
    {
        self.enqueue(
            priority,
            |lease| async move { lease.execute(operation).await },
        )
        .await
    }
    /// Explicitly retry a replay-safe operation while retaining its pool lease.
    /// The deadline includes queueing. Each attempt waits for the leased supervisor
    /// to become ready; queue errors, shutdown, and deadlines are never retried.
    /// See [`Supervisor::execute_with_retry`] for replay and cancellation semantics.
    pub async fn execute_with_retry<F, Fut, T, P>(
        &self,
        policy: RetryPolicy,
        operation: F,
        retry_if: P,
    ) -> Result<T, Error<L::Error>>
    where
        F: FnMut(Arc<L::Resource>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, L::Error>> + Send + 'static,
        T: Send + 'static,
        P: FnMut(&L::Error) -> bool + Send + 'static,
    {
        self.execute_with_priority_and_retry(0, policy, operation, retry_if)
            .await
    }
    /// Queue a retryable operation once at `priority`, retaining capacity throughout
    /// recovery. The overall deadline includes its initial wait in the queue.
    pub async fn execute_with_priority_and_retry<F, Fut, T, P>(
        &self,
        priority: i32,
        policy: RetryPolicy,
        operation: F,
        retry_if: P,
    ) -> Result<T, Error<L::Error>>
    where
        F: FnMut(Arc<L::Resource>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, L::Error>> + Send + 'static,
        T: Send + 'static,
        P: FnMut(&L::Error) -> bool + Send + 'static,
    {
        if policy.timeout.is_zero() {
            return Err(Error::Timeout);
        }
        let queued = self.enqueue(priority, move |lease| async move {
            lease.execute_with_retry(policy, operation, retry_if).await
        });
        tokio::time::timeout(policy.timeout, queued)
            .await
            .unwrap_or(Err(Error::Timeout))
    }
    async fn enqueue<F, Fut, T>(&self, priority: i32, operation: F) -> Result<T, Error<L::Error>>
    where
        F: FnOnce(ResourceLease<L>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, Error<L::Error>>> + Send + 'static,
        T: Send + 'static,
    {
        self.begin();
        let (sender, receiver) = oneshot::channel();
        let mut cancellation = {
            let mut contents = self.shared.contents.lock().unwrap();
            if *self.shared.stopping.borrow() {
                return Err(Error::Stopped);
            }
            if let Some(cause) = all_failed(&contents) {
                return Err(Error::Lifecycle(cause));
            }
            let sequence = contents.sequence;
            contents.sequence += 1;
            // Only built-in unbounded queues can skip admission. Custom queues
            // must always get their push/rejection/ordering opportunity.
            let lease = if self.shared.fast_admission
                && contents.queue.is_empty()
                && self.shared.waiters.load(Ordering::Acquire) == 0
            {
                reserve_ready(&self.shared, &contents)
            } else {
                None
            };
            if let Some(lease) = lease {
                // Register under the contents lock so shutdown cannot pass its
                // admission barrier before this task has been tracked.
                self.shared
                    .operations
                    .lock()
                    .unwrap()
                    .spawn(run_job(sender, Ok(lease), operation));
                self.shared.dispatcher.notify_one();
                None
            } else {
                let cancelled = Arc::new(AtomicBool::new(false));
                let cancellation = CancelOnDrop {
                    cancelled: cancelled.clone(),
                    dispatcher: self.shared.dispatcher.clone(),
                    armed: true,
                };
                contents
                    .queue
                    .push(Operation {
                        priority,
                        sequence,
                        cancelled,
                        job: Box::new(move |lease| Box::pin(run_job(sender, lease, operation))),
                    })
                    .map_err(Error::Queue)?;
                // Queue changes concern the dispatcher, not readiness/capacity waiters.
                self.shared.dispatcher.notify_one();
                Some(cancellation)
            }
        };
        let result = receiver.await.unwrap_or(Err(Error::Stopped));
        // The dispatcher has already removed completed work from the queue.
        // Lease release and task completion provide the necessary wakeups;
        // only an abandoned call needs a cancellation notification.
        if let Some(cancellation) = &mut cancellation {
            cancellation.armed = false;
        }
        drop(cancellation);
        if matches!(result, Err(Error::Lifecycle(_))) && *self.shared.stopping.borrow() {
            return Err(Error::Stopped);
        }
        result
    }
    /// Cancellation-safe shutdown closes leased resources and cancels queued/running work.
    pub async fn stop(&self) {
        self.shared.stopping.send_replace(true);
        self.shared.notify();
        {
            let _contents = self.shared.contents.lock().unwrap();
            if !self.owner.started.load(Ordering::Acquire) {
                self.shared.done.send_replace(true);
            }
        }
        let mut done = self.shared.done.subscribe();
        while !*done.borrow_and_update() {
            if done.changed().await.is_err() {
                return;
            }
        }
    }
}
async fn run_job<L, F, Fut, T>(
    mut sender: oneshot::Sender<Result<T, Error<L::Error>>>,
    admission: Admission<L>,
    operation: F,
) where
    L: Lifecycle,
    F: FnOnce(ResourceLease<L>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T, Error<L::Error>>> + Send + 'static,
    T: Send + 'static,
{
    if sender.is_closed() {
        return;
    }
    let lease = match admission {
        Ok(lease) => lease,
        Err(error) => {
            let _ = sender.send(Err(error));
            return;
        }
    };
    tokio::select! {
        biased;
        _ = sender.closed() => {},
        result = operation(lease) => { let _ = sender.send(result); }
    }
}
struct CancelOnDrop {
    cancelled: Arc<AtomicBool>,
    dispatcher: Arc<Notify>,
    armed: bool,
}
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.cancelled.store(true, Ordering::Release);
        self.dispatcher.notify_one();
    }
}

/// Exclusive reservation of a supervisor; reconnection does not end the lease.
pub struct ResourceLease<L: Lifecycle> {
    entry: Arc<Entry<L>>,
    shared: Arc<Shared<L>>,
    // Public leases keep the pool alive. Dispatcher leases must not own the pool,
    // otherwise dropping all public handles could never cancel an active job.
    owner: Option<Arc<PoolOwner>>,
}
impl<L: Lifecycle> ResourceLease<L> {
    /// Return capacity immediately. Ownership prevents use after release.
    pub fn release(self) {}
    pub fn supervisor(&self) -> &Supervisor<L> {
        &self.entry.supervisor
    }
    pub async fn acquire(&self) -> Result<ResourceHandle<L::Resource>, Error<L::Error>> {
        if *self.shared.stopping.borrow() {
            return Err(Error::Stopped);
        }
        self.entry.supervisor.acquire().await
    }
    pub async fn execute<F, Fut, T>(&self, operation: F) -> Result<T, Error<L::Error>>
    where
        F: FnOnce(Arc<L::Resource>) -> Fut,
        Fut: Future<Output = Result<T, L::Error>>,
    {
        let mut stopping = self.shared.stopping.subscribe();
        tokio::select! { biased; _ = crate::shutdown(&mut stopping) => Err(Error::Stopped), result = self.entry.supervisor.execute(operation) => result }
    }
    /// Retry a replay-safe operation on this reservation, following replacement
    /// generations. The deadline starts here because the lease is already held.
    pub async fn execute_with_retry<F, Fut, T, P>(
        &self,
        policy: RetryPolicy,
        operation: F,
        retry_if: P,
    ) -> Result<T, Error<L::Error>>
    where
        F: FnMut(Arc<L::Resource>) -> Fut,
        Fut: Future<Output = Result<T, L::Error>>,
        P: FnMut(&L::Error) -> bool,
    {
        let mut stopping = self.shared.stopping.subscribe();
        tokio::select! {
            biased;
            _ = crate::shutdown(&mut stopping) => Err(Error::Stopped),
            result = self.entry.supervisor.execute_with_retry(policy, operation, retry_if) => result,
        }
    }
}
impl<L: Lifecycle> Drop for ResourceLease<L> {
    fn drop(&mut self) {
        *self.entry.idle_since.lock().unwrap() = Instant::now();
        self.entry.leased.store(false, Ordering::Release);
        self.shared.notify();
    }
}
async fn borrow<L: Lifecycle>(shared: Arc<Shared<L>>) -> Result<ResourceLease<L>, Error<L::Error>> {
    shared.waiters.fetch_add(1, Ordering::AcqRel);
    let _waiter = Waiter(shared.clone());
    let mut changed = shared.changed.subscribe();
    let mut stopping = shared.stopping.subscribe();
    loop {
        if let Some(lease) = try_borrow(&shared)? {
            return Ok(lease);
        }
        tokio::select! { biased; _ = crate::shutdown(&mut stopping) => return Err(Error::Stopped), _ = changed.changed() => {} }
    }
}
// Queued acquisition is checked directly by the dispatcher. Only public
// borrowers register an independent capacity/shutdown wait.
fn try_borrow<L: Lifecycle>(
    shared: &Arc<Shared<L>>,
) -> Result<Option<ResourceLease<L>>, Error<L::Error>> {
    let mut contents = shared.contents.lock().unwrap();
    if *shared.stopping.borrow() {
        return Err(Error::Stopped);
    }
    if let Some(lease) = reserve_ready(shared, &contents) {
        return Ok(Some(lease));
    }
    if let Some(cause) = all_failed(&contents) {
        return Err(Error::Lifecycle(cause));
    }
    let demand = shared.waiters.load(Ordering::Acquire)
        + contents.queue.len()
        + contents
            .entries
            .iter()
            .filter(|entry| entry.leased.load(Ordering::Acquire))
            .count();
    if contents.entries.len() + contents.retiring < shared.config.max_size.min(demand) {
        shared.grow(&mut contents);
    }
    Ok(None)
}
fn all_failed<L: Lifecycle>(contents: &Contents<L>) -> Option<Arc<L::Error>> {
    let mut first = None;
    if contents.entries.is_empty() {
        return None;
    }
    for entry in &contents.entries {
        let cause = entry.supervisor.failure()?;
        if first.is_none() {
            first = Some(cause);
        }
    }
    first
}
fn reserve_ready<L: Lifecycle>(
    shared: &Arc<Shared<L>>,
    contents: &Contents<L>,
) -> Option<ResourceLease<L>> {
    for entry in &contents.entries {
        if entry.supervisor.state() == ResourceState::Connected
            && entry
                .leased
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            return Some(ResourceLease {
                entry: entry.clone(),
                shared: shared.clone(),
                owner: None,
            });
        }
    }
    None
}
struct Waiter<L: Lifecycle>(Arc<Shared<L>>);
impl<L: Lifecycle> Drop for Waiter<L> {
    fn drop(&mut self) {
        self.0.waiters.fetch_sub(1, Ordering::AcqRel);
        self.0.dispatcher.notify_one();
    }
}
async fn run<L: Lifecycle>(shared: Arc<Shared<L>>) {
    let mut stopping = shared.stopping.subscribe();
    let mut retiring = JoinSet::new();
    let mut reaper = Box::pin(sleep(
        shared.config.idle_timeout.min(Duration::from_secs(1)),
    ));
    loop {
        let queued = {
            let mut contents = shared.contents.lock().unwrap();
            contents.queue.retain(&mut |item| !item.is_cancelled());
            !contents.queue.is_empty()
        };
        let admission = if queued {
            match try_borrow(&shared) {
                Ok(lease) => lease.map(Ok),
                Err(error) => Some(Err(error)),
            }
        } else {
            None
        };
        tokio::select! {
            biased;
            _ = crate::shutdown(&mut stopping) => break,
            admission = async { match admission { Some(admission) => admission, None => std::future::pending().await } } => {
                // Reconsider priority after capacity becomes available.
                let item = shared.contents.lock().unwrap().queue.pop();
                if let Some(item) = item && !item.cancelled.load(Ordering::Acquire) { shared.operations.lock().unwrap().spawn((item.job)(admission)); }
            }
            _ = shared.dispatcher.notified() => {},
            _ = std::future::poll_fn(|cx| shared.operations.lock().unwrap().poll_join_next(cx)),
                if !shared.operations.lock().unwrap().is_empty() => {},
            _ = retiring.join_next(), if !retiring.is_empty() => {
                shared.contents.lock().unwrap().retiring -= 1; shared.notify();
            }
            _ = &mut reaper => {
                let mut contents = shared.contents.lock().unwrap();
                if contents.queue.is_empty() && shared.waiters.load(Ordering::Acquire) == 0 {
                    let mut index = 0;
                    while index < contents.entries.len() && contents.entries.len() > shared.config.min_size {
                        let entry = &contents.entries[index];
                        if entry.supervisor.failure().is_none() && !entry.leased.load(Ordering::Acquire) && entry.idle_since.lock().unwrap().elapsed() >= shared.config.idle_timeout {
                            let entry = contents.entries.remove(index); contents.retiring += 1;
                            retiring.spawn(stop_entry(entry)); shared.notify();
                        } else { index += 1; }
                    }
                }
                reaper.as_mut().reset(Instant::now() + shared.config.idle_timeout.min(Duration::from_secs(1)));
            }
        }
    }
    // Fast admission holds contents while registering its task. Crossing this
    // barrier after stopping is signalled ensures no task can arrive after drain.
    let entries = {
        let mut contents = shared.contents.lock().unwrap();
        contents.queue.clear();
        std::mem::take(&mut contents.entries)
    };
    shared.operations.lock().unwrap().abort_all();
    while std::future::poll_fn(|cx| shared.operations.lock().unwrap().poll_join_next(cx))
        .await
        .is_some()
    {}
    for entry in entries {
        retiring.spawn(stop_entry(entry));
    }
    while retiring.join_next().await.is_some() {}
    *shared.last_resource.lock().unwrap() = None;
    shared.contents.lock().unwrap().retiring = 0;
    shared.done.send_replace(true);
    shared.notify();
}
async fn stop_entry<L: Lifecycle>(entry: Arc<Entry<L>>) {
    entry.supervisor.stop().await;
    let relay = entry.relay.lock().unwrap().take();
    if let Some(relay) = relay {
        let _ = relay.await;
    }
}

/// Stable proxy backing typed client wrappers. Attributes are replayed through named setters.
pub struct ManagedResourceProxy<L: Lifecycle> {
    pub pool: Pool<L>,
}
impl<L: Lifecycle> Clone for ManagedResourceProxy<L> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
        }
    }
}
impl<L: Lifecycle> ManagedResourceProxy<L> {
    pub fn failure(&self) -> Option<Arc<L::Error>> {
        self.pool.failure()
    }
    pub fn reset_failed(&self) -> usize {
        self.pool.reset_failed()
    }
    pub fn new(pool: Pool<L>) -> Self {
        Self { pool }
    }
    /// Run once on an exclusive pooled resource.
    pub async fn execute<F, Fut, T>(&self, operation: F) -> Result<T, Error<L::Error>>
    where
        F: FnOnce(Arc<L::Resource>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, L::Error>> + Send + 'static,
        T: Send + 'static,
    {
        self.pool.execute(operation).await
    }
    /// Explicitly retry a replay-safe operation while retaining an exclusive lease.
    pub async fn execute_with_retry<F, Fut, T, P>(
        &self,
        policy: RetryPolicy,
        operation: F,
        retry_if: P,
    ) -> Result<T, Error<L::Error>>
    where
        F: FnMut(Arc<L::Resource>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, L::Error>> + Send + 'static,
        T: Send + 'static,
        P: FnMut(&L::Error) -> bool + Send + 'static,
    {
        self.pool
            .execute_with_retry(policy, operation, retry_if)
            .await
    }
    pub async fn connected(&self) -> Result<(), Error<L::Error>> {
        self.pool.connected().await
    }
    pub fn is_connected(&self) -> bool {
        self.pool.is_connected()
    }
    pub fn set_attribute(&self, name: impl Into<String>, setter: AttributeSetter<L::Resource>) {
        self.pool.set_attribute(name, setter);
    }
    pub fn attribute<T>(&self, read: impl FnOnce(&L::Resource) -> T) -> Option<T> {
        self.pool.with_latest(read)
    }
    pub fn set_value<T: Clone + Send + Sync + 'static>(
        &self,
        name: impl Into<String>,
        value: T,
        apply: impl Fn(&L::Resource, &T) + Send + Sync + 'static,
    ) {
        self.pool.set_value(name, value, apply);
    }
    pub fn value<T: Clone + Send + Sync + 'static>(&self, name: &str) -> Option<T> {
        self.pool.value(name)
    }
    pub async fn stop(&self) {
        self.pool.stop().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn relay_forwards_readiness_published_before_its_first_poll() {
        let (state, receiver) = watch::channel(crate::ResourceStatus::<()> {
            state: ResourceState::Connecting,
            handle: None,
        });
        let (changed, mut observer) = watch::channel(0_u64);
        let dispatcher = Arc::new(Notify::new());
        state.send_replace(crate::ResourceStatus {
            state: ResourceState::Connected,
            handle: None,
        });
        let relay = tokio::spawn(relay_state(receiver, changed, dispatcher.clone()));
        tokio::time::timeout(Duration::from_secs(1), observer.changed())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), dispatcher.notified())
            .await
            .unwrap();
        state.send_replace(crate::ResourceStatus {
            state: ResourceState::Stopped,
            handle: None,
        });
        tokio::time::timeout(Duration::from_secs(1), relay)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*observer.borrow(), 2);
    }
    struct Hooks;
    #[crate::async_trait]
    impl Lifecycle for Hooks {
        type Resource = ();
        type Error = std::convert::Infallible;
        async fn create(&self) -> Result<(), Self::Error> {
            Ok(())
        }
        async fn connect(&self, _: &()) -> Result<(), Self::Error> {
            Ok(())
        }
        async fn disconnect(&self, _: &()) -> Result<(), Self::Error> {
            Ok(())
        }
    }
    #[tokio::test]
    async fn shutdown_joins_notification_relays_even_when_supervisors_are_retained() {
        let pool = Pool::start(
            || Supervisor::new(Hooks, crate::Config::default()),
            PoolConfig::default(),
        );
        pool.connected().await.unwrap();
        let retained = pool.supervisors();
        pool.stop().await;
        assert!(
            pool.shared
                .contents
                .lock()
                .unwrap()
                .entries
                .iter()
                .all(|entry| entry.relay.lock().unwrap().is_none())
        );
        assert!(
            retained
                .iter()
                .all(|supervisor| supervisor.state() == ResourceState::Stopped)
        );
    }
    #[tokio::test]
    async fn notification_relays_are_joined_during_idle_retirement() {
        let pool = Pool::start(
            || Supervisor::new(Hooks, crate::Config::default()),
            PoolConfig {
                min_size: 0,
                idle_timeout: Duration::from_millis(5),
                ..PoolConfig::default()
            },
        );
        let lease = pool.borrow().await.unwrap();
        let entry = lease.entry.clone();
        drop(lease);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if entry.relay.lock().unwrap().is_none() {
                    break;
                }
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        pool.stop().await;
    }

    struct PendingHooks;
    #[crate::async_trait]
    impl Lifecycle for PendingHooks {
        type Resource = ();
        type Error = std::convert::Infallible;
        async fn create(&self) -> Result<(), Self::Error> {
            Ok(())
        }
        async fn connect(&self, _: &()) -> Result<(), Self::Error> {
            std::future::pending().await
        }
        async fn disconnect(&self, _: &()) -> Result<(), Self::Error> {
            Ok(())
        }
        fn watch_disconnect(&self, _: Arc<()>) -> Option<crate::LifecycleFuture<Self::Error>> {
            Some(Box::pin(std::future::pending()))
        }
    }
    #[tokio::test]
    async fn shutdown_joins_relays_while_connecting_with_watchdog_configured() {
        let pool = Pool::start(
            || Supervisor::new(PendingHooks, crate::Config::default()),
            PoolConfig::default(),
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while pool.state() != ResourceState::Connecting {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let entries = pool.shared.contents.lock().unwrap().entries.clone();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].relay.lock().unwrap().is_some());
        tokio::time::timeout(Duration::from_secs(1), pool.stop())
            .await
            .unwrap();
        assert!(entries[0].relay.lock().unwrap().is_none());
        assert_eq!(entries[0].supervisor.state(), ResourceState::Stopped);
        assert!(*pool.shared.done.borrow());
    }
}
