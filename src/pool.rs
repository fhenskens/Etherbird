use crate::{
    AttributeSetter, CreatedHook, Error, FifoQueue, Lifecycle, OperationQueue, PriorityQueue,
    ResourceHandle, ResourceState, RetryPolicy, Supervisor,
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
struct Entry<L: Lifecycle> {
    supervisor: Supervisor<L>,
    leased: AtomicBool,
    idle_since: Mutex<Instant>,
    relay: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
type Job<L> = Box<dyn FnOnce(ResourceLease<L>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;
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
                    *shared.last_resource.lock().unwrap() = Some(resource.clone());
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
        let mut state = entry.supervisor.subscribe();
        let changed = self.changed.clone();
        let dispatcher = self.dispatcher.clone();
        let relay = tokio::spawn(async move {
            loop {
                if state.borrow_and_update().state == ResourceState::Stopped {
                    break;
                }
                if state.changed().await.is_err() {
                    break;
                }
                changed.send_modify(|revision| *revision = revision.wrapping_add(1));
                dispatcher.notify_one();
            }
        });
        *entry.relay.lock().unwrap() = Some(relay);
        contents.entries.push(entry);
        self.notify();
    }
}
struct PoolOwner {
    stopping: watch::Sender<bool>,
    started: AtomicBool,
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
            Self::new_with_queue_factory(factory, config, PriorityQueue::default)
        } else {
            Self::new_with_queue_factory(factory, config, FifoQueue::default)
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
        if *self.shared.stopping.borrow() || self.owner.started.swap(true, Ordering::AcqRel) {
            return;
        }
        {
            let mut contents = self.shared.contents.lock().unwrap();
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
        ] {
            if states.contains(&candidate) {
                return candidate;
            }
        }
        ResourceState::Disconnected
    }
    pub async fn connected(&self) -> Result<(), Error<L::Error>> {
        let mut changed = self.subscribe();
        let mut stopping = self.shared.stopping.subscribe();
        loop {
            if *stopping.borrow() {
                return Err(Error::Stopped);
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
        let (mut sender, receiver) = oneshot::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut cancellation = CancelOnDrop {
            cancelled: cancelled.clone(),
            dispatcher: self.shared.dispatcher.clone(),
            armed: true,
        };
        {
            let mut contents = self.shared.contents.lock().unwrap();
            if *self.shared.stopping.borrow() {
                return Err(Error::Stopped);
            }
            let sequence = contents.sequence;
            contents.sequence += 1;
            contents
                .queue
                .push(Operation {
                    priority,
                    sequence,
                    cancelled,
                    job: Box::new(move |lease| {
                        Box::pin(async move {
                            if sender.is_closed() {
                                return;
                            }
                            tokio::select! {
                                biased;
                                _ = sender.closed() => {},
                                result = operation(lease) => { let _ = sender.send(result); }
                            }
                        })
                    }),
                })
                .map_err(Error::Queue)?;
            // Queue changes concern the dispatcher, not readiness/capacity waiters.
            self.shared.dispatcher.notify_one();
        }
        let result = receiver.await.unwrap_or(Err(Error::Stopped));
        // The dispatcher has already removed completed work from the queue.
        // Lease release and task completion provide the necessary wakeups;
        // only an abandoned call needs a cancellation notification.
        cancellation.armed = false;
        drop(cancellation);
        result
    }
    /// Cancellation-safe shutdown closes leased resources and cancels queued/running work.
    pub async fn stop(&self) {
        self.shared.stopping.send_replace(true);
        self.shared.notify();
        if !self.owner.started.load(Ordering::Acquire) {
            self.shared.done.send_replace(true);
        }
        let mut done = self.shared.done.subscribe();
        while !*done.borrow_and_update() {
            if done.changed().await.is_err() {
                return;
            }
        }
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
    for entry in &contents.entries {
        if entry.supervisor.state() == ResourceState::Connected
            && entry
                .leased
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            return Ok(Some(ResourceLease {
                entry: entry.clone(),
                shared: shared.clone(),
                owner: None,
            }));
        }
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
struct Waiter<L: Lifecycle>(Arc<Shared<L>>);
impl<L: Lifecycle> Drop for Waiter<L> {
    fn drop(&mut self) {
        self.0.waiters.fetch_sub(1, Ordering::AcqRel);
        self.0.dispatcher.notify_one();
    }
}
async fn run<L: Lifecycle>(shared: Arc<Shared<L>>) {
    let mut stopping = shared.stopping.subscribe();
    let mut operations = JoinSet::new();
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
        let lease = if queued {
            try_borrow(&shared).unwrap_or(None)
        } else {
            None
        };
        tokio::select! {
            biased;
            _ = crate::shutdown(&mut stopping) => break,
            lease = async { match lease { Some(lease) => lease, None => std::future::pending().await } } => {
                // Reconsider priority after capacity becomes available.
                let item = shared.contents.lock().unwrap().queue.pop();
                if let Some(item) = item && !item.cancelled.load(Ordering::Acquire) { operations.spawn((item.job)(lease)); }
            }
            _ = shared.dispatcher.notified() => {},
            _ = operations.join_next(), if !operations.is_empty() => {},
            _ = retiring.join_next(), if !retiring.is_empty() => {
                shared.contents.lock().unwrap().retiring -= 1; shared.notify();
            }
            _ = &mut reaper => {
                let mut contents = shared.contents.lock().unwrap();
                if contents.queue.is_empty() && shared.waiters.load(Ordering::Acquire) == 0 {
                    let mut index = 0;
                    while index < contents.entries.len() && contents.entries.len() > shared.config.min_size {
                        let entry = &contents.entries[index];
                        if !entry.leased.load(Ordering::Acquire) && entry.idle_since.lock().unwrap().elapsed() >= shared.config.idle_timeout {
                            let entry = contents.entries.remove(index); contents.retiring += 1;
                            retiring.spawn(stop_entry(entry)); shared.notify();
                        } else { index += 1; }
                    }
                }
                reaper.as_mut().reset(Instant::now() + shared.config.idle_timeout.min(Duration::from_secs(1)));
            }
        }
    }
    operations.abort_all();
    while operations.join_next().await.is_some() {}
    let entries = {
        let mut contents = shared.contents.lock().unwrap();
        contents.queue.clear();
        contents.entries.clone()
    };
    for entry in entries {
        retiring.spawn(stop_entry(entry));
    }
    while retiring.join_next().await.is_some() {}
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
    pub fn new(pool: Pool<L>) -> Self {
        Self { pool }
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

/// Generate a typed proxy with ordinary async client methods returning owned results.
/// Methods must return the lifecycle error type. Borrowed/streaming results need a manual wrapper.
#[macro_export]
macro_rules! managed_client {
    ($visibility:vis struct $name:ident for $lifecycle:ty { $($methods:tt)* }) => {
        #[derive(Clone)]
        $visibility struct $name { pub managed: $crate::ManagedResourceProxy<$lifecycle> }
        impl $name {
            pub fn new(pool: $crate::Pool<$lifecycle>) -> Self { Self { managed: $crate::ManagedResourceProxy::new(pool) } }
            $crate::managed_client_methods! { $lifecycle; $($methods)* }
        }
    };
}
#[doc(hidden)]
#[macro_export]
macro_rules! managed_client_methods {
    ($lifecycle:ty;) => {};
    ($lifecycle:ty; async fn $method:ident($($argument:ident: $ty:ty),* $(,)?) -> $output:ty; $($rest:tt)*) => {
        pub async fn $method(&self, $($argument: $ty),*) -> Result<$output, $crate::Error<<$lifecycle as $crate::Lifecycle>::Error>> {
            self.managed.pool.execute(move |resource| async move { resource.$method($($argument),*).await }).await
        }
        $crate::managed_client_methods! { $lifecycle; $($rest)* }
    };
    ($lifecycle:ty; fn $method:ident($($argument:ident: $ty:ty),* $(,)?) -> $output:ty; $($rest:tt)*) => {
        pub async fn $method(&self, $($argument: $ty),*) -> Result<$output, $crate::Error<<$lifecycle as $crate::Lifecycle>::Error>> {
            self.managed.pool.execute(move |resource| async move { resource.$method($($argument),*) }).await
        }
        $crate::managed_client_methods! { $lifecycle; $($rest)* }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
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
