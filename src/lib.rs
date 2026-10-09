//! Supervised asynchronous resources with automatic recovery.
//!
//! Implement [`Lifecycle`] and expose a typed client wrapper around [`Supervisor`].
//! Operations run once by default; retries require explicit per-operation opt-in.
//! Lifecycle futures must be cancellation-safe.

pub use async_trait::async_trait;
use std::{any::Any, fmt, future::Future, pin::Pin, sync::Arc, time::Duration};
use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{sleep, timeout};
#[cfg(feature = "pool")]
mod pool;
#[cfg(feature = "pool")]
mod queue;
#[cfg(feature = "pool")]
pub use pool::{ManagedResourceProxy, Pool, PoolConfig, QueuedOperation, ResourceLease};
mod proxy;
mod validation;
pub use proxy::{ManagedClientBackend, SupervisedResourceProxy};
#[cfg(feature = "pool")]
pub use queue::{BoundedFifoQueue, FifoQueue, OperationQueue, PriorityQueue, QueueError};
pub use validation::ConfigError;

/// Protocol-specific lifecycle hooks. Resources usually contain their own interior mutability.
///
/// Hooks must yield to the executor and tolerate cancellation by future drop. Creation must
/// release partially allocated resources on cancellation. Connect/setup failures run teardown.
/// Cleanup, disconnect and destroy each get a separate timeout; failure never skips later hooks.
#[async_trait]
pub trait Lifecycle: Send + Sync + 'static {
    type Resource: Send + Sync + 'static;
    type Error: std::error::Error + Send + Sync + 'static;
    async fn create(&self) -> Result<Self::Resource, Self::Error>;
    async fn connect(&self, resource: &Self::Resource) -> Result<(), Self::Error>;
    async fn on_resource_created(&self, _resource: &Self::Resource) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn setup(&self, _resource: &Self::Resource) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn cleanup(&self, _resource: &Self::Resource) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn disconnect(&self, resource: &Self::Resource) -> Result<(), Self::Error>;
    async fn destroy(&self, _resource: &Self::Resource) -> Result<(), Self::Error> {
        Ok(())
    }
    /// Optional connection-loss watch. No watchdog task is spawned when absent.
    fn watch_disconnect(
        &self,
        _resource: Arc<Self::Resource>,
    ) -> Option<LifecycleFuture<Self::Error>> {
        None
    }
    /// Read the transport's live connection indicator. Override alongside `wait_connected`.
    fn is_connected(&self, _resource: &Self::Resource) -> bool {
        true
    }
    /// Wait until the transport becomes connected, checking its indicator before waiting.
    async fn wait_connected(&self, _resource: &Self::Resource) {
        std::future::pending::<()>().await
    }
    /// Terminal errors skip disconnect, but still run cleanup and destroy.
    fn is_terminal(&self, _error: &Self::Error) -> bool {
        false
    }
    fn is_expected(&self, _error: &Self::Error) -> bool {
        false
    }
    /// Decide whether an operation error retires its resource. Independent of
    /// logging and replay: retaining a resource never authorizes another attempt.
    fn operation_failure(&self, _error: &Self::Error) -> OperationFailurePolicy {
        OperationFailurePolicy::Recover
    }
    /// Classify create/created/connect/setup and disconnect-watch errors.
    /// Fail latches a typed cause until explicit reset or stop/restart. It does
    /// not change cleanup/disconnect/destroy error handling or `is_terminal`.
    fn lifecycle_failure(&self, _error: &Self::Error) -> LifecycleFailurePolicy {
        LifecycleFailurePolicy::Retry
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Recovery decision for an operation error; independent of replay permission.
pub enum OperationFailurePolicy {
    /// Retire the failed generation using the existing teardown policy.
    Recover,
    /// Return the operation error while retaining the healthy generation.
    Retain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Scheduling decision for an initialization or disconnect-watch error.
pub enum LifecycleFailurePolicy {
    /// Continue automatic lifecycle recovery with configured backoff.
    Retry,
    /// Suspend attempts and fail readiness/admission until explicit reset.
    Fail,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceState {
    Connecting,
    Connected,
    Recovering,
    Disconnected,
    /// Automatic lifecycle attempts are suspended until explicit reset.
    Failed,
    Stopping,
    Stopped,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub resource_name: String,
    pub create_timeout: Option<Duration>,
    pub created_timeout: Option<Duration>,
    pub connect_timeout: Duration,
    pub setup_timeout: Duration,
    pub cleanup_timeout: Duration,
    pub disconnect_timeout: Duration,
    pub destroy_timeout: Option<Duration>,
    pub retry_delay: Duration,
    pub max_retry_delay: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            resource_name: "Resource".into(),
            create_timeout: None,
            created_timeout: None,
            connect_timeout: Duration::from_secs(20),
            setup_timeout: Duration::from_secs(10),
            cleanup_timeout: Duration::from_secs(10),
            disconnect_timeout: Duration::from_secs(10),
            destroy_timeout: None,
            retry_delay: Duration::from_secs(5),
            max_retry_delay: Duration::from_secs(300),
        }
    }
}

pub enum Error<E> {
    Stopped,
    /// The overall deadline for an operation or readiness wait elapsed.
    Timeout,
    Operation(E),
    /// A non-retryable lifecycle cause shared by waiting callers. Display and
    /// Debug redact it; inspect the value/source explicitly with appropriate care.
    Lifecycle(Arc<E>),
    #[cfg(feature = "pool")]
    Queue(QueueError),
}
impl<E: fmt::Debug> fmt::Debug for Error<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopped => f.write_str("Stopped"),
            Self::Timeout => f.write_str("Timeout"),
            Self::Operation(e) => f.debug_tuple("Operation").field(e).finish(),
            Self::Lifecycle(_) => f.write_str("Lifecycle(<redacted>)"),
            #[cfg(feature = "pool")]
            Self::Queue(e) => f.debug_tuple("Queue").field(e).finish(),
        }
    }
}
impl<E: fmt::Display> fmt::Display for Error<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopped => f.write_str("resource supervisor stopped"),
            Self::Timeout => f.write_str("operation retry deadline elapsed"),
            Self::Operation(e) => e.fmt(f),
            Self::Lifecycle(_) => f.write_str("non-retryable resource lifecycle failure"),
            #[cfg(feature = "pool")]
            Self::Queue(e) => e.fmt(f),
        }
    }
}
impl<E: std::error::Error + 'static> std::error::Error for Error<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Operation(e) => Some(e),
            Self::Lifecycle(e) => Some(e.as_ref()),
            Self::Stopped | Self::Timeout => None,
            #[cfg(feature = "pool")]
            Self::Queue(e) => Some(e),
        }
    }
}

/// Bounds for an explicitly retryable operation. The caller must ensure replay is safe.
///
/// The attempt count includes the initial call. The deadline covers acquisition,
/// every operation attempt, and recovery/teardown between attempts. Connection
/// failures while acquiring a ready resource do not consume operation attempts.
#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    pub max_attempts: std::num::NonZeroUsize,
    pub timeout: Duration,
}

/// A specific resource generation. Old handles do not silently become new resources.
pub struct ResourceHandle<R> {
    resource: Arc<R>,
    generation: u64,
}
impl<R> Clone for ResourceHandle<R> {
    fn clone(&self) -> Self {
        Self {
            resource: self.resource.clone(),
            generation: self.generation,
        }
    }
}
impl<R> ResourceHandle<R> {
    pub fn resource(&self) -> &Arc<R> {
        &self.resource
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
}
pub struct ResourceStatus<R> {
    pub state: ResourceState,
    pub handle: Option<ResourceHandle<R>>,
}
type Snapshot<R> = ResourceStatus<R>;
impl<R> Clone for Snapshot<R> {
    fn clone(&self) -> Self {
        Self {
            state: self.state,
            handle: self.handle.clone(),
        }
    }
}
struct Owner {
    stop: watch::Sender<bool>,
    started: AtomicBool,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
struct Recovery {
    generation: u64,
    terminal: bool,
    completed: Option<oneshot::Sender<()>>,
}
pub type LifecycleFuture<E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send>>;
pub type CreatedHook<R, E> = Arc<dyn Fn(Arc<R>) -> LifecycleFuture<E> + Send + Sync>;
pub type AttributeSetter<R> = Arc<dyn Fn(&R) + Send + Sync>;
struct Attributes<R, E> {
    setters: BTreeMap<String, AttributeSetter<R>>,
    last: Option<Arc<R>>,
    values: BTreeMap<String, Arc<dyn Any + Send + Sync>>,
    on_created: Option<CreatedHook<R, E>>,
    after_created: Option<CreatedHook<R, E>>,
}

/// Cloneable client-facing handle. Start inside a Tokio runtime.
/// Dropping the last supervisor requests shutdown; `stop().await` waits for teardown.
pub struct Supervisor<L: Lifecycle> {
    lifecycle: Arc<L>,
    owner: Arc<Owner>,
    snapshot: watch::Receiver<Snapshot<L::Resource>>,
    recovery: mpsc::UnboundedSender<Recovery>,
    attributes: Arc<Mutex<Attributes<L::Resource, L::Error>>>,
    config: Config,
    sender: watch::Sender<Snapshot<L::Resource>>,
    driver: Arc<tokio::sync::Mutex<Option<mpsc::UnboundedReceiver<Recovery>>>>,
    generations: Arc<AtomicU64>,
    failure: watch::Sender<Option<Arc<L::Error>>>,
}
impl<L: Lifecycle> Clone for Supervisor<L> {
    fn clone(&self) -> Self {
        Self {
            lifecycle: self.lifecycle.clone(),
            owner: self.owner.clone(),
            snapshot: self.snapshot.clone(),
            recovery: self.recovery.clone(),
            attributes: self.attributes.clone(),
            config: self.config.clone(),
            sender: self.sender.clone(),
            driver: self.driver.clone(),
            generations: self.generations.clone(),
            failure: self.failure.clone(),
        }
    }
}
impl<L: Lifecycle> Supervisor<L> {
    /// Validate before constructing channels or starting lifecycle work.
    pub fn try_new(lifecycle: L, config: Config) -> Result<Self, ConfigError> {
        config.validate()?;
        Ok(Self::new(lifecycle, config))
    }
    pub fn try_start(lifecycle: L, config: Config) -> Result<Self, ConfigError> {
        let supervisor = Self::try_new(lifecycle, config)?;
        supervisor.begin();
        Ok(supervisor)
    }
    pub fn start(lifecycle: L, config: Config) -> Self {
        let supervisor = Self::new(lifecycle, config);
        supervisor.begin();
        supervisor
    }
    /// Construct without spawning tasks, so hooks and setters can be configured first.
    pub fn new(lifecycle: L, config: Config) -> Self {
        let lifecycle = Arc::new(lifecycle);
        let (stop, stopping) = watch::channel(false);
        let (sender, snapshot) = watch::channel(Snapshot {
            state: ResourceState::Disconnected,
            handle: None,
        });
        let (recovery, requests) = mpsc::unbounded_channel();
        let attributes = Arc::new(Mutex::new(Attributes {
            setters: BTreeMap::new(),
            last: None,
            values: BTreeMap::new(),
            on_created: None,
            after_created: None,
        }));
        drop(stopping);
        Self {
            lifecycle,
            owner: Arc::new(Owner {
                stop,
                started: AtomicBool::new(false),
            }),
            snapshot,
            recovery,
            attributes,
            config,
            sender,
            driver: Arc::new(tokio::sync::Mutex::new(Some(requests))),
            generations: Arc::new(AtomicU64::new(0)),
            failure: watch::channel(None).0,
        }
    }
    /// Start supervision once. Acquisition starts an unstarted supervisor automatically.
    pub fn begin(&self) {
        if self.sender.send_if_modified(|status| {
            if status.state == ResourceState::Stopped {
                status.state = ResourceState::Connecting;
                true
            } else {
                false
            }
        }) {
            self.owner.stop.send_replace(false);
            self.owner.started.store(true, Ordering::Release);
            self.launch();
            return;
        }
        if !*self.owner.stop.borrow() && !self.owner.started.swap(true, Ordering::AcqRel) {
            publish(&self.sender, ResourceState::Connecting, None);
            self.launch();
        }
    }
    fn launch(&self) {
        let driver = self.driver.clone();
        let lifecycle = self.lifecycle.clone();
        let config = self.config.clone();
        let stopping = self.owner.stop.subscribe();
        let sender = self.sender.clone();
        let attributes = self.attributes.clone();
        let generations = self.generations.clone();
        let failure = self.failure.clone();
        tokio::spawn(async move {
            let mut guard = driver.lock().await;
            let requests = guard.take().expect("only one lifecycle worker");
            *guard = Some(
                maintain(
                    lifecycle,
                    config,
                    stopping,
                    WorkerStatus { sender, failure },
                    requests,
                    attributes,
                    generations,
                )
                .await,
            );
        });
    }
    /// Restart a fully stopped supervisor, preserving setters and public handles.
    /// Returns false if it is already running. Pools remain permanently stopped.
    pub async fn restart(&self) -> bool {
        if self.state() != ResourceState::Stopped {
            return false;
        }
        let guard = self.driver.lock().await;
        if self.state() != ResourceState::Stopped {
            return false;
        }
        self.owner.stop.send_replace(false);
        self.owner.started.store(true, Ordering::Release);
        publish(&self.sender, ResourceState::Connecting, None);
        drop(guard);
        self.launch();
        true
    }
    pub fn subscribe(&self) -> watch::Receiver<ResourceStatus<L::Resource>> {
        self.snapshot.clone()
    }
    pub fn current(&self) -> Option<ResourceHandle<L::Resource>> {
        self.snapshot.borrow().handle.clone()
    }
    /// Read an owned attribute from the latest created resource, even during recovery.
    /// Returns None after awaited stop; stored values remain readable.
    pub fn with_latest<T>(&self, read: impl FnOnce(&L::Resource) -> T) -> Option<T> {
        self.attributes.lock().unwrap().last.as_deref().map(read)
    }
    /// Replay a synchronous attribute setter on the latest resource and all replacements.
    /// Setters must be short, nonblocking, and must not reenter this supervisor.
    pub fn set_attribute(&self, name: impl Into<String>, setter: AttributeSetter<L::Resource>) {
        let mut attributes = self.attributes.lock().unwrap();
        if let Some(resource) = &attributes.last {
            setter(resource);
        }
        attributes.setters.insert(name.into(), setter);
    }
    /// Store a typed value that is readable before creation and replay it on replacement.
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
        let mut attributes = self.attributes.lock().unwrap();
        attributes.values.insert(name.clone(), stored);
        if let Some(resource) = &attributes.last {
            setter(resource);
        }
        attributes.setters.insert(name, setter);
    }
    pub fn value<T: Clone + Send + Sync + 'static>(&self, name: &str) -> Option<T> {
        self.attributes
            .lock()
            .unwrap()
            .values
            .get(name)?
            .downcast_ref::<T>()
            .cloned()
    }
    pub fn set_on_resource_created(&self, hook: CreatedHook<L::Resource, L::Error>) {
        self.attributes.lock().unwrap().on_created = Some(hook);
    }
    pub fn is_connected(&self) -> bool {
        self.current()
            .is_some_and(|handle| self.lifecycle.is_connected(&handle.resource))
    }
    pub fn stopped(&self) -> watch::Receiver<bool> {
        self.owner.stop.subscribe()
    }
    pub fn state(&self) -> ResourceState {
        self.snapshot.borrow().state
    }
    /// Inspect the latched cause without implicitly resetting it. Error values
    /// may contain secrets; the framework does not format this cause in logs.
    pub fn failure(&self) -> Option<Arc<L::Error>> {
        self.failure.borrow().clone()
    }
    /// Explicitly permit a failed lifecycle to try again after teardown. Does not
    /// restart a stopped supervisor. Stop takes precedence over reset.
    pub fn reset_failure(&self) -> bool {
        if *self.owner.stop.borrow() {
            return false;
        }
        self.failure
            .send_if_modified(|cause| cause.take().is_some())
    }
    pub async fn acquire(&self) -> Result<ResourceHandle<L::Resource>, Error<L::Error>> {
        self.begin();
        self.acquire_running().await
    }
    /// Acquire readiness within one overall timeout, following lifecycle recovery.
    /// Zero returns Timeout without starting supervision; an already stopped
    /// supervisor returns Stopped. The timeout starts when this future is polled.
    pub async fn acquire_with_timeout(
        &self,
        duration: Duration,
    ) -> Result<ResourceHandle<L::Resource>, Error<L::Error>> {
        with_call_timeout(duration, self.stopped(), self.acquire()).await
    }
    pub(crate) async fn acquire_running(
        &self,
    ) -> Result<ResourceHandle<L::Resource>, Error<L::Error>> {
        let mut snapshot = self.snapshot.clone();
        loop {
            if *self.owner.stop.borrow() || snapshot.has_changed().is_err() {
                return Err(Error::Stopped);
            }
            if let Some(cause) = self.failure() {
                return Err(Error::Lifecycle(cause));
            }
            if let Some(handle) = snapshot.borrow_and_update().handle.clone() {
                return Ok(handle);
            }
            snapshot.changed().await.map_err(|_| Error::Stopped)?;
        }
    }
    /// Request recovery without waiting for teardown.
    pub fn request_recovery(&self, handle: &ResourceHandle<L::Resource>, terminal: bool) {
        let _ = self.submit_recovery(handle, terminal, None);
    }
    /// Wait for bounded teardown. Does not wait for reconnection.
    pub async fn recover(
        &self,
        handle: &ResourceHandle<L::Resource>,
        terminal: bool,
    ) -> Result<(), Error<L::Error>> {
        let (sender, receiver) = oneshot::channel();
        if self.submit_recovery(handle, terminal, Some(sender)) {
            receiver.await.map_err(|_| Error::Stopped)?;
        }
        Ok(())
    }
    pub async fn recover_current(&self, terminal: bool) -> Result<(), Error<L::Error>> {
        if let Some(handle) = self.current() {
            self.recover(&handle, terminal).await?;
        }
        Ok(())
    }
    fn submit_recovery(
        &self,
        handle: &ResourceHandle<L::Resource>,
        terminal: bool,
        completed: Option<oneshot::Sender<()>>,
    ) -> bool {
        // Pointer identity also rejects handles from a different supervisor.
        let mut matched = self.sender.send_if_modified(|snapshot| {
            if snapshot
                .handle
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(&current.resource, &handle.resource))
            {
                snapshot.handle = None;
                snapshot.state = ResourceState::Recovering;
                true
            } else {
                false
            }
        });
        if !matched && self.state() == ResourceState::Recovering {
            matched = self
                .attributes
                .lock()
                .unwrap()
                .last
                .as_ref()
                .is_some_and(|resource| Arc::ptr_eq(resource, &handle.resource));
        }
        if matched {
            let _ = self.recovery.send(Recovery {
                generation: handle.generation,
                terminal,
                completed,
            });
        }
        matched
    }
    /// Run once. By default failures request recovery and return the original error
    /// without replay; `operation_failure` can retain a healthy generation instead.
    /// Dropping this future cancels the operation; protocol-specific cancellation safety
    /// remains the client's responsibility. Shutdown cancels operations driven by this method.
    pub async fn execute<F, Fut, T>(&self, operation: F) -> Result<T, Error<L::Error>>
    where
        F: FnOnce(Arc<L::Resource>) -> Fut,
        Fut: Future<Output = Result<T, L::Error>>,
    {
        self.begin();
        self.execute_running(operation).await
    }
    /// Run once within an overall timeout covering readiness, the operation and
    /// any bounded failure teardown. Never replay. Zero does not start work.
    /// Shutdown takes precedence. Expiry drops the operation future; adapters
    /// remain responsible for poisoning interrupted protocol exchanges.
    pub async fn execute_with_timeout<F, Fut, T>(
        &self,
        duration: Duration,
        operation: F,
    ) -> Result<T, Error<L::Error>>
    where
        F: FnOnce(Arc<L::Resource>) -> Fut,
        Fut: Future<Output = Result<T, L::Error>>,
    {
        with_call_timeout(duration, self.stopped(), self.execute(operation)).await
    }
    pub(crate) async fn execute_running<F, Fut, T>(
        &self,
        operation: F,
    ) -> Result<T, Error<L::Error>>
    where
        F: FnOnce(Arc<L::Resource>) -> Fut,
        Fut: Future<Output = Result<T, L::Error>>,
    {
        let handle = self.acquire_running().await?;
        let mut stopping = self.owner.stop.subscribe();
        let result = tokio::select! {
            biased;
            _ = shutdown(&mut stopping) => return Err(Error::Stopped),
            result = operation(handle.resource.clone()) => result,
        };
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                if self.lifecycle.operation_failure(&error) == OperationFailurePolicy::Recover {
                    let _ = self
                        .recover(&handle, self.lifecycle.is_terminal(&error))
                        .await;
                }
                Err(Error::Operation(error))
            }
        }
    }
    /// Retry an operation only when `retry_if` accepts its error and attempts remain.
    /// Each invocation of `operation` must create a fresh future and safely repeat
    /// the remote action. The next attempt waits for lifecycle readiness; a retained
    /// healthy generation may be reused without replacement.
    /// Exhaustion returns the last operation error; the overall deadline returns
    /// [`Error::Timeout`]. Shutdown returns [`Error::Stopped`]; cancellation drops
    /// the active attempt without starting another. As with [`Self::execute`],
    /// resource-specific cancellation safety is the adapter's responsibility.
    /// Terminal classification controls teardown, not whether replay is allowed.
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
        if policy.timeout.is_zero() {
            return Err(Error::Timeout);
        }
        self.begin();
        self.execute_with_retry_running(policy, operation, retry_if)
            .await
    }
    pub(crate) async fn execute_with_retry_running<F, Fut, T, P>(
        &self,
        policy: RetryPolicy,
        mut operation: F,
        mut retry_if: P,
    ) -> Result<T, Error<L::Error>>
    where
        F: FnMut(Arc<L::Resource>) -> Fut,
        Fut: Future<Output = Result<T, L::Error>>,
        P: FnMut(&L::Error) -> bool,
    {
        if policy.timeout.is_zero() {
            return Err(Error::Timeout);
        }
        let mut stopping = self.owner.stop.subscribe();
        let retry_stopping = stopping.clone();
        let attempts = async {
            for attempt in 1..=policy.max_attempts.get() {
                // Do not implicitly restart a stopped supervisor between attempts.
                if *retry_stopping.borrow() {
                    return Err(Error::Stopped);
                }
                match self.execute_running(&mut operation).await {
                    Err(Error::Operation(error))
                        if attempt < policy.max_attempts.get() && retry_if(&error) => {}
                    result => return result,
                }
            }
            unreachable!("a nonzero final attempt always returns")
        };
        tokio::select! {
            biased;
            _ = shutdown(&mut stopping) => Err(Error::Stopped),
            result = timeout(policy.timeout, attempts) => result.unwrap_or(Err(Error::Timeout)),
        }
    }
    /// Idempotent, cancellation-safe shutdown. Clears resource-derived caches,
    /// preserving stored configuration and registrations. External handles and
    /// abandoned timed-out hook tasks can retain resources beyond this return.
    pub async fn stop(&self) {
        self.request_stop();
        let mut snapshot = self.snapshot.clone();
        loop {
            if snapshot.borrow_and_update().state == ResourceState::Stopped {
                // Wait for the worker to return its receiver and release lifecycle state.
                drop(self.driver.lock().await);
                return;
            }
            if snapshot.changed().await.is_err() {
                return;
            }
        }
    }
    pub(crate) fn request_stop(&self) {
        self.owner.stop.send_replace(true);
        if !self.owner.started.load(Ordering::Acquire) {
            publish(&self.sender, ResourceState::Stopped, None);
        } else {
            self.sender.send_if_modified(|status| {
                if status.state == ResourceState::Stopped {
                    false
                } else {
                    status.state = ResourceState::Stopping;
                    status.handle = None;
                    true
                }
            });
        }
    }
}
/// Shared single-attempt timeout policy: zero never polls work; stop wins ties.
async fn with_call_timeout<T, E>(
    duration: Duration,
    mut stopping: watch::Receiver<bool>,
    work: impl Future<Output = Result<T, Error<E>>>,
) -> Result<T, Error<E>> {
    if *stopping.borrow() {
        return Err(Error::Stopped);
    }
    if duration.is_zero() {
        return Err(Error::Timeout);
    }
    let result = tokio::select! {
        biased;
        _ = shutdown(&mut stopping) => Err(Error::Stopped),
        result = timeout(duration, work) => result.unwrap_or(Err(Error::Timeout)),
    };
    if *stopping.borrow() {
        Err(Error::Stopped)
    } else {
        result
    }
}
async fn shutdown(receiver: &mut watch::Receiver<bool>) {
    loop {
        if *receiver.borrow_and_update() {
            return;
        }
        if receiver.changed().await.is_err() {
            return;
        }
    }
}
fn publish<R>(
    sender: &watch::Sender<Snapshot<R>>,
    state: ResourceState,
    handle: Option<ResourceHandle<R>>,
) {
    sender.send_replace(Snapshot { state, handle });
}
fn log_failure<L: Lifecycle>(lifecycle: &L, config: &Config, name: &str, error: &L::Error) {
    if lifecycle.is_expected(error) {
        tracing::warn!(resource = config.resource_name, hook = name, %error);
        tracing::debug!(resource = config.resource_name, hook = name, ?error);
    } else {
        tracing::error!(resource = config.resource_name, hook = name, %error);
    }
}
fn log_late_failure<L: Lifecycle>(lifecycle: &L, config: &Config, name: &str, error: &L::Error) {
    if classifiable_hook(name) && lifecycle.lifecycle_failure(error) == LifecycleFailurePolicy::Fail
    {
        if lifecycle.is_expected(error) {
            tracing::debug!(
                resource = config.resource_name,
                hook = name,
                "abandoned lifecycle hook failed (cause redacted)"
            );
        } else {
            tracing::error!(
                resource = config.resource_name,
                hook = name,
                "abandoned lifecycle hook failed (cause redacted)"
            );
        }
        return;
    }
    if lifecycle.is_expected(error) {
        tracing::debug!(
            resource = config.resource_name,
            hook = name,
            ?error,
            "abandoned hook failed"
        );
    } else {
        log_failure(lifecycle, config, name, error);
    }
}
fn classifiable_hook(name: &str) -> bool {
    matches!(
        name,
        "create"
            | "on_resource_created"
            | "connect"
            | "late connect"
            | "setup"
            | "watch_disconnect"
    )
}
struct HookFailure<E> {
    terminal: bool,
    cause: Option<Arc<E>>,
}
impl<E> HookFailure<E> {
    fn retry() -> Self {
        Self {
            terminal: false,
            cause: None,
        }
    }
}
fn hook_failure<L: Lifecycle>(
    lifecycle: &L,
    config: &Config,
    name: &str,
    error: L::Error,
) -> HookFailure<L::Error> {
    let terminal = lifecycle.is_terminal(&error);
    if classifiable_hook(name)
        && lifecycle.lifecycle_failure(&error) == LifecycleFailurePolicy::Fail
    {
        if lifecycle.is_expected(&error) {
            tracing::warn!(
                resource = config.resource_name,
                hook = name,
                "non-retryable lifecycle failure (cause redacted)"
            );
            tracing::debug!(
                resource = config.resource_name,
                hook = name,
                "non-retryable lifecycle failure (cause redacted)"
            );
        } else {
            tracing::error!(
                resource = config.resource_name,
                hook = name,
                "non-retryable lifecycle failure (cause redacted)"
            );
        }
        HookFailure {
            terminal,
            cause: Some(Arc::new(error)),
        }
    } else {
        log_failure(lifecycle, config, name, &error);
        HookFailure {
            terminal,
            cause: None,
        }
    }
}
async fn bounded<L: Lifecycle>(
    lifecycle: Arc<L>,
    config: Config,
    name: &'static str,
    duration: Option<Duration>,
    future: impl Future<Output = Result<(), L::Error>> + Send + 'static,
) -> Result<(), HookFailure<L::Error>> {
    let mut task = tokio::spawn(future);
    match deadline(duration, &mut task).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(hook_failure(lifecycle.as_ref(), &config, name, error)),
        Ok(Err(error)) => {
            tracing::error!(hook = name, %error, "hook task failed");
            Err(HookFailure::retry())
        }
        Err(_) => {
            tracing::warn!(
                resource = config.resource_name,
                hook = name,
                "hook abandoned after timeout"
            );
            tokio::spawn(async move {
                match task.await {
                    Ok(Err(error)) => log_late_failure(lifecycle.as_ref(), &config, name, &error),
                    Ok(Ok(())) => tracing::warn!(
                        resource = config.resource_name,
                        hook = name,
                        "abandoned hook completed after timeout"
                    ),
                    Err(error) => {
                        tracing::error!(hook = name, %error, "abandoned hook task failed")
                    }
                }
            });
            Err(HookFailure::retry())
        }
    }
}
async fn deadline<T>(
    duration: Option<Duration>,
    future: impl Future<Output = T>,
) -> Result<T, tokio::time::error::Elapsed> {
    match duration {
        Some(duration) => timeout(duration, future).await,
        None => Ok(future.await),
    }
}
async fn destroy<L: Lifecycle>(lifecycle: Arc<L>, config: Config, resource: Arc<L::Resource>) {
    let manager = lifecycle.clone();
    let _ = bounded(
        lifecycle,
        config.clone(),
        "destroy",
        config.destroy_timeout,
        async move { manager.destroy(&resource).await },
    )
    .await;
}
async fn close<L: Lifecycle>(
    lifecycle: Arc<L>,
    config: Config,
    resource: Arc<L::Resource>,
    terminal: bool,
) {
    let manager = lifecycle.clone();
    let value = resource.clone();
    let _ = bounded(
        lifecycle.clone(),
        config.clone(),
        "cleanup",
        Some(config.cleanup_timeout),
        async move { manager.cleanup(&value).await },
    )
    .await;
    if !terminal {
        let manager = lifecycle.clone();
        let value = resource.clone();
        let mut task = tokio::spawn(async move { manager.disconnect(&value).await });
        match timeout(config.disconnect_timeout, &mut task).await {
            Ok(Ok(Err(error))) => log_failure(lifecycle.as_ref(), &config, "disconnect", &error),
            Ok(Err(error)) => tracing::error!(%error, "disconnect task failed"),
            Ok(Ok(Ok(()))) => {}
            Err(_) => {
                tracing::warn!(
                    resource = config.resource_name,
                    "disconnect abandoned after timeout"
                );
                tokio::spawn(async move {
                    if let Ok(Err(error)) = task.await {
                        log_late_failure(lifecycle.as_ref(), &config, "disconnect", &error);
                    }
                    destroy(lifecycle, config, resource).await;
                });
                return;
            }
        }
    }
    destroy(lifecycle, config, resource).await;
}
async fn connect<L: Lifecycle>(
    lifecycle: Arc<L>,
    config: Config,
    resource: Arc<L::Resource>,
    stopping: &mut watch::Receiver<bool>,
) -> Result<(), HookFailure<L::Error>> {
    let manager = lifecycle.clone();
    let value = resource.clone();
    let mut task = tokio::spawn(async move { manager.connect(&value).await });
    tokio::select! {
        biased;
        _ = shutdown(stopping) => { task.abort(); let _ = task.await; Err(HookFailure::retry()) }
        result = timeout(config.connect_timeout, &mut task) => match result {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(error))) => {
                Err(hook_failure(lifecycle.as_ref(), &config, "connect", error))
            }
            Ok(Err(error)) => { tracing::error!(%error, "connect task failed"); Err(HookFailure::retry()) }
            Err(_) => {
                tracing::warn!(resource = config.resource_name, "connect abandoned after timeout");
                tokio::spawn(async move {
                    match task.await {
                        Ok(Ok(())) => close(lifecycle, config, resource, false).await,
                        Ok(Err(error)) => log_late_failure(lifecycle.as_ref(), &config, "late connect", &error),
                        Err(error) => tracing::error!(%error, "late connect task failed"),
                    }
                });
                Err(HookFailure::retry())
            }
        }
    }
}
struct WorkerStatus<L: Lifecycle> {
    sender: watch::Sender<Snapshot<L::Resource>>,
    failure: watch::Sender<Option<Arc<L::Error>>>,
}
fn latch_failure<L: Lifecycle>(status: &WorkerStatus<L>, cause: Arc<L::Error>) {
    status.failure.send_replace(Some(cause));
    publish(&status.sender, ResourceState::Failed, None);
}
async fn wait_for_reset<E>(
    failure: &watch::Sender<Option<Arc<E>>>,
    stopping: &mut watch::Receiver<bool>,
    requests: &mut mpsc::UnboundedReceiver<Recovery>,
) {
    let mut changes = failure.subscribe();
    loop {
        if changes.borrow_and_update().is_none() {
            return;
        }
        tokio::select! {
            biased;
            _ = shutdown(stopping) => return,
            _ = changes.changed() => {},
            request = requests.recv() => {
                match request {
                    Some(request) => {
                        if let Some(completed) = request.completed {
                            let _ = completed.send(());
                        }
                    }
                    None => return,
                }
            }
        }
    }
}
async fn maintain<L: Lifecycle>(
    lifecycle: Arc<L>,
    config: Config,
    mut stopping: watch::Receiver<bool>,
    status: WorkerStatus<L>,
    mut requests: mpsc::UnboundedReceiver<Recovery>,
    attributes: Arc<Mutex<Attributes<L::Resource, L::Error>>>,
    generations: Arc<AtomicU64>,
) -> mpsc::UnboundedReceiver<Recovery> {
    let sender = &status.sender;
    let mut delay = config.retry_delay;
    loop {
        while let Ok(request) = requests.try_recv() {
            if let Some(completed) = request.completed {
                let _ = completed.send(());
            }
        }
        if *stopping.borrow() {
            break;
        }
        publish(sender, ResourceState::Connecting, None);
        let manager = lifecycle.clone();
        let mut creating = tokio::spawn(async move { manager.create().await });
        let created = tokio::select! {
            biased;
            _ = shutdown(&mut stopping) => { creating.abort(); let _ = creating.await; break },
            result = deadline(config.create_timeout, &mut creating) => result,
        };
        match created {
            Ok(Ok(Ok(resource))) => {
                let resource = Arc::new(resource);
                let (on_created, after_created) = {
                    let mut attributes = attributes.lock().unwrap();
                    attributes.last = Some(resource.clone());
                    (
                        attributes.on_created.clone(),
                        attributes.after_created.clone(),
                    )
                };
                let manager = lifecycle.clone();
                let value = resource.clone();
                let settings = attributes.clone();
                let created = bounded(
                    lifecycle.clone(),
                    config.clone(),
                    "on_resource_created",
                    config.created_timeout,
                    async move {
                        manager.on_resource_created(&value).await?;
                        if let Some(hook) = on_created {
                            hook(value.clone()).await?;
                        }
                        {
                            let settings = settings.lock().unwrap();
                            for setter in settings.setters.values() {
                                setter(&value);
                            }
                        }
                        if let Some(hook) = after_created {
                            hook(value).await?;
                        }
                        Ok(())
                    },
                );
                let created = tokio::select! { biased; _ = shutdown(&mut stopping) => Err(HookFailure::retry()), result = created => result };
                let connected = if created.is_ok() {
                    connect(
                        lifecycle.clone(),
                        config.clone(),
                        resource.clone(),
                        &mut stopping,
                    )
                    .await
                } else {
                    created
                };
                let manager = lifecycle.clone();
                let value = resource.clone();
                let setup = if connected.is_ok() {
                    tokio::select! {
                        biased;
                        _ = shutdown(&mut stopping) => Err(HookFailure::retry()),
                        ready = bounded(lifecycle.clone(), config.clone(), "setup", Some(config.setup_timeout),
                            async move { manager.setup(&value).await }) => ready,
                    }
                } else {
                    connected
                };
                if setup.is_ok() {
                    let generation = generations.fetch_add(1, Ordering::AcqRel) + 1;
                    delay = config.retry_delay;
                    tracing::info!(resource = config.resource_name, "connected");
                    publish(
                        sender,
                        ResourceState::Connected,
                        Some(ResourceHandle {
                            resource: resource.clone(),
                            generation,
                        }),
                    );
                    let mut watchdog = lifecycle
                        .watch_disconnect(resource.clone())
                        .map(tokio::spawn);
                    let mut watchdog_done = false;
                    let mut completed = None;
                    let terminal = loop {
                        tokio::select! {
                            biased;
                            _ = shutdown(&mut stopping) => break HookFailure::retry(),
                            request = requests.recv() => {
                                match request {
                                    Some(request) if request.generation == generation => { completed = request.completed; break HookFailure { terminal: request.terminal, cause: None } },
                                    Some(request) => { if let Some(completed) = request.completed { let _ = completed.send(()); } continue },
                                    None => break HookFailure::retry(),
                                }
                            }
                            result = async { match watchdog.as_mut() { Some(task) => task.await, None => std::future::pending().await } } => {
                                watchdog_done = true;
                                break match result { Ok(Err(error)) => hook_failure(lifecycle.as_ref(), &config, "watch_disconnect", error), _ => HookFailure::retry() };
                            }
                        }
                    };
                    if !watchdog_done && let Some(watchdog) = watchdog {
                        watchdog.abort();
                        let _ = watchdog.await;
                    }
                    let state = if *stopping.borrow() {
                        ResourceState::Stopping
                    } else {
                        ResourceState::Recovering
                    };
                    publish(sender, state, None);
                    let failed = terminal.cause.is_some();
                    if let Some(cause) = terminal.cause {
                        latch_failure(&status, cause);
                    }
                    close(
                        lifecycle.clone(),
                        config.clone(),
                        resource,
                        terminal.terminal,
                    )
                    .await;
                    if let Some(completed) = completed {
                        let _ = completed.send(());
                    }
                    if failed {
                        wait_for_reset(&status.failure, &mut stopping, &mut requests).await;
                    }
                    continue;
                }
                let failure = setup.expect_err("failed setup");
                let failed = failure.cause.is_some();
                if let Some(cause) = failure.cause {
                    latch_failure(&status, cause);
                } else {
                    publish(sender, ResourceState::Disconnected, None);
                }
                close(
                    lifecycle.clone(),
                    config.clone(),
                    resource,
                    failure.terminal,
                )
                .await;
                if failed {
                    wait_for_reset(&status.failure, &mut stopping, &mut requests).await;
                    continue;
                }
            }
            Ok(Ok(Err(error))) => {
                let failure = hook_failure(lifecycle.as_ref(), &config, "create", error);
                if let Some(cause) = failure.cause {
                    latch_failure(&status, cause);
                    wait_for_reset(&status.failure, &mut stopping, &mut requests).await;
                    continue;
                }
            }
            Ok(Err(error)) => tracing::error!(%error, "creation task failed"),
            Err(_) => {
                tracing::warn!("resource creation timed out");
                let manager = lifecycle.clone();
                let settings = config.clone();
                tokio::spawn(async move {
                    if let Ok(Ok(resource)) = creating.await {
                        close(manager, settings, Arc::new(resource), false).await;
                    }
                });
            }
        }
        publish(sender, ResourceState::Disconnected, None);
        tracing::info!(resource = config.resource_name, retry_delay = ?delay, "retrying connection");
        tokio::select! { biased; _ = shutdown(&mut stopping) => break, _ = sleep(delay) => {} }
        delay = delay.saturating_mul(2).min(config.max_retry_delay);
    }
    while let Ok(request) = requests.try_recv() {
        if let Some(completed) = request.completed {
            let _ = completed.send(());
        }
    }
    attributes.lock().unwrap().last = None;
    status.failure.send_replace(None);
    publish(sender, ResourceState::Stopped, None);
    requests
}
