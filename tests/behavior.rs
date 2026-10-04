use etherbird::{
    Config, Error, Lifecycle, OperationQueue, Pool, PoolConfig, QueuedOperation, ResourceState,
    Supervisor, async_trait,
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::Notify,
    time::{sleep, timeout},
};

#[derive(Debug)]
struct Failure;
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("connection lost")
    }
}
struct CustomQueue {
    items: Vec<QueuedOperation<Manager>>,
    priorities: Arc<Mutex<Vec<i32>>>,
    removed: Arc<AtomicUsize>,
}
impl OperationQueue<QueuedOperation<Manager>> for CustomQueue {
    fn push(&mut self, item: QueuedOperation<Manager>) -> Result<(), etherbird::QueueError> {
        self.priorities.lock().unwrap().push(item.priority());
        self.items.push(item);
        Ok(())
    }
    fn pop(&mut self) -> Option<QueuedOperation<Manager>> {
        self.items.pop()
    }
    fn len(&self) -> usize {
        self.items.len()
    }
    fn retain(&mut self, predicate: &mut dyn FnMut(&QueuedOperation<Manager>) -> bool) {
        let before = self.items.len();
        self.items.retain(predicate);
        self.removed
            .fetch_add(before - self.items.len(), Ordering::SeqCst);
    }
}

#[tokio::test]
async fn custom_factory_controls_order_after_capacity_wait_and_receives_metadata() {
    let c = Arc::new(Control::default());
    let control = c.clone();
    let factory_calls = AtomicUsize::new(0);
    let priorities = Arc::new(Mutex::new(Vec::new()));
    let removed = Arc::new(AtomicUsize::new(0));
    let p = Pool::start_with_queue_factory(
        move || Supervisor::new(Manager(control.clone()), config()),
        PoolConfig::default(),
        || {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            CustomQueue {
                items: Vec::new(),
                priorities: priorities.clone(),
                removed: removed.clone(),
            }
        },
    );
    let lease = p.borrow().await.unwrap();
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut tasks = Vec::new();
    for (index, priority) in [10, -1, 0].into_iter().enumerate() {
        let other = p.clone();
        let output = order.clone();
        tasks.push(tokio::spawn(async move {
            other
                .execute_with_priority(priority, move |_| async move {
                    output.lock().unwrap().push(index);
                    Ok(())
                })
                .await
        }));
        until(|| priorities.lock().unwrap().len() == index + 1).await;
    }
    drop(lease);
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    assert_eq!(*order.lock().unwrap(), [2, 1, 0]);
    assert_eq!(*priorities.lock().unwrap(), [10, -1, 0]);
    assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
    p.stop().await;
}

#[tokio::test]
async fn custom_queue_prunes_cancelled_work_and_shutdown_drains_remaining_work() {
    let c = Arc::new(Control::default());
    let control = c.clone();
    let priorities = Arc::new(Mutex::new(Vec::new()));
    let removed = Arc::new(AtomicUsize::new(0));
    let p = Pool::start_with_queue_factory(
        move || Supervisor::new(Manager(control.clone()), config()),
        PoolConfig::default(),
        || CustomQueue {
            items: Vec::new(),
            priorities: priorities.clone(),
            removed: removed.clone(),
        },
    );
    let lease = p.borrow().await.unwrap();
    let other = p.clone();
    let cancelled = tokio::spawn(async move {
        other
            .execute(|_| async {
                panic!("cancelled custom job executed");
                #[allow(unreachable_code)]
                Ok(())
            })
            .await
    });
    until(|| priorities.lock().unwrap().len() == 1).await;
    cancelled.abort();
    let _ = cancelled.await;
    until(|| removed.load(Ordering::SeqCst) == 1).await;
    let other = p.clone();
    let queued = tokio::spawn(async move {
        other
            .execute(|_| async {
                panic!("job executed during shutdown");
                #[allow(unreachable_code)]
                Ok(())
            })
            .await
    });
    until(|| priorities.lock().unwrap().len() == 2).await;
    p.stop().await;
    assert!(matches!(queued.await.unwrap(), Err(Error::Stopped)));
    assert!(matches!(lease.acquire().await, Err(Error::Stopped)));
}

#[tokio::test]
async fn explicit_fifo_queue_overrides_priority_configuration() {
    let c = Arc::new(Control::default());
    let control = c.clone();
    let p = Pool::start_with_queue_factory(
        move || Supervisor::new(Manager(control.clone()), config()),
        PoolConfig {
            priority_queue: true,
            ..PoolConfig::default()
        },
        etherbird::FifoQueue::default,
    );
    let lease = p.borrow().await.unwrap();
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut tasks = Vec::new();
    for (index, priority) in [10, -1, 0].into_iter().enumerate() {
        let other = p.clone();
        let output = order.clone();
        tasks.push(tokio::spawn(async move {
            other
                .execute_with_priority(priority, move |_| async move {
                    output.lock().unwrap().push(index);
                    Ok(())
                })
                .await
        }));
        sleep(Duration::from_millis(2)).await;
    }
    drop(lease);
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    assert_eq!(*order.lock().unwrap(), [0, 1, 2]);
    p.stop().await;
}
impl std::error::Error for Failure {}
struct Client {
    id: usize,
    setting: AtomicUsize,
    closed: AtomicBool,
    lost: Notify,
    connected: AtomicBool,
    connection_changed: Notify,
    protocol_callback: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}
impl Client {
    fn identifier(&self) -> Result<usize, Failure> {
        Ok(self.id)
    }
    async fn ping(&self, value: usize) -> Result<usize, Failure> {
        Ok(value + self.id)
    }
}
#[derive(Default)]
struct Control {
    created: AtomicUsize,
    failures: AtomicUsize,
    created_failures: AtomicUsize,
    setup_failures: AtomicUsize,
    cleanup_failures: AtomicUsize,
    disconnect_failures: AtomicUsize,
    log: Mutex<Vec<(usize, &'static str)>>,
    connect_gate: Notify,
    block_connect: AtomicBool,
    block_first_connect: AtomicBool,
    cleanup_gate: Notify,
    block_cleanup: AtomicBool,
    disconnect_gate: Notify,
    block_disconnect: AtomicBool,
    create_gate: Notify,
    block_create: AtomicBool,
    setup_gate: Notify,
    block_setup: AtomicBool,
    block_first_setup: AtomicBool,
    disable_watchdog: AtomicBool,
    terminal: AtomicBool,
    expected: AtomicBool,
    active_watches: AtomicUsize,
}
struct Manager(Arc<Control>);
fn fail(counter: &AtomicUsize) -> bool {
    let mut remaining = counter.load(Ordering::SeqCst);
    while let Some(next) = remaining.checked_sub(1) {
        match counter.compare_exchange(remaining, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return true,
            Err(actual) => remaining = actual,
        }
    }
    false
}
#[async_trait]
impl Lifecycle for Manager {
    type Resource = Client;
    type Error = Failure;
    async fn create(&self) -> Result<Client, Failure> {
        let id = self.0.created.fetch_add(1, Ordering::SeqCst);
        if self.0.block_create.load(Ordering::SeqCst) {
            self.0.create_gate.notified().await;
        }
        Ok(Client {
            id,
            setting: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            lost: Notify::new(),
            connected: AtomicBool::new(false),
            connection_changed: Notify::new(),
            protocol_callback: Mutex::new(None),
        })
    }
    async fn on_resource_created(&self, r: &Client) -> Result<(), Failure> {
        self.0.log.lock().unwrap().push((r.id, "created"));
        if fail(&self.0.created_failures) {
            Err(Failure)
        } else {
            Ok(())
        }
    }
    async fn connect(&self, r: &Client) -> Result<(), Failure> {
        self.0.log.lock().unwrap().push((r.id, "connect"));
        if self.0.block_connect.load(Ordering::SeqCst)
            || (r.id == 0 && self.0.block_first_connect.load(Ordering::SeqCst))
        {
            self.0.connect_gate.notified().await;
        }
        if fail(&self.0.failures) {
            Err(Failure)
        } else {
            r.connected.store(true, Ordering::SeqCst);
            r.connection_changed.notify_waiters();
            let callback = r.protocol_callback.lock().unwrap().clone();
            if let Some(callback) = callback {
                callback();
            }
            Ok(())
        }
    }
    async fn setup(&self, r: &Client) -> Result<(), Failure> {
        self.0.log.lock().unwrap().push((r.id, "setup"));
        if self.0.block_setup.load(Ordering::SeqCst)
            || (r.id == 0 && self.0.block_first_setup.load(Ordering::SeqCst))
        {
            self.0.setup_gate.notified().await;
        }
        self.0.log.lock().unwrap().push((r.id, "setup_completed"));
        if fail(&self.0.setup_failures) {
            Err(Failure)
        } else {
            Ok(())
        }
    }
    async fn cleanup(&self, r: &Client) -> Result<(), Failure> {
        self.0.log.lock().unwrap().push((r.id, "cleanup"));
        if fail(&self.0.cleanup_failures) {
            return Err(Failure);
        }
        if self.0.block_cleanup.load(Ordering::SeqCst) {
            self.0.cleanup_gate.notified().await;
        }
        self.0.log.lock().unwrap().push((r.id, "cleanup_completed"));
        Ok(())
    }
    async fn disconnect(&self, r: &Client) -> Result<(), Failure> {
        self.0.log.lock().unwrap().push((r.id, "disconnect"));
        if fail(&self.0.disconnect_failures) {
            return Err(Failure);
        }
        if self.0.block_disconnect.load(Ordering::SeqCst) {
            self.0.disconnect_gate.notified().await;
        }
        r.closed.store(true, Ordering::SeqCst);
        r.connected.store(false, Ordering::SeqCst);
        r.connection_changed.notify_waiters();
        Ok(())
    }
    async fn destroy(&self, r: &Client) -> Result<(), Failure> {
        self.0.log.lock().unwrap().push((r.id, "destroy"));
        Ok(())
    }
    fn watch_disconnect(&self, r: Arc<Client>) -> Option<etherbird::LifecycleFuture<Failure>> {
        if self.0.disable_watchdog.load(Ordering::SeqCst) {
            return None;
        }
        let control = self.0.clone();
        Some(Box::pin(async move {
            control.active_watches.fetch_add(1, Ordering::SeqCst);
            let _watch = WatchGuard(control.clone());
            r.lost.notified().await;
            if control.terminal.load(Ordering::SeqCst) {
                Err(Failure)
            } else {
                Ok(())
            }
        }))
    }
    fn is_connected(&self, r: &Client) -> bool {
        r.connected.load(Ordering::SeqCst)
    }
    async fn wait_connected(&self, r: &Client) {
        loop {
            let notified = r.connection_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_connected(r) {
                return;
            }
            notified.await;
        }
    }
    fn is_terminal(&self, _: &Failure) -> bool {
        self.0.terminal.load(Ordering::SeqCst)
    }
    fn is_expected(&self, _: &Failure) -> bool {
        self.0.expected.load(Ordering::SeqCst)
    }
}
struct WatchGuard(Arc<Control>);
impl Drop for WatchGuard {
    fn drop(&mut self) {
        self.0.active_watches.fetch_sub(1, Ordering::SeqCst);
    }
}
struct LimitedQueue(CustomQueue);
impl OperationQueue<QueuedOperation<Manager>> for LimitedQueue {
    fn push(&mut self, item: QueuedOperation<Manager>) -> Result<(), etherbird::QueueError> {
        if self.0.len() >= 1 {
            return Err(etherbird::QueueError::Full);
        }
        self.0.push(item)
    }
    fn pop(&mut self) -> Option<QueuedOperation<Manager>> {
        self.0.pop()
    }
    fn len(&self) -> usize {
        self.0.len()
    }
    fn retain(&mut self, predicate: &mut dyn FnMut(&QueuedOperation<Manager>) -> bool) {
        self.0.retain(predicate);
    }
}
#[tokio::test]
async fn bounded_queue_rejects_work_without_leaking_capacity() {
    let c = Arc::new(Control::default());
    let control = c.clone();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let removed = Arc::new(AtomicUsize::new(0));
    let p = Pool::new_with_queue_factory(
        move || Supervisor::new(Manager(control.clone()), config()),
        PoolConfig::default(),
        || {
            LimitedQueue(CustomQueue {
                items: Vec::new(),
                priorities: seen.clone(),
                removed: removed.clone(),
            })
        },
    );
    let lease = p.borrow().await.unwrap();
    let other = p.clone();
    let queued = tokio::spawn(async move { other.execute(|_| async { Ok(()) }).await });
    until(|| seen.lock().unwrap().len() == 1).await;
    assert!(matches!(
        p.execute(|_| async { Ok(()) }).await,
        Err(Error::Queue(etherbird::QueueError::Full))
    ));
    let result: Result<(), _> = p
        .execute_with_retry(
            etherbird::RetryPolicy {
                max_attempts: 3.try_into().unwrap(),
                timeout: Duration::from_secs(1),
            },
            |_| async { panic!("rejected queue entry executed") },
            |_| panic!("queue error reached operation retry predicate"),
        )
        .await;
    assert!(matches!(
        result,
        Err(Error::Queue(etherbird::QueueError::Full))
    ));
    queued.abort();
    let _ = queued.await;
    until(|| removed.load(Ordering::SeqCst) == 1).await;
    lease.release();
    timeout(Duration::from_secs(1), p.execute(|_| async { Ok(()) }))
        .await
        .unwrap()
        .unwrap();
    p.stop().await;
}
#[tokio::test]
async fn proxy_attributes_remain_readable_while_connecting() {
    let c = Arc::new(Control::default());
    c.block_connect.store(true, Ordering::SeqCst);
    let control = c.clone();
    let p = Pool::new(
        move || Supervisor::new(Manager(control.clone()), config()),
        PoolConfig::default(),
    );
    let proxy = etherbird::ManagedResourceProxy::new(p.clone());
    proxy.set_value("setting", 99usize, |r, value| {
        r.setting.store(*value, Ordering::SeqCst)
    });
    p.begin();
    until(|| proxy.attribute(|r| r.setting.load(Ordering::SeqCst)) == Some(99)).await;
    assert!(p.resources().is_empty());
    assert_eq!(p.state(), ResourceState::Connecting);
    p.stop().await;
}
#[tokio::test]
async fn stored_protocol_callback_runs_without_borrowing_a_lease() {
    type Callback = Arc<dyn Fn() + Send + Sync>;
    let c = Arc::new(Control::default());
    let control = c.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let p = Pool::new(
        move || Supervisor::new(Manager(control.clone()), config()),
        PoolConfig::default(),
    );
    let proxy = etherbird::ManagedResourceProxy::new(p.clone());
    let callback: Callback = Arc::new(move || {
        count.fetch_add(1, Ordering::SeqCst);
    });
    proxy.set_value("protocol_made_connection", callback, |r, value| {
        *r.protocol_callback.lock().unwrap() = Some(value.clone());
    });
    let lease = p.borrow().await.unwrap();
    let old = lease.acquire().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    proxy.value::<Callback>("protocol_made_connection").unwrap()();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    p.recover(Some(&old), false).await.unwrap();
    lease.acquire().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    p.stop().await;
}

#[tokio::test]
async fn cleanup_and_disconnect_errors_do_not_skip_destroy() {
    let c = Arc::new(Control::default());
    c.cleanup_failures.store(1, Ordering::SeqCst);
    c.disconnect_failures.store(1, Ordering::SeqCst);
    let s = supervisor(&c);
    s.acquire().await.unwrap();
    s.stop().await;
    assert!(c.log.lock().unwrap().contains(&(0, "destroy")));
}
#[tokio::test]
async fn cancelled_recovery_waiter_does_not_cancel_teardown() {
    let c = Arc::new(Control::default());
    let s = supervisor(&c);
    let old = s.acquire().await.unwrap();
    c.block_cleanup.store(true, Ordering::SeqCst);
    let other = s.clone();
    let old_copy = old.clone();
    let recovery = tokio::spawn(async move { other.recover(&old_copy, false).await });
    until(|| c.log.lock().unwrap().contains(&(0, "cleanup"))).await;
    recovery.abort();
    let _ = recovery.await;
    c.block_cleanup.store(false, Ordering::SeqCst);
    c.cleanup_gate.notify_waiters();
    until(|| {
        s.current()
            .is_some_and(|r| r.generation() > old.generation())
    })
    .await;
    assert!(old.resource().closed.load(Ordering::SeqCst));
    s.stop().await;
}
#[tokio::test]
async fn lazy_pool_can_stop_without_spawning_resources_or_tasks() {
    let c = Arc::new(Control::default());
    let control = c.clone();
    let p = Pool::new(
        move || Supervisor::new(Manager(control.clone()), config()),
        PoolConfig::default(),
    );
    p.stop().await;
    assert_eq!(c.created.load(Ordering::SeqCst), 0);
    assert!(matches!(p.borrow().await, Err(Error::Stopped)));
}

#[tokio::test]
async fn live_indicator_changes_immediately_without_supervisor_recovery() {
    let c = Arc::new(Control::default());
    c.disable_watchdog.store(true, Ordering::SeqCst);
    let p = pool(&c, 1, 1, false);
    let proxy = etherbird::ManagedResourceProxy::new(p.clone());
    p.connected().await.unwrap();
    let old = p.resources().pop().unwrap();
    old.resource().connected.store(false, Ordering::SeqCst);
    assert!(!proxy.is_connected());
    assert_eq!(p.state(), ResourceState::Connected);
    let other = proxy.clone();
    let waiter = tokio::spawn(async move { other.connected().await });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    old.resource().connected.store(true, Ordering::SeqCst);
    old.resource().connection_changed.notify_waiters();
    timeout(Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(proxy.is_connected());
    p.stop().await;
}

#[tokio::test]
async fn live_connected_wait_follows_replacement_and_observes_any_slot() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 2, 2, false);
    until(|| p.resources().len() == 2).await;
    let resources = p.resources();
    resources[0]
        .resource()
        .connected
        .store(false, Ordering::SeqCst);
    assert!(p.is_connected());
    resources[1]
        .resource()
        .connected
        .store(false, Ordering::SeqCst);
    assert!(!p.is_connected());
    let other = p.clone();
    let waiter = tokio::spawn(async move { other.connected().await });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    p.recover(Some(&resources[0]), false).await.unwrap();
    timeout(Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    p.stop().await;
}

#[tokio::test]
async fn awaited_and_concurrent_recovery_wait_for_teardown() {
    let c = Arc::new(Control::default());
    let s = supervisor(&c);
    let old = s.acquire().await.unwrap();
    c.block_disconnect.store(true, Ordering::SeqCst);
    let other = s.clone();
    let handle = old.clone();
    let first = tokio::spawn(async move { other.recover(&handle, false).await });
    until(|| c.log.lock().unwrap().contains(&(0, "disconnect"))).await;
    let other = s.clone();
    let handle = old.clone();
    let second = tokio::spawn(async move { other.recover(&handle, false).await });
    tokio::task::yield_now().await;
    assert!(!first.is_finished());
    assert!(!second.is_finished());
    c.block_disconnect.store(false, Ordering::SeqCst);
    c.disconnect_gate.notify_waiters();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    assert!(old.resource().closed.load(Ordering::SeqCst));
    assert!(c.log.lock().unwrap().contains(&(0, "destroy")));
    s.stop().await;
}

#[tokio::test]
async fn stored_attributes_are_readable_before_creation_and_replayed_after_recovery() {
    let c = Arc::new(Control::default());
    let control = c.clone();
    let p = Pool::new(
        move || Supervisor::new(Manager(control.clone()), config()),
        PoolConfig::default(),
    );
    let proxy = etherbird::ManagedResourceProxy::new(p.clone());
    proxy.set_value("setting", 17usize, |r, value| {
        r.setting.store(*value, Ordering::SeqCst)
    });
    assert_eq!(proxy.value::<usize>("setting"), Some(17));
    assert_eq!(proxy.value::<String>("setting"), None);
    assert_eq!(c.created.load(Ordering::SeqCst), 0);
    assert_eq!(p.size(), 0);
    let lease = p.borrow().await.unwrap();
    let old = lease.acquire().await.unwrap();
    assert_eq!(old.resource().setting.load(Ordering::SeqCst), 17);
    p.recover(Some(&old), false).await.unwrap();
    let new = lease.acquire().await.unwrap();
    assert_eq!(new.resource().setting.load(Ordering::SeqCst), 17);
    assert_eq!(proxy.value::<usize>("setting"), Some(17));
    p.stop().await;
}

#[tokio::test]
async fn callback_chain_and_attributes_run_before_connect() {
    let c = Arc::new(Control::default());
    let control = c.clone();
    let callbacks = Arc::new(Mutex::new(Vec::new()));
    let original = callbacks.clone();
    let p = Pool::new(
        move || {
            let supervisor = Supervisor::new(Manager(control.clone()), config());
            let callbacks = original.clone();
            supervisor.set_on_resource_created(Arc::new(move |resource| {
                let callbacks = callbacks.clone();
                Box::pin(async move {
                    resource.setting.store(0, Ordering::SeqCst);
                    callbacks.lock().unwrap().push((resource.id, "original"));
                    Ok(())
                })
            }));
            supervisor
        },
        PoolConfig::default(),
    );
    let callbacks_copy = callbacks.clone();
    let assignments = Arc::new(AtomicUsize::new(0));
    let applied = assignments.clone();
    p.set_value("setting", 23usize, move |r, value| {
        applied.fetch_add(1, Ordering::SeqCst);
        r.setting.store(*value, Ordering::SeqCst)
    });
    p.set_on_resource_created(Arc::new(move |resource| {
        let callbacks = callbacks_copy.clone();
        Box::pin(async move {
            assert!(!resource.connected.load(Ordering::SeqCst));
            assert_eq!(resource.setting.load(Ordering::SeqCst), 23);
            callbacks.lock().unwrap().push((resource.id, "pool"));
            Ok(())
        })
    }));
    let lease = p.borrow().await.unwrap();
    let old = lease.acquire().await.unwrap();
    p.recover(Some(&old), false).await.unwrap();
    lease.acquire().await.unwrap();
    assert_eq!(assignments.load(Ordering::SeqCst), 2);
    assert_eq!(
        *callbacks.lock().unwrap(),
        [(0, "original"), (0, "pool"), (1, "original"), (1, "pool")]
    );
    p.stop().await;
}
#[tokio::test]
async fn lease_keeps_pool_alive_after_public_pool_handles_drop() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 1, false);
    let lease = p.borrow().await.unwrap();
    let old = lease.acquire().await.unwrap();
    drop(p);
    assert_eq!(
        lease
            .execute(|r| async move { r.ping(1).await })
            .await
            .unwrap(),
        1
    );
    assert!(!old.resource().closed.load(Ordering::SeqCst));
    lease.release();
    until(|| c.log.lock().unwrap().contains(&(0, "destroy"))).await;
}
#[test]
fn invalid_pool_limits_are_rejected() {
    for options in [
        PoolConfig {
            max_size: 0,
            ..PoolConfig::default()
        },
        PoolConfig {
            min_size: 2,
            max_size: 1,
            ..PoolConfig::default()
        },
        PoolConfig {
            idle_timeout: Duration::ZERO,
            ..PoolConfig::default()
        },
    ] {
        assert!(
            std::panic::catch_unwind(|| Pool::new(
                || Supervisor::new(Manager(Arc::new(Control::default())), config()),
                options
            ))
            .is_err()
        );
    }
}

#[tokio::test]
async fn lazy_start_explicit_start_and_implicit_supervisor_restart() {
    let c = Arc::new(Control::default());
    let control = c.clone();
    let p = Pool::new(
        move || Supervisor::new(Manager(control.clone()), config()),
        PoolConfig {
            min_size: 2,
            max_size: 2,
            ..PoolConfig::default()
        },
    );
    assert_eq!(p.size(), 0);
    assert_eq!(c.created.load(Ordering::SeqCst), 0);
    p.begin();
    p.begin();
    until(|| p.resources().len() == 2).await;
    assert_eq!(c.created.load(Ordering::SeqCst), 2);
    p.stop().await;
    p.begin();
    assert!(matches!(p.borrow().await, Err(Error::Stopped)));
    let s = Supervisor::new(Manager(c.clone()), config());
    s.set_value("setting", 37usize, |r, value| {
        r.setting.store(*value, Ordering::SeqCst)
    });
    let old = s.acquire().await.unwrap();
    s.stop().await;
    let new = s.acquire().await.unwrap();
    assert!(new.generation() > old.generation());
    assert_eq!(new.resource().setting.load(Ordering::SeqCst), 37);
    assert_eq!(s.value::<usize>("setting"), Some(37));
    s.stop().await;
}

#[tokio::test]
async fn watchdog_is_optional_and_terminal_watch_failure_skips_disconnect() {
    let c = Arc::new(Control::default());
    c.disable_watchdog.store(true, Ordering::SeqCst);
    let s = supervisor(&c);
    s.acquire().await.unwrap();
    tokio::task::yield_now().await;
    assert_eq!(c.active_watches.load(Ordering::SeqCst), 0);
    s.stop().await;
    c.disable_watchdog.store(false, Ordering::SeqCst);
    c.terminal.store(true, Ordering::SeqCst);
    let s = supervisor(&c);
    let old = s.acquire().await.unwrap();
    old.resource().lost.notify_one();
    until(|| {
        s.current()
            .is_some_and(|r| r.generation() > old.generation())
    })
    .await;
    let log = c.log.lock().unwrap().clone();
    let id = old.resource().id;
    assert!(log.contains(&(id, "destroy")));
    assert!(!log.contains(&(id, "disconnect")));
    s.stop().await;
    assert_eq!(c.active_watches.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn shutdown_wakes_live_connection_waiters_and_drains_watchdogs() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 1, false);
    p.connected().await.unwrap();
    until(|| c.active_watches.load(Ordering::SeqCst) == 1).await;
    p.resources()[0]
        .resource()
        .connected
        .store(false, Ordering::SeqCst);
    let other = p.clone();
    let waiter = tokio::spawn(async move { other.connected().await });
    tokio::task::yield_now().await;
    p.stop().await;
    assert!(matches!(waiter.await.unwrap(), Err(Error::Stopped)));
    assert_eq!(c.active_watches.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn recovery_only_closes_matching_slot() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 2, 2, false);
    let a = p.borrow().await.unwrap();
    let b = p.borrow().await.unwrap();
    let old = a.acquire().await.unwrap();
    let other = b.acquire().await.unwrap();
    p.recover(Some(&old), false).await.unwrap();
    assert!(!other.resource().closed.load(Ordering::SeqCst));
    assert_eq!(b.acquire().await.unwrap().generation(), other.generation());
    assert_eq!(p.size(), 2);
    assert!(!other.resource().closed.load(Ordering::SeqCst));
    p.stop().await;
}

#[tokio::test]
async fn retiring_resources_count_toward_maximum_capacity() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 0, 1, false);
    let lease = p.borrow().await.unwrap();
    c.block_disconnect.store(true, Ordering::SeqCst);
    drop(lease);
    until(|| c.log.lock().unwrap().contains(&(0, "disconnect"))).await;
    let other = p.clone();
    let waiting = tokio::spawn(async move { other.borrow().await });
    sleep(Duration::from_millis(5)).await;
    assert_eq!(c.created.load(Ordering::SeqCst), 1);
    assert!(!waiting.is_finished());
    c.block_disconnect.store(false, Ordering::SeqCst);
    c.disconnect_gate.notify_waiters();
    let lease = timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(c.created.load(Ordering::SeqCst), 2);
    drop(lease);
    p.stop().await;
}

#[tokio::test(start_paused = true)]
async fn retry_backoff_doubles_and_caps() {
    let c = Arc::new(Control::default());
    c.failures.store(4, Ordering::SeqCst);
    let s = supervisor(&c);
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert_eq!(c.created.load(Ordering::SeqCst), 1);
    for (delay, expected) in [(5, 2), (10, 3), (20, 4), (20, 5)] {
        tokio::time::advance(Duration::from_millis(delay)).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(c.created.load(Ordering::SeqCst), expected);
    }
    s.acquire().await.unwrap();
    s.stop().await;
}
etherbird::managed_client! { pub struct TypedClient for Manager { async fn ping(value: usize) -> usize; fn identifier() -> usize; } }
fn config() -> Config {
    Config {
        retry_delay: Duration::from_millis(5),
        max_retry_delay: Duration::from_millis(20),
        create_timeout: Some(Duration::from_millis(30)),
        setup_timeout: Duration::from_millis(30),
        connect_timeout: Duration::from_millis(30),
        cleanup_timeout: Duration::from_millis(30),
        disconnect_timeout: Duration::from_millis(30),
        ..Config::default()
    }
}
fn supervisor(control: &Arc<Control>) -> Supervisor<Manager> {
    Supervisor::start(Manager(control.clone()), config())
}
fn pool(control: &Arc<Control>, min: usize, max: usize, priority: bool) -> Pool<Manager> {
    let control = control.clone();
    Pool::start(
        move || Supervisor::new(Manager(control.clone()), config()),
        PoolConfig {
            min_size: min,
            max_size: max,
            idle_timeout: Duration::from_millis(50),
            priority_queue: priority,
        },
    )
}
async fn until(mut predicate: impl FnMut() -> bool) {
    timeout(Duration::from_secs(2), async {
        while !predicate() {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn lifecycle_order_and_idempotent_shutdown() {
    let c = Arc::new(Control::default());
    let s = supervisor(&c);
    let handle = s.acquire().await.unwrap();
    tokio::join!(s.stop(), s.stop());
    assert!(handle.resource().closed.load(Ordering::SeqCst));
    assert_eq!(s.state(), ResourceState::Stopped);
    assert!(*s.stopped().borrow());
    assert_eq!(
        c.log
            .lock()
            .unwrap()
            .iter()
            .map(|(_, hook)| *hook)
            .collect::<Vec<_>>(),
        [
            "created",
            "connect",
            "setup",
            "setup_completed",
            "cleanup",
            "cleanup_completed",
            "disconnect",
            "destroy"
        ]
    );
}
#[tokio::test]
async fn connection_and_setup_failures_retry_with_teardown() {
    let c = Arc::new(Control::default());
    c.failures.store(1, Ordering::SeqCst);
    c.setup_failures.store(1, Ordering::SeqCst);
    let s = supervisor(&c);
    let resource = s.acquire().await.unwrap();
    assert_eq!(resource.resource().id, 2);
    for id in [0, 1] {
        assert!(c.log.lock().unwrap().contains(&(id, "destroy")));
    }
    s.stop().await;
}
#[tokio::test]
async fn watchdog_recovers_without_an_operation_and_ignores_stale_handles() {
    let c = Arc::new(Control::default());
    let s = supervisor(&c);
    let old = s.acquire().await.unwrap();
    old.resource().lost.notify_one();
    until(|| {
        s.current()
            .is_some_and(|r| r.generation() > old.generation())
    })
    .await;
    let current = s.acquire().await.unwrap();
    s.recover(&old, false).await.unwrap();
    sleep(Duration::from_millis(10)).await;
    assert_eq!(
        s.acquire().await.unwrap().generation(),
        current.generation()
    );
    s.stop().await;
}
#[tokio::test]
async fn foreign_handles_cannot_recover_another_supervisor() {
    let c = Arc::new(Control::default());
    let a = supervisor(&c);
    let b = supervisor(&c);
    let a_handle = a.acquire().await.unwrap();
    let b_handle = b.acquire().await.unwrap();
    b.recover(&a_handle, false).await.unwrap();
    sleep(Duration::from_millis(10)).await;
    assert_eq!(
        b.acquire().await.unwrap().generation(),
        b_handle.generation()
    );
    tokio::join!(a.stop(), b.stop());
}
#[tokio::test]
async fn failed_operation_is_not_replayed() {
    let c = Arc::new(Control::default());
    let s = supervisor(&c);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let old = s.acquire().await.unwrap();
    let result: Result<(), _> = s
        .execute(move |_| async move {
            count.fetch_add(1, Ordering::SeqCst);
            Err(Failure)
        })
        .await;
    assert!(matches!(result, Err(Error::Operation(_))));
    until(|| {
        s.current()
            .is_some_and(|r| r.generation() > old.generation())
    })
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    s.stop().await;
}
#[tokio::test]
async fn terminal_recovery_skips_disconnect() {
    let c = Arc::new(Control::default());
    let s = supervisor(&c);
    let old = s.acquire().await.unwrap();
    s.recover(&old, true).await.unwrap();
    until(|| {
        s.current()
            .is_some_and(|r| r.generation() > old.generation())
    })
    .await;
    let log = c.log.lock().unwrap().clone();
    assert!(log.contains(&(0, "cleanup")));
    assert!(log.contains(&(0, "destroy")));
    assert!(!log.contains(&(0, "disconnect")));
    s.stop().await;
}
#[tokio::test]
async fn stopping_wakes_acquisition_and_cancels_operations() {
    let c = Arc::new(Control::default());
    let s = supervisor(&c);
    s.acquire().await.unwrap();
    let other = s.clone();
    let started = Arc::new(Notify::new());
    let signal = started.clone();
    let task = tokio::spawn(async move {
        other
            .execute(move |_| async move {
                signal.notify_one();
                std::future::pending::<Result<(), Failure>>().await
            })
            .await
    });
    started.notified().await;
    s.stop().await;
    assert!(matches!(task.await.unwrap(), Err(Error::Stopped)));
    c.block_connect.store(true, Ordering::SeqCst);
    let waiting = supervisor(&c);
    let other = waiting.clone();
    let started = Arc::new(Notify::new());
    let signal = started.clone();
    let task = tokio::spawn(async move {
        signal.notify_one();
        other.acquire().await
    });
    started.notified().await;
    waiting.stop().await;
    assert!(matches!(task.await.unwrap(), Err(Error::Stopped)));
}
#[tokio::test]
async fn last_handle_drop_requests_cleanup() {
    let c = Arc::new(Control::default());
    let s = supervisor(&c);
    s.acquire().await.unwrap();
    drop(s);
    until(|| c.log.lock().unwrap().contains(&(0, "destroy"))).await;
}
#[tokio::test]
async fn cleanup_timeout_does_not_block_disconnect() {
    let c = Arc::new(Control::default());
    let s = supervisor(&c);
    s.acquire().await.unwrap();
    c.block_cleanup.store(true, Ordering::SeqCst);
    timeout(Duration::from_millis(200), s.stop()).await.unwrap();
    assert!(c.log.lock().unwrap().contains(&(0, "disconnect")));
    c.cleanup_gate.notify_waiters();
}
#[tokio::test]
async fn disconnect_timeout_defers_destroy_until_late_completion() {
    let c = Arc::new(Control::default());
    let s = supervisor(&c);
    s.acquire().await.unwrap();
    c.block_disconnect.store(true, Ordering::SeqCst);
    s.stop().await;
    assert!(!c.log.lock().unwrap().contains(&(0, "destroy")));
    c.disconnect_gate.notify_waiters();
    until(|| c.log.lock().unwrap().contains(&(0, "destroy"))).await;
}
#[tokio::test]
async fn late_connect_success_is_closed_without_becoming_current() {
    let c = Arc::new(Control::default());
    c.block_connect.store(true, Ordering::SeqCst);
    let s = supervisor(&c);
    until(|| c.created.load(Ordering::SeqCst) >= 2).await;
    assert!(s.current().is_none());
    c.block_connect.store(false, Ordering::SeqCst);
    c.connect_gate.notify_waiters();
    let current = timeout(Duration::from_secs(1), s.acquire())
        .await
        .unwrap()
        .unwrap();
    assert!(current.resource().id >= 1);
    until(|| {
        c.log
            .lock()
            .unwrap()
            .iter()
            .filter(|&&(id, hook)| id == 0 && hook == "disconnect")
            .count()
            >= 2
    })
    .await;
    s.stop().await;
}
#[tokio::test]
async fn pool_grows_retires_and_releases_cancelled_borrows() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 2, false);
    let a = p.borrow().await.unwrap();
    let b = p.borrow().await.unwrap();
    assert_eq!(p.size(), 2);
    assert_ne!(
        a.acquire().await.unwrap().resource().id,
        b.acquire().await.unwrap().resource().id
    );
    let other = p.clone();
    let waiting = tokio::spawn(async move { other.borrow().await });
    tokio::task::yield_now().await;
    waiting.abort();
    let _ = waiting.await;
    drop(a);
    drop(b);
    until(|| p.size() == 1).await;
    let lease = p.borrow().await.unwrap();
    let resource = lease.acquire().await.unwrap();
    p.stop().await;
    assert!(resource.resource().closed.load(Ordering::SeqCst));
    assert!(matches!(lease.acquire().await, Err(Error::Stopped)));
}
#[tokio::test]
async fn lease_survives_reconnection() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 1, false);
    let lease = p.borrow().await.unwrap();
    let old = lease.acquire().await.unwrap();
    lease.supervisor().recover(&old, false).await.unwrap();
    until(|| {
        lease
            .supervisor()
            .current()
            .is_some_and(|r| r.generation() > old.generation())
    })
    .await;
    assert!(lease.acquire().await.unwrap().generation() > old.generation());
    p.stop().await;
}
#[tokio::test]
async fn priority_reconsidered_after_waiting_for_capacity() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 1, true);
    let lease = p.borrow().await.unwrap();
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut tasks = Vec::new();
    for priority in [10, -1, 0, -1] {
        let other = p.clone();
        let order = order.clone();
        let sequence = tasks.len();
        tasks.push(tokio::spawn(async move {
            other
                .execute_with_priority(priority, move |_| async move {
                    order.lock().unwrap().push(sequence);
                    Ok(())
                })
                .await
        }));
        sleep(Duration::from_millis(2)).await;
    }
    drop(lease);
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    assert_eq!(*order.lock().unwrap(), [1, 3, 2, 0]);
    p.stop().await;
}
#[tokio::test]
async fn fifo_and_queued_cancellation() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 1, false);
    let lease = p.borrow().await.unwrap();
    let other = p.clone();
    let cancelled = tokio::spawn(async move {
        other
            .execute(|_| async {
                panic!("cancelled job executed");
                #[allow(unreachable_code)]
                Ok(())
            })
            .await
    });
    sleep(Duration::from_millis(5)).await;
    cancelled.abort();
    let _ = cancelled.await;
    drop(lease);
    assert_eq!(
        p.execute(|r| async move { r.ping(10).await })
            .await
            .unwrap(),
        10
    );
    p.stop().await;
}
#[tokio::test]
async fn pool_shutdown_cancels_running_queued_and_waiting_callers() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 1, false);
    p.connected().await.unwrap();
    let other = p.clone();
    let started = Arc::new(Notify::new());
    let signal = started.clone();
    let running = tokio::spawn(async move {
        other
            .execute(move |_| async move {
                signal.notify_one();
                std::future::pending::<Result<(), Failure>>().await
            })
            .await
    });
    started.notified().await;
    let other = p.clone();
    let queued = tokio::spawn(async move { other.execute(|_| async { Ok(()) }).await });
    let other = p.clone();
    let waiter = tokio::spawn(async move { other.borrow().await });
    tokio::join!(p.stop(), p.stop());
    assert!(matches!(running.await.unwrap(), Err(Error::Stopped)));
    assert!(matches!(queued.await.unwrap(), Err(Error::Stopped)));
    assert!(matches!(waiter.await.unwrap(), Err(Error::Stopped)));
}
#[tokio::test]
async fn proxy_replays_attributes_and_forwards_typed_methods() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 0, 1, false);
    let client = TypedClient::new(p.clone());
    client.managed.set_attribute(
        "setting",
        Arc::new(|r| r.setting.store(42, Ordering::SeqCst)),
    );
    assert_eq!(client.ping(7).await.unwrap(), 7);
    assert_eq!(client.identifier().await.unwrap(), 0);
    let old = p.resources().pop().unwrap();
    p.recover(Some(&old), false).await.unwrap();
    until(|| {
        p.resources()
            .first()
            .is_some_and(|r| r.generation() > old.generation())
    })
    .await;
    assert_eq!(
        client
            .managed
            .attribute(|r| r.setting.load(Ordering::SeqCst)),
        Some(42)
    );
    assert_eq!(client.ping(7).await.unwrap(), 8);
    client.managed.stop().await;
}
#[tokio::test]
async fn initialization_hook_failures_preserve_terminal_classification() {
    for terminal in [false, true] {
        for hook in ["created", "callback", "setup"] {
            let c = Arc::new(Control::default());
            c.terminal.store(terminal, Ordering::SeqCst);
            match hook {
                "created" => c.created_failures.store(1, Ordering::SeqCst),
                "setup" => c.setup_failures.store(1, Ordering::SeqCst),
                _ => {}
            }
            let s = supervisor(&c);
            if hook == "callback" {
                s.set_on_resource_created(Arc::new(|resource| {
                    Box::pin(async move {
                        if resource.id == 0 {
                            Err(Failure)
                        } else {
                            Ok(())
                        }
                    })
                }));
            }
            let handle = s.acquire().await.unwrap();
            assert_eq!(handle.resource().id, 1);
            let events: Vec<_> = c
                .log
                .lock()
                .unwrap()
                .iter()
                .filter(|(id, _)| *id == 0)
                .map(|(_, event)| *event)
                .collect();
            assert!(events.contains(&"cleanup"), "{hook}, terminal={terminal}");
            assert!(events.contains(&"destroy"), "{hook}, terminal={terminal}");
            assert_eq!(
                events.contains(&"disconnect"),
                !terminal,
                "{hook}, terminal={terminal}"
            );
            assert_eq!(events.contains(&"connect"), hook == "setup");
            s.stop().await;
        }
    }
}

#[tokio::test]
async fn terminal_connect_failure_cleans_up_without_disconnect() {
    let c = Arc::new(Control::default());
    c.failures.store(1, Ordering::SeqCst);
    c.terminal.store(true, Ordering::SeqCst);
    let s = supervisor(&c);
    s.acquire().await.unwrap();
    let events: Vec<_> = c
        .log
        .lock()
        .unwrap()
        .iter()
        .filter(|(id, _)| *id == 0)
        .map(|(_, event)| *event)
        .collect();
    assert!(events.contains(&"connect"));
    assert!(events.contains(&"cleanup"));
    assert!(events.contains(&"destroy"));
    assert!(!events.contains(&"disconnect"));
    s.stop().await;
}

#[tokio::test]
async fn pool_setters_apply_once_to_each_resource_and_cached_resource() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 2, 2, false);
    until(|| p.resources().len() == 2).await;
    p.set_attribute(
        "increment",
        Arc::new(|r| {
            r.setting.fetch_add(1, Ordering::SeqCst);
        }),
    );
    p.set_value("increment_value", 2usize, |r, value| {
        r.setting.fetch_add(*value, Ordering::SeqCst);
    });
    assert_eq!(p.value::<usize>("increment_value"), Some(2));
    for resource in p.resources() {
        assert_eq!(resource.resource().setting.load(Ordering::SeqCst), 3);
    }
    p.stop().await;
    p.set_attribute(
        "cached_increment",
        Arc::new(|r| {
            r.setting.fetch_add(1, Ordering::SeqCst);
        }),
    );
    p.set_value("cached_value", 2usize, |r, value| {
        r.setting.fetch_add(*value, Ordering::SeqCst);
    });
    assert_eq!(p.with_latest(|r| r.setting.load(Ordering::SeqCst)), Some(6));
}
#[tokio::test]
async fn cancelling_shutdown_waiter_does_not_cancel_cleanup() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 1, false);
    p.connected().await.unwrap();
    c.block_cleanup.store(true, Ordering::SeqCst);
    let other = p.clone();
    let stopping = tokio::spawn(async move { other.stop().await });
    until(|| c.log.lock().unwrap().contains(&(0, "cleanup"))).await;
    stopping.abort();
    let _ = stopping.await;
    timeout(Duration::from_secs(1), p.stop()).await.unwrap();
    c.cleanup_gate.notify_waiters();
    assert_eq!(p.state(), ResourceState::Stopped);
}
#[tokio::test]
async fn supervisor_restarts_with_monotonic_generations_and_replayed_settings() {
    let c = Arc::new(Control::default());
    let s = supervisor(&c);
    let clone = s.clone();
    s.set_attribute(
        "setting",
        Arc::new(|r| r.setting.store(9, Ordering::SeqCst)),
    );
    let old = s.acquire().await.unwrap();
    assert!(!s.restart().await);
    s.stop().await;
    assert!(s.restart().await);
    let new = clone.acquire().await.unwrap();
    assert!(new.generation() > old.generation());
    assert_eq!(new.resource().setting.load(Ordering::SeqCst), 9);
    s.recover(&old, false).await.unwrap();
    assert_eq!(
        clone.acquire().await.unwrap().generation(),
        new.generation()
    );
    s.stop().await;
}
#[tokio::test]
async fn timed_out_creation_closes_late_resource() {
    let c = Arc::new(Control::default());
    c.block_create.store(true, Ordering::SeqCst);
    let s = supervisor(&c);
    until(|| c.created.load(Ordering::SeqCst) >= 2).await;
    c.block_create.store(false, Ordering::SeqCst);
    c.create_gate.notify_waiters();
    s.acquire().await.unwrap();
    until(|| c.log.lock().unwrap().contains(&(0, "destroy"))).await;
    s.stop().await;
}
#[tokio::test]
async fn late_setup_does_not_publish_discarded_resource() {
    let c = Arc::new(Control::default());
    c.block_setup.store(true, Ordering::SeqCst);
    let s = supervisor(&c);
    until(|| c.created.load(Ordering::SeqCst) >= 2).await;
    assert!(s.current().is_none());
    c.block_setup.store(false, Ordering::SeqCst);
    c.setup_gate.notify_waiters();
    assert!(s.acquire().await.unwrap().resource().id >= 1);
    s.stop().await;
}
#[tokio::test]
async fn cancelling_running_operation_returns_capacity() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 1, false);
    let started = Arc::new(Notify::new());
    let signal = started.clone();
    let other = p.clone();
    let task = tokio::spawn(async move {
        other
            .execute(move |_| async move {
                signal.notify_one();
                std::future::pending::<Result<(), Failure>>().await
            })
            .await
    });
    started.notified().await;
    task.abort();
    let _ = task.await;
    timeout(Duration::from_secs(1), p.execute(|_| async { Ok(()) }))
        .await
        .unwrap()
        .unwrap();
    p.stop().await;
}
#[tokio::test]
async fn minimum_is_eager_and_zero_minimum_retires_all_entries() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 2, 3, false);
    until(|| c.created.load(Ordering::SeqCst) == 2).await;
    assert_eq!(p.size(), 2);
    p.stop().await;
    let p = pool(&c, 0, 1, false);
    assert_eq!(p.size(), 0);
    let lease = p.borrow().await.unwrap();
    drop(lease);
    until(|| p.size() == 0).await;
    assert!(p.with_latest(|r| r.id).is_some());
    p.execute(|_| async { Ok(()) }).await.unwrap();
    p.stop().await;
}
#[tokio::test]
async fn recovery_immediately_withdraws_failed_resource() {
    let c = Arc::new(Control::default());
    let s = supervisor(&c);
    let old = s.acquire().await.unwrap();
    s.request_recovery(&old, false);
    assert!(s.current().is_none());
    assert_eq!(s.state(), ResourceState::Recovering);
    let new = s.acquire().await.unwrap();
    assert!(new.generation() > old.generation());
    s.stop().await;
}
#[tokio::test]
async fn pending_demand_grows_pool_while_entries_connect() {
    let c = Arc::new(Control::default());
    c.block_connect.store(true, Ordering::SeqCst);
    let p = pool(&c, 1, 3, false);
    let mut tasks = Vec::new();
    for _ in 0..3 {
        let other = p.clone();
        tasks.push(tokio::spawn(async move { other.borrow().await }));
    }
    until(|| p.size() == 3).await;
    c.block_connect.store(false, Ordering::SeqCst);
    c.connect_gate.notify_waiters();
    let mut leases = Vec::new();
    for task in tasks {
        leases.push(task.await.unwrap().unwrap());
    }
    assert_eq!(p.size(), 3);
    p.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_calls_preserve_capacity_and_complete_shutdown() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 4, false);
    let mut tasks = Vec::new();
    for _ in 0..64 {
        let other = p.clone();
        tasks.push(tokio::spawn(async move {
            other
                .execute(|resource| async move {
                    tokio::task::yield_now().await;
                    resource.ping(1).await
                })
                .await
        }));
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    assert!(p.size() <= 4);
    p.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operation_panics_preserve_dispatcher_siblings_and_capacity() {
    for panic_when_constructed in [true, false] {
        let c = Arc::new(Control::default());
        let p = pool(&c, 2, 2, false);
        let entered = Arc::new(AtomicBool::new(false));
        let release = Arc::new(Notify::new());
        let other = p.clone();
        let entered_job = entered.clone();
        let release_job = release.clone();
        let sibling = tokio::spawn(async move {
            other
                .execute(move |resource| async move {
                    entered_job.store(true, Ordering::SeqCst);
                    release_job.notified().await;
                    Ok(resource.id)
                })
                .await
        });
        until(|| entered.load(Ordering::SeqCst)).await;
        let failed = p
            .execute(move |_| {
                assert!(!panic_when_constructed, "operation constructor panic");
                async {
                    tokio::task::yield_now().await;
                    panic!("operation poll panic");
                    #[allow(unreachable_code)]
                    Ok(())
                }
            })
            .await;
        assert!(matches!(failed, Err(Error::Stopped)));
        assert!(!sibling.is_finished());
        // A slow sibling keeps its own lease while the panicked slot is reused.
        let reused = timeout(
            Duration::from_secs(2),
            p.execute(|resource| async move { Ok(resource.id) }),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(c.created.load(Ordering::SeqCst), 2);
        release.notify_one();
        assert_ne!(sibling.await.unwrap().unwrap(), reused);
        p.stop().await;
    }
}

#[tokio::test]
async fn shutdown_survives_running_operation_destructor_panic() {
    struct PanicsOnDrop;
    impl Drop for PanicsOnDrop {
        fn drop(&mut self) {
            panic!("operation destructor panic");
        }
    }
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 1, false);
    let entered = Arc::new(AtomicBool::new(false));
    let entered_job = entered.clone();
    let other = p.clone();
    let job = tokio::spawn(async move {
        other
            .execute(move |_| async move {
                let _guard = PanicsOnDrop;
                entered_job.store(true, Ordering::SeqCst);
                std::future::pending::<Result<(), Failure>>().await
            })
            .await
    });
    until(|| entered.load(Ordering::SeqCst)).await;
    timeout(Duration::from_secs(2), p.stop()).await.unwrap();
    assert!(matches!(job.await.unwrap(), Err(Error::Stopped)));
    assert!(c.log.lock().unwrap().contains(&(0, "destroy")));
}

#[tokio::test]
async fn pool_shutdown_cancels_work_even_when_its_caller_is_not_polled() {
    use std::{future::Future, task::Poll};
    struct Cancelled(Arc<AtomicBool>);
    impl Drop for Cancelled {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 1, false);
    let entered = Arc::new(AtomicBool::new(false));
    let cancelled = Arc::new(AtomicBool::new(false));
    let entered_job = entered.clone();
    let cancelled_job = cancelled.clone();
    let call = p.execute(move |_| async move {
        let _guard = Cancelled(cancelled_job);
        entered_job.store(true, Ordering::SeqCst);
        std::future::pending::<Result<(), Failure>>().await
    });
    tokio::pin!(call);
    // Queue the call, then leave its future unpolled throughout shutdown.
    std::future::poll_fn(|cx| {
        assert!(call.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    until(|| entered.load(Ordering::SeqCst)).await;
    timeout(Duration::from_secs(2), p.stop()).await.unwrap();
    assert!(cancelled.load(Ordering::SeqCst));
    assert!(c.log.lock().unwrap().contains(&(0, "destroy")));
    assert!(matches!(call.await, Err(Error::Stopped)));
}

#[tokio::test]
async fn queued_work_wakes_dispatcher_without_broadcasting_capacity_changes() {
    let c = Arc::new(Control::default());
    let control = c.clone();
    let priorities = Arc::new(Mutex::new(Vec::new()));
    let removed = Arc::new(AtomicUsize::new(0));
    let p = Pool::start_with_queue_factory(
        move || Supervisor::new(Manager(control.clone()), config()),
        PoolConfig::default(),
        || CustomQueue {
            items: Vec::new(),
            priorities: priorities.clone(),
            removed: removed.clone(),
        },
    );
    let lease = p.borrow().await.unwrap();
    // Let the connection relay settle before observing queue-only changes.
    tokio::task::yield_now().await;
    let mut changes = p.subscribe();
    changes.borrow_and_update();
    let other = p.clone();
    let job = tokio::spawn(async move { other.execute(|_| async { Ok(()) }).await });
    until(|| priorities.lock().unwrap().len() == 1).await;
    assert!(!changes.has_changed().unwrap());
    job.abort();
    let _ = job.await;
    until(|| removed.load(Ordering::SeqCst) == 1).await;
    assert!(!changes.has_changed().unwrap());
    drop(lease);
    timeout(Duration::from_secs(2), changes.changed())
        .await
        .unwrap()
        .unwrap();
    p.execute(|_| async { Ok(()) }).await.unwrap();
    p.stop().await;
}

// Scenario tests use gates to hold the relevant state until asserted.
#[tokio::test]
async fn queued_work_grows_pool_when_first_connection_is_slow() {
    let c = Arc::new(Control::default());
    c.block_first_connect.store(true, Ordering::SeqCst);
    let control = c.clone();
    let p = Pool::start(
        move || {
            Supervisor::new(
                Manager(control.clone()),
                Config {
                    connect_timeout: Duration::from_secs(10),
                    ..config()
                },
            )
        },
        PoolConfig {
            max_size: 2,
            ..PoolConfig::default()
        },
    );
    until(|| c.log.lock().unwrap().contains(&(0, "connect"))).await;
    let mut jobs = Vec::new();
    for _ in 0..3 {
        let other = p.clone();
        jobs.push(tokio::spawn(async move {
            other
                .execute(|r| async move {
                    assert_eq!(r.id, 1);
                    Ok(42)
                })
                .await
        }));
    }
    for job in jobs {
        assert_eq!(
            timeout(Duration::from_secs(1), job)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            42
        );
    }
    assert_eq!(c.created.load(Ordering::SeqCst), 2);
    assert!(!c.log.lock().unwrap().contains(&(0, "setup")));
    p.stop().await;
    assert!(c.log.lock().unwrap().contains(&(0, "destroy")));
}

#[tokio::test(start_paused = true)]
async fn busy_lease_survives_idle_timeout_then_retires() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 0, 1, false);
    let lease = p.borrow().await.unwrap();
    let r = lease.acquire().await.unwrap();
    tokio::time::advance(Duration::from_millis(200)).await;
    tokio::task::yield_now().await;
    assert_eq!(p.size(), 1);
    assert!(!r.resource().closed.load(Ordering::SeqCst));
    assert!(
        !c.log
            .lock()
            .unwrap()
            .contains(&(r.resource().id, "cleanup"))
    );
    drop(lease);
    until(|| p.size() == 0).await;
    assert!(r.resource().closed.load(Ordering::SeqCst));
    assert!(
        c.log
            .lock()
            .unwrap()
            .contains(&(r.resource().id, "destroy"))
    );
    let next = p.borrow().await.unwrap();
    assert_ne!(next.acquire().await.unwrap().resource().id, r.resource().id);
    assert_eq!(c.created.load(Ordering::SeqCst), 2);
    p.stop().await;
}

#[tokio::test(start_paused = true)]
async fn idle_pool_shrinks_to_minimum_without_closing_retained_lease() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 3, false);
    let retained = p.borrow().await.unwrap();
    let r = retained.acquire().await.unwrap();
    let a = p.borrow().await.unwrap();
    let b = p.borrow().await.unwrap();
    a.acquire().await.unwrap();
    b.acquire().await.unwrap();
    assert_eq!(p.size(), 3);
    drop(a);
    drop(b);
    until(|| p.size() == 1).await;
    assert_eq!(
        c.log
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, h)| *h == "destroy")
            .count(),
        2
    );
    assert!(!r.resource().closed.load(Ordering::SeqCst));
    drop(retained);
    tokio::time::advance(Duration::from_millis(200)).await;
    tokio::task::yield_now().await;
    assert_eq!(p.size(), 1);
    p.stop().await;
}

#[tokio::test]
async fn proxy_updates_every_slot_and_replays_updates_on_replacement() {
    type Callback = Arc<dyn Fn() + Send + Sync>;
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 2, false);
    let proxy = etherbird::ManagedResourceProxy::new(p.clone());
    let a = p.borrow().await.unwrap();
    let b = p.borrow().await.unwrap();
    let old = a.acquire().await.unwrap();
    let healthy = b.acquire().await.unwrap();
    let callback: Callback = Arc::new(|| {});
    proxy.set_value("callback", callback.clone(), |r, value| {
        *r.protocol_callback.lock().unwrap() = Some(value.clone());
    });
    proxy.set_value("setting", 73usize, |r, value| {
        r.setting.store(*value, Ordering::SeqCst)
    });
    for r in [&old, &healthy] {
        assert_eq!(r.resource().setting.load(Ordering::SeqCst), 73);
        assert!(Arc::ptr_eq(
            r.resource()
                .protocol_callback
                .lock()
                .unwrap()
                .as_ref()
                .unwrap(),
            &callback
        ));
    }
    old.resource().connected.store(false, Ordering::SeqCst);
    assert!(proxy.is_connected());
    p.recover(Some(&old), false).await.unwrap();
    let new = a.acquire().await.unwrap();
    assert_eq!(new.resource().setting.load(Ordering::SeqCst), 73);
    assert!(Arc::ptr_eq(
        new.resource()
            .protocol_callback
            .lock()
            .unwrap()
            .as_ref()
            .unwrap(),
        &callback
    ));
    assert!(!healthy.resource().closed.load(Ordering::SeqCst));
    p.stop().await;
}

#[tokio::test(start_paused = true)]
async fn superseded_watchdog_signal_does_not_recover_replacement() {
    let c = Arc::new(Control::default());
    let s = supervisor(&c);
    let old = s.acquire().await.unwrap();
    until(|| c.active_watches.load(Ordering::SeqCst) == 1).await;
    s.recover(&old, false).await.unwrap();
    let replacement = s.acquire().await.unwrap();
    until(|| c.active_watches.load(Ordering::SeqCst) == 1).await;
    // Rust aborts and joins the old watcher; signalling its resource must be harmless.
    old.resource().lost.notify_one();
    tokio::time::advance(Duration::from_millis(100)).await;
    tokio::task::yield_now().await;
    assert_eq!(s.current().unwrap().generation(), replacement.generation());
    assert_eq!(c.created.load(Ordering::SeqCst), 2);
    assert!(!replacement.resource().closed.load(Ordering::SeqCst));
    assert_eq!(
        c.log
            .lock()
            .unwrap()
            .iter()
            .filter(|&&(id, h)| id == 0 && h == "destroy")
            .count(),
        1
    );
    s.stop().await;
    assert_eq!(c.active_watches.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn terminal_operation_error_runs_once_and_skips_disconnect() {
    let c = Arc::new(Control::default());
    c.terminal.store(true, Ordering::SeqCst);
    let s = supervisor(&c);
    let old = s.acquire().await.unwrap();
    let calls = AtomicUsize::new(0);
    let result: Result<(), Error<Failure>> = s
        .execute(|_| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err(Failure) }
        })
        .await;
    assert!(matches!(result, Err(Error::Operation(Failure))));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        c.log
            .lock()
            .unwrap()
            .contains(&(old.resource().id, "cleanup"))
    );
    assert!(
        c.log
            .lock()
            .unwrap()
            .contains(&(old.resource().id, "destroy"))
    );
    assert!(
        !c.log
            .lock()
            .unwrap()
            .contains(&(old.resource().id, "disconnect"))
    );
    let next = s.acquire().await.unwrap();
    assert_ne!(next.resource().id, old.resource().id);
    assert_eq!(c.created.load(Ordering::SeqCst), 2);
    s.stop().await;
}

#[tokio::test]
async fn failed_pool_operation_recovers_only_its_slot_while_other_operation_runs() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 2, false);
    let healthy_id = Arc::new(AtomicUsize::new(usize::MAX));
    let seen = healthy_id.clone();
    let release = Arc::new(Notify::new());
    let gate = release.clone();
    let other = p.clone();
    let healthy = tokio::spawn(async move {
        other
            .execute(move |r| async move {
                seen.store(r.id, Ordering::SeqCst);
                gate.notified().await;
                assert!(!r.closed.load(Ordering::SeqCst));
                Ok(42)
            })
            .await
    });
    until(|| healthy_id.load(Ordering::SeqCst) != usize::MAX).await;
    let failed_id = Arc::new(AtomicUsize::new(usize::MAX));
    let failed_seen = failed_id.clone();
    let result: Result<(), Error<Failure>> = p
        .execute(move |r| {
            failed_seen.store(r.id, Ordering::SeqCst);
            async { Err(Failure) }
        })
        .await;
    assert!(matches!(result, Err(Error::Operation(Failure))));
    until(|| c.created.load(Ordering::SeqCst) == 3).await;
    let failed_id = failed_id.load(Ordering::SeqCst);
    assert_ne!(failed_id, healthy_id.load(Ordering::SeqCst));
    assert!(c.log.lock().unwrap().contains(&(failed_id, "destroy")));
    assert!(
        !c.log
            .lock()
            .unwrap()
            .contains(&(healthy_id.load(Ordering::SeqCst), "cleanup"))
    );
    assert!(!healthy.is_finished());
    release.notify_one();
    assert_eq!(healthy.await.unwrap().unwrap(), 42);
    p.stop().await;
}

#[tokio::test]
async fn concurrent_operations_reach_maximum_and_excess_work_waits() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 3, false);
    let active = Arc::new(AtomicUsize::new(0));
    let released = Arc::new(tokio::sync::Semaphore::new(0));
    let ids = Arc::new(Mutex::new(Vec::new()));
    let mut jobs = Vec::new();
    for index in 0..5 {
        let other = p.clone();
        let active = active.clone();
        let released = released.clone();
        let ids = ids.clone();
        jobs.push(tokio::spawn(async move {
            other
                .execute(move |r| async move {
                    ids.lock().unwrap().push(r.id);
                    active.fetch_add(1, Ordering::SeqCst);
                    released.acquire().await.unwrap().forget();
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(index)
                })
                .await
        }));
    }
    until(|| active.load(Ordering::SeqCst) == 3).await;
    assert_eq!(p.size(), 3);
    assert_eq!(c.created.load(Ordering::SeqCst), 3);
    assert_eq!(ids.lock().unwrap().len(), 3);
    let mut first = ids.lock().unwrap().clone();
    first.sort_unstable();
    first.dedup();
    assert_eq!(first.len(), 3);
    assert!(jobs.iter().all(|j| !j.is_finished()));
    released.add_permits(5);
    let mut results = Vec::new();
    for job in jobs {
        results.push(job.await.unwrap().unwrap());
    }
    results.sort_unstable();
    assert_eq!(results, [0, 1, 2, 3, 4]);
    assert_eq!(c.created.load(Ordering::SeqCst), 3);
    p.stop().await;
}

#[tokio::test]
async fn connecting_entries_count_toward_capacity_and_shutdown_cleans_them() {
    let c = Arc::new(Control::default());
    c.block_connect.store(true, Ordering::SeqCst);
    let control = c.clone();
    let p = Pool::start(
        move || {
            Supervisor::new(
                Manager(control.clone()),
                Config {
                    connect_timeout: Duration::from_secs(10),
                    ..config()
                },
            )
        },
        PoolConfig {
            max_size: 2,
            ..PoolConfig::default()
        },
    );
    let mut waiters = Vec::new();
    for _ in 0..5 {
        let other = p.clone();
        waiters.push(tokio::spawn(async move { other.borrow().await }));
    }
    until(|| {
        c.log
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, h)| *h == "connect")
            .count()
            == 2
    })
    .await;
    assert_eq!(p.size(), 2);
    assert_eq!(c.created.load(Ordering::SeqCst), 2);
    assert!(waiters.iter().all(|w| !w.is_finished()));
    let supervisors = p.supervisors();
    timeout(Duration::from_secs(1), p.stop()).await.unwrap();
    for waiter in waiters {
        assert!(matches!(waiter.await.unwrap(), Err(Error::Stopped)));
    }
    assert_eq!(c.active_watches.load(Ordering::SeqCst), 0);
    for s in supervisors {
        assert_eq!(s.state(), ResourceState::Stopped);
    }
    for id in 0..2 {
        assert!(c.log.lock().unwrap().contains(&(id, "destroy")));
    }
    assert_eq!(c.created.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn lease_drop_on_error_and_unwind_returns_same_resource() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 1, false);
    let lease = p.borrow().await.unwrap();
    let id = lease.acquire().await.unwrap().resource().id;
    let result: Result<(), Failure> = async {
        let _lease = lease;
        Err(Failure)
    }
    .await;
    assert!(result.is_err());
    let lease = timeout(Duration::from_secs(1), p.borrow())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lease.acquire().await.unwrap().resource().id, id);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _lease = lease;
            panic!("client scope failed");
        }))
        .is_err()
    );
    let lease = timeout(Duration::from_secs(1), p.borrow())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lease.acquire().await.unwrap().resource().id, id);
    assert_eq!(c.created.load(Ordering::SeqCst), 1);
    p.stop().await;
}

#[tokio::test]
async fn borrower_waits_at_capacity_and_reuses_released_resource() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 1, false);
    let first = p.borrow().await.unwrap();
    let id = first.acquire().await.unwrap().resource().id;
    let other = p.clone();
    let waiting = tokio::spawn(async move { other.borrow().await });
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());
    assert_eq!(c.created.load(Ordering::SeqCst), 1);
    first.release(); // Ownership makes subsequent use a compile-time error.
    let second = timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(second.acquire().await.unwrap().resource().id, id);
    p.stop().await;
}

#[tokio::test]
async fn proxy_attribute_reads_do_not_let_methods_bypass_lease() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 1, false);
    let proxy = TypedClient::new(p.clone());
    let lease = p.borrow().await.unwrap();
    let id = lease.acquire().await.unwrap().resource().id;
    assert_eq!(
        proxy
            .managed
            .attribute(|r| (r.id, r.setting.load(Ordering::SeqCst))),
        Some((id, 0))
    );
    assert!(proxy.managed.value::<usize>("missing").is_none());
    let operation = tokio::spawn(async move { proxy.identifier().await });
    tokio::task::yield_now().await;
    assert!(!operation.is_finished());
    lease.release();
    assert_eq!(
        timeout(Duration::from_secs(1), operation)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        id
    );
    p.stop().await;
}

#[tokio::test]
async fn setup_completion_gates_readiness_and_state_transitions() {
    let c = Arc::new(Control::default());
    c.block_setup.store(true, Ordering::SeqCst);
    let s = Supervisor::new(
        Manager(c.clone()),
        Config {
            setup_timeout: Duration::from_secs(10),
            ..config()
        },
    );
    assert_eq!(s.state(), ResourceState::Disconnected);
    s.begin();
    until(|| c.log.lock().unwrap().contains(&(0, "setup"))).await;
    assert_eq!(s.state(), ResourceState::Connecting);
    assert!(s.current().is_none());
    let other = s.clone();
    let acquiring = tokio::spawn(async move { other.acquire().await });
    tokio::task::yield_now().await;
    assert!(!acquiring.is_finished());
    c.block_setup.store(false, Ordering::SeqCst);
    c.setup_gate.notify_waiters();
    let old = acquiring.await.unwrap().unwrap();
    assert_eq!(s.state(), ResourceState::Connected);
    c.block_cleanup.store(true, Ordering::SeqCst);
    s.request_recovery(&old, false);
    until(|| c.log.lock().unwrap().contains(&(0, "cleanup"))).await;
    assert_eq!(s.state(), ResourceState::Recovering);
    assert!(s.current().is_none());
    assert!(!old.resource().closed.load(Ordering::SeqCst));
    c.block_cleanup.store(false, Ordering::SeqCst);
    c.cleanup_gate.notify_waiters();
    let next = s.acquire().await.unwrap();
    assert_ne!(next.resource().id, old.resource().id);
    assert_eq!(s.state(), ResourceState::Connected);
    s.stop().await;
    assert_eq!(s.state(), ResourceState::Stopped);
}

#[tokio::test]
async fn concurrent_pool_stop_cleans_each_resource_once() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 1, false);
    let lease = p.borrow().await.unwrap();
    let r = lease.acquire().await.unwrap();
    tokio::join!(p.stop(), p.stop(), p.stop());
    assert!(r.resource().closed.load(Ordering::SeqCst));
    for hook in ["cleanup", "disconnect", "destroy"] {
        assert_eq!(
            c.log
                .lock()
                .unwrap()
                .iter()
                .filter(|&&(id, h)| id == r.resource().id && h == hook)
                .count(),
            1
        );
    }
    assert!(matches!(lease.acquire().await, Err(Error::Stopped)));
}

#[tokio::test(start_paused = true)]
async fn hard_connect_timeout_retries_before_abandoned_connect_finishes() {
    let c = Arc::new(Control::default());
    c.block_first_connect.store(true, Ordering::SeqCst);
    let s = supervisor(&c);
    let healthy = timeout(Duration::from_secs(1), s.acquire())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(healthy.resource().id, 1);
    assert!(!c.log.lock().unwrap().contains(&(0, "setup")));
    c.block_first_connect.store(false, Ordering::SeqCst);
    c.connect_gate.notify_waiters();
    until(|| {
        c.log
            .lock()
            .unwrap()
            .iter()
            .filter(|&&(id, h)| id == 0 && h == "destroy")
            .count()
            == 2
    })
    .await;
    assert_eq!(s.current().unwrap().generation(), healthy.generation());
    assert!(!healthy.resource().closed.load(Ordering::SeqCst));
    s.stop().await;
}

#[tokio::test(start_paused = true)]
async fn hard_setup_timeout_retries_and_abandoned_setup_can_finish() {
    let c = Arc::new(Control::default());
    c.block_first_setup.store(true, Ordering::SeqCst);
    let s = supervisor(&c);
    let healthy = timeout(Duration::from_secs(1), s.acquire())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(healthy.resource().id, 1);
    assert!(c.log.lock().unwrap().contains(&(0, "destroy")));
    assert!(!c.log.lock().unwrap().contains(&(0, "setup_completed")));
    c.setup_gate.notify_waiters();
    until(|| c.log.lock().unwrap().contains(&(0, "setup_completed"))).await;
    assert_eq!(s.current().unwrap().generation(), healthy.generation());
    s.stop().await;
}

#[tokio::test(start_paused = true)]
async fn hard_cleanup_timeout_recovers_and_abandoned_cleanup_can_finish() {
    let c = Arc::new(Control::default());
    let s = supervisor(&c);
    let old = s.acquire().await.unwrap();
    c.block_cleanup.store(true, Ordering::SeqCst);
    timeout(Duration::from_secs(1), s.recover(&old, false))
        .await
        .unwrap()
        .unwrap();
    let healthy = s.acquire().await.unwrap();
    assert_ne!(healthy.resource().id, old.resource().id);
    assert!(old.resource().closed.load(Ordering::SeqCst));
    assert!(c.log.lock().unwrap().contains(&(0, "destroy")));
    assert!(!c.log.lock().unwrap().contains(&(0, "cleanup_completed")));
    c.block_cleanup.store(false, Ordering::SeqCst);
    c.cleanup_gate.notify_waiters();
    until(|| c.log.lock().unwrap().contains(&(0, "cleanup_completed"))).await;
    s.stop().await;
}

#[tokio::test(start_paused = true)]
async fn hard_disconnect_timeout_recovers_before_late_disconnect_and_destroy() {
    let c = Arc::new(Control::default());
    let s = supervisor(&c);
    let old = s.acquire().await.unwrap();
    c.block_disconnect.store(true, Ordering::SeqCst);
    timeout(Duration::from_secs(1), s.recover(&old, false))
        .await
        .unwrap()
        .unwrap();
    let healthy = s.acquire().await.unwrap();
    assert_ne!(healthy.resource().id, old.resource().id);
    assert!(!old.resource().closed.load(Ordering::SeqCst));
    assert!(!c.log.lock().unwrap().contains(&(0, "destroy")));
    c.block_disconnect.store(false, Ordering::SeqCst);
    c.disconnect_gate.notify_waiters();
    until(|| c.log.lock().unwrap().contains(&(0, "destroy"))).await;
    assert!(old.resource().closed.load(Ordering::SeqCst));
    assert_eq!(s.current().unwrap().generation(), healthy.generation());
    s.stop().await;
}

#[tokio::test]
async fn proxy_reads_and_updates_connecting_resource_while_method_waits() {
    let c = Arc::new(Control::default());
    c.block_connect.store(true, Ordering::SeqCst);
    let control = c.clone();
    let p = Pool::start(
        move || {
            Supervisor::new(
                Manager(control.clone()),
                Config {
                    connect_timeout: Duration::from_secs(10),
                    ..config()
                },
            )
        },
        PoolConfig::default(),
    );
    until(|| c.log.lock().unwrap().contains(&(0, "connect"))).await;
    let proxy = TypedClient::new(p.clone());
    assert!(!proxy.managed.is_connected());
    assert_eq!(proxy.managed.attribute(|r| r.id), Some(0));
    proxy.managed.set_value("setting", 99usize, |r, v| {
        r.setting.store(*v, Ordering::SeqCst)
    });
    assert_eq!(
        proxy
            .managed
            .attribute(|r| r.setting.load(Ordering::SeqCst)),
        Some(99)
    );
    let job = tokio::spawn(async move { proxy.identifier().await });
    tokio::task::yield_now().await;
    assert!(!job.is_finished());
    c.block_connect.store(false, Ordering::SeqCst);
    c.connect_gate.notify_waiters();
    assert_eq!(
        timeout(Duration::from_secs(1), job)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        0
    );
    assert_eq!(
        p.resources()[0].resource().setting.load(Ordering::SeqCst),
        99
    );
    p.stop().await;
}

#[tokio::test]
async fn cancelled_borrower_does_not_take_returned_capacity() {
    let c = Arc::new(Control::default());
    let p = pool(&c, 1, 1, false);
    let first = p.borrow().await.unwrap();
    let id = first.acquire().await.unwrap().resource().id;
    let other = p.clone();
    let waiting = tokio::spawn(async move { other.borrow().await });
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());
    waiting.abort();
    assert!(matches!(waiting.await, Err(error) if error.is_cancelled()));
    first.release();
    let next = timeout(Duration::from_secs(1), p.borrow())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.acquire().await.unwrap().resource().id, id);
    assert_eq!(c.created.load(Ordering::SeqCst), 1);
    p.stop().await;
}

mod retry_tests {
    use super::*;
    use etherbird::RetryPolicy;

    fn policy(attempts: usize, duration: Duration) -> RetryPolicy {
        RetryPolicy {
            max_attempts: attempts.try_into().unwrap(),
            timeout: duration,
        }
    }

    #[tokio::test]
    async fn opt_in_waits_for_replacement_setup_before_repeating() {
        let c = Arc::new(Control::default());
        let s = supervisor(&c);
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let control = c.clone();
        let other = s.clone();
        let task = tokio::spawn(async move {
            other
                .execute_with_retry(
                    policy(3, Duration::from_secs(1)),
                    move |r| {
                        let count = count.clone();
                        let control = control.clone();
                        async move {
                            assert!(
                                control
                                    .log
                                    .lock()
                                    .unwrap()
                                    .contains(&(r.id, "setup_completed"))
                            );
                            if count.fetch_add(1, Ordering::SeqCst) == 0 {
                                control.block_setup.store(true, Ordering::SeqCst);
                                Err(Failure)
                            } else {
                                Ok(r.id)
                            }
                        }
                    },
                    |_| true,
                )
                .await
        });
        until(|| c.log.lock().unwrap().contains(&(1, "setup"))).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!task.is_finished());
        c.block_setup.store(false, Ordering::SeqCst);
        c.setup_gate.notify_waiters();
        assert_eq!(task.await.unwrap().unwrap(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(c.log.lock().unwrap().contains(&(0, "destroy")));
        s.stop().await;
    }

    #[tokio::test]
    async fn attempt_limit_and_predicate_return_the_operation_error() {
        for max_attempts in [1, 3] {
            for accepted in [false, true] {
                let c = Arc::new(Control::default());
                let s = supervisor(&c);
                let mut calls = 0;
                let mut predicates = 0;
                let result: Result<(), _> = s
                    .execute_with_retry(
                        policy(max_attempts, Duration::from_secs(1)),
                        |_| {
                            calls += 1;
                            async { Err(Failure) }
                        },
                        |_| {
                            predicates += 1;
                            accepted
                        },
                    )
                    .await;
                assert!(matches!(result, Err(Error::Operation(Failure))));
                assert_eq!(calls, if accepted { max_attempts } else { 1 });
                assert_eq!(
                    predicates,
                    if max_attempts == 1 {
                        0
                    } else if accepted {
                        max_attempts - 1
                    } else {
                        1
                    }
                );
                s.stop().await;
            }
        }
    }

    #[tokio::test]
    async fn terminal_classification_controls_teardown_but_predicate_controls_replay() {
        let c = Arc::new(Control::default());
        c.terminal.store(true, Ordering::SeqCst);
        let s = supervisor(&c);
        let mut calls = 0;
        let id = s
            .execute_with_retry(
                policy(2, Duration::from_secs(1)),
                |r| {
                    calls += 1;
                    let failed = calls == 1;
                    async move { if failed { Err(Failure) } else { Ok(r.id) } }
                },
                |_| true,
            )
            .await
            .unwrap();
        assert_eq!(id, 1);
        assert_eq!(calls, 2);
        let log = c.log.lock().unwrap().clone();
        assert!(log.contains(&(0, "cleanup")));
        assert!(log.contains(&(0, "destroy")));
        assert!(!log.contains(&(0, "disconnect")));
        s.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn overall_deadline_includes_initial_connection_and_running_attempt() {
        for waiting_for_connection in [false, true] {
            let c = Arc::new(Control::default());
            c.block_connect
                .store(waiting_for_connection, Ordering::SeqCst);
            let s = supervisor(&c);
            let mut calls = 0;
            let result: Result<(), _> = s
                .execute_with_retry(
                    policy(3, Duration::from_millis(10)),
                    |_| {
                        calls += 1;
                        std::future::pending()
                    },
                    |_| true,
                )
                .await;
            assert!(matches!(result, Err(Error::Timeout)));
            assert_eq!(calls, usize::from(!waiting_for_connection));
            s.stop().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn overall_deadline_does_not_reset_during_teardown() {
        let c = Arc::new(Control::default());
        c.block_cleanup.store(true, Ordering::SeqCst);
        let s = supervisor(&c);
        let mut calls = 0;
        let result: Result<(), _> = s
            .execute_with_retry(
                policy(3, Duration::from_millis(10)),
                |_| {
                    calls += 1;
                    async { Err(Failure) }
                },
                |_| true,
            )
            .await;
        assert!(matches!(result, Err(Error::Timeout)));
        assert_eq!(calls, 1);
        assert!(c.log.lock().unwrap().contains(&(0, "cleanup")));
        c.block_cleanup.store(false, Ordering::SeqCst);
        c.cleanup_gate.notify_waiters();
        s.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn zero_deadline_runs_no_operation() {
        let c = Arc::new(Control::default());
        let s = Supervisor::new(Manager(c.clone()), config());
        let mut called = false;
        let result = s
            .execute_with_retry(
                policy(3, Duration::ZERO),
                |_| {
                    called = true;
                    async { Ok(()) }
                },
                |_| true,
            )
            .await;
        assert!(matches!(result, Err(Error::Timeout)));
        assert!(!called);
        assert_eq!(c.created.load(Ordering::SeqCst), 0);
        s.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_is_shared_by_all_attempts() {
        let c = Arc::new(Control::default());
        let s = supervisor(&c);
        let started = tokio::time::Instant::now();
        let mut calls = 0;
        let result: Result<(), _> = s
            .execute_with_retry(
                policy(3, Duration::from_millis(20)),
                |_| {
                    calls += 1;
                    async {
                        sleep(Duration::from_millis(8)).await;
                        Err(Failure)
                    }
                },
                |_| true,
            )
            .await;
        assert!(matches!(result, Err(Error::Timeout)));
        assert_eq!(calls, 3);
        assert_eq!(started.elapsed(), Duration::from_millis(20));
        s.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn pool_deadline_cancels_active_attempt_and_returns_capacity() {
        let c = Arc::new(Control::default());
        let p = pool(&c, 1, 1, false);
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let result: Result<(), _> = p
            .execute_with_retry(
                policy(3, Duration::from_millis(10)),
                move |_| {
                    count.fetch_add(1, Ordering::SeqCst);
                    std::future::pending()
                },
                |_| true,
            )
            .await;
        assert!(matches!(result, Err(Error::Timeout)));
        p.execute(|_| async { Ok(()) }).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        p.stop().await;
    }

    #[tokio::test]
    async fn pool_shutdown_stops_retrying_work_and_releases_its_lease() {
        let c = Arc::new(Control::default());
        let p = pool(&c, 1, 1, false);
        let control = c.clone();
        let other = p.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let task = tokio::spawn(async move {
            other
                .execute_with_retry(
                    policy(3, Duration::from_secs(1)),
                    move |_| {
                        count.fetch_add(1, Ordering::SeqCst);
                        control.block_connect.store(true, Ordering::SeqCst);
                        async { Err::<(), _>(Failure) }
                    },
                    |_| true,
                )
                .await
        });
        until(|| c.log.lock().unwrap().contains(&(1, "connect"))).await;
        p.stop().await;
        assert!(matches!(task.await.unwrap(), Err(Error::Stopped)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(p.state(), ResourceState::Stopped);
    }

    #[tokio::test]
    async fn pool_retries_retain_the_lease_and_priority_position() {
        let c = Arc::new(Control::default());
        let p = pool(&c, 1, 1, true);
        let order = Arc::new(Mutex::new(Vec::new()));
        let log = order.clone();
        let control = c.clone();
        let other = p.clone();
        let retrying = tokio::spawn(async move {
            let mut calls = 0;
            other
                .execute_with_priority_and_retry(
                    10,
                    policy(2, Duration::from_secs(1)),
                    move |r| {
                        calls += 1;
                        let first = calls == 1;
                        log.lock().unwrap().push(r.id);
                        if first {
                            control.block_connect.store(true, Ordering::SeqCst);
                        }
                        async move { if first { Err(Failure) } else { Ok(r.id) } }
                    },
                    |_| true,
                )
                .await
        });
        until(|| c.log.lock().unwrap().contains(&(1, "connect"))).await;
        let log = order.clone();
        let other = p.clone();
        let higher_priority = tokio::spawn(async move {
            other
                .execute_with_priority(-10, move |_| async move {
                    log.lock().unwrap().push(99);
                    Ok(())
                })
                .await
        });
        tokio::task::yield_now().await;
        assert!(!higher_priority.is_finished());
        c.block_connect.store(false, Ordering::SeqCst);
        c.connect_gate.notify_waiters();
        assert_eq!(retrying.await.unwrap().unwrap(), 1);
        higher_priority.await.unwrap().unwrap();
        assert_eq!(*order.lock().unwrap(), [0, 1, 99]);
        p.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn pool_deadline_includes_queueing_and_cancels_the_queued_job() {
        let c = Arc::new(Control::default());
        let p = pool(&c, 1, 1, false);
        let held = p.borrow().await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let result = p
            .execute_with_retry(
                policy(3, Duration::from_millis(10)),
                move |_| {
                    count.fetch_add(1, Ordering::SeqCst);
                    async { Ok(()) }
                },
                |_| true,
            )
            .await;
        assert!(matches!(result, Err(Error::Timeout)));
        held.release();
        p.execute(|_| async { Ok(()) }).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        p.stop().await;
    }

    #[tokio::test]
    async fn cancelling_pool_retry_during_recovery_releases_capacity() {
        let c = Arc::new(Control::default());
        let p = pool(&c, 1, 1, false);
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let control = c.clone();
        let other = p.clone();
        let task = tokio::spawn(async move {
            other
                .execute_with_retry(
                    policy(3, Duration::from_secs(1)),
                    move |_| {
                        count.fetch_add(1, Ordering::SeqCst);
                        control.block_connect.store(true, Ordering::SeqCst);
                        async { Err::<(), _>(Failure) }
                    },
                    |_| true,
                )
                .await
        });
        until(|| c.log.lock().unwrap().contains(&(1, "connect"))).await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        c.block_connect.store(false, Ordering::SeqCst);
        c.connect_gate.notify_waiters();
        timeout(Duration::from_secs(1), p.execute(|_| async { Ok(()) }))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        p.stop().await;
    }

    #[tokio::test]
    async fn shutdown_stops_retries_during_recovery_without_restarting_supervisor() {
        let c = Arc::new(Control::default());
        let s = supervisor(&c);
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let control = c.clone();
        let other = s.clone();
        let task = tokio::spawn(async move {
            other
                .execute_with_retry(
                    policy(3, Duration::from_secs(1)),
                    move |_| {
                        count.fetch_add(1, Ordering::SeqCst);
                        control.block_cleanup.store(true, Ordering::SeqCst);
                        async { Err::<(), _>(Failure) }
                    },
                    |_| true,
                )
                .await
        });
        until(|| c.log.lock().unwrap().contains(&(0, "cleanup"))).await;
        s.stop().await;
        assert!(matches!(task.await.unwrap(), Err(Error::Stopped)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(s.state(), ResourceState::Stopped);
        assert_eq!(c.created.load(Ordering::SeqCst), 1);
        c.cleanup_gate.notify_waiters();
    }
}
