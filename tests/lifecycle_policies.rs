use etherbird::{
    Config, Error, Lifecycle, LifecycleFailurePolicy, OperationFailurePolicy, ResourceState,
    RetryPolicy, Supervisor, async_trait,
};
use std::{
    io,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;

#[derive(Default)]
struct Control {
    stage: AtomicUsize,
    reject: AtomicBool,
    terminal: AtomicBool,
    created: AtomicUsize,
    setups: AtomicUsize,
    cleaned: AtomicUsize,
    disconnected: AtomicUsize,
    destroyed: AtomicUsize,
    transient: AtomicUsize,
    block_cleanup: AtomicBool,
    cleanup_release: Notify,
    resources: Mutex<Vec<Weak<Resource>>>,
    watch: Notify,
}
struct Resource {
    id: usize,
    setting: AtomicUsize,
}
impl Resource {
    async fn healthy_rejection(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "peer rejected request",
        ))
    }
    async fn transport_failure(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "transport failed",
        ))
    }
    fn identifier(&self) -> io::Result<usize> {
        Ok(self.id)
    }
}
#[derive(Clone)]
struct Hooks(Arc<Control>);
impl Hooks {
    fn check(&self, stage: usize) -> io::Result<()> {
        if self.0.stage.load(Ordering::SeqCst) == stage && self.0.reject.load(Ordering::SeqCst) {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "SECRET credentials rejected",
            ))
        } else {
            Ok(())
        }
    }
}
#[async_trait]
impl Lifecycle for Hooks {
    type Resource = Resource;
    type Error = io::Error;
    async fn create(&self) -> io::Result<Resource> {
        let id = self.0.created.fetch_add(1, Ordering::SeqCst);
        self.check(1)?;
        Ok(Resource {
            id,
            setting: AtomicUsize::new(0),
        })
    }
    async fn on_resource_created(&self, _: &Resource) -> io::Result<()> {
        self.check(2)
    }
    async fn connect(&self, _: &Resource) -> io::Result<()> {
        self.check(3)
    }
    async fn setup(&self, _: &Resource) -> io::Result<()> {
        self.0.setups.fetch_add(1, Ordering::SeqCst);
        if self
            .0
            .transient
            .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Err(io::Error::new(io::ErrorKind::ConnectionReset, "transient"));
        }
        self.check(4)
    }
    async fn cleanup(&self, _: &Resource) -> io::Result<()> {
        self.0.cleaned.fetch_add(1, Ordering::SeqCst);
        if self.0.block_cleanup.load(Ordering::SeqCst) {
            self.0.cleanup_release.notified().await;
        }
        Ok(())
    }
    async fn disconnect(&self, _: &Resource) -> io::Result<()> {
        self.0.disconnected.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn destroy(&self, _: &Resource) -> io::Result<()> {
        self.0.destroyed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn watch_disconnect(
        &self,
        resource: Arc<Resource>,
    ) -> Option<etherbird::LifecycleFuture<io::Error>> {
        self.0
            .resources
            .lock()
            .unwrap()
            .push(Arc::downgrade(&resource));
        let hooks = self.clone();
        Some(Box::pin(async move {
            let _resource = resource;
            hooks.0.watch.notified().await;
            hooks.check(5)?;
            Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "watch failure",
            ))
        }))
    }
    fn lifecycle_failure(&self, error: &io::Error) -> LifecycleFailurePolicy {
        if error.kind() == io::ErrorKind::PermissionDenied {
            LifecycleFailurePolicy::Fail
        } else {
            LifecycleFailurePolicy::Retry
        }
    }
    fn operation_failure(&self, error: &io::Error) -> OperationFailurePolicy {
        if error.kind() == io::ErrorKind::InvalidInput {
            OperationFailurePolicy::Retain
        } else {
            OperationFailurePolicy::Recover
        }
    }
    fn is_terminal(&self, _: &io::Error) -> bool {
        self.0.terminal.load(Ordering::SeqCst)
    }
}
etherbird::managed_client! {
    struct Client for Hooks {
        async fn healthy_rejection() -> ();
        async fn transport_failure() -> ();
        fn identifier() -> usize;
    }
}
fn config() -> Config {
    Config {
        retry_delay: Duration::from_millis(1),
        max_retry_delay: Duration::from_millis(4),
        ..Config::default()
    }
}
fn supervisor(control: &Arc<Control>) -> Supervisor<Hooks> {
    Supervisor::try_new(Hooks(control.clone()), config()).unwrap()
}
async fn until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition did not become true");
}
fn assert_failed<T>(result: Result<T, Error<io::Error>>) -> Arc<io::Error> {
    match result {
        Err(Error::Lifecycle(cause)) => {
            assert_eq!(cause.kind(), io::ErrorKind::PermissionDenied);
            cause
        }
        _ => panic!("expected non-retryable lifecycle failure"),
    }
}

#[test]
fn checked_configuration_has_explicit_boundary_rules() {
    Config::default().validate().unwrap();
    let mut cfg = Config {
        connect_timeout: Duration::ZERO,
        setup_timeout: Duration::ZERO,
        create_timeout: Some(Duration::ZERO),
        created_timeout: None,
        destroy_timeout: None,
        cleanup_timeout: Duration::ZERO,
        disconnect_timeout: Duration::ZERO,
        ..config()
    };
    cfg.validate().unwrap();
    cfg.retry_delay = Duration::ZERO;
    assert_eq!(cfg.validate().unwrap_err().field, "retry_delay");
    cfg.retry_delay = Duration::from_secs(1);
    assert_eq!(cfg.validate().unwrap_err().field, "max_retry_delay");
    cfg.max_retry_delay = cfg.retry_delay;
    cfg.validate().unwrap();
    RetryPolicy {
        max_attempts: NonZeroUsize::new(1).unwrap(),
        timeout: Duration::ZERO,
    }
    .validate()
    .unwrap();
    let control = Arc::new(Control::default());
    cfg.retry_delay = Duration::ZERO;
    assert!(Supervisor::try_start(Hooks(control.clone()), cfg.clone()).is_err());
    assert_eq!(control.created.load(Ordering::SeqCst), 0);
    // Legacy unchecked construction still accepts the same configuration.
    let _legacy = Supervisor::new(Hooks(control), cfg);
}

#[tokio::test(start_paused = true)]
async fn every_initialization_stage_latches_and_requires_explicit_reset() {
    for stage in 1..=4 {
        let control = Arc::new(Control::default());
        control.stage.store(stage, Ordering::SeqCst);
        control.reject.store(true, Ordering::SeqCst);
        let sup = supervisor(&control);
        let other = sup.clone();
        let (first, second) = tokio::join!(sup.acquire(), other.acquire());
        let first = assert_failed(first);
        let second = assert_failed(second);
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(sup.state(), ResourceState::Failed);
        assert!(sup.current().is_none());
        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(control.created.load(Ordering::SeqCst), 1);
        assert_failed(
            sup.execute(|_| async {
                panic!("failed resource admitted");
                #[allow(unreachable_code)]
                Ok(())
            })
            .await,
        );
        let wrapped = Error::Lifecycle(first);
        assert!(!format!("{wrapped:?} {wrapped}").contains("SECRET"));
        control.reject.store(false, Ordering::SeqCst);
        assert!(sup.reset_failure());
        assert!(!sup.reset_failure());
        assert_eq!(sup.acquire().await.unwrap().resource().id, 1);
        sup.stop().await;
        assert!(sup.failure().is_none());
        assert!(!sup.reset_failure());
        assert_eq!(
            control.destroyed.load(Ordering::SeqCst),
            if stage == 1 { 1 } else { 2 }
        );
    }
}

#[tokio::test]
async fn created_callback_failure_is_classified_and_stop_has_precedence() {
    let control = Arc::new(Control::default());
    let sup = supervisor(&control);
    sup.set_on_resource_created(Arc::new(|_| {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "SECRET callback",
            ))
        })
    }));
    assert_failed(sup.acquire().await);
    let proxy = etherbird::SupervisedResourceProxy::new(sup.clone());
    assert_failed(proxy.connected().await);
    proxy.stop().await;
    assert!(matches!(proxy.connected().await, Err(Error::Stopped)));
    assert!(matches!(
        proxy.execute(|_| async { Ok(()) }).await,
        Err(Error::Stopped)
    ));
    assert!(!proxy.reset_failure());
    assert!(!sup.reset_failure());
    assert_eq!(control.destroyed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn transient_failures_and_terminal_teardown_remain_separate() {
    let control = Arc::new(Control::default());
    control.transient.store(1, Ordering::SeqCst);
    control.terminal.store(true, Ordering::SeqCst);
    let sup = supervisor(&control);
    assert_eq!(sup.acquire().await.unwrap().resource().id, 1);
    assert_eq!(control.disconnected.load(Ordering::SeqCst), 0);
    assert_eq!(control.destroyed.load(Ordering::SeqCst), 1);
    assert!(sup.failure().is_none());
    sup.stop().await;
    assert_eq!(control.disconnected.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn watchdog_can_suspend_lifecycle_without_recreating_resource() {
    let control = Arc::new(Control::default());
    control.stage.store(5, Ordering::SeqCst);
    control.reject.store(true, Ordering::SeqCst);
    let sup = supervisor(&control);
    let old = sup.acquire().await.unwrap();
    control.watch.notify_one();
    until(|| sup.failure().is_some()).await;
    assert_failed(sup.acquire().await);
    control.reject.store(false, Ordering::SeqCst);
    assert!(sup.reset_failure());
    let replacement = sup.acquire().await.unwrap();
    assert!(replacement.generation() > old.generation());
    sup.request_recovery(&old, false);
    assert_eq!(
        sup.current().unwrap().generation(),
        replacement.generation()
    );
    sup.stop().await;
}

#[tokio::test]
async fn generated_direct_client_retains_healthy_errors_and_recovers_transport_errors() {
    let control = Arc::new(Control::default());
    let client = Client::new(supervisor(&control));
    assert_eq!(client.identifier().await.unwrap(), 0);
    assert!(matches!(
        client.healthy_rejection().await,
        Err(Error::Operation(_))
    ));
    assert_eq!(client.identifier().await.unwrap(), 0);
    assert_eq!(control.created.load(Ordering::SeqCst), 1);
    assert!(matches!(
        client.transport_failure().await,
        Err(Error::Operation(_))
    ));
    assert_eq!(client.identifier().await.unwrap(), 1);
    let mut attempts = 0;
    let result = client
        .managed
        .execute_with_retry(
            RetryPolicy {
                max_attempts: NonZeroUsize::new(2).unwrap(),
                timeout: Duration::from_secs(1),
            },
            |resource| {
                attempts += 1;
                async move { resource.healthy_rejection().await }
            },
            |_| true,
        )
        .await;
    assert!(matches!(result, Err(Error::Operation(_))));
    assert_eq!(attempts, 2);
    assert_eq!(control.created.load(Ordering::SeqCst), 2);
    client.managed.stop().await;
}

#[tokio::test]
async fn generated_direct_clones_fail_readiness_and_calls_without_implicit_reset() {
    let control = Arc::new(Control::default());
    control.stage.store(4, Ordering::SeqCst);
    control.reject.store(true, Ordering::SeqCst);
    let client = Client::new(supervisor(&control));
    let other = client.clone();
    let (ready, call) = tokio::join!(client.managed.connected(), other.identifier());
    assert_failed(ready);
    assert_failed(call);
    assert_failed(other.identifier().await);
    assert_eq!(control.created.load(Ordering::SeqCst), 1);
    client.managed.stop().await;
    assert!(matches!(other.identifier().await, Err(Error::Stopped)));
}

#[tokio::test]
async fn failure_wakes_callers_before_teardown_and_stop_wins_over_reset() {
    let control = Arc::new(Control::default());
    control.stage.store(4, Ordering::SeqCst);
    control.reject.store(true, Ordering::SeqCst);
    control.block_cleanup.store(true, Ordering::SeqCst);
    let sup = supervisor(&control);
    assert_failed(sup.acquire().await);
    until(|| control.cleaned.load(Ordering::SeqCst) == 1).await;
    assert_eq!(control.destroyed.load(Ordering::SeqCst), 0);
    control.reject.store(false, Ordering::SeqCst);
    assert!(sup.reset_failure());
    let stop = tokio::spawn({
        let sup = sup.clone();
        async move { sup.stop().await }
    });
    until(|| sup.state() == ResourceState::Stopping).await;
    stop.abort();
    assert!(stop.await.unwrap_err().is_cancelled());
    assert!(!sup.reset_failure());
    control.cleanup_release.notify_one();
    sup.stop().await;
    assert_eq!(control.created.load(Ordering::SeqCst), 1);
    assert_eq!(control.destroyed.load(Ordering::SeqCst), 1);
    assert_eq!(sup.with_latest(|r| r.id), None);
}

#[tokio::test]
async fn replacement_releases_old_resource_and_user_callback_capture_can_retain_new_one() {
    let control = Arc::new(Control::default());
    let sup = supervisor(&control);
    let first = sup.acquire().await.unwrap();
    let old = Arc::downgrade(first.resource());
    sup.recover(&first, false).await.unwrap();
    drop(first);
    let next = sup.acquire().await.unwrap();
    assert!(old.upgrade().is_none());
    let weak = Arc::downgrade(next.resource());
    let captured = next.resource().clone();
    sup.set_on_resource_created(Arc::new(move |_| {
        let _captured = &captured;
        Box::pin(async { Ok(()) })
    }));
    drop(next);
    sup.stop().await;
    assert!(weak.upgrade().is_some());
    sup.set_on_resource_created(Arc::new(|_| Box::pin(async { Ok(()) })));
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn stopped_supervisor_releases_cached_resource_but_keeps_values_and_replays_on_restart() {
    let control = Arc::new(Control::default());
    let sup = supervisor(&control);
    sup.set_value("setting", 7usize, |r, value| {
        r.setting.store(*value, Ordering::SeqCst);
    });
    let handle = sup.acquire().await.unwrap();
    let weak = Arc::downgrade(handle.resource());
    sup.stop().await;
    assert_eq!(sup.with_latest(|r| r.id), None);
    assert_eq!(sup.value::<usize>("setting"), Some(7));
    assert!(weak.upgrade().is_some());
    drop(handle);
    assert!(weak.upgrade().is_none());
    assert!(sup.restart().await);
    let replacement = sup.acquire().await.unwrap();
    assert_eq!(replacement.resource().setting.load(Ordering::SeqCst), 7);
    assert_eq!(replacement.resource().id, 1);
    drop(replacement);
    sup.stop().await;
    assert!(
        control
            .resources
            .lock()
            .unwrap()
            .iter()
            .all(|r| r.upgrade().is_none())
    );
}

#[cfg(feature = "pool")]
mod pooled {
    use super::*;
    use etherbird::{Pool, PoolConfig};

    fn pool(control: &Arc<Control>, config: PoolConfig) -> Pool<Hooks> {
        let control = control.clone();
        Pool::try_new(move || supervisor(&control), config).unwrap()
    }

    #[test]
    fn invalid_pool_configuration_never_invokes_factories() {
        let control = Arc::new(Control::default());
        for cfg in [
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
            assert!(cfg.validate().is_err());
            let factory_control = control.clone();
            assert!(
                Pool::try_start_with_queue_factory(
                    move || supervisor(&factory_control),
                    cfg,
                    || {
                        panic!("invalid configuration invoked queue factory");
                        #[allow(unreachable_code)]
                        etherbird::FifoQueue::default()
                    },
                )
                .is_err()
            );
        }
        assert_eq!(control.created.load(Ordering::SeqCst), 0);
        PoolConfig::default().validate().unwrap();
        PoolConfig {
            min_size: 0,
            ..PoolConfig::default()
        }
        .validate()
        .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn all_failed_pool_wakes_queued_calls_borrowers_and_readiness_without_growth() {
        let control = Arc::new(Control::default());
        control.stage.store(4, Ordering::SeqCst);
        control.reject.store(true, Ordering::SeqCst);
        let pool = pool(
            &control,
            PoolConfig {
                min_size: 1,
                max_size: 4,
                idle_timeout: Duration::from_millis(1),
                ..PoolConfig::default()
            },
        );
        let client = Client::from_pool(pool.clone());
        let clone = client.clone();
        let (first, second, ready, borrower) = tokio::join!(
            client.identifier(),
            clone.identifier(),
            pool.connected(),
            pool.borrow()
        );
        assert_failed(first);
        assert_failed(second);
        assert_failed(ready);
        assert_failed(borrower);
        assert_eq!(pool.state(), ResourceState::Failed);
        let count = control.created.load(Ordering::SeqCst);
        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::task::yield_now().await;
        assert_eq!(control.created.load(Ordering::SeqCst), count);
        assert_eq!(pool.size(), count);
        control.reject.store(false, Ordering::SeqCst);
        assert_eq!(pool.reset_failed(), count);
        client.identifier().await.unwrap();
        pool.stop().await;
        assert_eq!(pool.size(), 0);
        assert_eq!(pool.reset_failed(), 0);
        assert!(matches!(client.identifier().await, Err(Error::Stopped)));
    }

    #[tokio::test]
    async fn healthy_pool_slot_serves_work_when_another_slot_is_failed() {
        let bad = Arc::new(Control::default());
        bad.stage.store(4, Ordering::SeqCst);
        bad.reject.store(true, Ordering::SeqCst);
        let good = Arc::new(Control::default());
        let index = Arc::new(AtomicUsize::new(0));
        let pool = Pool::new(
            {
                let bad = bad.clone();
                let good = good.clone();
                move || {
                    if index.fetch_add(1, Ordering::SeqCst) == 0 {
                        supervisor(&bad)
                    } else {
                        supervisor(&good)
                    }
                }
            },
            PoolConfig {
                min_size: 2,
                max_size: 2,
                ..PoolConfig::default()
            },
        );
        pool.connected().await.unwrap();
        until(|| pool.supervisors().iter().any(|s| s.failure().is_some())).await;
        assert!(pool.failure().is_none());
        assert_eq!(pool.state(), ResourceState::Connected);
        let client = Client::from_pool(pool.clone());
        for _ in 0..3 {
            assert!(matches!(
                client.healthy_rejection().await,
                Err(Error::Operation(_))
            ));
            assert_eq!(client.identifier().await.unwrap(), 0);
        }
        assert_eq!(bad.created.load(Ordering::SeqCst), 1);
        assert_eq!(good.created.load(Ordering::SeqCst), 1);
        let lease = pool.borrow().await.unwrap();
        assert!(matches!(
            lease
                .execute(|r| async move { r.healthy_rejection().await })
                .await,
            Err(Error::Operation(_))
        ));
        assert_eq!(lease.acquire().await.unwrap().resource().id, 0);
        drop(lease);
        assert!(matches!(
            client.transport_failure().await,
            Err(Error::Operation(_))
        ));
        assert_eq!(client.identifier().await.unwrap(), 1);
        pool.stop().await;
    }

    #[tokio::test]
    async fn pool_stop_releases_caches_even_with_retained_supervisors_and_lease() {
        let control = Arc::new(Control::default());
        let pool = pool(&control, PoolConfig::default());
        let lease = pool.borrow().await.unwrap();
        let retained = pool.supervisors();
        let handle = lease.acquire().await.unwrap();
        let weak = Arc::downgrade(handle.resource());
        drop(handle);
        pool.stop().await;
        assert!(weak.upgrade().is_none());
        assert_eq!(pool.with_latest(|r| r.id), None);
        assert_eq!(pool.size(), 0);
        assert_eq!(retained[0].with_latest(|r| r.id), None);
        assert!(matches!(lease.acquire().await, Err(Error::Stopped)));
    }

    #[tokio::test(start_paused = true)]
    async fn late_created_callback_cannot_restore_stopped_pool_cache() {
        let control = Arc::new(Control::default());
        let release = Arc::new(Notify::new());
        let started = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let weak = Arc::new(Mutex::new(None));
        let pool = Pool::start(
            {
                let control = control.clone();
                let release = release.clone();
                let started = started.clone();
                let done = done.clone();
                let weak = weak.clone();
                move || {
                    let sup = Supervisor::new(
                        Hooks(control.clone()),
                        Config {
                            created_timeout: Some(Duration::from_millis(1)),
                            retry_delay: Duration::from_secs(60),
                            ..Config::default()
                        },
                    );
                    sup.set_on_resource_created(Arc::new({
                        let release = release.clone();
                        let started = started.clone();
                        let done = done.clone();
                        let weak = weak.clone();
                        move |resource| {
                            let release = release.clone();
                            let started = started.clone();
                            let done = done.clone();
                            *weak.lock().unwrap() = Some(Arc::downgrade(&resource));
                            Box::pin(async move {
                                started.store(true, Ordering::SeqCst);
                                release.notified().await;
                                done.store(true, Ordering::SeqCst);
                                Ok(())
                            })
                        }
                    }));
                    sup
                }
            },
            PoolConfig::default(),
        );
        until(|| started.load(Ordering::SeqCst)).await;
        tokio::time::advance(Duration::from_millis(2)).await;
        until(|| control.destroyed.load(Ordering::SeqCst) == 1).await;
        pool.stop().await;
        assert!(weak.lock().unwrap().as_ref().unwrap().upgrade().is_some());
        release.notify_one();
        until(|| done.load(Ordering::SeqCst)).await;
        until(|| weak.lock().unwrap().as_ref().unwrap().upgrade().is_none()).await;
        assert_eq!(pool.with_latest(|r| r.id), None);
        assert_eq!(pool.size(), 0);
    }

    #[tokio::test]
    async fn cancelled_exchange_can_poison_resource_independently_of_retain_policy() {
        struct Interrupted(Arc<Control>);
        impl Drop for Interrupted {
            fn drop(&mut self) {
                self.0.watch.notify_one();
            }
        }
        let control = Arc::new(Control::default());
        let pool = pool(&control, PoolConfig::default());
        let client = Client::from_pool(pool.clone());
        assert_eq!(client.identifier().await.unwrap(), 0);
        let active = Arc::new(Notify::new());
        let operation = tokio::spawn({
            let pool = pool.clone();
            let control = control.clone();
            let active = active.clone();
            async move {
                pool.execute(move |_| async move {
                    let _guard = Interrupted(control);
                    active.notify_one();
                    std::future::pending::<io::Result<()>>().await
                })
                .await
            }
        });
        active.notified().await;
        operation.abort();
        assert!(operation.await.unwrap_err().is_cancelled());
        until(|| control.created.load(Ordering::SeqCst) == 2).await;
        assert_eq!(client.identifier().await.unwrap(), 1);
        assert!(matches!(
            client.healthy_rejection().await,
            Err(Error::Operation(_))
        ));
        assert_eq!(client.identifier().await.unwrap(), 1);
        pool.stop().await;
    }
}
