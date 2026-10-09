use etherbird::{
    Config, Error, Lifecycle, LifecycleFailurePolicy, SupervisedResourceProxy, Supervisor,
    async_trait,
};
use std::{
    future::pending,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
#[derive(Clone)]
struct Hooks {
    created: Arc<AtomicUsize>,
    slow: bool,
    reject: bool,
}
#[async_trait]
impl Lifecycle for Hooks {
    type Resource = ();
    type Error = io::Error;
    async fn create(&self) -> io::Result<()> {
        self.created.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn connect(&self, _: &()) -> io::Result<()> {
        if self.slow {
            pending::<()>().await;
        }
        if self.reject {
            return Err(io::Error::from(io::ErrorKind::PermissionDenied));
        }
        Ok(())
    }
    async fn disconnect(&self, _: &()) -> io::Result<()> {
        Ok(())
    }
    fn lifecycle_failure(&self, _: &io::Error) -> LifecycleFailurePolicy {
        LifecycleFailurePolicy::Fail
    }
}
fn supervisor(slow: bool, reject: bool) -> (Supervisor<Hooks>, Arc<AtomicUsize>) {
    let created = Arc::new(AtomicUsize::new(0));
    (
        Supervisor::new(
            Hooks {
                created: created.clone(),
                slow,
                reject,
            },
            Config::default(),
        ),
        created,
    )
}
struct Dropped(Arc<AtomicUsize>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
const LIMIT: Duration = Duration::from_millis(10);
#[tokio::test(start_paused = true)]
async fn zero_never_starts_and_stop_precedes_zero() {
    let (s, created) = supervisor(false, false);
    assert!(matches!(
        s.acquire_with_timeout(Duration::ZERO).await,
        Err(Error::Timeout)
    ));
    assert!(matches!(
        s.execute_with_timeout(Duration::ZERO, |_| async {
            panic!("must not run");
            #[allow(unreachable_code)]
            Ok::<(), io::Error>(())
        })
        .await,
        Err(Error::Timeout)
    ));
    assert_eq!(created.load(Ordering::SeqCst), 0);
    s.stop().await;
    assert!(matches!(
        s.acquire_with_timeout(Duration::ZERO).await,
        Err(Error::Stopped)
    ));
    assert!(matches!(
        s.execute_with_timeout(Duration::ZERO, |_| async { Ok(()) })
            .await,
        Err(Error::Stopped)
    ));
}
#[tokio::test(start_paused = true)]
async fn deadline_covers_readiness_and_preserves_lifecycle_cause() {
    let (s, _) = supervisor(true, false);
    assert!(matches!(
        s.acquire_with_timeout(LIMIT).await,
        Err(Error::Timeout)
    ));
    s.stop().await;
    let (s, _) = supervisor(false, true);
    assert!(
        matches!(s.execute_with_timeout(LIMIT, |_| async { Ok(()) }).await, Err(Error::Lifecycle(ref cause)) if cause.kind() == io::ErrorKind::PermissionDenied)
    );
    s.stop().await;
}
#[tokio::test(start_paused = true)]
async fn direct_expiry_drops_active_work_once() {
    let (s, _) = supervisor(false, false);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let drops = Arc::new(AtomicUsize::new(0));
    let dropped = drops.clone();
    assert!(matches!(
        s.execute_with_timeout(LIMIT, move |_| async move {
            count.fetch_add(1, Ordering::SeqCst);
            let _guard = Dropped(dropped);
            pending::<io::Result<()>>().await
        })
        .await,
        Err(Error::Timeout)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        s.execute_with_timeout(LIMIT, |_| async { Ok(42) })
            .await
            .unwrap(),
        42
    );
    s.stop().await;
}
#[tokio::test(start_paused = true)]
async fn direct_shutdown_beats_completion() {
    let (s, _) = supervisor(false, false);
    let stopper = s.clone();
    assert!(matches!(
        s.execute_with_timeout(LIMIT, move |_| async move {
            stopper.stop().await;
            Ok(42)
        })
        .await,
        Err(Error::Stopped)
    ));
    s.stop().await;
}
#[tokio::test(start_paused = true)]
async fn direct_proxy_timeout_and_permanent_shutdown() {
    let (s, _) = supervisor(true, false);
    let proxy = SupervisedResourceProxy::new(s);
    assert!(matches!(
        proxy.connected_with_timeout(LIMIT).await,
        Err(Error::Timeout)
    ));
    proxy.stop().await;
    assert!(matches!(
        proxy.connected_with_timeout(Duration::ZERO).await,
        Err(Error::Stopped)
    ));
    assert!(matches!(
        proxy
            .execute_with_timeout(LIMIT, |_| async { Ok(()) })
            .await,
        Err(Error::Stopped)
    ));
}
#[tokio::test(start_paused = true)]
async fn operation_error_preserves_cause_without_replay() {
    let (s, _) = supervisor(false, false);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    assert!(matches!(s.execute_with_timeout(LIMIT, move |_| async move {
        count.fetch_add(1, Ordering::SeqCst); Err::<(), _>(io::Error::from(io::ErrorKind::ConnectionReset))
    }).await, Err(Error::Operation(ref e)) if e.kind() == io::ErrorKind::ConnectionReset));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    s.stop().await;
}
#[cfg(feature = "pool")]
mod pooled {
    use super::*;
    use etherbird::{
        BoundedFifoQueue, ManagedResourceProxy, OperationQueue, Pool, PoolConfig, QueueError,
    };
    fn pool(capacity: usize, slow: bool, reject: bool) -> Pool<Hooks> {
        Pool::try_new_with_queue_factory(
            move || supervisor(slow, reject).0,
            PoolConfig::default(),
            move || BoundedFifoQueue::new(capacity),
        )
        .unwrap()
    }
    #[test]
    fn bounded_fifo_order_capacity_and_drop_contract() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut q = BoundedFifoQueue::new(2);
        assert_eq!(q.capacity(), 2);
        q.push((1, Dropped(dropped.clone()))).unwrap();
        q.push((2, Dropped(dropped.clone()))).unwrap();
        assert_eq!(q.push((3, Dropped(dropped.clone()))), Err(QueueError::Full));
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        q.retain(&mut |(id, _)| *id == 2);
        assert_eq!(dropped.load(Ordering::SeqCst), 2);
        q.push((4, Dropped(dropped.clone()))).unwrap();
        assert_eq!(q.pop().unwrap().0, 2);
        assert_eq!(q.pop().unwrap().0, 4);
        assert!(q.is_empty());
        q.push((5, Dropped(dropped.clone()))).unwrap();
        q.clear();
        assert_eq!(dropped.load(Ordering::SeqCst), 5);
        assert_eq!(BoundedFifoQueue::new(0).push(1), Err(QueueError::Full));
    }
    #[tokio::test(start_paused = true)]
    async fn queue_timeout_never_runs_late_and_reclaims_capacity() {
        let p = pool(1, false, false);
        let lease = p.borrow().await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        assert!(matches!(
            p.execute_with_timeout(LIMIT, move |_| async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await,
            Err(Error::Timeout)
        ));
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        drop(lease);
        assert_eq!(
            p.execute_with_priority_and_timeout(7, LIMIT, |_| async { Ok(42) })
                .await
                .unwrap(),
            42
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        p.stop().await;
    }
    #[tokio::test(start_paused = true)]
    async fn full_and_zero_capacity_preserve_admission_errors() {
        let p = pool(1, false, false);
        let lease = p.borrow().await.unwrap();
        let other = p.clone();
        let waiting = tokio::spawn(async move { other.execute(|_| async { Ok(()) }).await });
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            p.execute_with_timeout(LIMIT, |_| async { Ok(()) }).await,
            Err(Error::Queue(QueueError::Full))
        ));
        waiting.abort();
        let _ = waiting.await;
        drop(lease);
        p.stop().await;
        let p = pool(0, false, false);
        p.connected().await.unwrap();
        assert!(matches!(
            p.execute_with_timeout(LIMIT, |_| async { Ok(()) }).await,
            Err(Error::Queue(QueueError::Full))
        ));
        p.stop().await;
    }
    #[tokio::test(start_paused = true)]
    async fn active_expiry_is_cancelled_once_and_stop_drains_it() {
        let p = pool(1, false, false);
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let drops = Arc::new(AtomicUsize::new(0));
        let dropped = drops.clone();
        assert!(matches!(
            p.execute_with_timeout(LIMIT, move |_| async move {
                count.fetch_add(1, Ordering::SeqCst);
                let _guard = Dropped(dropped);
                pending::<io::Result<()>>().await
            })
            .await,
            Err(Error::Timeout)
        ));
        p.stop().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    #[tokio::test(start_paused = true)]
    async fn pool_readiness_lifecycle_and_shutdown_contract() {
        let p = pool(1, true, false);
        assert!(matches!(
            p.connected_with_timeout(LIMIT).await,
            Err(Error::Timeout)
        ));
        p.stop().await;
        assert!(matches!(
            p.connected_with_timeout(Duration::ZERO).await,
            Err(Error::Stopped)
        ));
        assert!(matches!(
            p.execute_with_timeout(Duration::ZERO, |_| async { Ok(()) })
                .await,
            Err(Error::Stopped)
        ));
        let p = pool(1, false, true);
        assert!(matches!(
            p.connected_with_timeout(LIMIT).await,
            Err(Error::Lifecycle(_))
        ));
        assert!(matches!(
            p.execute_with_timeout(LIMIT, |_| async { Ok(()) }).await,
            Err(Error::Lifecycle(_))
        ));
        p.stop().await;
    }
    #[tokio::test(start_paused = true)]
    async fn lease_and_managed_proxy_timeouts() {
        let p = pool(1, false, false);
        let lease = p.borrow().await.unwrap();
        assert_eq!(
            lease
                .execute_with_timeout(LIMIT, |_| async { Ok(42) })
                .await
                .unwrap(),
            42
        );
        assert!(matches!(
            lease
                .execute_with_timeout(LIMIT, |_| pending::<io::Result<()>>())
                .await,
            Err(Error::Timeout)
        ));
        drop(lease);
        let proxy = ManagedResourceProxy::new(p);
        proxy.connected_with_timeout(LIMIT).await.unwrap();
        assert_eq!(
            proxy
                .execute_with_timeout(LIMIT, |_| async { Ok(42) })
                .await
                .unwrap(),
            42
        );
        proxy.stop().await;
    }
}
