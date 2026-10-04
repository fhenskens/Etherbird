//! One shared device: urgent work overtakes telemetry, and overload is rejected.
//! Run: cargo run --example scheduling
use etherbird::{
    Config, Error, Lifecycle, OperationQueue, Pool, PoolConfig, PriorityQueue, QueueError,
    QueuedOperation, Supervisor, async_trait,
};
use std::{
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::time::timeout;

struct Device(Arc<Mutex<Vec<&'static str>>>);
struct Hooks(Arc<Mutex<Vec<&'static str>>>);
#[async_trait]
impl Lifecycle for Hooks {
    type Resource = Device;
    type Error = Infallible;
    async fn create(&self) -> Result<Device, Infallible> {
        Ok(Device(self.0.clone()))
    }
    async fn connect(&self, _: &Device) -> Result<(), Infallible> {
        Ok(())
    }
    async fn disconnect(&self, _: &Device) -> Result<(), Infallible> {
        Ok(())
    }
}

// Bound waiting work independently of the pool's connection limit. Lower priority
// numbers run first; the library's operation metadata preserves FIFO on ties.
struct BoundedPriority {
    queue: PriorityQueue<QueuedOperation<Hooks>>,
    accepted: Arc<AtomicUsize>,
}
impl OperationQueue<QueuedOperation<Hooks>> for BoundedPriority {
    fn push(&mut self, operation: QueuedOperation<Hooks>) -> Result<(), QueueError> {
        if self.queue.len() == 3 {
            return Err(QueueError::Full);
        }
        self.queue.push(operation)?;
        self.accepted.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn pop(&mut self) -> Option<QueuedOperation<Hooks>> {
        self.queue.pop()
    }
    fn len(&self) -> usize {
        self.queue.len()
    }
    fn retain(&mut self, predicate: &mut dyn FnMut(&QueuedOperation<Hooks>) -> bool) {
        self.queue.retain(predicate);
    }
}

async fn demonstrate() -> Result<(), Box<dyn std::error::Error>> {
    let order = Arc::new(Mutex::new(Vec::new()));
    let device = order.clone();
    let accepted = Arc::new(AtomicUsize::new(0));
    let pool = Pool::new_with_queue_factory(
        move || Supervisor::new(Hooks(device.clone()), Config::default()),
        PoolConfig {
            min_size: 1,
            max_size: 1,
            ..PoolConfig::default()
        },
        || BoundedPriority {
            queue: PriorityQueue::default(),
            accepted: accepted.clone(),
        },
    );
    let result = timeout(Duration::from_secs(5), async {
        // A maintenance lease holds the device while requests arrive.
        let maintenance = pool.borrow().await?;
        let mut callers = Vec::new();
        for (index, (name, priority)) in [("telemetry", 10), ("alarm", 0), ("control", 0)]
            .into_iter()
            .enumerate()
        {
            let client = pool.clone();
            callers.push(tokio::spawn(async move {
                client
                    .execute_with_priority(priority, move |device| async move {
                        device.0.lock().unwrap().push(name);
                        Ok(())
                    })
                    .await
            }));
            // Observe acceptance, so this demonstrates arrival order without sleeps.
            while accepted.load(Ordering::SeqCst) < index + 1 {
                tokio::task::yield_now().await;
            }
        }
        assert!(matches!(
            pool.execute(|_| async { Ok(()) }).await,
            Err(Error::Queue(QueueError::Full))
        ));
        assert!(order.lock().unwrap().is_empty());
        drop(maintenance);
        for caller in callers {
            caller.await??;
        }
        assert_eq!(*order.lock().unwrap(), ["alarm", "control", "telemetry"]);
        // Rejection does not poison capacity: a subsequent request still succeeds.
        pool.execute(|_| async { Ok(()) }).await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await;
    pool.stop().await;
    result??;
    println!("PASS: alarm, control, telemetry; FIFO priority ties; full queue rejects excess work");
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    demonstrate().await
}

#[tokio::test]
async fn priority_and_overload() {
    demonstrate().await.unwrap();
}
