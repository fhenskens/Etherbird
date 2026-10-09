// Separate executable: thread-local tracing capture is isolated from other tests.
use etherbird::{Config, Lifecycle, Supervisor, async_trait};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
#[derive(Debug)]
struct Failure;
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("connection lost")
    }
}
impl std::error::Error for Failure {}
#[derive(Default)]
struct Control {
    expected: AtomicBool,
    failures: AtomicUsize,
    non_retryable: AtomicBool,
}
struct Manager(Arc<Control>);
#[async_trait]
impl Lifecycle for Manager {
    type Resource = ();
    type Error = Failure;
    async fn create(&self) -> Result<(), Failure> {
        Ok(())
    }
    async fn connect(&self, _: &()) -> Result<(), Failure> {
        let mut remaining = self.0.failures.load(Ordering::SeqCst);
        while let Some(next) = remaining.checked_sub(1) {
            match self.0.failures.compare_exchange(
                remaining,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Err(Failure),
                Err(actual) => remaining = actual,
            }
        }
        Ok(())
    }
    async fn disconnect(&self, _: &()) -> Result<(), Failure> {
        Ok(())
    }
    fn is_expected(&self, _: &Failure) -> bool {
        self.0.expected.load(Ordering::SeqCst)
    }
    fn lifecycle_failure(&self, _: &Failure) -> etherbird::LifecycleFailurePolicy {
        if self.0.non_retryable.load(Ordering::SeqCst) {
            etherbird::LifecycleFailurePolicy::Fail
        } else {
            etherbird::LifecycleFailurePolicy::Retry
        }
    }
}

#[tokio::test]
async fn non_retryable_causes_are_not_formatted_in_framework_diagnostics() {
    let output = LogCapture(Arc::new(Mutex::new(Vec::new())));
    let writer = output.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || writer.clone())
        .finish();
    let _subscriber = tracing::subscriber::set_default(subscriber);
    let control = Arc::new(Control::default());
    control.non_retryable.store(true, Ordering::SeqCst);
    control.failures.store(1, Ordering::SeqCst);
    control.expected.store(true, Ordering::SeqCst);
    let sup = Supervisor::start(Manager(control), config());
    assert!(matches!(
        sup.acquire().await,
        Err(etherbird::Error::Lifecycle(_))
    ));
    sup.stop().await;
    let log = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
    assert!(log.contains("non-retryable lifecycle failure"));
    assert!(!log.contains("connection lost"));
    assert!(!log.contains("Failure"));
    assert!(!log.contains("retrying connection"));
    assert!(!log.contains("ERROR"));
    assert!(log.contains("WARN"));
    assert!(log.contains("DEBUG"));
}
fn config() -> Config {
    Config {
        retry_delay: Duration::from_millis(1),
        ..Config::default()
    }
}
#[derive(Clone)]
struct LogCapture(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for LogCapture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
#[tokio::test]
async fn expected_errors_use_warning_and_debug_with_resource_name() {
    for expected in [true, false] {
        for resource_name in ["Resource", "MQTT", "Modbus TCP", "Port %s"] {
            let output = LogCapture(Arc::new(Mutex::new(Vec::new())));
            let writer = output.clone();
            let subscriber = tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_max_level(tracing::Level::DEBUG)
                .with_writer(move || writer.clone())
                .finish();
            let _subscriber = tracing::subscriber::set_default(subscriber);
            let c = Arc::new(Control::default());
            c.expected.store(expected, Ordering::SeqCst);
            c.failures.store(1, Ordering::SeqCst);
            let s = Supervisor::start(
                Manager(c),
                Config {
                    resource_name: resource_name.into(),
                    ..config()
                },
            );
            s.acquire().await.unwrap();
            s.stop().await;
            let log = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
            assert!(log.contains(resource_name));
            assert!(log.contains("connection lost"));
            assert!(log.contains("connected"));
            assert!(log.contains("retrying connection"));
            assert!(log.contains("retry_delay"));
            let failures: Vec<_> = log
                .lines()
                .filter(|line| line.contains("hook=\"connect\""))
                .collect();
            if expected {
                assert_eq!(
                    failures.iter().filter(|line| line.contains("WARN")).count(),
                    1
                );
                assert_eq!(
                    failures
                        .iter()
                        .filter(|line| line.contains("DEBUG") && line.contains("Failure"))
                        .count(),
                    1
                );
                assert!(!log.contains("ERROR"));
            } else {
                assert_eq!(failures.len(), 1);
                assert!(failures[0].contains("ERROR"));
            }
        }
    }
}
