//! Exercise the public macro and direct backend with and without pool support.
use etherbird::{Config, Error, Lifecycle, RetryPolicy, Supervisor, async_trait};
use std::{
    io,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Resource {
    id: usize,
    setting: AtomicUsize,
}
impl Resource {
    fn identifier(&self) -> io::Result<usize> {
        Ok(self.id)
    }
    async fn echo(&self, value: usize) -> io::Result<usize> {
        Ok(value)
    }
}
struct Hooks(Arc<AtomicUsize>);
#[async_trait]
impl Lifecycle for Hooks {
    type Resource = Resource;
    type Error = io::Error;
    async fn create(&self) -> io::Result<Resource> {
        Ok(Resource {
            id: self.0.fetch_add(1, Ordering::SeqCst),
            setting: AtomicUsize::new(0),
        })
    }
    async fn connect(&self, _: &Resource) -> io::Result<()> {
        Ok(())
    }
    async fn disconnect(&self, _: &Resource) -> io::Result<()> {
        Ok(())
    }
}
etherbird::managed_client! {
    struct Client for Hooks {
        fn identifier() -> usize;
        async fn echo(value: usize) -> usize;
    }
}

#[tokio::test]
async fn default_client_restores_configuration_and_stays_closed_after_retry() {
    let created = Arc::new(AtomicUsize::new(0));
    // An unparameterized Client must be directly supervised even with pool enabled.
    let client: Client = Client::new(Supervisor::new(
        Hooks(created.clone()),
        Config {
            retry_delay: Duration::from_millis(1),
            ..Config::default()
        },
    ));
    client.managed.set_value("setting", 17usize, |r, value| {
        r.setting.store(*value, Ordering::SeqCst);
    });
    assert_eq!(created.load(Ordering::SeqCst), 0);
    assert_eq!(client.managed.value::<usize>("setting"), Some(17));
    assert_eq!(client.identifier().await.unwrap(), 0);
    assert_eq!(client.echo(7).await.unwrap(), 7);
    let mut attempts = 0;
    let replacement = client
        .managed
        .execute_with_retry(
            RetryPolicy {
                max_attempts: NonZeroUsize::new(2).unwrap(),
                timeout: Duration::from_secs(2),
            },
            |r| {
                attempts += 1;
                let fail = attempts == 1;
                async move {
                    assert_eq!(r.setting.load(Ordering::SeqCst), 17);
                    if fail {
                        Err(io::Error::other("connection lost"))
                    } else {
                        Ok(r.id)
                    }
                }
            },
            |_| true,
        )
        .await
        .unwrap();
    assert_eq!(replacement, 1);
    let clone = client.clone();
    client.managed.stop().await;
    assert!(matches!(clone.echo(7).await, Err(Error::Stopped)));
    assert_eq!(created.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn default_client_allows_concurrent_caller_owned_operations() {
    let client: Client = Client::from_supervisor(Supervisor::new(
        Hooks(Arc::new(AtomicUsize::new(0))),
        Config::default(),
    ));
    let barrier = tokio::sync::Barrier::new(2);
    let operation = || {
        client.managed.execute(|_| async {
            barrier.wait().await;
            Ok(())
        })
    };
    let (first, second) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(operation(), operation())
    })
    .await
    .unwrap();
    first.unwrap();
    second.unwrap();
    client.managed.stop().await;
}

#[cfg(feature = "pool")]
#[tokio::test]
async fn same_declaration_can_explicitly_select_a_pool() {
    let client: Client<etherbird::ManagedResourceProxy<Hooks>> =
        Client::from_pool(etherbird::Pool::new(
            || Supervisor::new(Hooks(Arc::new(AtomicUsize::new(0))), Config::default()),
            etherbird::PoolConfig::default(),
        ));
    assert_eq!(client.echo(9).await.unwrap(), 9);
    client.managed.stop().await;
}
