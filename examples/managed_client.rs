use etherbird::{Config, Lifecycle, Supervisor, async_trait};
#[cfg(feature = "pool")]
use etherbird::{Pool, PoolConfig};
use std::sync::atomic::{AtomicBool, Ordering};

struct Connection {
    connected: AtomicBool,
    prefix: std::sync::Mutex<String>,
    changed: tokio::sync::Notify,
}
impl Connection {
    async fn ping(&self, message: String) -> Result<String, std::io::Error> {
        if !self.connected.load(Ordering::Acquire) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "connection closed",
            ));
        }
        Ok(format!("{}: {message}", self.prefix.lock().unwrap()))
    }
}
struct Hooks;
#[async_trait]
impl Lifecycle for Hooks {
    type Resource = Connection;
    type Error = std::io::Error;
    async fn create(&self) -> Result<Connection, Self::Error> {
        Ok(Connection {
            connected: AtomicBool::new(false),
            prefix: std::sync::Mutex::new(String::new()),
            changed: tokio::sync::Notify::new(),
        })
    }
    async fn connect(&self, connection: &Connection) -> Result<(), Self::Error> {
        connection.connected.store(true, Ordering::Release);
        connection.changed.notify_waiters();
        Ok(())
    }
    async fn disconnect(&self, connection: &Connection) -> Result<(), Self::Error> {
        connection.connected.store(false, Ordering::Release);
        connection.changed.notify_waiters();
        Ok(())
    }
    fn is_connected(&self, connection: &Connection) -> bool {
        connection.connected.load(Ordering::Acquire)
    }
    async fn wait_connected(&self, connection: &Connection) {
        loop {
            let notification = connection.changed.notified();
            tokio::pin!(notification);
            notification.as_mut().enable();
            if self.is_connected(connection) {
                return;
            }
            notification.await;
        }
    }
}
etherbird::managed_client! {
    pub struct Client for Hooks {
        async fn ping(message: String) -> String;
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "pool")]
    {
        let pool = Pool::new(
            || Supervisor::new(Hooks, Config::default()),
            PoolConfig::default(),
        );
        let client = Client::from_pool(pool);
        client
            .managed
            .set_value("prefix", "pong".to_string(), |connection, value| {
                *connection.prefix.lock().unwrap() = value.clone();
            });
        assert_eq!(
            client.managed.value::<String>("prefix"),
            Some("pong".into())
        );
        println!("{}", client.ping("hello".into()).await?);
        client.managed.stop().await;
    }
    // This resource supports concurrent calls, so the same generated methods can
    // use direct supervision without exclusive leases or pool dispatch.
    let direct = Client::from_supervisor(Supervisor::new(Hooks, Config::default()));
    direct
        .managed
        .set_value("prefix", "direct pong".to_string(), |connection, value| {
            *connection.prefix.lock().unwrap() = value.clone();
        });
    println!("{}", direct.ping("hello".into()).await?);
    direct.managed.stop().await;
    Ok(())
}
