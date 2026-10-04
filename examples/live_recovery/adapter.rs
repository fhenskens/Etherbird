//! Real dependency clients; Etherbird owns their replacement and retry policy.
use etherbird::{Lifecycle, LifecycleFuture, async_trait};
use futures_util::{SinkExt, StreamExt};
use std::{
    collections::HashSet,
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Mutex, watch},
    time::{sleep, timeout},
};
use tokio_modbus::prelude::*;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

fn modbus_error(error: tokio_modbus::Error) -> io::Error {
    match error {
        tokio_modbus::Error::Transport(error) => error,
        error => io::Error::new(io::ErrorKind::InvalidData, error),
    }
}

#[derive(Clone)]
pub enum Endpoint {
    WebSocket(String),
    Modbus(SocketAddr),
}
impl Endpoint {
    pub fn name(&self) -> &'static str {
        match self {
            Self::WebSocket(_) => "websocket",
            Self::Modbus(_) => "modbus",
        }
    }
}
enum Transport {
    WebSocket(Box<Socket>),
    Modbus(tokio_modbus::client::Context),
}
pub struct Resource {
    pub id: usize,
    endpoint: Endpoint,
    transport: Mutex<Option<Transport>>,
    connected: watch::Sender<bool>,
}

// Dropping a request with an outstanding reply invalidates this session.
// This also covers caller cancellation, which does not return an operation error.
struct Exchange<'a> {
    transport: &'a mut Option<Transport>,
    connected: &'a watch::Sender<bool>,
    completed: bool,
}
impl Drop for Exchange<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.transport.take();
            self.connected.send_replace(false);
        }
    }
}
impl Resource {
    pub async fn sample(&self) -> io::Result<u64> {
        self.transact(false).await
    }
    async fn transact(&self, heartbeat: bool) -> io::Result<u64> {
        // Lease/heartbeat contention is not a dead peer: start the response
        // deadline only once this call owns the transport.
        let mut transport = self.transport.lock().await;
        let mut exchange = Exchange {
            transport: &mut transport,
            connected: &self.connected,
            completed: false,
        };
        let result = timeout(Duration::from_millis(750), async {
            match exchange.transport.as_mut() {
                Some(Transport::WebSocket(socket)) => {
                    websocket_request(socket, if heartbeat { "HEARTBEAT" } else { "SAMPLE" }).await
                }
                Some(Transport::Modbus(context)) => {
                    let values = context
                        .read_holding_registers(0, 3)
                        .await
                        .map_err(modbus_error)?
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                    if values.len() != 3 || values[1..] != [1000, 2000] {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid device values",
                        ));
                    }
                    Ok(u64::from(values[0]))
                }
                None => Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "resource closed",
                )),
            }
        })
        .await
        .unwrap_or_else(|_| {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "protocol response deadline",
            ))
        });
        exchange.completed = result.is_ok();
        result
    }
    pub async fn blocking_websocket(&self) -> io::Result<()> {
        let mut transport = self.transport.lock().await;
        let mut exchange = Exchange {
            transport: &mut transport,
            connected: &self.connected,
            completed: false,
        };
        let Some(Transport::WebSocket(socket)) = exchange.transport.as_mut() else {
            return Err(io::Error::new(io::ErrorKind::NotConnected, "not WebSocket"));
        };
        // The fixture confirms receipt, then withholds its reply until the outage.
        let result = websocket_request(socket, "BLOCK").await;
        exchange.completed = result.is_ok();
        result.map(|_| ())
    }
    pub async fn blocking_modbus(&self) -> io::Result<()> {
        let mut transport = self.transport.lock().await;
        let mut exchange = Exchange {
            transport: &mut transport,
            connected: &self.connected,
            completed: false,
        };
        let Some(Transport::Modbus(context)) = exchange.transport.as_mut() else {
            return Err(io::Error::new(io::ErrorKind::NotConnected, "not Modbus"));
        };
        let result = context
            .read_holding_registers(1, 3)
            .await
            .map_err(modbus_error)
            .and_then(|result| {
                result.map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
            });
        exchange.completed = result.is_ok();
        result.map(|_| ())
    }
}

async fn websocket_request(socket: &mut Socket, request: &str) -> io::Result<u64> {
    socket
        .send(Message::Text(request.to_owned().into()))
        .await
        .map_err(io::Error::other)?;
    while let Some(message) = socket.next().await {
        match message.map_err(io::Error::other)? {
            Message::Text(value) => {
                return value
                    .parse()
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error));
            }
            Message::Ping(_) => socket.flush().await.map_err(io::Error::other)?,
            Message::Pong(_) => {}
            Message::Close(_) => break,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected WebSocket frame",
                ));
            }
        }
    }
    Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "WebSocket closed",
    ))
}
#[derive(Clone)]
pub struct Hooks {
    pub endpoint: Endpoint,
    pub created: Arc<AtomicUsize>,
    pub attempts: Arc<AtomicUsize>,
    pub destroyed: Arc<AtomicUsize>,
    destroyed_ids: Arc<StdMutex<HashSet<usize>>>,
}
impl Hooks {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            created: Arc::new(AtomicUsize::new(0)),
            attempts: Arc::new(AtomicUsize::new(0)),
            destroyed: Arc::new(AtomicUsize::new(0)),
            destroyed_ids: Arc::default(),
        }
    }
}
#[async_trait]
impl Lifecycle for Hooks {
    type Resource = Resource;
    type Error = io::Error;
    async fn create(&self) -> io::Result<Resource> {
        let id = self.created.fetch_add(1, Ordering::SeqCst);
        tracing::debug!(resource = self.endpoint.name(), id, hook = "create");
        let (connected, _) = watch::channel(false);
        Ok(Resource {
            id,
            endpoint: self.endpoint.clone(),
            transport: Mutex::new(None),
            connected,
        })
    }
    async fn connect(&self, resource: &Resource) -> io::Result<()> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        tracing::info!(
            resource = self.endpoint.name(),
            id = resource.id,
            hook = "connect"
        );
        let transport = match &resource.endpoint {
            Endpoint::WebSocket(url) => Transport::WebSocket(Box::new(
                tokio_tungstenite::connect_async(url.as_str())
                    .await
                    .map_err(io::Error::other)?
                    .0,
            )),
            Endpoint::Modbus(address) => Transport::Modbus(tcp::attach(
                super::transport::ModbusTransport(tokio::net::TcpStream::connect(*address).await?),
            )),
        };
        *resource.transport.lock().await = Some(transport);
        resource.connected.send_replace(true);
        Ok(())
    }
    async fn setup(&self, resource: &Resource) -> io::Result<()> {
        // Subscription state is session-local: restore it on every replacement.
        if let Some(Transport::WebSocket(socket)) = resource.transport.lock().await.as_mut() {
            websocket_request(socket, "SUBSCRIBE samples").await?;
        }
        resource.transact(true).await?;
        tracing::info!(
            resource = self.endpoint.name(),
            id = resource.id,
            hook = "setup",
            "ready"
        );
        Ok(())
    }
    async fn cleanup(&self, resource: &Resource) -> io::Result<()> {
        tracing::debug!(
            resource = self.endpoint.name(),
            id = resource.id,
            hook = "cleanup"
        );
        Ok(())
    }
    async fn disconnect(&self, resource: &Resource) -> io::Result<()> {
        resource.connected.send_replace(false);
        let transport = resource.transport.lock().await.take();
        match transport {
            Some(Transport::Modbus(mut context)) => {
                context.disconnect().await.map_err(io::Error::other)?
            }
            Some(Transport::WebSocket(mut socket)) => socket
                .as_mut()
                .close(None)
                .await
                .map_err(io::Error::other)?,
            None => {}
        }
        tracing::debug!(
            resource = self.endpoint.name(),
            id = resource.id,
            hook = "disconnect"
        );
        Ok(())
    }
    async fn destroy(&self, resource: &Resource) -> io::Result<()> {
        resource.connected.send_replace(false);
        resource.transport.lock().await.take();
        // A late successful connect can need another teardown of an already
        // discarded resource. Count resources, rather than hook invocations.
        if self.destroyed_ids.lock().unwrap().insert(resource.id) {
            self.destroyed.fetch_add(1, Ordering::SeqCst);
        }
        tracing::debug!(
            resource = self.endpoint.name(),
            id = resource.id,
            hook = "destroy"
        );
        Ok(())
    }
    fn is_connected(&self, resource: &Resource) -> bool {
        *resource.connected.borrow()
    }
    async fn wait_connected(&self, resource: &Resource) {
        let mut connected = resource.connected.subscribe();
        let _ = connected.wait_for(|value| *value).await;
    }
    fn watch_disconnect(&self, resource: Arc<Resource>) -> Option<LifecycleFuture<io::Error>> {
        Some(Box::pin(async move {
            loop {
                sleep(Duration::from_millis(300)).await;
                // Protocol heartbeat detects dead sockets even when there are no callers.
                resource.transact(true).await?;
            }
        }))
    }
    fn is_expected(&self, error: &io::Error) -> bool {
        error.kind() != io::ErrorKind::InvalidData
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn repeated_teardown_counts_each_resource_once() {
        let hooks = Hooks::new(Endpoint::Modbus("127.0.0.1:1".parse().unwrap()));
        let first = hooks.create().await.unwrap();
        let second = hooks.create().await.unwrap();
        hooks.destroy(&first).await.unwrap();
        hooks.destroy(&first).await.unwrap();
        assert_eq!(hooks.created.load(Ordering::SeqCst), 2);
        assert_eq!(
            hooks.destroyed.load(Ordering::SeqCst),
            1,
            "repeated teardown must not hide an undestroyed resource"
        );
        hooks.destroy(&second).await.unwrap();
        assert_eq!(hooks.destroyed.load(Ordering::SeqCst), 2);
    }
}
