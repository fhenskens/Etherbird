//! Supervise a newline-delimited instrument over a real tokio-serial port.
//! Run: cargo run --example serial_device -- COM3 115200 10
//! The device protocol and Unix pseudo-terminal demo are documented in docs/EXAMPLES.md.
use etherbird::{Config, Lifecycle, LifecycleFuture, async_trait};
use std::{io, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{Mutex, watch},
    time::{sleep, timeout},
};
use tokio_serial::SerialPortBuilderExt;

// Keeping byte I/O behind this factory also permits a pseudo-terminal fixture.
pub(super) trait PortIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> PortIo for T {}
pub(super) type Port = Box<dyn PortIo>;
pub(super) type OpenPort = Arc<dyn Fn() -> io::Result<Port> + Send + Sync>;

pub(super) struct Instrument {
    pub(super) port: Mutex<Option<Port>>,
    pub(super) connected: watch::Sender<bool>,
}

// A cancelled or timed-out exchange can leave a partial reply. Retire its port
// instead of allowing the next operation to consume that reply as its own.
struct Exchange<'a> {
    port: &'a mut Option<Port>,
    connected: &'a watch::Sender<bool>,
    completed: bool,
}
impl Drop for Exchange<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.port.take();
            self.connected.send_replace(false);
        }
    }
}
impl Instrument {
    pub(super) async fn request(&self, command: &[u8]) -> io::Result<String> {
        let mut port = self.port.lock().await;
        let mut exchange = Exchange {
            port: &mut port,
            connected: &self.connected,
            completed: false,
        };
        let result = timeout(Duration::from_millis(750), async {
            let port = exchange
                .port
                .as_mut()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "serial port closed"))?;
            port.write_all(command).await?;
            port.flush().await?;
            let mut line = Vec::new();
            loop {
                let byte = port.read_u8().await?;
                if byte == b'\n' {
                    return String::from_utf8(line)
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error));
                }
                if line.len() == 64 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "reply too long"));
                }
                if byte != b'\r' {
                    line.push(byte);
                }
            }
        })
        .await
        .unwrap_or_else(|_| {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "instrument reply deadline",
            ))
        });
        exchange.completed = result.is_ok();
        result
    }
    pub(super) async fn sample(&self) -> io::Result<u64> {
        self.request(b"SAMPLE\n")
            .await?
            .parse()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }
    async fn heartbeat(&self) -> io::Result<()> {
        if self.request(b"PING\n").await? != "PONG" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid heartbeat",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct Hooks {
    pub(super) open: OpenPort,
}
impl Hooks {
    pub(super) fn serial(path: String, baud: u32) -> Self {
        Self {
            open: Arc::new(move || {
                // Reapply the builder configuration on every new port.
                let port = tokio_serial::new(&path, baud)
                    .open_native_async()
                    .map_err(io::Error::other)?;
                Ok(Box::new(port))
            }),
        }
    }
}
#[async_trait]
impl Lifecycle for Hooks {
    type Resource = Instrument;
    type Error = io::Error;
    async fn create(&self) -> io::Result<Instrument> {
        Ok(Instrument {
            port: Mutex::new(None),
            connected: watch::channel(false).0,
        })
    }
    async fn connect(&self, instrument: &Instrument) -> io::Result<()> {
        *instrument.port.lock().await = Some((self.open)()?);
        instrument.connected.send_replace(true);
        Ok(())
    }
    async fn setup(&self, instrument: &Instrument) -> io::Result<()> {
        if instrument.request(b"HELLO\n").await? != "READY" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "instrument not ready",
            ));
        }
        tracing::info!("instrument handshake complete");
        Ok(())
    }
    async fn disconnect(&self, instrument: &Instrument) -> io::Result<()> {
        instrument.connected.send_replace(false);
        instrument.port.lock().await.take();
        Ok(())
    }
    async fn destroy(&self, instrument: &Instrument) -> io::Result<()> {
        self.disconnect(instrument).await
    }
    fn is_connected(&self, instrument: &Instrument) -> bool {
        *instrument.connected.borrow()
    }
    async fn wait_connected(&self, instrument: &Instrument) {
        let mut connected = instrument.connected.subscribe();
        let _ = connected.wait_for(|value| *value).await;
    }
    fn watch_disconnect(&self, instrument: Arc<Instrument>) -> Option<LifecycleFuture<io::Error>> {
        Some(Box::pin(async move {
            loop {
                sleep(Duration::from_secs(1)).await;
                instrument.heartbeat().await?;
            }
        }))
    }
    fn is_expected(&self, error: &io::Error) -> bool {
        error.kind() != io::ErrorKind::InvalidData
    }
}

pub(super) fn config() -> Config {
    Config {
        resource_name: "serial-instrument".into(),
        retry_delay: Duration::from_millis(200),
        max_retry_delay: Duration::from_secs(2),
        ..Config::default()
    }
}
