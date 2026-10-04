//! Isolated loopback endpoints owned exclusively by the live check.
use futures_util::{SinkExt, StreamExt};
use std::{io, net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};
use tokio_tungstenite::tungstenite::Message;
#[derive(Default)]
pub struct DeviceState {
    pub paused: std::sync::atomic::AtomicBool,
    pub block_probe: std::sync::atomic::AtomicBool,
    pub received: std::sync::atomic::AtomicUsize,
    pub blocked: std::sync::atomic::AtomicUsize,
    pub sequence: std::sync::atomic::AtomicU16,
}
pub struct ModbusServer {
    pub address: SocketAddr,
    pub state: Arc<DeviceState>,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<io::Result<()>>>,
}
impl ModbusServer {
    pub async fn start(address: SocketAddr, state: Arc<DeviceState>) -> io::Result<Self> {
        let listener = TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        let (stop, mut stopping) = watch::channel(false);
        let device = state.clone();
        let task = tokio::spawn(async move {
            let mut clients = JoinSet::new();
            loop {
                tokio::select! {
                    _ = stopping.changed() => break,
                    accepted = listener.accept() => {
                        let (stream, _) = accepted?;
                        clients.spawn(serve_modbus(stream, device.clone()));
                    }
                    result = clients.join_next(), if !clients.is_empty() => {
                        if let Some(Err(error)) = result {
                            return Err(io::Error::other(error));
                        }
                    }
                }
            }
            drop(listener);
            clients.abort_all();
            while clients.join_next().await.is_some() {}
            Ok(())
        });
        tracing::info!(%address, "Modbus TCP device started");
        Ok(Self {
            address,
            state,
            stop,
            task: Some(task),
        })
    }
    pub async fn down(&mut self) -> io::Result<()> {
        self.stop.send_replace(true);
        if let Some(task) = self.task.take() {
            task.await??;
        }
        tracing::info!(address = %self.address, "Modbus listener and accepted sockets closed");
        Ok(())
    }
    pub async fn up(&mut self) -> io::Result<()> {
        self.state
            .paused
            .store(false, std::sync::atomic::Ordering::SeqCst);
        self.state
            .block_probe
            .store(false, std::sync::atomic::Ordering::SeqCst);
        *self = Self::start(self.address, self.state.clone()).await?;
        Ok(())
    }
}
impl Drop for ModbusServer {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
async fn serve_modbus(mut stream: TcpStream, state: Arc<DeviceState>) -> io::Result<()> {
    use std::sync::atomic::Ordering::SeqCst;
    loop {
        let mut header = [0; 7];
        stream.read_exact(&mut header).await?;
        let length = u16::from_be_bytes([header[4], header[5]]) as usize;
        if !(2..=254).contains(&length) || header[2..4] != [0, 0] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid MBAP header",
            ));
        }
        let mut pdu = vec![0; length - 1];
        stream.read_exact(&mut pdu).await?;
        state.received.fetch_add(1, SeqCst);
        if pdu.len() == 5 && pdu[0] == 3 && pdu[1..3] == [0, 1] && state.block_probe.load(SeqCst) {
            state.blocked.fetch_add(1, SeqCst);
            std::future::pending::<()>().await;
        }
        if state.paused.load(SeqCst) {
            std::future::pending::<()>().await;
        }
        let response = if pdu.len() == 5
            && pdu[0] == 3
            && (pdu[1..3] == [0, 0] || pdu[1..3] == [0, 1])
            && u16::from_be_bytes([pdu[3], pdu[4]]) == 3
        {
            let sequence = state.sequence.fetch_add(1, SeqCst).wrapping_add(1);
            let mut response = vec![3, 6];
            for value in [sequence, 1000, 2000] {
                response.extend_from_slice(&value.to_be_bytes());
            }
            response
        } else {
            vec![pdu[0] | 0x80, 2]
        };
        header[4..6].copy_from_slice(&((response.len() + 1) as u16).to_be_bytes());
        stream.write_all(&header).await?;
        stream.write_all(&response).await?;
    }
}

#[derive(Default)]
pub struct FeedState {
    pub blocked: std::sync::atomic::AtomicUsize,
    pub subscriptions: std::sync::atomic::AtomicUsize,
    sequence: std::sync::atomic::AtomicUsize,
}
pub struct WebSocketServer {
    pub address: SocketAddr,
    pub state: Arc<FeedState>,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<io::Result<()>>>,
}
impl WebSocketServer {
    pub async fn start(address: SocketAddr, state: Arc<FeedState>) -> io::Result<Self> {
        let listener = TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        let (stop, mut stopping) = watch::channel(false);
        let feed = state.clone();
        let task = tokio::spawn(async move {
            let mut clients = JoinSet::new();
            loop {
                tokio::select! {
                    _ = stopping.changed() => break,
                    accepted = listener.accept() => {
                        let (stream, _) = accepted?;
                        clients.spawn(serve_websocket(stream, feed.clone()));
                    }
                    result = clients.join_next(), if !clients.is_empty() => {
                        if let Some(Err(error)) = result {
                            return Err(io::Error::other(error));
                        }
                    }
                }
            }
            drop(listener);
            clients.abort_all();
            while clients.join_next().await.is_some() {}
            Ok(())
        });
        Ok(Self {
            address,
            state,
            stop,
            task: Some(task),
        })
    }
    pub fn url(&self) -> String {
        format!("ws://{}/samples", self.address)
    }
    pub async fn down(&mut self) -> io::Result<()> {
        self.stop.send_replace(true);
        if let Some(task) = self.task.take() {
            task.await??;
        }
        Ok(())
    }
    pub async fn up(&mut self) -> io::Result<()> {
        *self = Self::start(self.address, self.state.clone()).await?;
        Ok(())
    }
}
impl Drop for WebSocketServer {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
async fn serve_websocket(stream: TcpStream, state: Arc<FeedState>) -> io::Result<()> {
    use std::sync::atomic::Ordering::SeqCst;
    let mut socket = tokio_tungstenite::accept_async(stream)
        .await
        .map_err(io::Error::other)?;
    let mut subscribed = false;
    while let Some(message) = socket.next().await {
        let response = match message.map_err(io::Error::other)? {
            Message::Text(command) => match command.as_str() {
                "SUBSCRIBE samples" => {
                    subscribed = true;
                    state.subscriptions.fetch_add(1, SeqCst);
                    0
                }
                "HEARTBEAT" if subscribed => 0,
                "SAMPLE" if subscribed => state.sequence.fetch_add(1, SeqCst) + 1,
                "BLOCK" if subscribed => {
                    state.blocked.fetch_add(1, SeqCst);
                    std::future::pending::<usize>().await
                }
                _ => {
                    socket.close(None).await.map_err(io::Error::other)?;
                    return Ok(());
                }
            },
            Message::Ping(_) => {
                socket.flush().await.map_err(io::Error::other)?;
                continue;
            }
            Message::Pong(_) => continue,
            Message::Close(_) => return Ok(()),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected frame",
                ));
            }
        };
        socket
            .send(Message::Text(response.to_string().into()))
            .await
            .map_err(io::Error::other)?;
    }
    Ok(())
}
