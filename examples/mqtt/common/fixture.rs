//! Minimal MQTT peer shared by the comparison scenarios.
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, watch},
    task::JoinHandle,
    time::timeout,
};

pub(crate) struct Peer {
    pub(crate) address: SocketAddr,
    task: JoinHandle<()>,
    pub(crate) acknowledgements: Arc<Semaphore>,
    pub(crate) subscriptions: watch::Receiver<usize>,
    pub(crate) published: watch::Receiver<usize>,
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Peer {
    pub(crate) async fn start(address: SocketAddr, reject: bool) -> io::Result<Self> {
        let listener = TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        let acknowledgements = Arc::new(Semaphore::new(0));
        let permits = acknowledgements.clone();
        let (subscriptions_tx, subscriptions) = watch::channel(0);
        let (published_tx, published) = watch::channel(0);
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                // One subscriber and one session at a time, just like the example.
                let _ = serve(
                    &mut stream,
                    &permits,
                    reject,
                    &subscriptions_tx,
                    &published_tx,
                )
                .await;
            }
        });
        Ok(Self {
            address,
            task,
            acknowledgements,
            subscriptions,
            published,
        })
    }

    pub(crate) async fn subscribed(&self) -> io::Result<()> {
        let mut subscriptions = self.subscriptions.clone();
        timeout(Duration::from_secs(5), async {
            while *subscriptions.borrow_and_update() == 0 {
                subscriptions.changed().await.map_err(io::Error::other)?;
            }
            Ok(())
        })
        .await?
    }

    pub(crate) async fn stop(mut self) {
        self.task.abort();
        // Await cancellation so the listener and accepted socket are closed
        // before another peer binds the same port.
        let _ = (&mut self.task).await;
    }
}

async fn packet(stream: &mut TcpStream) -> io::Result<(u8, Vec<u8>)> {
    let header = stream.read_u8().await?;
    let mut length = 0usize;
    let mut multiplier = 1;
    for _ in 0..4 {
        let byte = stream.read_u8().await?;
        length += usize::from(byte & 127) * multiplier;
        if byte & 128 == 0 {
            if length > 4096 {
                return Err(io::Error::other("fixture packet too large"));
            }
            let mut body = vec![0; length];
            stream.read_exact(&mut body).await?;
            return Ok((header, body));
        }
        multiplier *= 128;
    }
    Err(io::Error::other("invalid remaining length"))
}

async fn serve(
    stream: &mut TcpStream,
    permits: &Semaphore,
    reject: bool,
    subscriptions: &watch::Sender<usize>,
    published: &watch::Sender<usize>,
) -> io::Result<()> {
    let (header, connect) = packet(stream).await?;
    if header != 0x10 || connect.len() < 10 || connect[7] & 2 == 0 {
        return Err(io::Error::other("expected clean-session CONNECT"));
    }
    stream.write_all(&[0x20, 2, 0, 0]).await?; // CONNACK, no stored session
    let (header, subscribe) = packet(stream).await?;
    if header != 0x82 || subscribe.len() < 2 {
        return Err(io::Error::other("expected SUBSCRIBE"));
    }
    let mut offset = 2;
    let mut topics = Vec::new();
    while offset < subscribe.len() {
        if offset + 2 > subscribe.len() {
            return Err(io::Error::other("short topic"));
        }
        let length = usize::from(u16::from_be_bytes([
            subscribe[offset],
            subscribe[offset + 1],
        ]));
        offset += 2;
        if offset + length >= subscribe.len() {
            return Err(io::Error::other("short filter"));
        }
        topics.push(subscribe[offset..offset + length].to_vec());
        offset += length + 1; // filter plus requested QoS
    }
    if topics
        != [
            b"etherbird/temperature".to_vec(),
            b"etherbird/humidity".to_vec(),
        ]
    {
        return Err(io::Error::other("desired subscriptions were not restored"));
    }
    subscriptions.send_modify(|count| *count += 1);
    permits.acquire().await.map_err(io::Error::other)?.forget();
    stream
        .write_all(&[
            0x90,
            4,
            subscribe[0],
            subscribe[1],
            if reject { 0x80 } else { 1 },
            1,
        ])
        .await?;
    if !reject {
        for topic in topics {
            // Fixture delivery is QoS 0, which is allowed for a QoS 1 subscription.
            let mut publish = vec![0x30, (2 + topic.len() + 2) as u8];
            publish.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            publish.extend_from_slice(&topic);
            publish.extend_from_slice(b"42");
            stream.write_all(&publish).await?;
        }
    }
    loop {
        let (header, _) = packet(stream).await?;
        match header {
            0xc0 => stream.write_all(&[0xd0, 0]).await?, // PINGRESP
            0xe0 => return Ok(()),
            0x30 => {
                published.send_modify(|count| *count += 1);
            }
            _ => return Err(io::Error::other("unexpected fixture request")),
        }
    }
}
