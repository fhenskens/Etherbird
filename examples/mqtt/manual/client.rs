//! Session coordination written by the application, without Etherbird.
use crate::common::protocol::{Message, Session};
use rumqttc::QoS;
use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{broadcast, mpsc, watch},
    task::JoinHandle,
    time::{sleep, timeout},
};

#[derive(Clone)]
struct Ready {
    generation: u64,
    session: Arc<Session>,
}

pub(crate) struct Client {
    ready: watch::Receiver<Option<Ready>>,
    snapshot: watch::Sender<Option<Ready>>,
    stopping: watch::Sender<bool>,
    recover: mpsc::UnboundedSender<u64>,
    manager: Mutex<Option<JoinHandle<()>>>,
    done: watch::Receiver<bool>,
}

impl Drop for Client {
    fn drop(&mut self) {
        self.stopping.send_replace(true);
    }
}

async fn stopped(stopping: &mut watch::Receiver<bool>) {
    while !*stopping.borrow_and_update() {
        if stopping.changed().await.is_err() {
            break;
        }
    }
}

fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "application client stopped")
}

impl Client {
    pub(crate) fn start(
        host: String,
        port: u16,
        budget: Duration,
    ) -> (Self, broadcast::Receiver<Message>) {
        let (messages, receiver) = broadcast::channel(64);
        let (snapshot, ready) = watch::channel(None);
        let client_snapshot = snapshot.clone();
        let (finished, done) = watch::channel(false);
        let (stopping, mut stop) = watch::channel(false);
        let (recover, mut requests) = mpsc::unbounded_channel();
        let manager = tokio::spawn(async move {
            let topics = crate::common::application::topics();
            let mut generation = 0;
            let mut delay = Duration::from_millis(100);
            loop {
                if *stop.borrow() {
                    break;
                }
                while requests.try_recv().is_ok() {} // Discard stale recovery requests.
                let session = Arc::new(Session::open(
                    host.clone(),
                    port,
                    format!("manual-{}", std::process::id()),
                    topics.clone(),
                    messages.clone(),
                ));
                let initialize = async {
                    timeout(budget, session.connected()).await??;
                    timeout(budget, session.setup(&topics)).await??;
                    Ok::<(), io::Error>(())
                };
                let initialized = tokio::select! {
                    biased;
                    _ = stopped(&mut stop) => false,
                    result = initialize => result.is_ok(),
                };
                if !initialized {
                    session.close().await;
                    tokio::select! {
                        biased;
                        _ = stopped(&mut stop) => break,
                        _ = sleep(delay) => {},
                    }
                    delay = delay.saturating_mul(2).min(Duration::from_secs(1));
                    continue;
                }
                delay = Duration::from_millis(100);
                generation += 1;
                snapshot.send_replace(Some(Ready {
                    generation,
                    session: session.clone(),
                }));
                loop {
                    tokio::select! {
                        biased;
                        _ = stopped(&mut stop) => break,
                        _ = session.disconnected() => break,
                        request = requests.recv() => {
                            if request.is_none() || request == Some(generation) { break; }
                            // A failure from an old operation cannot retire this session.
                        }
                    }
                }
                snapshot.send_replace(None); // Withdraw readiness before teardown.
                session.close().await;
            }
            snapshot.send_replace(None);
            finished.send_replace(true);
        });
        (
            Self {
                ready,
                snapshot: client_snapshot,
                stopping,
                recover,
                manager: Mutex::new(Some(manager)),
                done,
            },
            receiver,
        )
    }

    async fn acquire(&self) -> io::Result<Ready> {
        let mut ready = self.ready.clone();
        let mut stop = self.stopping.subscribe();
        loop {
            if *stop.borrow() {
                return Err(closed());
            }
            if let Some(session) = ready.borrow_and_update().clone() {
                return Ok(session);
            }
            tokio::select! {
                biased;
                _ = stopped(&mut stop) => return Err(closed()),
                result = ready.changed() => { result.map_err(|_| closed())?; }
            }
        }
    }

    // This is the same application method provided by the typed managed proxy.
    pub(crate) async fn publish(
        &self,
        topic: String,
        qos: QoS,
        retain: bool,
        payload: Vec<u8>,
    ) -> io::Result<()> {
        let ready = self.acquire().await?;
        let mut stop = self.stopping.subscribe();
        let result = tokio::select! {
            biased;
            _ = stopped(&mut stop) => return Err(closed()),
            result = ready.session.publish(topic, qos, retain, payload) => result,
        };
        if result.is_err() {
            self.snapshot.send_if_modified(|current| {
                if current
                    .as_ref()
                    .is_some_and(|current| current.generation == ready.generation)
                {
                    *current = None;
                    true
                } else {
                    false
                }
            });
            let _ = self.recover.send(ready.generation);
        }
        result
    }
}

impl Client {
    pub(crate) async fn ready(&self) -> io::Result<u64> {
        Ok(self.acquire().await?.generation)
    }
    pub(crate) fn generation(&self) -> Option<u64> {
        if *self.stopping.borrow() {
            return None;
        }
        self.ready.borrow().as_ref().map(|ready| ready.generation)
    }
    pub(crate) async fn close(&self) {
        self.stopping.send_replace(true);
        let manager = self.manager.lock().unwrap().take();
        if let Some(manager) = manager {
            let _ = manager.await;
        }
        let mut done = self.done.clone();
        while !*done.borrow_and_update() {
            if done.changed().await.is_err() {
                break;
            }
        }
    }
}

pub(crate) fn connect(
    host: String,
    port: u16,
    budget: Duration,
) -> (Client, broadcast::Receiver<Message>) {
    Client::start(host, port, budget)
}
