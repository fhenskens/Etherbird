//! MQTT protocol work shared by both coordinators. No Etherbird dependency.
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS, SubscribeReasonCode};
use std::{
    io,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{broadcast, watch},
    task::JoinHandle,
};

static CONNECTION: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub(crate) enum Status {
    Connecting,
    Connected,
    Ready,
    Failed(String),
}

#[derive(Clone, Debug)]
pub(crate) struct Message {
    pub(crate) topic: String,
    pub(crate) payload: Vec<u8>,
}

pub(crate) struct Session {
    pub(crate) client: AsyncClient,
    pub(crate) status: watch::Receiver<Status>,
    pub(crate) driver: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Some(driver) = self.driver.get_mut().unwrap().take() {
            driver.abort();
        }
    }
}

async fn wait_status(mut status: watch::Receiver<Status>, ready: bool) -> io::Result<()> {
    loop {
        match status.borrow_and_update().clone() {
            Status::Ready => return Ok(()),
            Status::Connected if !ready => return Ok(()),
            Status::Failed(reason) => return Err(io::Error::other(reason)),
            _ => {}
        }
        status
            .changed()
            .await
            .map_err(|_| io::Error::other("MQTT driver stopped"))?;
    }
}

impl Session {
    pub(crate) fn open(
        host: String,
        port: u16,
        client_id: String,
        topics: Vec<String>,
        messages: broadcast::Sender<Message>,
    ) -> Self {
        // An abandoned TCP attempt may deliver CONNECT late through a proxy.
        // Unique clean-session IDs prevent it from evicting the new connection.
        // This policy would be inappropriate for persistent MQTT sessions.
        let client_id = format!("{client_id}-{}", CONNECTION.fetch_add(1, Ordering::Relaxed));
        let mut options = MqttOptions::new(&client_id, &host, port);
        // Deliberate policy: this subscriber owns no persistent MQTT session or
        // outbound QoS state. Every connection installs the full desired set.
        options.set_clean_session(true);
        options.set_keep_alive(Duration::from_secs(5));
        let (client, mut events) = AsyncClient::new(options, 16);
        let (status_tx, status) = watch::channel(Status::Connecting);
        let messages = messages.clone();
        let expected = topics.len();
        let driver = tokio::spawn(async move {
            loop {
                match events.poll().await {
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        status_tx.send_replace(Status::Connected);
                    }
                    Ok(Event::Incoming(Packet::SubAck(ack))) => {
                        // Only setup submits subscriptions: one batch, one SUBACK.
                        if ack.return_codes.len() != expected
                            || ack
                                .return_codes
                                .iter()
                                .any(|code| matches!(code, SubscribeReasonCode::Failure))
                        {
                            status_tx.send_replace(Status::Failed(
                                "broker rejected subscriptions".into(),
                            ));
                            break;
                        }
                        status_tx.send_replace(Status::Ready);
                    }
                    Ok(Event::Incoming(Packet::Publish(message))) => {
                        // Broadcast never blocks the event loop. Slow consumers
                        // receive an explicit Lagged error instead of silent loss.
                        let _ = messages.send(Message {
                            topic: message.topic,
                            payload: message.payload.to_vec(),
                        });
                    }
                    Ok(_) => {}
                    Err(error) => {
                        status_tx.send_replace(Status::Failed(error.to_string()));
                        // The selected coordinator owns clean-session replacement.
                        // Continuing poll would start another reconnect loop.
                        break;
                    }
                }
            }
        });
        Session {
            client,
            status,
            driver: Mutex::new(Some(driver)),
        }
    }
    pub(crate) async fn connected(&self) -> io::Result<()> {
        wait_status(self.status.clone(), false).await
    }
    pub(crate) async fn setup(&self, topics: &[String]) -> io::Result<()> {
        self.client
            .subscribe_many(
                topics
                    .iter()
                    .map(|topic| rumqttc::SubscribeFilter::new(topic.clone(), QoS::AtLeastOnce)),
            )
            .await
            .map_err(io::Error::other)?;
        wait_status(self.status.clone(), true).await
    }
    pub(crate) async fn disconnected(&self) -> io::Result<()> {
        let mut status = self.status.clone();
        loop {
            if let Status::Failed(reason) = status.borrow_and_update().clone() {
                return Err(io::Error::other(reason));
            }
            status
                .changed()
                .await
                .map_err(|_| io::Error::other("MQTT driver stopped"))?;
        }
    }
    pub(crate) async fn publish(
        &self,
        topic: String,
        qos: QoS,
        retain: bool,
        payload: Vec<u8>,
    ) -> io::Result<()> {
        self.client
            .publish(topic, qos, retain, payload)
            .await
            .map_err(io::Error::other)
    }
    pub(crate) async fn close(&self) {
        let driver = self.driver.lock().unwrap().take();
        if let Some(driver) = driver {
            driver.abort();
            let _ = driver.await;
        }
    }
}
