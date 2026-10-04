//! The same lifecycle hooks with concurrent direct Supervisor::execute calls.
#[path = "../etherbird/lifecycle.rs"]
mod lifecycle;
use crate::common::{
    application::{self, Application},
    protocol,
};
use etherbird::{Config, Error, Supervisor};
use lifecycle::Hooks;
use protocol::Message;
use rumqttc::QoS;
use std::{
    io,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio::sync::broadcast;

pub(crate) struct Client {
    supervisor: Supervisor<Hooks>,
    closed: AtomicBool,
}
impl Application for Client {
    type Error = Error<io::Error>;
    async fn ready(&self) -> Result<u64, Self::Error> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        Ok(self.supervisor.acquire().await?.generation())
    }
    fn generation(&self) -> Option<u64> {
        self.supervisor
            .current()
            .map(|resource| resource.generation())
    }
    async fn publish(
        &self,
        topic: String,
        qos: QoS,
        retain: bool,
        payload: Vec<u8>,
    ) -> Result<(), Self::Error> {
        // A standalone supervisor is restartable. This application facade closes
        // permanently, like the manual client and pooled proxy in the comparison.
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        self.supervisor
            .execute(
                move |session| async move { session.publish(topic, qos, retain, payload).await },
            )
            .await
    }
    async fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.supervisor.stop().await;
    }
}

pub(crate) fn connect(
    host: String,
    port: u16,
    budget: Duration,
) -> (Client, broadcast::Receiver<Message>) {
    let (messages, receiver) = broadcast::channel(64);
    let supervisor = Supervisor::new(
        Hooks {
            host,
            port,
            client_id: format!("supervisor-{}", std::process::id()),
            topics: application::topics(),
            messages,
        },
        Config {
            resource_name: "MQTT comparison".into(),
            connect_timeout: budget,
            setup_timeout: budget,
            retry_delay: Duration::from_millis(100),
            max_retry_delay: Duration::from_secs(1),
            ..Config::default()
        },
    );
    supervisor.begin();
    (
        Client {
            supervisor,
            closed: AtomicBool::new(false),
        },
        receiver,
    )
}
