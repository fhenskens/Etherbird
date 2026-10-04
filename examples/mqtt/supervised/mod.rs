//! The same generated methods and lifecycle hooks, with direct supervision.
#[path = "../etherbird/lifecycle.rs"]
mod lifecycle;
#[path = "../etherbird/proxy.rs"]
mod proxy;
use crate::common::{
    application::{self, Application},
    protocol,
};
use etherbird::{Config, Error, SupervisedResourceProxy, Supervisor};
use lifecycle::Hooks;
use protocol::Message;
use rumqttc::QoS;
use std::{io, time::Duration};
use tokio::sync::broadcast;

pub(crate) type Client = proxy::Client<SupervisedResourceProxy<Hooks>>;
impl Application for Client {
    type Error = Error<io::Error>;
    async fn ready(&self) -> Result<u64, Self::Error> {
        self.managed.connected().await?;
        Ok(self.managed.current().ok_or(Error::Stopped)?.generation())
    }
    fn generation(&self) -> Option<u64> {
        self.managed.current().map(|resource| resource.generation())
    }
    async fn publish(
        &self,
        topic: String,
        qos: QoS,
        retain: bool,
        payload: Vec<u8>,
    ) -> Result<(), Self::Error> {
        Client::publish(self, topic, qos, retain, payload).await
    }
    async fn close(&self) {
        self.managed.stop().await;
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
    (Client::from_supervisor(supervisor), receiver)
}
