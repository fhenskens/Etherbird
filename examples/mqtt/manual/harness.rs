//! Administrative bridge for the common checks; not message-handling code.
use super::client::Client;
use crate::common::application::Application;
use rumqttc::QoS;
use std::io;

impl Application for Client {
    type Error = io::Error;
    async fn ready(&self) -> io::Result<u64> {
        Client::ready(self).await
    }
    fn generation(&self) -> Option<u64> {
        Client::generation(self)
    }
    async fn publish(
        &self,
        topic: String,
        qos: QoS,
        retain: bool,
        payload: Vec<u8>,
    ) -> io::Result<()> {
        Client::publish(self, topic, qos, retain, payload).await
    }
    async fn close(&self) {
        Client::close(self).await;
    }
}
