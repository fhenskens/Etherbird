//! Administrative bridge for the common checks; not message-handling code.
use super::client::Client;
use crate::common::application::Application;
use rumqttc::QoS;
use std::io;

// Test instrumentation and administration. Message handling in scenario.rs calls
// client.publish(...) with no acquisition or recovery callbacks in either version.
impl Application for Client {
    type Error = etherbird::Error<io::Error>;
    async fn ready(&self) -> Result<u64, Self::Error> {
        self.managed.connected().await?;
        let lease = self.managed.pool.borrow().await?;
        Ok(lease.supervisor().acquire().await?.generation())
    }
    fn generation(&self) -> Option<u64> {
        self.managed
            .pool
            .supervisors()
            .iter()
            .find_map(|supervisor| supervisor.current().map(|handle| handle.generation()))
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
