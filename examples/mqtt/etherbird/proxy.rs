//! Identical application methods for pooled and directly supervised clients.
use super::lifecycle::Hooks;
use rumqttc::QoS;

etherbird::managed_client! {
    pub(crate) struct Client for Hooks {
        async fn publish(topic: String, qos: QoS, retain: bool, payload: Vec<u8>) -> ();
    }
}
