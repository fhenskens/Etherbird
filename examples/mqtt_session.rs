//! Supervisor-based MQTT recovery. See examples/README.md and mqtt/session/.
#[path = "mqtt/common/docker.rs"]
mod docker;
#[path = "mqtt/session/mod.rs"]
mod implementation;
#[path = "mqtt/etherbird/lifecycle.rs"]
mod lifecycle;
#[path = "mqtt/common/protocol.rs"]
mod protocol;
#[path = "mqtt/common/fixture.rs"]
mod wire;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    implementation::run().await
}
