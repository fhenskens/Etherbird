//! Modbus TCP and WebSocket recovery. See examples/README.md and live_recovery/.
#[path = "live_recovery/mod.rs"]
mod implementation;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    implementation::run().await
}
