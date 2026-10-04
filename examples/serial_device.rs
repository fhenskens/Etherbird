//! Supervise a serial instrument. See examples/README.md and serial_device/.
#[path = "serial_device/mod.rs"]
mod implementation;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    implementation::run().await
}
