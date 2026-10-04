//! MQTT session comparison. See mqtt/README.md for the common and implementation code.
#[path = "mqtt/common/mod.rs"]
mod common;
#[path = "mqtt/etherbird/mod.rs"]
mod implementation;

#[tokio::main]
async fn main() -> common::scenario::Result<()> {
    implementation::run(std::env::args().skip(1).collect()).await
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn shared_session_contract() {
        super::common::scenario::contract(super::implementation::connect)
            .await
            .unwrap();
    }
}
