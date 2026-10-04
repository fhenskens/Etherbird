//! MQTT session comparison. See mqtt/README.md for the common and implementation code.
#[path = "mqtt/common/mod.rs"]
mod common;
#[path = "mqtt/etherbird/mod.rs"]
mod implementation;

fn main() -> common::scenario::Result<()> {
    common::runtime::run(implementation::run)
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
