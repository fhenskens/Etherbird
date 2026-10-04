//! Direct supervision without pool dispatch; shares the MQTT workload and checks.
#[path = "mqtt/common/mod.rs"]
mod common;
#[path = "mqtt/supervised/mod.rs"]
mod implementation;

fn main() -> common::scenario::Result<()> {
    common::runtime::run(|args| common::scenario::run("supervisor", implementation::connect, args))
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
