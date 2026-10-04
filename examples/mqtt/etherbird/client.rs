//! Typed proxy and construction. Compare with ../manual/client.rs.
use super::lifecycle::Hooks;
use crate::common::{application, protocol};
use etherbird::{Config, Pool, PoolConfig, Supervisor};
use protocol::Message;
use std::time::Duration;
use tokio::sync::broadcast;

pub(crate) use super::proxy::Client;

pub(crate) fn connect(
    host: String,
    port: u16,
    budget: Duration,
) -> (Client, broadcast::Receiver<Message>) {
    let (messages, receiver) = broadcast::channel(64);
    let hooks = Hooks {
        host,
        port,
        client_id: format!("managed-{}", std::process::id()),
        topics: application::topics(),
        messages,
    };
    let pool = Pool::start(
        move || {
            Supervisor::new(
                hooks.clone(),
                Config {
                    resource_name: "MQTT comparison".into(),
                    connect_timeout: budget,
                    setup_timeout: budget,
                    retry_delay: Duration::from_millis(100),
                    max_retry_delay: Duration::from_secs(1),
                    ..Config::default()
                },
            )
        },
        PoolConfig {
            min_size: 1,
            max_size: 1,
            ..PoolConfig::default()
        },
    );
    (Client::new(pool), receiver)
}
