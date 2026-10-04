//! Supervisor-based subscriber and its recovery harnesses.
mod broker;
mod fixture;
use crate::{
    lifecycle::Hooks,
    protocol::{Message, Session},
};
use etherbird::{Config, Lifecycle, Supervisor};
use rumqttc::QoS;
use std::{io, time::Duration};
use tokio::{sync::broadcast, time::timeout};

fn managed(host: String, port: u16) -> (Supervisor<Hooks>, broadcast::Receiver<Message>) {
    let (messages, receiver) = broadcast::channel(64);
    let supervisor = Supervisor::start(
        hooks(
            host,
            port,
            "subscriber",
            vec!["etherbird/temperature".into(), "etherbird/humidity".into()],
            messages,
        ),
        Config {
            resource_name: "MQTT subscriber".into(),
            connect_timeout: Duration::from_secs(3),
            setup_timeout: Duration::from_secs(3),
            retry_delay: Duration::from_millis(100),
            max_retry_delay: Duration::from_secs(1),
            ..Config::default()
        },
    );
    (supervisor, receiver)
}

fn hooks(
    host: String,
    port: u16,
    role: &str,
    topics: Vec<String>,
    messages: broadcast::Sender<Message>,
) -> Hooks {
    Hooks {
        host,
        port,
        client_id: format!("etherbird-{role}-{}", std::process::id()),
        topics,
        messages,
    }
}

// Run once. Successful return means queue admission, not broker delivery.
// The broker test independently verifies reception on the other client.
async fn publish(
    supervisor: &Supervisor<Hooks>,
    topic: String,
    payload: Vec<u8>,
) -> Result<(), etherbird::Error<io::Error>> {
    supervisor
        .execute(|session| async move {
            session
                .publish(topic, QoS::AtMostOnce, false, payload)
                .await
        })
        .await
}

pub(crate) async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["--demo"] {
        return fixture::demo().await;
    }
    if args.first().is_some_and(|arg| arg == "--broker-demo") && args.len() <= 2 {
        let outages = args
            .get(1)
            .map(|arg| arg.parse())
            .transpose()?
            .unwrap_or(10);
        return broker::demo(outages).await;
    }
    if args.len() != 2 {
        return Err("usage: mqtt_session --demo | --broker-demo [outages=10] | <broker-host> <port>; subscribes to etherbird/temperature and etherbird/humidity for 60 seconds".into());
    }
    let (supervisor, mut messages) = managed(args[0].clone(), args[1].parse()?);
    let mut states = supervisor.subscribe();
    let result = timeout(Duration::from_secs(60), async {
        timeout(Duration::from_secs(10), supervisor.acquire()).await??;
        println!("Ready: both subscriptions acknowledged");
        states.borrow_and_update();
        loop {
            tokio::select! {
                result = states.changed() => {
                    result?;
                    println!("Session: {:?}", states.borrow_and_update().state);
                }
                message = messages.recv() => match message {
                    Ok(message) => println!(
                        "{}: {}",
                        message.topic,
                        String::from_utf8_lossy(&message.payload)
                    ),
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        eprintln!("Consumer missed {count} messages")
                    }
                    Err(error) => return Err::<(), Box<dyn std::error::Error>>(error.into()),
                }
            }
        }
    })
    .await;
    supervisor.stop().await;
    match result {
        Ok(result) => result,
        Err(_) => Ok(()),
    }
}
