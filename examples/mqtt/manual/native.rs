//! Minimal idiomatic rumqttc usage: native reconnect, one initial subscription.
//! This deliberately does not implement the application session contract. The
//! manual client in client.rs shows a complete implementation without Etherbird.
use crate::common::{
    docker::{self, Broker},
    protocol::Message,
    scenario::{self, Result},
};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::time::Duration;
use tokio::{
    sync::{broadcast, watch},
    task::JoinHandle,
    time::{sleep, timeout},
};

struct Native {
    client: AsyncClient,
    connected: watch::Receiver<bool>,
    acknowledgements: watch::Receiver<usize>,
    driver: JoinHandle<()>,
}
impl Drop for Native {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

impl Native {
    async fn start(port: u16) -> Result<(Self, broadcast::Receiver<Message>)> {
        let mut options =
            MqttOptions::new(format!("native-{}", std::process::id()), "127.0.0.1", port);
        options.set_clean_session(true);
        options.set_keep_alive(Duration::from_secs(5));
        let (client, mut events) = AsyncClient::new(options, 16);
        let (connection_tx, connected) = watch::channel(false);
        let (ack_tx, acknowledgements) = watch::channel(0);
        let (messages, receiver) = broadcast::channel(64);
        let driver = tokio::spawn(async move {
            loop {
                match events.poll().await {
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        connection_tx.send_replace(true);
                    }
                    Ok(Event::Incoming(Packet::SubAck(_))) => {
                        ack_tx.send_modify(|count| *count += 1);
                    }
                    Ok(Event::Incoming(Packet::Publish(message))) => {
                        let _ = messages.send(Message {
                            topic: message.topic,
                            payload: message.payload.to_vec(),
                        });
                    }
                    Ok(_) => {}
                    Err(_) => {
                        connection_tx.send_replace(false);
                        sleep(Duration::from_millis(100)).await;
                        // Continue poll: rumqttc reconnects without a supervisor.
                    }
                }
            }
        });
        client
            .subscribe_many(
                crate::common::application::topics()
                    .into_iter()
                    .map(|topic| rumqttc::SubscribeFilter::new(topic, QoS::AtLeastOnce)),
            )
            .await?;
        Ok((
            Self {
                client,
                connected,
                acknowledgements,
                driver,
            },
            receiver,
        ))
    }

    async fn connection(&self, expected: bool) -> Result<()> {
        let mut status = self.connected.clone();
        timeout(Duration::from_secs(15), async {
            while *status.borrow_and_update() != expected {
                status.changed().await?;
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .await??;
        Ok(())
    }

    async fn acknowledged(&self, expected: usize) -> Result<()> {
        let mut status = self.acknowledgements.clone();
        timeout(Duration::from_secs(15), async {
            while *status.borrow_and_update() < expected {
                status.changed().await?;
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .await??;
        Ok(())
    }
}

pub(crate) async fn demo(args: Vec<String>) -> Result<()> {
    if args != ["--broker-demo"] {
        return Err(
            "native-only observation: --native-only --broker-demo (requires Docker)".into(),
        );
    }
    docker::pull().await?;
    let (mut broker, port) = Broker::start().await?;
    let mut admitted = false;
    let mut reconnected = false;
    let mut missing = false;
    println!(
        "Native-only observations; artifacts {}",
        broker.artifacts.display()
    );
    let result = async {
        let (native, mut messages) = Native::start(port).await?;
        native.acknowledged(1).await?;
        let (control, mut observed) =
            scenario::witness(port, "native-control", crate::common::application::topics()).await?;
        let topic = crate::common::application::topics().remove(0);
        control
            .client
            .publish(&topic, QoS::AtLeastOnce, false, b"before".to_vec())
            .await?;
        scenario::receive(&mut observed, &topic, b"before").await?;
        scenario::receive(&mut messages, &topic, b"before").await?;
        control.close().await;
        docker::docker(&["kill", "--signal", "KILL", &broker.name]).await?;
        native.connection(false).await?;
        // This is correct rumqttc behavior: publish promises queue admission.
        // Applications needing session readiness must supply that extra policy.
        timeout(
            Duration::from_secs(1),
            native
                .client
                .publish("probe", QoS::AtMostOnce, false, b"offline".to_vec()),
        )
        .await??;
        println!("OBSERVED: publish().await succeeds while offline (queue admission)");
        admitted = true;
        docker::docker(&["start", &broker.name]).await?;
        broker.wait(port).await?;
        native.connection(true).await?;
        println!("PASS: rumqttc reconnects by itself");
        reconnected = true;
        let (control, mut observed) =
            scenario::witness(port, "native-control", crate::common::application::topics()).await?;
        control
            .client
            .publish(&topic, QoS::AtLeastOnce, false, b"after".to_vec())
            .await?;
        scenario::receive(&mut observed, &topic, b"after").await?; // Broker actually routed it.
        if timeout(Duration::from_millis(500), messages.recv())
            .await
            .is_ok()
        {
            return Err("native-only client unexpectedly received after restart".into());
        }
        if *native.acknowledgements.borrow() != 1 {
            return Err("unexpected automatic resubscription".into());
        }
        println!("OBSERVED: TCP reconnect succeeded, but lost subscriptions were not restored");
        missing = true;
        // A manual resubscription repairs delivery, proving the cause of the gap.
        native
            .client
            .subscribe_many(
                crate::common::application::topics()
                    .into_iter()
                    .map(|topic| rumqttc::SubscribeFilter::new(topic, QoS::AtLeastOnce)),
            )
            .await?;
        native.acknowledged(2).await?;
        for topic in crate::common::application::topics() {
            control
                .client
                .publish(&topic, QoS::AtLeastOnce, false, b"repaired".to_vec())
                .await?;
            scenario::receive(&mut observed, &topic, b"repaired").await?;
            scenario::receive(&mut messages, &topic, b"repaired").await?;
        }
        control.close().await;
        native.driver.abort();
        println!("PASS: explicit resubscription restores both topics");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    let cleanup = broker.finish().await;
    std::fs::write(
        broker.artifacts.join("native-observations.json"),
        format!(
            "{{\n  \"passed\": {},\n  \"queue_admission_while_offline\": {admitted},\n  \"native_reconnect\": {reconnected},\n  \"missing_subscriptions_after_restart\": {missing}\n}}\n",
            result.is_ok() && cleanup.is_ok()
        ),
    )?;
    result?;
    cleanup?;
    Ok(())
}
