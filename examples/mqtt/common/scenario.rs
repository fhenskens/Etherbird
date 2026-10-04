//! The same failure checks and broker harness for both implementations.
use super::{
    docker::{self, Broker},
    protocol::{Message, Session},
    wire::Peer,
};
use rumqttc::QoS;
use std::{
    error::Error,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::{
    sync::broadcast,
    time::{sleep, timeout},
};

pub(crate) use super::application::Result;
use super::application::{self, Application, handle_message, topics};
const LIMIT: Duration = Duration::from_secs(15);
const BUDGET: Duration = Duration::from_secs(3);
const OUTPUTS: [&str; 2] = ["etherbird/output/temperature", "etherbird/output/humidity"];
fn observed_topics() -> Vec<String> {
    OUTPUTS
        .iter()
        .copied()
        .chain(["etherbird/status", "etherbird/request"])
        .map(String::from)
        .collect()
}

// Independently polled application tasks share one client and its readiness policy.
async fn concurrent_calls<C: Application>(
    client: &C,
    budget: Duration,
    completed: &AtomicUsize,
) -> Result<()> {
    let telemetry = async {
        handle_message(
            client,
            Message {
                topic: topics()[0].clone(),
                payload: b"42".to_vec(),
            },
        )
        .await?;
        completed.fetch_add(1, Ordering::SeqCst);
        Ok::<(), Box<dyn Error>>(())
    };
    let status = async {
        application::publish_status(client).await?;
        completed.fetch_add(1, Ordering::SeqCst);
        Ok::<(), Box<dyn Error>>(())
    };
    let request = async {
        application::publish_request(client, b"request".to_vec(), budget).await??;
        completed.fetch_add(1, Ordering::SeqCst);
        Ok::<(), Box<dyn Error>>(())
    };
    tokio::try_join!(telemetry, status, request)?;
    Ok(())
}

fn require(condition: bool, message: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

pub(crate) async fn receive(
    receiver: &mut broadcast::Receiver<Message>,
    topic: &str,
    payload: &[u8],
) -> Result<Message> {
    let message = timeout(LIMIT, receiver.recv()).await??;
    require(
        message.topic == topic && message.payload == payload,
        "unexpected topic or payload",
    )?;
    Ok(message)
}

async fn fixture_messages(messages: &mut broadcast::Receiver<Message>) -> Result<()> {
    for topic in topics() {
        receive(messages, &topic, b"42").await?;
    }
    Ok(())
}

async fn offline<C: Application>(client: &C) -> Result<()> {
    timeout(LIMIT, async {
        while client.generation().is_some() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

pub(crate) async fn contract<C: Application>(
    factory: impl Fn(String, u16, Duration) -> (C, broadcast::Receiver<Message>),
) -> Result<()> {
    let first = Peer::start("127.0.0.1:0".parse()?, false).await?;
    let address = first.address;
    let (client, mut messages) = factory(address.ip().to_string(), address.port(), BUDGET);
    let mut peer = Some(first);
    let result = async {
        peer.as_ref().unwrap().subscribed().await?;
        require(
            timeout(Duration::from_millis(100), client.ready())
                .await
                .is_err(),
            "readiness bypassed SUBACK",
        )?;
        // Dropping a caller before readiness must not publish later.
        require(
            timeout(
                Duration::from_millis(100),
                client.publish(
                    "cancelled".into(),
                    QoS::AtMostOnce,
                    false,
                    b"cancelled".to_vec(),
                ),
            )
            .await
            .is_err(),
            "publish bypassed setup",
        )?;
        peer.as_ref().unwrap().acknowledgements.add_permits(1);
        let mut generation = timeout(LIMIT, client.ready()).await??;
        fixture_messages(&mut messages).await?;
        require(
            *peer.as_ref().unwrap().published.borrow() == 0,
            "cancelled operation was forwarded",
        )?;
        for _ in 0..3 {
            peer.take().unwrap().stop().await;
            offline(&client).await?;
            let completed = AtomicUsize::new(0);
            let pending = concurrent_calls(&client, LIMIT, &completed);
            tokio::pin!(pending);
            require(
                timeout(Duration::from_millis(100), &mut pending)
                    .await
                    .is_err(),
                "publish completed offline",
            )?;
            peer = Some(Peer::start(address, false).await?);
            peer.as_ref().unwrap().subscribed().await?;
            require(
                application::publish_request(
                    &client,
                    b"expired".to_vec(),
                    Duration::from_millis(50),
                )
                .await
                .is_err(),
                "request deadline did not cancel its readiness wait",
            )?;
            require(
                timeout(Duration::from_millis(100), &mut pending)
                    .await
                    .is_err(),
                "publish completed before replacement SUBACK",
            )?;
            require(
                completed.load(Ordering::SeqCst) == 0,
                "an application caller bypassed replacement readiness",
            )?;
            peer.as_ref().unwrap().acknowledgements.add_permits(1);
            timeout(LIMIT, &mut pending).await??;
            let replacement = timeout(LIMIT, client.ready()).await??;
            require(replacement > generation, "generation did not advance")?;
            generation = replacement;
            fixture_messages(&mut messages).await?;
            timeout(LIMIT, async {
                while *peer.as_ref().unwrap().published.borrow() < 3 {
                    sleep(Duration::from_millis(5)).await;
                }
            })
            .await?;
            // A barrier publish is FIFO behind any incorrectly retained request.
            client
                .publish("barrier".into(), QoS::AtMostOnce, false, vec![])
                .await?;
            timeout(LIMIT, async {
                while *peer.as_ref().unwrap().published.borrow() < 4 {
                    sleep(Duration::from_millis(5)).await;
                }
            })
            .await?;
            sleep(Duration::from_millis(50)).await;
            require(
                *peer.as_ref().unwrap().published.borrow() == 4,
                "expired request was forwarded after recovery",
            )?;
        }
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    tokio::join!(client.close(), client.close());
    result?;
    require(
        timeout(
            LIMIT,
            client.publish("after-stop".into(), QoS::AtMostOnce, false, vec![]),
        )
        .await?
        .is_err(),
        "stopped client restarted",
    )?;
    drop(peer);
    println!(
        "PASS: concurrent telemetry/status/requests, SUBACK-gated readiness, cancelled requests, and three recoveries"
    );

    let rejected = Peer::start("127.0.0.1:0".parse()?, true).await?;
    rejected.acknowledgements.add_permits(100);
    let (client, _) = factory(
        rejected.address.ip().to_string(),
        rejected.address.port(),
        BUDGET,
    );
    let result = async {
        rejected.subscribed().await?;
        require(
            timeout(Duration::from_millis(400), client.ready())
                .await
                .is_err(),
            "rejected subscriptions became ready",
        )?;
        require(
            rejected.acknowledgements.available_permits() < 100,
            "rejection was not exercised",
        )
    }
    .await;
    client.close().await;
    result?;
    println!("PASS: rejected subscription never becomes ready");

    let held = Peer::start("127.0.0.1:0".parse()?, false).await?;
    let (client, mut messages) = factory(
        held.address.ip().to_string(),
        held.address.port(),
        Duration::from_millis(200),
    );
    let result = async {
        held.subscribed().await?;
        sleep(Duration::from_millis(350)).await;
        require(
            client.generation().is_none(),
            "timed-out setup became ready",
        )?;
        held.acknowledgements.add_permits(100);
        timeout(LIMIT, client.ready()).await??;
        require(
            *held.subscriptions.borrow() >= 2,
            "setup deadline did not retire the original connection",
        )?;
        fixture_messages(&mut messages).await
    }
    .await;
    client.close().await;
    result?;
    println!("PASS: setup deadline retires the session and retries");

    let held = Peer::start("127.0.0.1:0".parse()?, false).await?;
    let (client, _) = factory(held.address.ip().to_string(), held.address.port(), BUDGET);
    let result = async {
        held.subscribed().await?;
        let telemetry = handle_message(
            &client,
            Message {
                topic: topics()[0].clone(),
                payload: vec![],
            },
        );
        let status = application::publish_status(&client);
        let request = application::publish_request(&client, vec![], LIMIT);
        tokio::pin!(telemetry, status, request);
        let pending = async {
            let (telemetry, status, request) =
                tokio::join!(&mut telemetry, &mut status, &mut request);
            require(
                telemetry.is_err() && status.is_err() && request?.is_err(),
                "shutdown did not release all three application callers",
            )
        };
        tokio::pin!(pending);
        require(
            timeout(Duration::from_millis(100), &mut pending)
                .await
                .is_err(),
            "publish bypassed setup",
        )?;
        timeout(LIMIT, client.close()).await?;
        require(
            timeout(LIMIT, &mut pending).await?.is_ok(),
            "shutdown did not release all waiting callers",
        )
    }
    .await;
    client.close().await;
    result?;
    println!("PASS: shutdown during setup releases waiting callers");
    Ok(())
}

pub(crate) async fn witness(
    port: u16,
    role: &str,
    subscriptions: Vec<String>,
) -> Result<(Session, broadcast::Receiver<Message>)> {
    let (messages, receiver) = broadcast::channel(64);
    let session = Session::open(
        "127.0.0.1".into(),
        port,
        format!("{role}-{}", std::process::id()),
        subscriptions.clone(),
        messages,
    );
    timeout(LIMIT, session.connected()).await??;
    timeout(LIMIT, session.setup(&subscriptions)).await??;
    Ok((session, receiver))
}

async fn traffic<C: Application>(
    client: &C,
    messages: &mut broadcast::Receiver<Message>,
    witness: &Session,
    observed: &mut broadcast::Receiver<Message>,
    epoch: usize,
    counts: &mut Counts,
) -> Result<()> {
    for sequence in 0..10 {
        for (index, topic) in topics().iter().enumerate() {
            let payload = format!("epoch={epoch};sequence={sequence};topic={index}").into_bytes();
            witness
                .client
                .publish(topic, QoS::AtLeastOnce, false, payload.clone())
                .await?;
            let message = receive(messages, topic, &payload).await?;
            counts.incoming += 1;
            let telemetry = async {
                handle_message(client, message)
                    .await
                    .map_err(|error| Box::new(error) as Box<dyn Error>)
            };
            let status = async {
                application::publish_status(client)
                    .await
                    .map_err(|error| Box::new(error) as Box<dyn Error>)
            };
            let request = async {
                application::publish_request(client, payload.clone(), LIMIT).await??;
                Ok::<(), Box<dyn Error>>(())
            };
            timeout(LIMIT, async {
                tokio::try_join!(telemetry, status, request)
            })
            .await??;
            let mut expected = vec![
                (OUTPUTS[index], payload.clone()),
                ("etherbird/status", b"online".to_vec()),
                ("etherbird/request", payload),
            ];
            for _ in 0..3 {
                let delivery = timeout(LIMIT, observed.recv()).await??;
                let position = expected
                    .iter()
                    .position(|(topic, payload)| {
                        delivery.topic == *topic && delivery.payload == *payload
                    })
                    .ok_or("unexpected or duplicate application delivery")?;
                expected.remove(position);
            }
            counts.outgoing += 1;
            counts.status += 1;
            counts.requests += 1;
        }
    }
    Ok(())
}

#[derive(Default)]
struct Counts {
    incoming: usize,
    outgoing: usize,
    status: usize,
    requests: usize,
}

async fn real<C: Application>(
    label: &str,
    factory: impl Fn(String, u16, Duration) -> (C, broadcast::Receiver<Message>),
    outages: usize,
) -> Result<()> {
    require(
        (1..=100).contains(&outages),
        "outage count must be between 1 and 100",
    )?;
    docker::pull().await?;
    let (mut broker, port) = Broker::start().await?;
    println!(
        "{label}: broker {}; artifacts {}",
        broker.name,
        broker.artifacts.display()
    );
    let (client, mut messages) = factory("127.0.0.1".into(), port, BUDGET);
    let mut completed = 0;
    let mut counts = Counts::default();
    let result = async {
        let mut generation = timeout(LIMIT, client.ready()).await??;
        let (mut peer, mut observed) = witness(port, "comparison-peer", observed_topics()).await?;
        traffic(&client, &mut messages, &peer, &mut observed, 0, &mut counts).await?;
        for epoch in 1..=outages {
            docker::docker(&["kill", "--signal", "KILL", &broker.name]).await?;
            offline(&client).await?;
            let completed_calls = AtomicUsize::new(0);
            let pending = concurrent_calls(&client, LIMIT, &completed_calls);
            tokio::pin!(pending);
            require(timeout(Duration::from_millis(300), &mut pending).await.is_err(), "operation completed offline")?;
            peer.close().await;
            docker::docker(&["start", &broker.name]).await?;
            broker.wait(port).await?;
            timeout(LIMIT, &mut pending).await??;
            let replacement = timeout(LIMIT, client.ready()).await??;
            require(replacement > generation, "generation did not advance")?;
            generation = replacement;
            (peer, observed) = witness(port, "comparison-peer", observed_topics()).await?;
            traffic(&client, &mut messages, &peer, &mut observed, epoch, &mut counts).await?;
            completed += 1;
            println!("PASS {label}: outage {epoch}/{outages}; generation {generation}; both input and output topics verified");
        }
        peer.close().await;
        Ok::<(), Box<dyn Error>>(())
    }.await;
    client.close().await;
    let cleanup = broker.finish().await;
    if let Err(error) = &result {
        std::fs::write(broker.artifacts.join("failure.txt"), error.to_string())?;
    }
    std::fs::write(
        broker.artifacts.join("comparison.json"),
        format!(
            "{{\n  \"implementation\": \"{label}\",\n  \"passed\": {},\n  \"requested_outages\": {outages},\n  \"completed_outages\": {completed},\n  \"verified_incoming\": {},\n  \"verified_outgoing\": {},\n  \"verified_status\": {},\n  \"verified_requests\": {}\n}}\n",
            result.is_ok() && cleanup.is_ok(),
            counts.incoming,
            counts.outgoing,
            counts.status,
            counts.requests
        ),
    )?;
    result?;
    cleanup?;
    println!("PASS {label}: {outages} real broker outages; same application code");
    Ok(())
}

pub(crate) async fn run<C: Application>(
    label: &str,
    factory: impl Fn(String, u16, Duration) -> (C, broadcast::Receiver<Message>),
    args: Vec<String>,
) -> Result<()> {
    match args.as_slice() {
        [] => contract(factory).await,
        [flag] if flag == "--demo" => contract(factory).await,
        [flag] if flag == "--benchmark" => super::benchmark::run(label, factory).await,
        [flag, rest @ ..] if flag == "--broker-demo" && rest.len() <= 1 => {
            real(
                label,
                factory,
                rest.first()
                    .map(|count| count.parse())
                    .transpose()?
                    .unwrap_or(10),
            )
            .await
        }
        [host, port] => {
            let (client, messages) = factory(host.clone(), port.parse()?, BUDGET);
            application::run(client, messages).await
        }
        _ => Err(
            "usage: --demo | --benchmark | --broker-demo [outages=10] | <broker-host> <port>"
                .into(),
        ),
    }
}
