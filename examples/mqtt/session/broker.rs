//! Real Mosquitto restart harness. Docker is only required by --broker-demo.
use super::*;
use crate::docker::{Broker, docker};
use rumqttc::QoS;
use tokio::time::sleep;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const LIMIT: Duration = Duration::from_secs(15);
const INPUTS: [&str; 2] = ["etherbird/temperature", "etherbird/humidity"];
const OUTPUTS: [&str; 2] = ["etherbird/output/temperature", "etherbird/output/humidity"];

// An independent test peer, with no Etherbird supervisor. It is recreated after
// each restart, while the application supervisor and receiver are retained.
async fn peer(port: u16) -> Result<(Session, broadcast::Receiver<Message>)> {
    let (messages, receiver) = broadcast::channel(64);
    let hooks = hooks(
        "127.0.0.1".into(),
        port,
        "test-peer",
        OUTPUTS.iter().map(|topic| (*topic).into()).collect(),
        messages,
    );
    let session = hooks.create().await?;
    timeout(LIMIT, hooks.connect(&session)).await??;
    timeout(LIMIT, hooks.setup(&session)).await??;
    Ok((session, receiver))
}

async fn receive(
    receiver: &mut broadcast::Receiver<Message>,
    topic: &str,
    payload: &[u8],
) -> Result<()> {
    let message = timeout(LIMIT, receiver.recv()).await??;
    if message.topic != topic || message.payload != payload {
        return Err(format!(
            "unexpected message on {}: {:?}; expected {topic}: {:?}",
            message.topic, message.payload, payload
        )
        .into());
    }
    Ok(())
}

#[derive(Default)]
struct Stats {
    recovered: usize,
    incoming: usize,
    outgoing: usize,
    waited_operations: usize,
}

async fn traffic(
    supervisor: &Supervisor<Hooks>,
    messages: &mut broadcast::Receiver<Message>,
    peer: &Session,
    outgoing: &mut broadcast::Receiver<Message>,
    epoch: usize,
    stats: &mut Stats,
) -> Result<()> {
    for sequence in 0..10 {
        for index in 0..2 {
            let payload = format!("epoch={epoch};sequence={sequence};topic={index}").into_bytes();
            peer.client
                .publish(INPUTS[index], QoS::AtLeastOnce, false, payload.clone())
                .await?;
            receive(messages, INPUTS[index], &payload).await?;
            stats.incoming += 1;
            timeout(
                LIMIT,
                publish(supervisor, OUTPUTS[index].into(), payload.clone()),
            )
            .await??;
            receive(outgoing, OUTPUTS[index], &payload).await?;
            stats.outgoing += 1;
        }
    }
    Ok(())
}

pub(super) async fn demo(outages: usize) -> Result<()> {
    if !(1..=100).contains(&outages) {
        return Err("outage count must be between 1 and 100".into());
    }
    let _ = tracing_subscriber::fmt().with_target(false).try_init();
    crate::docker::pull().await?;
    let (mut broker, port) = Broker::start().await?;
    println!("Broker: {} on 127.0.0.1:{port}", broker.name);
    println!("Artifacts: {}", broker.artifacts.display());
    let (supervisor, mut messages) = managed("127.0.0.1".into(), port);
    let mut stats = Stats::default();
    let result = async {
        let mut handle = timeout(LIMIT, supervisor.acquire()).await??;
        let (mut witness, mut outgoing) = peer(port).await?;
        traffic(&supervisor, &mut messages, &witness, &mut outgoing, 0, &mut stats).await?;
        for epoch in 1..=outages {
            docker(&["kill", "--signal", "KILL", &broker.name]).await?;
            timeout(LIMIT, async {
                while supervisor.current().is_some() { sleep(Duration::from_millis(10)).await; }
            }).await?;
            // Poll an actual application publish while offline, keeping the same
            // future alive until recovery rather than reissuing it after restart.
            let marker = format!("waiting-during-outage={epoch}").into_bytes();
            let operation = publish(&supervisor, "etherbird/probe".into(), marker);
            tokio::pin!(operation);
            if timeout(Duration::from_millis(300), &mut operation).await.is_ok() {
                return Err("publish completed while broker was offline".into());
            }
            if supervisor.current().is_some() { return Err("resource remained ready while offline".into()); }
            // Stop the independent peer; its subscriptions are deliberately not
            // restored by Etherbird and it has no pending application messages.
            witness.driver.lock().unwrap().as_ref().unwrap().abort();
            docker(&["start", &broker.name]).await?;
            let binding = docker(&["port", &broker.name, "1883/tcp"]).await?;
            if binding != format!("127.0.0.1:{port}") {
                return Err(format!("broker endpoint changed after restart: {binding}").into());
            }
            broker.wait(port).await?;
            // The pending operation may complete before the witness subscribes.
            // Its queue admission is checked, but its delivery is not counted.
            timeout(LIMIT, &mut operation).await??;
            stats.waited_operations += 1;
            let replacement = timeout(LIMIT, supervisor.acquire()).await??;
            if replacement.generation() <= handle.generation() {
                return Err("recovery reused the old resource generation".into());
            }
            if handle.resource().driver.lock().unwrap().is_some() {
                return Err("retired resource retained its driver".into());
            }
            handle = replacement;
            (witness, outgoing) = peer(port).await?;
            traffic(&supervisor, &mut messages, &witness, &mut outgoing, epoch, &mut stats).await?;
            stats.recovered += 1;
            println!("PASS: outage {epoch}/{outages}; generation {}; {} inbound / {} outbound deliveries verified", handle.generation(), stats.incoming, stats.outgoing);
        }
        // Every expected message was consumed. Unexpected duplicates fail too.
        if messages.try_recv().is_ok() || outgoing.try_recv().is_ok() {
            return Err("unexpected extra message after verified traffic".into());
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    }.await;
    supervisor.stop().await;
    let cleanup = broker.finish().await;
    let passed = result.is_ok() && cleanup.is_ok();
    std::fs::write(
        broker.artifacts.join("summary.json"),
        format!(
            "{{\n  \"passed\": {passed},\n  \"requested_outages\": {outages},\n  \"recovered_outages\": {},\n  \"verified_incoming\": {},\n  \"verified_outgoing\": {},\n  \"operations_waited_through_outage\": {}\n}}\n",
            stats.recovered, stats.incoming, stats.outgoing, stats.waited_operations
        ),
    )?;
    result?;
    cleanup?;
    println!("PASS: all {outages} broker outages recovered; container removed");
    Ok(())
}
