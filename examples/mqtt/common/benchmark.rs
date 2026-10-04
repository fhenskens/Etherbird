//! Shared steady-state benchmark of manual, pooled, and directly supervised clients.
use super::protocol::Message;
use super::{
    application::{Application, Result},
    wire::Peer,
};
use futures_util::future::join_all;
use rumqttc::QoS;
use std::time::{Duration, Instant};
use tokio::{
    sync::broadcast,
    time::{sleep, timeout},
};

fn scheduler_snapshot(metrics: &tokio::runtime::RuntimeMetrics) -> (u64, Duration) {
    (0..metrics.num_workers()).fold((0, Duration::ZERO), |(parks, busy), worker| {
        (
            parks + metrics.worker_park_count(worker),
            busy + metrics.worker_total_busy_duration(worker),
        )
    })
}

async fn drained(peer: &Peer, expected: usize) -> Result<()> {
    timeout(Duration::from_secs(30), async {
        while *peer.published.borrow() < expected {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    if *peer.published.borrow() != expected {
        return Err("unexpected publish count".into());
    }
    Ok(())
}

pub(crate) async fn run<C: Application>(
    label: &str,
    factory: impl Fn(String, u16, Duration) -> (C, broadcast::Receiver<Message>),
) -> Result<()> {
    let peer = Peer::start("127.0.0.1:0".parse()?, false).await?;
    peer.acknowledgements.add_permits(1);
    let (client, _messages) = factory(
        "127.0.0.1".into(),
        peer.address.port(),
        Duration::from_secs(3),
    );
    let result = timeout(Duration::from_secs(120), async {
        let generation = timeout(Duration::from_secs(15), client.ready()).await??;
        let mut expected = 0;
        // Warm up the code and connection outside the timed samples.
        for _ in 0..500 {
            client.publish("benchmark".into(), QoS::AtMostOnce, false, vec![42; 64]).await?;
            expected += 1;
        }
        drained(&peer, expected).await?;
        let metrics = tokio::runtime::Handle::current().metrics();
        println!("implementation,workers,round,calls,admission_ms,drained_ms,calls_per_second,p50_us,p95_us,p99_us,runtime_workers,worker_parks,worker_busy_ms");
        for workers in [1, 3, 16] {
            for round in 1..=5 {
                let before = scheduler_snapshot(&metrics);
                let started = Instant::now();
                // Structured concurrency mirrors the shared application's tasks.
                let callers = (0..workers).map(|_| async {
                    let mut latencies = Vec::with_capacity(2000);
                    for _ in 0..2000 {
                        let call = Instant::now();
                        client.publish("benchmark".into(), QoS::AtMostOnce, false, vec![42; 64]).await?;
                        latencies.push(call.elapsed().as_nanos() as u64);
                    }
                    Ok::<_, C::Error>(latencies)
                });
                let mut latencies = Vec::with_capacity(workers * 2000);
                for caller in join_all(callers).await { latencies.extend(caller?); }
                let admission = started.elapsed();
                expected += latencies.len();
                drained(&peer, expected).await?;
                let drain = started.elapsed();
                let after = scheduler_snapshot(&metrics);
                if client.generation() != Some(generation) { return Err("connection changed during measurement".into()); }
                latencies.sort_unstable();
                let percentile = |percent: usize| latencies[(latencies.len() * percent).div_ceil(100) - 1] as f64 / 1000.0;
                println!("{label},{workers},{round},{},{:.3},{:.3},{:.0},{:.3},{:.3},{:.3},{},{},{:.3}",
                    latencies.len(), admission.as_secs_f64() * 1000.0, drain.as_secs_f64() * 1000.0,
                    latencies.len() as f64 / admission.as_secs_f64(), percentile(50), percentile(95), percentile(99),
                    metrics.num_workers(), after.0 - before.0, (after.1 - before.1).as_secs_f64() * 1000.0);
            }
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    }).await;
    client.close().await;
    peer.stop().await;
    result?
}
