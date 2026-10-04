//! Minimal MQTT 3.1.1 peer for exercising the real rumqttc client, not a broker.
use super::*;
use crate::wire::Peer;

async fn samples(
    messages: &mut broadcast::Receiver<Message>,
) -> Result<(), Box<dyn std::error::Error>> {
    for topic in ["etherbird/temperature", "etherbird/humidity"] {
        let message = timeout(Duration::from_secs(5), messages.recv()).await??;
        assert_eq!(message.topic, topic);
        assert_eq!(message.payload, b"42");
    }
    Ok(())
}

pub(super) async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    let peer = Peer::start("127.0.0.1:0".parse()?, false).await?;
    let address = peer.address;
    let (supervisor, mut messages) = managed(address.ip().to_string(), address.port());
    let result = timeout(Duration::from_secs(15), async {
        peer.subscribed().await?;
        assert!(
            timeout(Duration::from_millis(150), supervisor.acquire())
                .await
                .is_err(),
            "must wait for SUBACK"
        );
        peer.acknowledgements.add_permits(1);
        let first = supervisor.acquire().await?;
        samples(&mut messages).await?;
        assert_eq!(*peer.published.borrow(), 0);
        println!("PASS: readiness waits for both subscription acknowledgements");
        peer.stop().await;
        timeout(Duration::from_secs(5), async {
            while supervisor.current().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let replacement = Peer::start(address, false).await?;
        replacement.subscribed().await?;
        assert!(
            timeout(Duration::from_millis(150), supervisor.acquire())
                .await
                .is_err(),
            "replacement must also wait for SUBACK"
        );
        replacement.acknowledgements.add_permits(1);
        let second = supervisor.acquire().await?;
        assert_ne!(first.generation(), second.generation());
        assert!(
            first.resource().driver.lock().unwrap().is_none(),
            "retained old handles must not keep their event loop running"
        );
        samples(&mut messages).await?;
        println!("PASS: broker restart restores every topic through the same receiver");
        drop(replacement);
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await;
    supervisor.stop().await;
    result?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn subscriptions_gate_readiness_and_survive_restart() {
        demo().await.unwrap();
    }

    #[tokio::test]
    async fn rejected_subscription_never_becomes_ready() {
        let peer = Peer::start("127.0.0.1:0".parse().unwrap(), true)
            .await
            .unwrap();
        peer.acknowledgements.add_permits(100);
        let (supervisor, mut messages) =
            managed(peer.address.ip().to_string(), peer.address.port());
        peer.subscribed().await.unwrap();
        assert!(
            timeout(Duration::from_millis(400), supervisor.acquire())
                .await
                .is_err()
        );
        assert!(supervisor.current().is_none());
        assert!(messages.try_recv().is_err());
        assert!(
            peer.acknowledgements.available_permits() < 100,
            "test must exercise a rejected SUBACK"
        );
        supervisor.stop().await;
        drop(peer);
    }

    #[tokio::test]
    async fn disconnect_during_setup_restores_all_subscriptions() {
        let peer = Peer::start("127.0.0.1:0".parse().unwrap(), false)
            .await
            .unwrap();
        let address = peer.address;
        let (supervisor, mut messages) = managed(address.ip().to_string(), address.port());
        peer.subscribed().await.unwrap();
        assert!(supervisor.current().is_none());
        peer.stop().await;
        let replacement = Peer::start(address, false).await.unwrap();
        replacement.subscribed().await.unwrap();
        assert!(supervisor.current().is_none());
        replacement.acknowledgements.add_permits(1);
        timeout(Duration::from_secs(5), supervisor.acquire())
            .await
            .unwrap()
            .unwrap();
        samples(&mut messages).await.unwrap();
        supervisor.stop().await;
    }
}
