//! The actual application and client contract. This code is identical for both clients.
use super::protocol::Message;
use rumqttc::QoS;
use std::{error::Error, time::Duration};
use tokio::{sync::broadcast, time::timeout};
pub(crate) type Result<T> = std::result::Result<T, Box<dyn Error>>;

pub(crate) fn topics() -> Vec<String> {
    vec!["etherbird/temperature".into(), "etherbird/humidity".into()]
}

// Administration belongs to the harness. The application's delivery handler only
// uses publish, with the same arguments as rumqttc::AsyncClient::publish.
pub(crate) trait Application {
    type Error: Error + 'static;
    async fn ready(&self) -> std::result::Result<u64, Self::Error>;
    fn generation(&self) -> Option<u64>;
    async fn publish(
        &self,
        topic: String,
        qos: QoS,
        retain: bool,
        payload: Vec<u8>,
    ) -> std::result::Result<(), Self::Error>;
    async fn close(&self);
}

// This application code is literally shared by both examples. Neither delivery
// path contains a connection check, resubscription, or recovery callback.
pub(crate) async fn handle_message<C: Application>(
    client: &C,
    message: Message,
) -> std::result::Result<(), C::Error> {
    client
        .publish(
            message.topic.replacen("etherbird/", "etherbird/output/", 1),
            QoS::AtMostOnce,
            false,
            message.payload,
        )
        .await
}

pub(crate) async fn run<C: Application>(
    client: C,
    mut messages: broadcast::Receiver<Message>,
) -> Result<()> {
    let result = timeout(Duration::from_secs(60), async {
        timeout(Duration::from_secs(15), client.ready()).await??;
        loop {
            let message = match messages.recv().await {
                Ok(message) => message,
                Err(error) => return Err::<(), Box<dyn Error>>(error.into()),
            };
            handle_message(&client, message).await?;
        }
    })
    .await;
    client.close().await;
    match result {
        Ok(result) => result,
        Err(_) => Ok(()),
    }
}
