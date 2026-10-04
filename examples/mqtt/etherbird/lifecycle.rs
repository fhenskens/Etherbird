//! Etherbird-specific lifecycle declarations. MQTT mechanics live in protocol.rs.
use super::protocol::{Message, Session};
use etherbird::{Lifecycle, LifecycleFuture, async_trait};
use std::{io, sync::Arc};
use tokio::sync::broadcast;

#[derive(Clone)]
pub(crate) struct Hooks {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) client_id: String,
    pub(crate) topics: Vec<String>,
    pub(crate) messages: broadcast::Sender<Message>,
}
#[async_trait]
impl Lifecycle for Hooks {
    type Resource = Session;
    type Error = io::Error;
    async fn create(&self) -> io::Result<Session> {
        Ok(Session::open(
            self.host.clone(),
            self.port,
            self.client_id.clone(),
            self.topics.clone(),
            self.messages.clone(),
        ))
    }
    async fn connect(&self, session: &Session) -> io::Result<()> {
        session.connected().await
    }
    async fn setup(&self, session: &Session) -> io::Result<()> {
        session.setup(&self.topics).await
    }
    fn watch_disconnect(&self, session: Arc<Session>) -> Option<LifecycleFuture<io::Error>> {
        Some(Box::pin(async move { session.disconnected().await }))
    }
    async fn disconnect(&self, session: &Session) -> io::Result<()> {
        session.close().await;
        Ok(())
    }
}
