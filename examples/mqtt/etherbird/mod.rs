//! Etherbird implementation: typed client, lifecycle declarations, and test bridge.
mod client;
mod harness;
mod lifecycle;
mod proxy;
use crate::common::{protocol, scenario};
pub(crate) use client::connect;

pub(crate) async fn run(args: Vec<String>) -> scenario::Result<()> {
    scenario::run("etherbird", connect, args).await
}
