//! Manual implementation: coordinator, native observations, and test bridge.
mod client;
mod harness;
mod native;
use crate::common::scenario;
pub(crate) use client::connect;

pub(crate) async fn run(mut args: Vec<String>) -> scenario::Result<()> {
    if args.first().is_some_and(|arg| arg == "--native-only") {
        args.remove(0);
        return native::demo(args).await;
    }
    scenario::run("manual", connect, args).await
}
