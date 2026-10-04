//! Application and test infrastructure shared unchanged by both implementations.
pub(crate) mod application;
mod benchmark;
pub(crate) mod docker;
pub(crate) mod protocol;
pub(crate) mod runtime;
pub(crate) mod scenario;
#[path = "fixture.rs"]
pub(crate) mod wire;
