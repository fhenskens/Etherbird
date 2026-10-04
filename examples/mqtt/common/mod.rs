//! Application and test infrastructure shared unchanged by both implementations.
pub(crate) mod application;
pub(crate) mod docker;
pub(crate) mod protocol;
pub(crate) mod scenario;
#[path = "fixture.rs"]
pub(crate) mod wire;
