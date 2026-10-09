# Release 0.4.0

Release date: 2026-10-09. Version 0.4 introduces generic lifecycle failure and
operation recovery policies, checked construction, and resource-cache release
after shutdown. [CHANGELOG.md](../CHANGELOG.md) records the changes and migration;
[lifecycle policies](LIFECYCLE_POLICIES.md) defines the contracts.

Install with `etherbird = "0.4"`, or enable exclusive pooling with
`etherbird = { version = "0.4", features = ["pool"] }`.

## Compatibility

The new `Error::Lifecycle` and `ResourceState::Failed` variants require updating
exhaustive matches. Resource-derived attributes return None after awaited stop;
use stored values for configuration that must remain readable. Default failure
policies retain prior automatic recovery. Existing unchecked constructors retain
their accepted configurations and panic behavior.

Adapters can opt into fail-fast setup and healthy operation errors independently
of logging and replay. They remain responsible for protocol validation, cancellation
poisoning, closing streams and destroying session state. External handles, user
captures and abandoned timed-out hooks can retain objects after bounded teardown.

## Verification

Before publication, validate the release commit with:

```sh
cargo fmt --check
cargo test --locked
cargo test --locked --all-features
cargo clippy --all-targets --locked -- -D warnings
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo doc --no-deps --all-features --locked
cargo package --locked
cargo publish --dry-run --locked
```

The local Windows default and pool suites pass, including 16 policy regressions
and redacted lifecycle logging checks. All-target Clippy, documentation generation,
Rust 1.88 tests and MQTT/Modbus/WebSocket recovery demos also pass.
Cross-platform verification runs through [CI](https://github.com/fhenskens/Etherbird/actions/workflows/ci.yml)
on Windows, Linux and macOS, with default/pool builds and a Linux serial fixture.
The [real MQTT broker workflow](https://github.com/fhenskens/Etherbird/actions/workflows/broker-recovery.yml)
checks actual broker outages and rejected setup.

External acceptance used a temporary rocsteady 0.1.0-alpha.1 consumer with the
authentication latch/gate and nested-result workaround replaced by 0.4 policies.
All 43 tests pass on Windows using current Rust and Rust 1.88. The checked-in
rocsteady dependency remains on 0.3.1 until its separate production upgrade;
development-only path dependencies are not part of the published package.

Inspect packaged contents for local experiments, credentials and benchmark data.
Package verification must build the extracted crate; publishing must use the clean,
committed release contents. A successful dry-run is not proof of publication.

## Release locations

- [crates.io version 0.4.0](https://crates.io/crates/etherbird/0.4.0)
- [Versioned API documentation](https://docs.rs/etherbird/0.4.0/etherbird/)
- [GitHub release and tag](https://github.com/fhenskens/Etherbird/releases/tag/v0.4.0)

Publication, tag creation and GitHub release creation are verified against these
services. Rocsteady adoption is tracked separately in its outstanding-work roadmap.
