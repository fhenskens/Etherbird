# Release 0.4.1

Release date: 2026-10-09. This compatible release adds bounded FIFO admission and
single-attempt operation/readiness timeouts. Existing signatures, defaults and
retry behavior remain unchanged. Install `etherbird = "0.4.1"`, or enable pooling:
`etherbird = { version = "0.4.1", features = ["pool"] }`.

The [changelog](../CHANGELOG.md) records release history; the
[lifecycle guide](LIFECYCLE_POLICIES.md) defines current contracts.

## Contracts and compatibility

`BoundedFifoQueue` caps waiting entries and returns `QueueError::Full` on rejection.
Zero capacity rejects every admission. Call timeouts include readiness/recovery,
queueing where applicable, execution and any awaited failure teardown. Zero timeout
starts no work; shutdown takes precedence. These calls never replay.

Pooled expiry signals cancellation to tracked jobs; awaited stop drains those jobs.
Direct futures remain caller-owned. Adapters still retire interrupted protocol
exchanges and explicitly close streams/destroy session state. A timeout does not
establish remote completion. This release does not change 0.4.0 failure/reset or
post-stop ownership contracts.

## Verification

Windows stable default/all-feature/all-target suites, doctests and strict Clippy
pass. The 12 new queue/timeout regressions pass on Rust 1.88 with pooling; the six
direct tests pass without pooling. An isolated Rocsteady consumer removes its local
FIFO and timeout/shutdown wrapper using only public APIs: all 44 tests pass on
stable and Rust 1.88; nine doctests pass on stable. Its production dependency is
upgraded separately after publication.

Release checks:

```sh
cargo fmt --check
cargo test --locked --all-targets
cargo test --locked --all-features --all-targets
cargo test --locked --all-features --doc
cargo clippy --locked --all-features --all-targets -- -D warnings
cargo doc --locked --all-features --no-deps
cargo package --locked
cargo publish --dry-run --locked
```

Publish the clean committed release after package verification and dry-run.
Cross-platform CI covers Windows/Linux/macOS with default and pooled builds plus
a Linux serial fixture. Hosted results must be checked for the release commit;
local Windows results alone do not establish Linux/macOS validation.

## Release locations

- [crates.io](https://crates.io/crates/etherbird/0.4.1)
- [API documentation](https://docs.rs/etherbird/0.4.1/etherbird/)
- [GitHub release](https://github.com/fhenskens/etherbird/releases/tag/v0.4.1)
- [Cross-platform CI](https://github.com/fhenskens/etherbird/actions/workflows/ci.yml)
- [Broker recovery CI](https://github.com/fhenskens/etherbird/actions/workflows/broker-recovery.yml)

Verify these service endpoints after publication; a successful dry-run is not
proof that a version has been published. Rocsteady tracks its own adoption and
supported-platform validation separately.