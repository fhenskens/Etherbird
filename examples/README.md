# Examples

These examples show how application calls can stay straightforward while a connection
is replaced and its setup restored. Start with [managed_client.rs](managed_client.rs)
for the lifecycle and typed proxy API, or read [the MQTT comparison](mqtt/README.md)
to see the coordination Etherbird can take off an application's hands.

The comparison launchers have the same responsibilities. Common application and
protocol code, manual coordination, and Etherbird configuration are in separate
directories. Other examples have small command launchers with their adapters and
fixtures beside them.

| Command | Demonstrates | Implementation |
| --- | --- | --- |
| `cargo run --example managed_client` | Minimal typed proxy and replayed configuration | [Self-contained introductory example](managed_client.rs) |
| `cargo run --example mqtt_without_etherbird -- --demo` | Complete application-written session coordination | [Manual MQTT client](mqtt/manual/client.rs) |
| `cargo run --example mqtt_with_etherbird -- --demo` | Same application, with readiness and recovery delegated to Etherbird | [Etherbird MQTT client](mqtt/etherbird/client.rs), [lifecycle hooks](mqtt/etherbird/lifecycle.rs) |
| `cargo run --example mqtt_session -- --demo` | Direct supervisor usage, subscription readiness, and restart recovery | [MQTT session harness](mqtt/session/mod.rs) |
| `cargo run --example live_recovery` | Modbus read retries and WebSocket subscription restoration under outages | [Runner](live_recovery/mod.rs), [adapter](live_recovery/adapter.rs), [fixtures](live_recovery/fixtures.rs) |
| `cargo run --example serial_device -- --help` | Reopening and handshaking a serial instrument | [CLI](serial_device/mod.rs), [adapter](serial_device/adapter.rs), [fixtures and tests](serial_device/demo.rs) |

The MQTT `--demo` checks and socket harness require no external service. MQTT
`--broker-demo [outages]` runs against an isolated real Mosquitto container and
requires Docker. The serial `--demo` uses Unix pseudo-terminals; serial byte-stream
tests also run on Windows.

Read [adapter details](../docs/EXAMPLES.md), [the MQTT comparison](../docs/MQTT_COMPARISON.md),
and [socket recovery scenarios](../docs/LIVE_RECOVERY.md) for the contracts and limits.

```text
examples/
  *.rs                   Cargo command launchers; managed_client.rs is self-contained
  mqtt/
    common/              Shared application, protocol, failure checks, and broker tooling
    manual/              Manual coordinator, test bridge, and native rumqttc observations
    etherbird/           Managed proxy, lifecycle declarations, and test bridge
    session/             Direct-supervisor recovery harness
  live_recovery/         Socket runner, adapters, and local service fixtures
  serial_device/         Serial CLI, device adapter, and hardware-free checks
```
