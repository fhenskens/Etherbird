# MQTT: compare the same application

After a broker outage, this application needs its subscriptions restored and
acknowledged before it resumes publishing. Its message handler uses the same ordinary
`publish` calls in both versions. The comparison shows how Etherbird can move the
readiness, recovery, and shutdown coordination into a reusable library.

Three concurrent application tasks share the client: telemetry forwarding, a status
publisher every five seconds, and a request publisher every seven seconds with a
two-second deadline. Run either example with `<broker-host> <port>` to use that
workload for sixty seconds. Application tasks contain no reconnect callbacks.

The shared `--demo` checks hold subscription acknowledgements through three
replacements while all three callers wait. An additional request expires during
each replacement and must never be published later. Shutdown during setup must
release every waiting caller. The real-broker checks verify telemetry, status,
and request deliveries from concurrent callers during each connected phase;
they do not claim lossless delivery across outages.

The top-level [manual launcher](../mqtt_without_etherbird.rs) and
[Etherbird launcher](../mqtt_with_etherbird.rs) have identical structure. They load
the common code, select an implementation, and run the same application and tests.

Read these sections in order:

1. **Common:** [application.rs](common/application.rs) contains the message handler
   and client interface. [protocol.rs](common/protocol.rs) implements MQTT polling,
   subscription acknowledgement handling, and delivery routing for both clients.
2. **Manual:** [client.rs](manual/client.rs) implements readiness watches, lifecycle
   deadlines, reconnect backoff, generation tracking, operation cancellation, and
   shutdown. [harness.rs](manual/harness.rs) adapts its administrative methods to the
   shared checks; [native.rs](manual/native.rs) is an optional native-behavior probe.
3. **Etherbird:** [client.rs](etherbird/client.rs) declares the typed proxy and
   configures a single-slot pool. [lifecycle.rs](etherbird/lifecycle.rs) declares
   protocol-specific hooks. [harness.rs](etherbird/harness.rs) supplies the same
   administrative test bridge as the manual version.

The [failure scenarios](common/scenario.rs), [wire fixture](common/fixture.rs), and
[Docker broker tooling](common/docker.rs) are shared. They are test infrastructure,
separate from the application and either coordinator.

```sh
cargo run --example mqtt_without_etherbird -- --demo
cargo run --example mqtt_with_etherbird -- --demo
# Identical real-broker checks; requires Docker:
cargo run --example mqtt_without_etherbird -- --broker-demo 3
cargo run --example mqtt_with_etherbird -- --broker-demo 3
# Observe native reconnect, queue admission, and missing restored subscriptions:
cargo run --example mqtt_without_etherbird -- --native-only --broker-demo
```

Both complete implementations work. Etherbird replaces the application-owned
coordinator; it still needs lifecycle configuration and the shared MQTT protocol
logic. Compare the implementation folders, rather than the launchers or the
optional native-only probe. Both use the same `publish(topic, qos, retain, payload)`
call shape in the common message handler.

See [the detailed comparison](../../docs/MQTT_COMPARISON.md) for the session contract,
verification, and clean-session limitations. The [session harness](session/mod.rs)
also demonstrates direct supervisor use, without the typed proxy.
