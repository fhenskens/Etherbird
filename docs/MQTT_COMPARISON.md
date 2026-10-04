# MQTT session coordination, with and without Etherbird

For the runtime tradeoff, see [performance measurements](MQTT_PERFORMANCE.md).

These examples implement the same application: subscribe to temperature and humidity,
then publish each received value to its corresponding output topic. The actual
[message handler](../examples/mqtt/common/application.rs) and
[test scenarios](../examples/mqtt/common/scenario.rs) are shared.

The shared application also has independent periodic status and request publishers.
Requests carry a caller-owned deadline. All three tasks use the same client without
connection checks or recovery callbacks. Shared fixture checks exercise their
readiness waits together, request cancellation during recovery, and shutdown releasing
all waiting callers. The broker harness verifies all three publishers' deliveries
in connected traffic phases after each restart.

The application keeps the same publish calls as connections come and go. With
Etherbird, the client declares its lifecycle and lets the library coordinate when
those calls can proceed. This is the benefit the comparison exercises: keeping
session recovery out of the message handler and reusing the coordination across
resource types.

The required session contract is stronger than a live TCP connection:

- Restore both desired subscriptions after every clean-session reconnect.
- Wait for successful broker acknowledgements before allowing application operations.
- Bound connection and setup attempts, then retry with capped backoff.
- Cancel waiting callers without forwarding their operations later.
- Withdraw failed generations before teardown and ignore stale recovery requests.
- Release waiting callers during shutdown and stop the MQTT driver.

Both complete implementations meet this contract. The manual version shows the
application-owned coordination needed to provide it; the Etherbird version shows
which responsibilities can be delegated to the library.

## Reading the comparison

Both top-level files are equivalent launchers: load the common modules, load the
selected implementation, run it, and invoke the same contract test. The supporting
code is grouped in [the MQTT examples directory](../examples/mqtt/README.md):

| Responsibility | Common | Manual | Etherbird |
| --- | --- | --- | --- |
| Application handler and client contract | [application.rs](../examples/mqtt/common/application.rs) | Uses the common application | Uses the common application |
| MQTT polling, subscriptions, and acknowledgements | [protocol.rs](../examples/mqtt/common/protocol.rs) | Uses the shared transport | Uses the shared transport |
| Client and recovery coordination | — | [client.rs](../examples/mqtt/manual/client.rs) implements the coordinator | [client.rs](../examples/mqtt/etherbird/client.rs) declares the proxy and configures Etherbird |
| Lifecycle hooks | — | Orchestrated directly by the manual coordinator | [lifecycle.rs](../examples/mqtt/etherbird/lifecycle.rs) declares protocol-specific hooks |
| Administrative test bridge | — | [harness.rs](../examples/mqtt/manual/harness.rs) | [harness.rs](../examples/mqtt/etherbird/harness.rs) |
| Failure scenarios and fixtures | [scenario.rs](../examples/mqtt/common/scenario.rs) and [fixture.rs](../examples/mqtt/common/fixture.rs) | Runs identical checks | Runs identical checks |

Compare the complete implementation folders rather than launcher sizes. Both keep
test bookkeeping separate from their clients. The manual client owns the state
machine; the Etherbird client delegates that coordination to the library while
retaining explicit construction and lifecycle definitions.

## 1. Without Etherbird

```sh
cargo run --example mqtt_without_etherbird -- --demo
# Real Mosquitto and three abrupt outages; requires Docker:
cargo run --example mqtt_without_etherbird -- --broker-demo 3
```

The [entry point](../examples/mqtt_without_etherbird.rs) uses an application-written
[session coordinator](../examples/mqtt/manual/client.rs). That coordinator owns the
ready-resource watch, generation counter, connection/setup deadlines, recovery
requests, backoff, operation cancellation, and shutdown completion signal. Its
`publish` method waits for a ready session, runs the operation once, and requests
recovery on error. The application has to maintain and test this machinery.

This demonstrates that Etherbird is optional: Tokio and rumqttc are enough to
implement the required behavior. It also exposes the coordination code that
Etherbird can supply generically.

### Observe rumqttc's native behavior

```sh
cargo run --example mqtt_without_etherbird -- --native-only --broker-demo
```

This separate observation mode uses [a minimal native client](../examples/mqtt/manual/native.rs)
that subscribes once and continuously polls rumqttc's event loop. It confirms that:

1. rumqttc reconnects automatically when polling continues.
2. `publish().await` can succeed while offline: it promises internal queue admission.
3. After a broker restart with no stored session, delivery stops until the application
   explicitly restores its subscriptions.
4. Explicit resubscription restores both topics. An independent control subscriber
   verifies that the broker routed the test messages during the missing-subscription check.

These are observations about the extra application contract, not claims that
rumqttc's reconnect or publish APIs are broken. A small application can implement
resubscription on CONNACK directly, and persistent sessions can retain subscriptions.
The complete manual example adds the wider contract above, rather than relying on
the minimal observation mode.

## 2. With Etherbird and a managed proxy

```sh
cargo run --example mqtt_with_etherbird -- --demo
cargo run --example mqtt_with_etherbird -- --broker-demo 3
```

The [Etherbird client](../examples/mqtt/etherbird/client.rs) declares a typed proxy:

```rust
etherbird::managed_client! {
    pub(crate) struct Client for Hooks {
        async fn publish(topic: String, qos: QoS, retain: bool, payload: Vec<u8>) -> ();
    }
}
```

The [lifecycle module](../examples/mqtt/etherbird/lifecycle.rs) defines creation,
connection, subscription setup, disconnect watching, and teardown. The proxy uses
a pool with exactly one slot, so it represents one managed MQTT session.

Application calls retain the same shape:

```rust
client.publish(topic, QoS::AtMostOnce, false, payload).await?;
```

The proxy acquires a resource only after lifecycle setup has completed. A caller
waiting during an outage proceeds after the replacement's SUBACK. Etherbird owns
the coordinator, deadline/backoff policy, generation checks, queue cancellation,
and shutdown; those mechanisms disappear from the example's application code.

This is a typed facade over the methods declared in the macro, not an automatic
`Deref` wrapper for every rumqttc method. Arguments here are owned `String` and
`Vec<u8>` values. Errors become `etherbird::Error<io::Error>`, with explicit stopped
and queue outcomes. Streaming delivery remains a separate stable receiver.

## What remains protocol-specific

Both complete examples use the same [protocol implementation](../examples/mqtt/common/protocol.rs).
It owns rumqttc polling, MQTT acknowledgement interpretation, subscription submission,
message routing, and driver cancellation. Etherbird does not implement those features.
rumqttc still supplies MQTT encoding, keepalive, connection-failure detection, and
protocol acknowledgements.

Every replacement uses a unique client ID because these are clean sessions. A late
CONNECT from an abandoned attempt must not evict the live replacement at the broker.
Both implementations share this transport policy; persistent sessions would require
a stable identity and a different recovery design.

The complete examples deliberately choose clean-session replacement and give
reconnect ownership to their coordinator. They do not run rumqttc's native reconnect
loop alongside that coordinator. The native-only observation retains rumqttc's
event loop across reconnects. The comparison therefore demonstrates an application
policy and the code needed to coordinate it; it is not a claim that replacing the
event loop is the best architecture for every MQTT application.

## Identical verification

`--demo` runs without Docker and checks delayed SUBACK, rejected subscriptions,
three connection replacements, operations waiting through setup and outages,
cancelled operations, setup timeout, concurrent shutdown, and shutdown during setup.
Both example tests invoke the same contract function.

`--broker-demo` runs the same message handler against an isolated Mosquitto 2.0.22
container. Every phase verifies twenty incoming values on two topics and twenty
outgoing values on two other topics. Each SIGKILL outage must withdraw readiness;
an operation polled while offline must wait and resume after setup. Every recovered
session must have a newer generation. Three outages verify eighty deliveries in
each direction; the default ten verify 220 in each direction. Logs and an accurate
`comparison.json` are saved under `target/mqtt-broker/`, and the container is removed.
Linux CI runs both complete implementations and the native observations.

The traffic checks concern delivery during connected phases. They do not prove
lossless delivery through outages, durable application processing, or delivery
confirmation from `publish().await`. Clean sessions can lose in-flight messages,
and the application publishes QoS 0 without operation replay in both versions.

The demonstrated benefit is reusable coordination for a stronger session contract.
For an application needing only native reconnect and a short resubscription handler,
using rumqttc directly may be simpler. Etherbird becomes more useful as lifecycle,
cancellation, and shutdown requirements accumulate or recur across different clients.
