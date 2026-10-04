# Example adapters

See [the examples index](../examples/README.md) for commands and source navigation.

These examples demonstrate clients where the application supplies connection
replacement and session setup. They use actual dependency clients. Operations run
once by default; Modbus telemetry reads explicitly opt into bounded retries.
The dependencies are development dependencies; they do not become requirements
for applications using Etherbird.

| Library | Implementation | What Etherbird supplies |
| --- | --- | --- |
| [tokio-modbus](https://docs.rs/tokio-modbus/latest/tokio_modbus/) | [TCP adapter](../examples/live_recovery/adapter.rs), `Endpoint::Modbus` | Replacement contexts, bounded register requests, opt-in read retries, idle health probes, pooled scheduling, and shutdown of leased resources. |
| [tokio-tungstenite](https://docs.rs/tokio-tungstenite/latest/tokio_tungstenite/) | [WebSocket adapter](../examples/live_recovery/adapter.rs), `Endpoint::WebSocket` | Reconnecting sessions, subscription restoration before readiness, idle heartbeats, and a stable managed resource facade. |
| [tokio-serial](https://docs.rs/tokio-serial/latest/tokio_serial/) | [Serial adapter](../examples/serial_device/adapter.rs) | Port reopening, configuration reapplication, repeated instrument handshakes, response deadlines, and idle health probes. |
| [rumqttc](https://docs.rs/rumqttc/latest/rumqttc/) | [MQTT subscriber](../examples/mqtt_session.rs) | Subscription restoration, SUBACK-gated readiness, one event-loop owner, and a stable message receiver across connection replacements. |

## MQTT subscription sessions

For a direct comparison, see [MQTT with and without Etherbird](MQTT_COMPARISON.md).
The two examples share application code and checks; the Etherbird version uses a
typed managed proxy with lifecycle definitions in a separate module. The manual
version supplies its own complete coordinator and an optional native-only mode
that demonstrates rumqttc's existing capabilities and the extra session policies.

`mqtt_with_supervisor` uses those same generated proxy methods and lifecycle hooks
with `Client::from_supervisor(supervisor)`. It demonstrates concurrent supervision
without pool dispatch for a resource that supports shared publish calls. The
pooled variant retains exclusive leases and independently tracked operations;
the direct variant leaves operation polling and cancellation with its callers.

```sh
# Self-contained loopback restart check, on every supported platform:
cargo run --example mqtt_session -- --demo
# Real Mosquitto broker, ten abrupt outages; requires Docker:
cargo run --example mqtt_session -- --broker-demo
# Optional outage count (1 through 100):
cargo run --example mqtt_session -- --broker-demo 25
# Subscribe against a local MQTT 3.1.1 broker for 60 seconds:
cargo run --example mqtt_session -- localhost 1883
```

The subscriber registers `etherbird/temperature` and `etherbird/humidity` at QoS 1.
Connection establishment waits for CONNACK, and setup submits one subscription
batch and waits for a successful SUBACK covering every filter. Rejection or loss
during setup prevents readiness. `Supervisor::acquire()` waits for the complete
session; `Supervisor::state()` exposes its lifecycle status. Every new connection
reinstalls the desired subscriptions, while the application keeps the same message
receiver. No delivery handler needs to perform recovery.

A dedicated task continuously polls rumqttc. It exits on connection errors so
Etherbird alone controls recovery and backoff. Application delivery uses a bounded
broadcast channel of 64 messages; slow consumers receive `Lagged` errors, reported
by the example, rather than blocking MQTT progress. Messages, including retained
messages, can arrive during subscription setup and are buffered in this channel.
Readiness does not guarantee uninterrupted delivery through an outage.

This example deliberately uses clean sessions. The broker harness also uses a
run-once QoS 0 publish helper; successful return means queue admission, and a
separate client verifies delivery. It does not preserve broker session
state, offline messages, or in-flight QoS exchanges across replacement connections.
QoS acknowledgements remain rumqttc's responsibility and do not acknowledge
application processing. A durable subscriber or reliable publisher needs a different
policy that preserves protocol state; do not copy connection replacement or generic
publish retries into such an adapter. The supplied transport is plain TCP for a local
broker; configure rumqttc authentication and TLS before adapting it to other services.

The demo uses the actual rumqttc client against a small MQTT wire-protocol fixture,
not a full broker. It withholds SUBACK to verify readiness, then closes the listener
and socket and restarts at the same address with no stored subscriptions. It verifies
both topics are requested again, both messages arrive through the original receiver,
and the resource generation changes. Example tests also cover subscription rejection
and connection loss before setup completes. CI runs these tests and the demo.

### Repeated outages with a real broker

[The broker harness](../examples/mqtt/session/broker.rs) launches an isolated
`eclipse-mosquitto:2.0.22` Docker container on a free loopback port kept fixed across restarts.
Persistence is disabled. It keeps one application supervisor and message receiver
through the entire test; an independent test peer, without a supervisor, is
recreated after each broker restart.

Every connected phase sends twenty uniquely identified messages to the application's
two subscribed topics and publishes twenty messages from the managed application to
two different topics observed by the peer. It verifies every topic and payload.
Between phases it kills the broker with SIGKILL, checks readiness is withdrawn, and
polls an application publish for 300 ms while the broker remains down. That same
pending operation resumes after restart; the replacement must have a newer resource
generation, both subscriptions must work again, and the old driver must be stopped
even while its resource handle is retained.

The default ten outages verify 220 incoming and 220 outgoing deliveries, plus ten
operations that wait through an outage. The waiting probes use a separate topic;
only their queue admission is checked, and they are excluded from delivery counts.
This measures recovery between verified traffic phases, not delivery guarantees for
messages interrupted by an outage. Clean sessions and QoS 0 can lose such messages.

The harness saves `broker.log`, `mosquitto.conf`, and `summary.json` under
`target/mqtt-broker/<unique-container-name>/` and removes its own container on exit.
Docker must be running and able to pull the pinned image. CI runs this test on Linux;
the hardware-free fixture remains available on Windows, Linux, and macOS without
Docker.

## Modbus TCP and WebSockets

```sh
cargo run --features pool --example live_recovery
```

This starts both loopback services, exercises outages and recovery, and exits with
an error if a check fails. No external service or hardware is needed. See
[live recovery checks](LIVE_RECOVERY.md) for the scenarios and saved artifacts.

The shared adapter dispatches to each dependency's native client. The WebSocket
service requires a subscription on every new session, illustrating why repeating
application setup matters beyond reconnecting a TCP socket. The Modbus adapter
illustrates exclusive mutable access and response deadlines for unresponsive devices.

Modbus telemetry uses `Pool::execute_with_retry` with at most three operation attempts
and a ten-second overall deadline. The error predicate permits transient transport
failures and rejects malformed replies and Modbus exceptions. The live harness also
interrupts a read, verifies that it waits for a ready replacement, and checks that
the caller receives a successful result on its second attempt. A separate interrupted
request uses ordinary `execute` and verifies one invocation. WebSocket and serial
application operations continue to use ordinary run-once execution.

Use one supervised resource, or a pool with a maximum of one, when the peer cannot
support parallel sessions. If operations on that session must also be serialized,
use exclusive pooled calls or enforce serialization in the adapter. Direct
supervision permits concurrent operations. A general WebSocket event feed also needs its own reader
task and a channel for received events; this fixture uses serialized request/reply
messages so the lifecycle behavior is easy to follow.

## Serial instruments

Run against a compatible device or serial simulator:

```sh
# Windows
cargo run --features pool --example serial_device -- COM3 115200 10
# Linux; use a stable device path where available
cargo run --features pool --example serial_device -- /dev/serial/by-id/<device> 115200 10
# macOS
cargo run --features pool --example serial_device -- /dev/cu.usbserial-<device> 115200 10
```

The arguments are the port path, optional baud rate (default 115200), sample count
(default 10), and gain (default 1). The run has an overall 60-second deadline and always
awaits managed-client shutdown afterward. Failed samples are reported; the next
application request waits for recovery rather than replaying the failed sample.

The example's instrument protocol uses UTF-8, newline-delimited commands and replies:

| Command | Required reply | Purpose |
| --- | --- | --- |
| `HELLO` | `READY` | Setup handshake on every newly opened port. |
| `GAIN <u32>` | `OK` | Restore desired measurement gain after the handshake, before readiness. |
| `SAMPLE` | An unsigned decimal integer, such as `42` | Application measurement. |
| `PING` | `PONG` | Idle health probe once per second. |

Replies may use LF or CRLF and are bounded to 64 payload bytes. Each exchange has a
750 ms deadline after taking the port mutex. Cancellation, errors, and timeouts
close the port so a pending reply cannot be mistaken for a later request's reply.
Writes go directly to the unbuffered serial stream; the device reply confirms
receipt. The example avoids flushing because the Unix serial drain can block
the runtime and prevent its asynchronous deadlines from advancing.
The serial builder reapplies the configured baud rate on each open. A single
slot in a managed pool owns the physical port; proxy calls acquire that slot.
The optional fourth CLI argument selects gain (default 1). A named proxy value
stores it before lazy startup and reapplies it to each new resource; setup sends
the value to the device. Changing an already-running client's stored value affects
the next setup; this example does not implement immediate device reconfiguration.

The port factory is an injection point for device discovery or other port settings.
The supplied hardware factory retries the same path. If a reattached USB device
receives a different path, adapt that factory to discover the device by identity.
Replace the illustrative handshake and measurement protocol with your instrument's
commands before using this example with it.

### Hardware-free checks

On Linux and macOS, run an outage check using real `tokio-serial` pseudo-terminal pairs:

```sh
cargo run --features pool --example serial_device -- --demo
```

The fixture completes setup, receives a sample command, and closes its port before
replying. Etherbird creates a replacement, repeats the handshake, restores gain 7,
and reads 294 from the fixture's base measurement of 42. Each new fixture starts
without a gain and requires configuration before sampling, so the result verifies
device configuration restoration. Assertions also check that the failed operation ran once and that
the replacement generation changed. Each replacement uses a fresh pseudo-terminal
pair; this checks serial I/O and session replacement, not physical USB rediscovery.

Windows does not provide this pseudo-terminal API. The serial adapter compiles on
Windows, and its recovery and cancellation tests use Tokio duplex byte streams on
all three CI platforms. Unix also runs a test with real pseudo-terminals in a
child process with a 25-second deadline, so a stuck native serial call fails the
test instead of hanging the suite. Physical
device unplug/replug behavior still requires a hardware check.

```sh
cargo test --example serial_device
```

The [managed client example](../examples/managed_client.rs) remains a smaller,
dependency-free illustration of generated typed methods and attribute replay.
