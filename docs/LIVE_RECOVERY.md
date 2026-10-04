# Live recovery checks

Run real `tokio-modbus` and `tokio-tungstenite` clients over TCP, interrupt their
local endpoints, and inspect the recovery logs:

```sh
cargo run --example live_recovery
# Optional: exercise only the Modbus adapter.
cargo run --example live_recovery -- --modbus-only
```

The default run starts a loopback Modbus TCP simulator and a loopback WebSocket
service. It needs no Docker, credentials, or physical devices. CI runs both
clients together on Windows, Linux, and macOS. The harness controls only listeners
and accepted sockets that it creates.

## Scenarios

Each pool has a minimum of one resource and a maximum of three. Startup checks
reserve every slot, verify that an additional read waits without creating a fourth
connection, then return one lease and verify that the read completes. The Modbus
check also cancels a waiting read before capacity returns. This demonstrates
capacity backpressure; the default waiting queue is unbounded. For bounded queue
rejection and priority ordering, run `cargo run --example scheduling`.

The harness first
holds three concurrent leases to force demand growth, then launches three sampling
workers per endpoint. These fixtures permit multiple connections; real devices
may require a maximum of one.

| Scenario | Check |
| --- | --- |
| Healthy traffic | WebSocket samples and Modbus register values arrive concurrently. |
| WebSocket interrupted mid-request | The service confirms receipt of a `BLOCK` command before its sockets are closed. The caller receives an operation error, its closure runs once, connection attempts continue, and Modbus keeps working. |
| Modbus interrupted mid-request | The device receives a special register request and withholds its response. Closing its sockets returns an operation error while WebSocket traffic continues. |
| Endpoints restarted | The same pools serve requests using replacement resource IDs. Run-once probe closures are not replayed. |
| Opt-in Modbus read retry | A read is interrupted, waits while the device is down, and runs a second time after replacement setup. The caller receives success; the harness verifies two invocations on different resources. |
| WebSocket subscriptions restored | Every new session must acknowledge `SUBSCRIBE samples` before heartbeat or sample requests succeed. Recovery checks also verify that the subscription count increases. |
| Idle disconnects | With application workers stopped, protocol heartbeats detect both endpoint outages and trigger recovery. |
| Silent Modbus device | TCP connects but register requests receive no response. The adapter's response deadline detects the stall; restarting the device restores service. |
| Shutdown with a held lease | Shutdown closes the leased transport, and every created resource has a matching destruction call. |

The Modbus simulator implements only the register reads used here, returning a
changing sequence value plus two known marker registers. The WebSocket fixture
implements a small text command protocol with session-local subscription state.
These are controlled wire-level fixtures, not complete production services.

## Artifacts and logs

Each run prints its artifact directory:

```text
target/live-recovery/<timestamp>-<pid>/events.log
target/live-recovery/<timestamp>-<pid>/summary.json
```

`events.log` contains timestamped tracing output: resource names, creation IDs,
lifecycle hooks, state changes, traffic results, retry delays, and scenario outcomes.
`summary.json` records the overall result and per-resource traffic successes/errors,
connection attempts, creation counts, and destruction counts. Deliberately interrupted
probe failures are logged separately. Counts vary with timing.

Expected socket failures produce warning and debug diagnostics. Successful setup
logs readiness, and replacement checks log both the old and new resource IDs.

## Adapter choices

[adapter.rs](../examples/live_recovery/adapter.rs) stores dependency clients behind
a Tokio mutex because requests need mutable access. The response deadline begins
after acquiring that mutex, so contention with an active operation is not treated
as a silent peer. The example uses a 750 ms response deadline, 300 ms heartbeat
interval, and retry delays of 200 ms increasing to an 800 ms cap.

A failed, timed-out, or cancelled exchange drops the transport and marks it
disconnected. This prevents another caller from consuming a partial or late reply.
The watchdog subsequently requests replacement even if the original caller cancelled
its operation. Connection status is exposed through a watch channel.

WebSocket setup restores the subscription before the supervisor publishes readiness.
The service rejects samples and heartbeats on unsubscribed sessions. This adapter
uses request/response messages; an unsolicited event feed needs a dedicated reader
and routing of replies/events rather than multiple consumers reading the socket.
The example enables plain `ws://` connections; enable a TLS feature on
`tokio-tungstenite` when adapting it for `wss://` services.

Ordinary `execute` returns a failed operation to its caller. Modbus telemetry reads
explicitly use `execute_with_retry` with three total attempts and a ten-second deadline,
including queueing and recovery. Their predicate permits transient transport errors;
protocol exceptions and invalid data are excluded. A dedicated retry probe reads the
same registers on both attempts and verifies that setup completed before replay.
WebSocket operations and the separate run-once probes retain their original behavior.
Invocation-count checks do not prove exactly-once delivery of remote side effects.

Shutdown verification counts distinct destroyed resources, rather than destroy-hook
calls. An abandoned connect that succeeds late can trigger another teardown of the
same resource. The harness waits within its deadline for deferred teardown to finish
and still fails if any created resource has not been destroyed.

These adapters expose `io::Error`. The Modbus transport wrapper reports a response
stream ending as `UnexpectedEof`, avoiding tokio-modbus 0.17's use of an unrelated
last OS error for clean EOF. Other transport errors retain their original kind;
protocol errors and device exceptions map to `InvalidData`. Production adapters
should retain protocol error types and distinguish protocol exceptions, invalid
configuration, and transport failures according to the application's policy.

For serial port reopening, repeated device handshakes, and Unix pseudo-terminal
checks, see [example adapters](EXAMPLES.md).
