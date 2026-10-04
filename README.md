# Etherbird

Keep recovery in one place, and application calls straightforward.

Etherbird supervises asynchronous resources through connection loss, replacement,
cancellation, and shutdown. Define how your client connects and becomes usable, then
expose it through ordinary typed async methods. Application code can wait for a ready
resource without carrying reconnect and setup callbacks through every request.

A connection often needs more work before it is useful: restoring MQTT subscriptions,
completing a device handshake, or reapplying configuration. Etherbird runs your setup
hooks before admitting operations and repeats them when it replaces the resource.
The managed facade stays stable while the underlying connection changes.

Operations run once by default; safe-to-repeat operations can opt into bounded retries.

The API is experimental.

## See it in an application

The [MQTT comparison](examples/mqtt/README.md) runs the same message handler with
two working clients. One owns its recovery coordinator; the other supplies lifecycle
hooks and delegates readiness, recovery, cancellation, and shutdown to Etherbird.
Both face the same failure checks, including subscription acknowledgements and repeated
broker outages. It shows where reusable supervision can reduce application machinery
while keeping the protocol logic explicit.

The [other examples](examples/README.md) apply the same approach to Modbus reads,
WebSocket subscriptions, and serial instruments. Etherbird is especially useful when
these lifecycle requirements recur across clients in an application.

## Components

- `Lifecycle`: async creation, created callback, connect, setup, cleanup, disconnect,
  destruction, and disconnect watching; terminal and expected error classifiers.
- `Supervisor`: independently maintained resource, capped exponential connection retries,
  observable state, generation-aware recovery, and cancellation-safe shutdown.
- `Pool`: eager minimum size, demand growth to maximum size, idle retirement, exclusive
  leases, FIFO or priority operation scheduling, and lifecycle/membership notifications.
- `ManagedResourceProxy`: stable client facade, live connection status, owned attribute
  reads, and named attribute setters reapplied to replacement resources.
- `managed_client!`: generates async forwarding methods for synchronous and async client calls.

The [feature guide](docs/FEATURES.md) describes the APIs and behavioral guarantees.

Run the complete hook and typed-wrapper example:

```sh
cargo run --example managed_client
```

```rust,ignore
etherbird::managed_client! {
    pub struct ManagedModbus for ModbusLifecycle {
        async fn read_holding_registers(address: u16, count: u16) -> Vec<u16>;
    }
}

let pool = Pool::new(
    || Supervisor::new(ModbusLifecycle::new(), Config::default()),
    PoolConfig { min_size: 1, max_size: 4, ..PoolConfig::default() },
);
let client = ManagedModbus::new(pool);
let registers = client.read_holding_registers(0, 8).await?;
client.managed.stop().await;
```

## Behavioral contract

Resources implement `Send + Sync` and use interior mutability where needed (for example,
`tokio::sync::Mutex` around a client whose methods require `&mut self`). A lease reserves
one supervisor and may support concurrent calls if the underlying client allows them.
It survives reconnection; a `ResourceHandle` refers to one particular generation.
`Pool::new` constructs without spawning tasks. `begin()` starts supervision and eager
minimum sizing; `borrow` and `execute` call it automatically. `Pool::start` is the
construct-and-start convenience API. A standalone supervisor's `begin` or `acquire`
can restart it after shutdown. Pools stay stopped.

Lower priority numbers run first, with FIFO ordering among equal priorities. Priority
is reconsidered after waiting for capacity; running operations are never preempted.
The default queues are unbounded. Custom queues can reject work with `QueueError`,
including `QueueError::Full` for a bounded queue, returned as `Error::Queue`.

## Custom operation queues

`Pool::start_with_queue_factory(resource_factory, config, queue_factory)` installs
a custom operation queue. The factory is called
once per pool and returns an empty `OperationQueue<QueuedOperation<L>>`.
The explicit queue overrides `PoolConfig::priority_queue`; operations always retain
their original priority and insertion sequence for custom scheduling.

Implement `push`, `pop`, `len`, and `retain`. `push` returns a result so a custom
queue can reject work without blocking or losing accepted operations. Queue methods run synchronously under
the pool lock and must be short and nonblocking. `retain` supports removal of
cancelled operations; `clear` has a default implementation that drains the queue
during shutdown. The dispatcher pops only once resource capacity is available,
so a custom policy can reconsider order while callers wait.

For example, this stack schedules the most recently queued operation first:

```rust
use etherbird::{OperationQueue, QueueError};

struct Lifo<T>(Vec<T>);
impl<T: Send> OperationQueue<T> for Lifo<T> {
    fn push(&mut self, item: T) -> Result<(), QueueError> { self.0.push(item); Ok(()) }
    fn pop(&mut self) -> Option<T> { self.0.pop() }
    fn len(&self) -> usize { self.0.len() }
    fn retain(&mut self, predicate: &mut dyn FnMut(&T) -> bool) {
        self.0.retain(predicate);
    }
}
```

Supply `|| Lifo(Vec::new())` as the queue factory. The public `FifoQueue` and
`PriorityQueue` types can also be supplied explicitly. Queued operations expose
`priority()`, `sequence()`, and `is_cancelled()`; executing their work remains the
pool's responsibility.

## Lifecycle and shutdown

By default, operation errors request recovery and return the original error. Terminal errors skip
normal disconnect but still run cleanup and destruction. Stale and foreign resource
handles cannot recover a replacement connection. Disconnect watching runs even when
there are no operations. `subscribe()` broadcasts state and membership changes.
`recover(...).await` waits for the affected resource's teardown, not reconnection.
`recover_current` handles a standalone resource without an expected handle. For an
explicit fire-and-forget request, use `request_recovery`. Cancelling a recovery waiter
does not cancel its teardown.

## Opt-in operation retries

`Supervisor`, `ResourceLease`, and `Pool` expose `execute_with_retry(policy, operation,
retry_if)`. Use it only when repeating the remote action is safe, such as a telemetry
register read or a request protected by an idempotency key. A dropped connection does
not prove that a remote write failed to take effect.

```rust,ignore
let policy = etherbird::RetryPolicy {
    max_attempts: std::num::NonZeroUsize::new(3).unwrap(),
    timeout: std::time::Duration::from_secs(10),
};
let sample = pool.execute_with_retry(
    policy,
    |resource| async move { resource.sample().await },
    |error| matches!(error.kind(), std::io::ErrorKind::ConnectionReset
        | std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::TimedOut),
).await?;
```

The attempt count includes the initial operation. The factory is `FnMut` and must
create a fresh future for each attempt. Only operation errors accepted by `retry_if`
are repeated. Each failed attempt requests normal lifecycle recovery; its next
attempt waits for a ready replacement, including completed setup. Connection failures
while awaiting readiness do not consume operation attempts, but consume deadline time.
Lifecycle connection retries retain their configured backoff.

The overall deadline covers queueing, acquisition, execution, and recovery/teardown.
Exhaustion returns the last `Error::Operation`; deadline expiry returns `Error::Timeout`.
Queue errors and shutdown are not retried. Cancellation drops active work and prevents
later attempts; protocol-specific cancellation safety remains the adapter's responsibility.
Terminal classification decides whether teardown skips disconnect; the predicate
independently decides whether repeating that operation is appropriate.

Pooled retries retain one lease through recovery. `execute_with_priority_and_retry`
queues the whole call at the requested priority; retries do not create additional queue
entries. Generated client methods continue to run once; opt in through the exposed
pool or a manually written typed method. The Modbus live example demonstrates both
a retried register read and a run-once interrupted request.

## Lifecycle status and shutdown

`Lifecycle::is_connected` reads the client's live connection indicator. Override it
alongside `wait_connected` to expose the transport's status independently of supervisor
state. The wait hook must check the indicator and then wait without losing notifications.
Pool/proxy `is_connected` checks every current client; `connected().await` follows
transport notifications and resource replacements. Without an indicator hook, readiness
is used as the connection indicator. `watch_disconnect` is an optional owned future;
no watchdog task is spawned when absent.

Connect, setup, cleanup, and disconnect have separate deadlines.
Creation, the created callback, and destruction have no deadline by default;
optional deadlines can be configured. The initial retry delay defaults to five seconds
and doubles to a 300-second cap. Cleanup failure never prevents disconnect.
Timed-out hook tasks may finish later. A late successful connect is closed, never
published; a timed-out disconnect finishes before destruction. Consequently teardown
hooks must tolerate repeated calls. Timeout means the supervisor stops waiting, not
that a resource has physically closed. Tasks must yield to Tokio: no deadline can
interrupt blocking code that monopolizes an executor thread. Creation cancelled during
shutdown must release partially allocated resources through Rust ownership/Drop.

`stop().await` requests shutdown once and waits for teardown; cancelling the
caller does not cancel cleanup. Pools cancel queued and running managed operations,
wake acquisition waiters, and close resources including leased ones. Manually acquired
resource handles can still exist afterward: their clients must enforce closed state.
Dropping the last public handle requests shutdown; await `stop()` before runtime exit
when cleanup must finish. Pool notification relays are joined during shutdown and
retirement even when clients retain supervisor handles. A stopped pool cannot be restarted.
Standalone supervisors also support explicit `restart().await`; generation numbers continue increasing
and registered attributes survive restart.

Attribute setters are synchronous, short, nonblocking callbacks. Register a setter
under a name to apply configuration to existing and future clients. Attribute reads
use `with_latest`/`attribute` and return owned values, including while recovery is in
progress. Protocol callbacks belong on the lifecycle/resource, so they do not borrow
pool capacity. Setter callbacks must not reenter the pool or supervisor.
Pool factories should return `Supervisor::new(...)`, so registered setters are installed
before supervision begins. Factories returning an already running supervisor are supported,
but configuration may then be applied after that resource's initial setup.
Use `set_value(name, value, apply)` and `value::<T>(name)` for assigned attributes:
the stored value can be read before creation and is replayed onto every replacement.
Stored callback values such as `Arc<dyn Fn() + Send + Sync>` can be retrieved and
called directly without borrowing capacity.
`set_on_resource_created` installs runtime callbacks and preserves a supervisor's
existing created callback when it joins a pool. For logging, configure a `tracing`
subscriber; expected errors produce warning and debug diagnostics, while unexpected
errors retain error diagnostics and the configured resource name.

Rust cannot discover and intercept arbitrary dependency methods. `managed_client!`
generates forwarding methods from declared signatures (`async fn` or `fn`). Both
forms expose async managed methods. Underlying methods must return owned
results using the lifecycle's error type; borrowed results and streaming APIs need
manual wrappers. Resource-specific lifecycle hooks remain application code.

## Development

```sh
cargo fmt --check
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps
```

Tests exercise failed hooks, proactive disconnects, stale generation reports, late
connection completion, delayed disconnect destruction, priority scheduling, cancellation,
leased shutdown, attribute replay, and generated forwarding methods.

On every push and pull request, [CI](.github/workflows/ci.yml) builds all examples,
runs their tests and hardware-free MQTT/socket demos on Linux, Windows, and macOS,
and runs the serial pseudo-terminal demo in a separate Linux job. Each demo has
an explicit time limit.

The [broker recovery workflow](.github/workflows/broker-recovery.yml) runs weekly
and can also be started manually. It checks both MQTT comparison clients, the
direct-supervisor example, and native rumqttc behavior against real Mosquitto
outages, retaining broker logs and summaries as artifacts.

## Examples

The [examples index](examples/README.md) maps each command to its implementation.
Start with the minimal managed client, or read the grouped MQTT comparison to see
the same application with manual coordination and a managed proxy.

For real socket outage checks using `tokio-modbus` and `tokio-tungstenite` clients:

```sh
cargo run --example live_recovery
# Modbus TCP only:
cargo run --example live_recovery -- --modbus-only
```

The harness runs local fixtures without Docker, interrupts requests, restores
WebSocket subscriptions on replacement sessions, checks idle watchdogs and silent
peers, and writes logs and a JSON summary under `target/live-recovery/`.

For a supervised `tokio-serial` instrument:

```sh
cargo run --example serial_device -- COM3 115200 10
# Linux/macOS: exercise real serial pseudo-terminals without hardware:
cargo run --example serial_device -- --demo
```

For a `rumqttc` subscriber that restores both subscriptions and waits for broker
acknowledgements before readiness:

```sh
cargo run --example mqtt_session -- --demo
# Real Mosquitto broker and ten abrupt outages (requires Docker):
cargo run --example mqtt_session -- --broker-demo
# Or connect to a local MQTT broker:
cargo run --example mqtt_session -- localhost 1883
```

The MQTT demo checks broker restart recovery through a stable message receiver.
It uses clean sessions; persistent MQTT sessions and publish replay require a
different adapter policy.

Compare the same MQTT application with a manual coordinator and a managed proxy:

```sh
cargo run --example mqtt_without_etherbird -- --demo
cargo run --example mqtt_with_etherbird -- --demo
```

Both pass identical readiness, cancellation, deadline, recovery, and shutdown
checks. [The comparison](docs/MQTT_COMPARISON.md) shows the manual coordination code,
the separate lifecycle definitions, and how the proxy preserves the application's
`publish(topic, qos, retain, payload)` call. Each example also supports real broker
outage checks with `--broker-demo`.

The serial adapter reopens the port, reapplies its configuration, and repeats the
device handshake before exposing the replacement. See [example adapters](docs/EXAMPLES.md)
for the instrument protocol and integration choices, and [live recovery checks](docs/LIVE_RECOVERY.md)
for the socket scenarios and artifacts.

Licensed under the Apache License 2.0.
