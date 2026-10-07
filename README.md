# Etherbird

**Keep the client. Replace the connection.**

Etherbird keeps async clients usable through connection failures. Define how to
create, connect, and set up a resource once; the library replaces failed resources,
restores their configuration and session, and waits for setup to finish before
running application calls. Your application keeps using the same client.

It works with network connections, device handles, and other resources that need
more than reconnection to become usable again. Your adapter supplies the protocol
logic, such as authentication, subscriptions, or a device handshake.

## Useful features

- **A stable, typed client:** `managed_client!` generates ordinary async methods
  around your resource, keeping acquisition and recovery out of application calls.
- **Restored configuration:** named values and setters apply to existing resources
  and every replacement; stored values remain readable during an outage.
- **Recovery while idle:** an optional disconnect watcher detects connection loss
  even when no operation is running. Old failure reports cannot retire a replacement.
- **Bounded recovery:** configurable lifecycle deadlines and capped exponential
  connection backoff prevent callers from managing their own reconnect loops.
- **Explicit operation retries:** calls run once by default. Safe-to-repeat calls
  can opt into an attempt limit, overall deadline, and error predicate.
- **Observable state and orderly shutdown:** connection indicators, state notifications,
  lifecycle callbacks, and tracing help you monitor resources. Awaited shutdown
  completes teardown even if another shutdown caller is cancelled.
- **Optional pooling:** cap resource counts, grow on demand, retire idle resources,
  and reserve exclusive leases. Schedule operations with FIFO, priority, or a custom
  queue that can reject excess work. Pooled shutdown cancels and drains tracked work.

Start with direct supervision for a client that supports concurrent calls. Enable
pooling when resources need exclusive access, capacity limits, or queued scheduling.

```toml
etherbird = "0.3"
# With pooling:
# etherbird = { version = "0.3", features = ["pool"] }
```

The API is experimental. See the [managed client guide](docs/MANAGED_CLIENTS.md)
for construction and ownership, and the [feature guide](docs/FEATURES.md) for
lifecycle, cancellation, shutdown, and recovery guarantees.

## Performance

In the local MQTT admission benchmarks, direct supervision performs in roughly
the same range as equivalent manual coordination. Pooling adds measurable cost
for tiny, high-frequency operations, especially under saturated multithreaded
workloads; it also provides exclusive access, scheduling, and tracked operation
shutdown. Results depend on workload and runtime.

Read the [MQTT performance analysis](docs/MQTT_PERFORMANCE.md#generated-direct-supervision-proxy)
and [pool admission and allocation analysis](docs/POOL_DISPATCH_PERFORMANCE.md)
for methodology, measurements, and raw data. MQTT timings measure local publish
admission, not broker acknowledgement or message delivery.

## Concrete examples

| Example | What it shows |
| --- | --- |
| [MQTT application comparison](examples/mqtt/README.md) | The same telemetry, status, and request code with manual coordination, direct supervision, or pooling. Restores subscriptions and checks readiness, cancellation, and repeated broker outages. |
| [Modbus and WebSocket recovery](examples/live_recovery/mod.rs) · [scenario guide](docs/LIVE_RECOVERY.md) | Bounded device connections, queued register reads, opt-in retries, and restored WebSocket subscriptions after socket failures. |
| [Serial instrument](examples/serial_device/adapter.rs) · [adapter guide](docs/EXAMPLES.md) | Reopens a serial port, reapplies device gain, and repeats the handshake before admitting calls. |
| [Typed client and lifecycle](examples/managed_client.rs) | A small, self-contained introduction to lifecycle hooks, generated methods, direct and pooled clients, and configuration replay. |
| [Priority and overload](examples/scheduling.rs) | Urgent work overtakes telemetry, equal priorities retain FIFO order, and a bounded custom queue rejects excess requests. |
| [Direct MQTT session](examples/mqtt/session/mod.rs) | Uses `Supervisor` directly with a stable receiver, subscription acknowledgements, and restart recovery. |

Run the application examples from a checkout:

```sh
# MQTT comparison with local fixtures; no broker required:
cargo run --example mqtt_with_supervisor -- --demo
cargo run --example mqtt_without_etherbird -- --demo
cargo run --features pool --example mqtt_with_etherbird -- --demo

# Modbus and WebSocket fixtures; no external service required:
cargo run --features pool --example live_recovery

# Serial demo with Unix pseudo-terminals; no hardware required:
cargo run --features pool --example serial_device -- --demo
```

MQTT examples also support `--broker-demo` for real Mosquitto outages through
Docker. See the [examples index](examples/README.md) for all commands and platform
requirements, and the [MQTT comparison analysis](docs/MQTT_COMPARISON.md) for the
coordination code each approach requires.

## Generic usage

These snippets run inside an async function on a Tokio runtime. They assume a
`Hooks` adapter implementing `Lifecycle`, whose resource exposes
`async fn read(&self) -> Result<u16, std::io::Error>`. Implement `create`, `connect`,
and `disconnect`; use `setup` for authentication, subscriptions, or a handshake.
Optional hooks add cleanup, destruction, disconnect watching, live connection
status, and error classification. The [typed client example](examples/managed_client.rs)
contains a complete adapter.

### 1. Supervise one resource

`execute` waits for a ready resource and runs the operation once. An operation
error requests recovery and returns the error to its caller.

```rust,ignore
use etherbird::{Config, Supervisor};

let supervisor = Supervisor::new(Hooks, Config::default());
let result = supervisor.execute(|resource| async move { resource.read().await }).await;
supervisor.stop().await;
let sample = result?;
```

### 2. Give the application ordinary client methods

Declare forwarding signatures once. The underlying resource must support
concurrent calls for this direct client.

```rust,ignore
use etherbird::{Config, Supervisor};

etherbird::managed_client! {
    struct Client for Hooks {
        async fn read() -> u16;
    }
}

let client = Client::new(Supervisor::new(Hooks, Config::default()));
let result = client.read().await;
client.managed.stop().await;
let sample = result?;
```

### 3. Add bounded, exclusive access

With the `pool` feature, the same `Client` declaration can use up to four resources.
Each generated method reserves one resource for its operation.

```rust,ignore
use etherbird::{Config, Pool, PoolConfig, Supervisor};

let pool = Pool::new(
    || Supervisor::new(Hooks, Config::default()),
    PoolConfig { min_size: 1, max_size: 4, ..PoolConfig::default() },
);
let client = Client::from_pool(pool);
let result = client.read().await;
client.managed.stop().await;
let sample = result?;
```

### 4. Combine recovery, configuration, scheduling, and retries

This builds on the same `Client` and `Hooks`. The resource additionally provides
short synchronous `set_gain(u16)` and `gain() -> u16` methods. Its adapter's `setup`
restores the session and `watch_disconnect` reports idle connection loss.

```rust,ignore
use etherbird::{Config, Pool, PoolConfig, RetryPolicy, Supervisor};
use std::{io, num::NonZeroUsize, sync::Arc, time::Duration};

let pool = Pool::new(
    || Supervisor::new(Hooks, Config {
        resource_name: "instrument".into(),
        create_timeout: Some(Duration::from_secs(5)),
        created_timeout: Some(Duration::from_secs(5)),
        connect_timeout: Duration::from_secs(5),
        setup_timeout: Duration::from_secs(5),
        cleanup_timeout: Duration::from_secs(2),
        disconnect_timeout: Duration::from_secs(2),
        destroy_timeout: Some(Duration::from_secs(2)),
        retry_delay: Duration::from_millis(100),
        max_retry_delay: Duration::from_secs(5),
    }),
    PoolConfig {
        min_size: 1,
        max_size: 4,
        idle_timeout: Duration::from_secs(30),
        priority_queue: true,
    },
);

// Install configuration and callbacks before lazy startup.
pool.set_value("gain", 2_u16, |resource, gain| resource.set_gain(*gain));
pool.set_on_resource_created(Arc::new(|_resource| Box::pin(async {
    tracing::info!("created instrument resource");
    Ok(())
})));
let changes = pool.subscribe(); // A watch receiver for state/membership changes.
let client = Client::from_pool(pool.clone());
assert_eq!(client.managed.value::<u16>("gain"), Some(2));

let result = async {
    client.managed.connected().await?;
    let first = client.read().await?; // Ordinary calls still run once.
    let gain = client.managed.attribute(|resource| resource.gain());

    // Lower numbers have higher priority. Retry this read only, within one deadline.
    let sample = pool.execute_with_priority_and_retry(
        0,
        RetryPolicy {
            max_attempts: NonZeroUsize::new(3).unwrap(),
            timeout: Duration::from_secs(10),
        },
        |resource| async move { resource.read().await },
        |error| matches!(error.kind(),
            io::ErrorKind::ConnectionReset | io::ErrorKind::UnexpectedEof),
    ).await?;

    // Reserve one resource across multiple calls, including any recovery.
    let lease = pool.borrow().await?;
    let reserved = lease.execute(|resource| async move { resource.read().await }).await?;
    lease.release();
    Ok::<_, etherbird::Error<io::Error>>((first, sample, reserved, gain))
}.await;

// Cancel and drain managed work, then tear down every resource, even on error.
client.managed.stop().await;
let readings = result?;
```

Use a [custom queue](examples/scheduling.rs) when you also need overload rejection
or a different scheduling policy; the built-in queues are unbounded. State watchers
can await `changes.changed()` and inspect `pool.state()`. Configure a `tracing`
subscriber to collect lifecycle diagnostics.

Retry only operations that are safe to repeat: a disconnected write may already
have taken effect remotely. Completion retains the underlying client's meaning;
protocol or application acknowledgements belong in the adapter or application.
Resources must be `Send + Sync`; use interior mutability where their APIs need
mutable access. Direct calls run in their callers, while pooled calls are tracked
and drained by the library during shutdown.

[API reference](https://docs.rs/etherbird) ·
[Engineering tasks](docs/OUTSTANDING_WORK.md) ·
[CI](.github/workflows/ci.yml) ·
[Real broker recovery checks](.github/workflows/broker-recovery.yml)

Licensed under the Apache License 2.0.
