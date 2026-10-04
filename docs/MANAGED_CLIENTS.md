# Choosing a managed client

Choose how operations share a resource when constructing a generated client:

| Requirement | Direct supervision | Pooling |
| --- | --- | --- |
| Construction | `Client::from_supervisor(supervisor)` | `Client::new(pool)` |
| Backing facade | `SupervisedResourceProxy<L>` | `ManagedResourceProxy<L>` |
| Resources | One supervised resource | Configured minimum and maximum |
| Operation access | Concurrent calls on the shared resource | Exclusive lease per managed call |
| Waiting | Lifecycle readiness | Lifecycle readiness and available capacity |
| Scheduling | Caller and Tokio scheduling | FIFO, priority, or a custom queue |
| Operation ownership | Caller polls the operation | Pool tracks a separate Tokio task |
| Shutdown | Caller observes shutdown on its next poll | Pool cancels and drains tasks before teardown |
| Configuration restoration | Named setters and stored values | Named setters and stored values |
| Operation replay | Opt in per call | Opt in per call |

Direct supervision fits a resource whose operations safely overlap, such as the
MQTT example's shared publish client. Pooling fits exclusive request/response
exchanges, bounded resource counts, or application scheduling requirements.
`Send + Sync` alone does not establish that a protocol permits overlapping calls.
Setting `max_size = 1` retains a pool's exclusive leases and queue semantics.

## Declare methods once

The lifecycle adapter supplies `Hooks` and a resource with this method:

```rust,ignore
etherbird::managed_client! {
    pub struct Client for Hooks {
        async fn ping(message: String) -> String;
    }
}

let direct = Client::from_supervisor(Supervisor::new(Hooks, Config::default()));
let reply = direct.ping("hello".into()).await?;
direct.managed.stop().await;

let pool = Pool::new(
    || Supervisor::new(Hooks, Config::default()),
    PoolConfig::default(),
);
let pooled = Client::new(pool);
let reply = pooled.ping("hello".into()).await?;
pooled.managed.stop().await;
```

The [runnable introductory example](../examples/managed_client.rs) implements the
complete adapter and exercises both constructions. The MQTT variants share
[one method declaration](../examples/mqtt/etherbird/proxy.rs) and
[lifecycle hooks](../examples/mqtt/etherbird/lifecycle.rs), with separate
[direct](../examples/mqtt/supervised/mod.rs) and
[pooled](../examples/mqtt/etherbird/client.rs) construction.

Construction infers the backend. When spelling out a direct client's type, use
`Client<SupervisedResourceProxy<Hooks>>`; `Client` defaults to the pooled backend.
Generated dispatch uses `ManagedClientBackend` with static dispatch.
Both `fn` and `async fn` declarations expose async methods returning owned values
and `Error<Hooks::Error>`. Borrowed results and streaming interfaces need a manual
wrapper. The adapter still defines protocol readiness and recovery hooks.

## Startup and configuration

Wrap `Supervisor::new` or use `Pool::new` to configure the client before tasks
start. The first operation or `managed.connected().await` starts supervision.
Use an already started supervisor or pool to begin supervision immediately;
initial setup may then run before subsequently assigned configuration.

Both facades provide `set_attribute`, `attribute`, `set_value`, and `value`.
Stored values are readable before creation; setters apply to existing resources
and replacements before setup. Attribute reads can inspect the latest created
resource during recovery, so they do not imply readiness. `is_connected()` queries
the live indicator; `connected().await` follows setup, transport notifications,
and replacement generations.

## Errors, retries, and cancellation

Generated methods run once. An operation error requests generation-aware recovery
and returns `Error::Operation`; lifecycle reconnect attempts follow the configured
backoff. Recovery does not imply replay of the failed operation.

For safe-to-repeat operations, both facades expose
`managed.execute_with_retry(policy, operation, retry_if)`. You can also expose it
through a manually written typed method. Attempts and the overall deadline are
bounded; only operation errors accepted by the predicate are replayed. Pooled
retries keep their exclusive lease through recovery; direct retries can overlap
other calls. See [the retry contract](../README.md#opt-in-operation-retries).

Dropping a direct call drops its active operation future. Shutdown wakes callers,
but cancellation runs when those futures are polled again or dropped. An unpolled
caller may retain its operation while teardown completes. Direct-operation panics
propagate to the caller; pooled tasks isolate operation panics and return
`Error::Stopped` for the affected call. Protocol-specific cancellation safety
remains the adapter's responsibility in either mode.

Proxy clones share configuration and permanent shutdown. Cancelling an awaited
`stop()` does not cancel lifecycle cleanup. After stopping, proxy calls return
`Error::Stopped`; do not independently restart retained clones of a supervisor
wrapped in a direct proxy. Standalone supervisors retain their separate restart API.

## Performance

Direct supervision avoids pool queue storage, result channels, and per-operation
tasks. The [MQTT measurements](MQTT_PERFORMANCE.md#generated-direct-supervision-proxy)
show performance comparable to manual coordination in the captured workloads.
Pooling has measurable dispatch overhead, particularly for very small operations
on multithreaded WSL. These measurements do not establish a universal overhead
percentage or justify bypassing exclusive access required by a protocol.
