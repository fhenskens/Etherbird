# Capabilities and guarantees

This guide describes Etherbird's capabilities and behavioral guarantees. Regression
scenarios live alongside their assertions in the [tests](../tests/behavior.rs).
For construction and execution-mode selection, see
[the managed client guide](MANAGED_CLIENTS.md).

| Feature | API / behavior |
| --- | --- |
| Factory, connect, setup, cleanup, disconnect, destroy | `Lifecycle` hooks; created callback before connect, setup before readiness, teardown continues after errors |
| Sync or async callbacks and operations | Sync code can run within async lifecycle hooks; macro supports `fn` and `async fn` client methods |
| Lifecycle options and defaults | Configurable resource name, 20s connect, 10s setup/cleanup/disconnect, 5s initial retry, 300s retry cap; creation/created/destruction unbounded by default |
| Expected errors, per-resource logging | `Lifecycle::is_expected` plus caller-configured tracing subscriber; expected failure warning/debug, unexpected failure error |
| Independent resource supervision | Each pool entry owns its own supervisor and connection generation |
| Awaited manual recovery | `recover(...).await`, `recover_current`; completion means teardown, not reconnection |
| Reject stale recovery reports | Identity and generation checks, including foreign handles |
| Terminal errors | Terminal initialization, operation, and watchdog errors skip normal disconnect; cleanup and destroy still run. Explicit forced recovery can also skip disconnect |
| Optional proactive disconnect watcher | Optional `watch_disconnect` future; absent means no watchdog task |
| Hard connect timeout and late success cleanup | Timed-out attempt stays running; a late success is closed and never published |
| Hard setup timeout | Abandon stalled setup; close discarded resource and retry |
| Hard cleanup timeout | Abandon stalled cleanup and continue disconnect |
| Hard disconnect timeout | Stop waiting; destruction follows late disconnect completion |
| Lazy startup and eager minimum on start | `Pool::new`, `begin`, lazy borrow/execute; `Pool::start` convenience |
| Pool limit validation | Invalid minimum/maximum relationships, zero maximum, and zero idle timeout rejected; negative sizes cannot be represented |
| Restart standalone supervisors | `begin`/`acquire` restart after stop; optional explicit `restart` |
| Demand growth, maximum capacity, connecting entries counted | Demand includes queued work, waiting borrowers, and leases; connecting and retiring entries count toward capacity |
| Idle retirement and zero minimum | Retire only unleased entries above minimum; zero-minimum pool regrows |
| Leases and concurrent client-owned tasks | Reservation survives recovery; `release(self)` or Drop returns capacity, with ownership preventing use after release; public leases keep their pool alive |
| FIFO, stable priority, reconsideration after capacity wait | `FifoQueue`, `PriorityQueue`; lower priority first, FIFO on ties |
| Arbitrary queue factory | `new_with_queue_factory`, `start_with_queue_factory`, `OperationQueue` trait; scheduling metadata exposed |
| Bounded/custom queue rejection | Queue push can return `QueueError::Full` or custom rejection |
| Operation failures and cancellation | By default, failed operation runs once, recovers its slot, returns original error; queued cancellations never execute, running cancellation releases capacity |
| Shutdown queued/running operations and borrowed resources | Cancel jobs, wake callers, close all entries including leased ones, drain notification relays |
| Concurrent/cancelled shutdown callers | One background teardown; caller cancellation does not interrupt it |
| Broadcast state, membership, stopped events | Watch subscriptions and `stopped()` receivers |
| Live connection indicator and waits across replacement | `Lifecycle::is_connected` / `wait_connected`, pool/proxy query and await across all clients |
| Attribute reads while connecting/recovering | `attribute` / `with_latest` reads current resource or last created client |
| Assigned attributes readable before creation | `set_value` / `value::<T>` stores owned configuration independently of resource existence |
| Attribute/callback replay and runtime created callbacks | Named setters, typed stored callbacks, `set_on_resource_created` chained with original callback |
| Typed client method facade | `managed_client!` declares ordinary methods; `new(pool)` selects exclusive pooling and `from_supervisor(supervisor)` selects concurrent direct supervision |

## Typed clients and resource ownership

The client facade declares forwarding signatures at compile time. Borrowed results
and streaming APIs require a typed manual wrapper. Named values and closures store
resource configuration and protocol callbacks. Owned leases release capacity on Drop
or explicit release. Watch channels broadcast state changes, and tracing subscribers
collect lifecycle diagnostics.

`SupervisedResourceProxy` supports the same configuration setters, owned attribute
reads, and run-once or opt-in retry operations on one concurrency-safe resource.
It has no exclusive leases, queue policy, or per-operation Tokio tasks. Calls are
polled by their callers; cancellation drops an operation and shutdown is observed
when the caller next polls. It does not independently drain an unpolled operation
before teardown. Pooled proxies retain that stronger shutdown guarantee and task
panic isolation. Both proxy forms share permanent shutdown across clones.

Runnable MQTT, Modbus, WebSocket, and serial adapters are documented in
[example adapters](EXAMPLES.md). The [MQTT comparison](MQTT_COMPARISON.md) shows
the same application with manual coordination and an Etherbird managed client.

## Opt-in operation retries

The API supports per-operation `RetryPolicy` through
`execute_with_retry` on supervisors, leases, pools, and both proxy backends. Ordinary execution retains
the run-once contract. Retry policies bound attempts and the overall deadline; a
caller-supplied predicate decides which failures may be retried. Only opt in when
repeating the operation is safe. The Modbus live harness demonstrates read replay
after a socket outage alongside the original run-once behavior.
