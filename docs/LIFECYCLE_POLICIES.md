# Lifecycle failure, validation and shutdown contracts

Failure policies, checked construction and shutdown ownership are available since
0.4.0; single-attempt timeouts and bounded FIFO are available since 0.4.1.
The additions to Error and ResourceState and the post-stop
attribute change require migration of exhaustive matches and resource-derived reads.

## Error classification

`Lifecycle::lifecycle_failure(error)` defaults to `LifecycleFailurePolicy::Retry`.
Return `Fail` for create, created hooks/callbacks, connect, setup or disconnect-watch
errors that cannot succeed without an explicit caller decision. Etherbird retains
the cause in `Arc<E>`, publishes `ResourceState::Failed`, and wakes readiness and
admission waiters with `Error::Lifecycle`. Failed initialization never publishes a
ready handle. Required teardown still runs with the existing deadlines.

`Supervisor::failure()` and direct proxy `failure()` expose the latched cause.
Error Display/Debug and framework failure logs redact that cause; explicit access
to the value or error source may expose adapter-provided secrets. Adapters should
still design secret-safe error values. Timeouts/task failures remain retryable;
late abandoned-hook errors cannot suspend or retire a replacement generation.

`Lifecycle::operation_failure(error)` defaults to `OperationFailurePolicy::Recover`.
Return `Retain` for a valid peer rejection or local error that leaves the resource
healthy. The caller receives `Error::Operation(error)` and no recovery is requested.
This applies to Supervisor, direct proxies, leases, pool dispatch and generated
methods. A watchdog or another concurrent call can independently request recovery.

Neither policy changes `is_expected` logging classification, nor grants permission
to replay. Ordinary calls run once. Explicit retry predicates, attempt limits and
the overall deadline still decide replay; an opted-in retry after a retained error
may reuse the same resource. `is_terminal` continues to skip disconnect during
teardown while preserving cleanup/destroy. Teardown errors never latch a failure.

For an adapter using `std::io::Error`, the classification methods inside its
`Lifecycle` implementation might be:

```rust,ignore
fn lifecycle_failure(&self, error: &Self::Error) -> etherbird::LifecycleFailurePolicy {
    if error.kind() == std::io::ErrorKind::PermissionDenied {
        etherbird::LifecycleFailurePolicy::Fail
    } else {
        etherbird::LifecycleFailurePolicy::Retry
    }
}

fn operation_failure(&self, error: &Self::Error) -> etherbird::OperationFailurePolicy {
    if error.kind() == std::io::ErrorKind::InvalidInput {
        etherbird::OperationFailurePolicy::Retain
    } else {
        etherbird::OperationFailurePolicy::Recover
    }
}
```

Use the adapter's actual protocol evidence when classifying errors. An InvalidInput
error caused by malformed remote data may require recovery rather than retention.

## Explicit reset and pool aggregation

Acquire, execute, begin and connected never clear a failure latch.
`Supervisor::reset_failure()` (also on the direct proxy) returns true when it
clears a suspended cause, permitting attempts after current teardown finishes.
It returns false with no cause or after shutdown has been requested. Update the
adapter's configuration before resetting; Etherbird adds no generic credential
replacement API. Stop/restart of a standalone supervisor clears the failure; a
stopped proxy/pool remains permanently closed.

Failed pool entries count toward maximum capacity and are not idle-retired or
automatically replaced. Healthy slots keep serving work; leased/recovering slots
remain eligible, so a failing peer slot does not poison the pool. When every entry
has a latched cause, borrow, connected and queued/subsequent operations fail with
the first entry's typed cause in membership order, even if unused capacity remains.
`Pool::failure()` exposes this aggregate cause; inspect `supervisors()` for every
slot's cause. Pool state prioritizes healthy/connecting/recovering slots over Failed.
`Pool::reset_failed()` resets current suspended entries and returns their count.

Shutdown takes precedence over a reset and stops further attempts. It continues
bounded teardown and pooled operation draining. Framework failure classification
does not itself make interrupted protocol exchanges safe: adapters must poison
interrupted sessions or report connection loss, including when heartbeat is disabled.
Direct operations remain caller-owned and are not independently drained on stop.

## Checked construction

Use `Config::validate()` or `Supervisor::try_new` / `try_start` for positive
`retry_delay` and `max_retry_delay >= retry_delay`. Zero mandatory or optional hook
deadlines are allowed as immediate deadlines; `None` disables an optional deadline.
Tokio's timeout semantics still apply to immediately ready futures.

`PoolConfig::validate()` and Pool's `try_new` / `try_start` (including the
`*_with_queue_factory` forms) require positive max_size, min_size <= max_size,
and positive idle_timeout. Zero min_size is valid. Checked custom queue construction
returns a validation error for a nonempty queue; invalid PoolConfig never invokes
the queue or supervisor factory. A factory must independently validate the
Supervisor Config it creates; Pool cannot inspect a factory before invoking it.

`ConfigError` identifies the field and rule using static text. Existing unchecked
constructors keep their prior behavior. `RetryPolicy::validate()` accepts every
representable policy: attempts are NonZeroUsize and a zero deadline returns Timeout
without running an operation. Adapter-specific stricter deadline, address,
heartbeat and protocol rules remain adapter responsibilities.

## Ownership after stop

After awaited stop, Supervisor's latest-resource cache and Pool's resource cache
and entries are released. Resource-derived attribute reads return None; `value`
still returns stored configuration. Setters/callback registrations remain for
documented standalone restart/replay. Attributes remain readable during ordinary
outages and idle retirement; they do not imply readiness.

An external Arc, ResourceHandle, cloned ResourceStatus, caller-owned future or user
callback/value capture can retain a resource. Existing timeout behavior can also
leave abandoned create/created/connect/setup/cleanup/disconnect/destroy work
holding references after stop; late disconnect defers destroy until it finishes.
These tasks cannot repopulate the stopped pool's cache. Bounded shutdown is not a
promise that every Weak reference expires immediately.

Adapters must close streams and destroy generation/session state in teardown,
independently of object destruction. Dropping a resource is not a secret-erasure
guarantee. Pool stop cancels and drains tracked jobs before resource teardown;
direct stop preserves its documented caller-owned operation limitation.

## Single-attempt timeouts and bounded admission

`Supervisor::execute_with_timeout`, `Pool::execute_with_timeout` and both proxy
variants run a `FnOnce` operation with no retry predicate or replay. The timeout
starts on polling and includes readiness/recovery, queued admission for pooled
calls, operation execution and any failure teardown awaited by the underlying
call. `Pool::execute_with_priority_and_timeout` preserves explicit scheduling
priority. `ResourceLease::execute_with_timeout` starts with an already-held lease;
expiry does not release a lease still owned by the caller.

For readiness alone use `Supervisor::acquire_with_timeout`,
`Pool::connected_with_timeout` or either proxy's `connected_with_timeout`.
Zero timeout returns `Error::Timeout` without starting supervision, admitting
work or invoking the closure. An already requested shutdown returns `Stopped`
even with zero timeout; observed shutdown also takes precedence over completion
and expiry. Ordinary positive deadlines follow Tokio timeout semantics for
immediately ready futures. A timeout cannot preempt synchronous blocking work.
Single-attempt methods do not implicitly restart an already stopped supervisor;
use its explicit restart/start lifecycle API if restart is intended.

Expiry drops caller-owned direct work. Pooled expiry closes the result channel
and signals queued/active job cancellation; active future destruction happens
when the tracked task next runs. Awaited pool stop cancels and drains these tasks.
Cancellation does not infer remote completion or automatically poison a protocol
stream. Adapters must still retire unsafe interrupted exchanges themselves.
Readiness timeout leaves supervision running; an operation timeout is not a
request to stop the whole client.

`BoundedFifoQueue::new(capacity)` bounds waiting entries, independently of active
operations and pool resource count. Every admission visits this queue even when
a resource is ready. Zero capacity rejects every operation with `QueueError::Full`.
Rejected items are dropped; `retain` removes cancelled items and reclaims space.
The dispatcher performs cancellation pruning asynchronously, so timeout return
does not promise that queue space has already been reclaimed.

```rust,no_run
# #[cfg(feature = "pool")]
# fn example<L: etherbird::Lifecycle>(factory: impl Fn() -> etherbird::Supervisor<L> + Send + Sync + 'static) {
use etherbird::{BoundedFifoQueue, Pool, PoolConfig};
let pool = Pool::try_new_with_queue_factory(
    factory,
    PoolConfig { min_size: 1, max_size: 1, ..Default::default() },
    || BoundedFifoQueue::new(64),
).unwrap();
# }
```

Protocol deadlines, response validation, heartbeat coordination and compatibility
error mapping remain adapter concerns. See [release verification](RELEASE.md) for
the tested environments and external-consumer evidence.