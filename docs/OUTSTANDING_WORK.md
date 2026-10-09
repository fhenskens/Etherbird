# Outstanding work

## Reduce pooled operation dispatch overhead

Status: open. The inline polling spike was discarded; its measurements are retained.
Scope: `Pool::execute` and the typed managed proxy's pooled call path.

Small operations can spend more time in pool coordination than in useful work.
The [MQTT scheduler investigation](MQTT_PERFORMANCE.md#checking-scheduler-handoffs)
found that the pooled path on Ubuntu/WSL admitted about 39,524 publishes/sec with
three callers on the default runtime, compared with 200,671 on current-thread.
Pooled suite CPU dropped from roughly 7.2 seconds to 0.8 seconds, and voluntary
context switches dropped from about 192,000 to 117. Direct supervision performed
in the same range as manual coordination. Windows responded differently.

Subsequent [WSL CPU profiles](MQTT_PERFORMANCE.md#wsl-cpu-profiles) succeeded with
`perf`: roughly 32% of default-runtime pooled samples include the futex syscall,
with wakeup stacks through Tokio scheduling and the pool's operation future.
The pooled suite used 6.09 CPU seconds versus 0.84 on current-thread; the latter
still executed about 2.2 times the manual suite's instructions.

These observations implicate scheduling handoffs but do not identify the precise
cost of each notification, allocation, lock, or task dispatch. This is an
optimization opportunity, not evidence that every pooled workload has the same cost.

Work to investigate:

- Extend the captured WSL stack profiles with a comparison on another Linux
  environment; distinguish WSL effects from general behavior. Initial allocation
  measurements are available in the [admission report](POOL_DISPATCH_PERFORMANCE.md).
- Reduce coordination within the existing Tokio task model. Inline polling
  reduced Linux handoffs but regressed Windows and concentrated operation polls
  in one dispatcher; its code and Cargo feature were removed.
- Consider caller-side execution only with a design that preserves independent
  shutdown cancellation. An unpolled caller must not keep an operation alive or
  prevent teardown; this contract now has an explicit regression test.
- Examine redundant notifications, cancellation queue scans, boxed jobs, result
  channels, and repeated subscriptions before choosing an implementation.

The first retained cleanup disarms cancellation notifications after a completed
call. The [repeat benchmark](MQTT_PERFORMANCE.md#removing-redundant-completion-notifications)
found no meaningful throughput/CPU improvement, so this is not considered a solution
to the main slowdown. The larger investigation remains open.

The next retained change uses targeted dispatcher notifications and removes the
dispatcher's duplicate capacity subscription/acquisition allocation. The
[comparison](MQTT_PERFORMANCE.md#targeted-dispatcher-wakeups) suggests about 10%
lower pooled WSL default-runtime CPU and 18% higher throughput at sixteen callers,
with little change to Windows default throughput. Three-caller WSL default
throughput still shows the original large penalty. More runs and workloads are
needed; remaining coordination costs have not been isolated.

Completion criteria:

- Reproduce the existing benchmark baseline, then compare the candidate against
  manual, direct-supervisor, and current pooled implementations with 1, 3, and 16
  callers on current-thread, two-worker, and default runtimes. Measure throughput,
  tail latency, CPU, context switches, and allocations; report platform variation.
- Preserve exclusive leases and capacity limits, FIFO and priority ordering,
  custom queue rejection, readiness gating, cancellation of queued and running
  calls, retry deadlines, recovery isolation, and cancellation-safe shutdown.
- Check slow operations and multiple pool slots as well as tiny MQTT publishes,
  so reducing dispatch overhead does not sacrifice useful concurrency or fairness.
- Pass the existing regression tests and recovery demos, add focused regressions
  for any changed execution behavior, and update the performance guide with results.

The [inline dispatch experiment](MQTT_PERFORMANCE.md#inline-dispatch-experiment)
records the discarded prototype and platform tradeoffs. The retained
[uncontended admission path](POOL_DISPATCH_PERFORMANCE.md) removes three allocation
events when a ready built-in pool has no queued work or public waiters. WSL default
MQTT throughput improved about 19% with one caller and 15% with three; Windows default
changed little and some contended instrumented cases slowed. Delayed operations
and blocked borrowers retained four-slot concurrency. The shared task registry's
coordination cost and remaining handoffs still need work. Real slow transports,
independently scheduled borrower stress, Windows CPU, macOS, and non-WSL Linux
measurements remain outstanding. Runtime selection is a diagnostic control, not
a universal workaround.

The [generated direct proxy](MQTT_PERFORMANCE.md#generated-direct-supervision-proxy)
now provides the same typed application methods for concurrency-safe resources
without pool dispatch. Its MQTT throughput remains comparable with the manual
coordinator in the captured runs. It is an explicit alternative with caller-owned
operation futures, not a replacement for exclusive leases, custom queue policies,
or independent operation draining during pool shutdown. Optimising those pooled
guarantees remains open.

## Lifecycle improvements identified by rocsteady

Recorded 2026-10-09 from rocsteady's 0.1.0-alpha.1 implementation against published
Etherbird 0.3.1. Framework items below are implemented for 0.4.0;
[release verification](RELEASE.md) tracks delivery. Downstream adoption remains open. Rocsteady
workarounds do not constitute implementation in Etherbird. rocsteady tracks adoption in its
`docs/OUTSTANDING_WORK.md`, under "Adopt upstream Etherbird lifecycle improvements".

The [scoped implementation plan](LIFECYCLE_IMPROVEMENTS.md) records ownership,
design contracts, delivery order, compatibility decisions and acceptance checks.
All four items are framework work; protocol classification and explicit session
destruction remain adapter responsibilities. The
[implemented contracts](LIFECYCLE_POLICIES.md) and [migration notes](../CHANGELOG.md)
document the actual APIs; checked items mean framework implementation, not downstream adoption.

### EB-ROC-1: non-retryable lifecycle failures

- [x] Define a policy separating recoverable lifecycle failure from failure that
  cannot succeed without changing caller configuration (for example rejected
  credentials or an unsupported setup procedure).
- [x] Stop further create/connect/setup attempts for the latter case and promptly
  fail readiness waiters, queued operations and subsequent calls with an actionable
  cause. Define how failure state and cause are observed without logging secrets.
- [x] Specify recovery/reset/reconfiguration behavior, pool aggregation when other
  slots are healthy, and shutdown precedence; preserve bounded admission and teardown.
- [x] Keep this distinct from current `Lifecycle::is_terminal`: that hook controls
  teardown (skipping disconnect), but the supervisor still reconnects afterward.
- [x] Test direct Supervisor, pooled admission and generated clients: rejected setup
  is never ready, waiting clones wake, no repeated setup occurs, transport failures
  still recover, and awaited stop remains correct.

Motivation: rocsteady currently implements an authentication watch/latch, gates its
connector, and races every managed call against the latch because Etherbird cannot
represent this failure policy directly. The implemented API uses LifecycleFailurePolicy, Error::Lifecycle, failure accessors and explicit reset.

### EB-ROC-2: operation failure classification

- [x] Provide an explicit policy for returning an operation error while retaining a
  healthy resource, separately from errors requiring recovery/retirement.
- [x] Distinguish recovery classification from `is_expected` logging and from
  opt-in replay predicates; retaining a resource must not imply replay is safe.
- [x] Cover direct, pooled, leased and generated-client paths without requiring
  callers to nest Result inside a successful operation result.
- [x] Test healthy peer rejection/local validation without replacement, transport or
  malformed-response failures with recovery, cancellation retirement and no replay.

Motivation: rocsteady's `preserve_device_error` nests healthy device/local errors
inside successful Etherbird results to avoid reconnecting after a valid rejection.
Wire-error interpretation remains the adapter's responsibility.

### EB-ROC-3: configuration validation

- [x] Define validity rules and actionable validation errors for lifecycle deadlines,
  optional timeouts and retry/backoff bounds; audit PoolConfig as part of the design.
- [x] Explicitly decide zero-duration semantics and whether disabled bounds use None;
  do not silently change configurations currently accepted by constructors.
- [x] Expose validation/checked construction so adapters can delegate framework
  configuration checks without duplicating them. Choose a compatibility/migration path.
- [x] Test boundary values, inverted retry bounds, optional timeouts, defaults and
  invalid configurations failing before lifecycle/channel activity.

Motivation: rocsteady currently validates positive Etherbird lifecycle deadlines,
positive initial retry delay and max_retry_delay >= retry_delay itself.

### EB-ROC-4: resource retention after awaited shutdown

- [x] Audit strong references held by Supervisor attributes, Pool entries,
  last_resource caches, callbacks, watchers and tracked jobs after stop completes.
- [x] Define which internal references should be released at stop and implement that
  behavior, with an explicit policy for attribute reads after stop.
- [x] Preserve documented stored configuration/attribute behavior during outages;
  distinguish retention while recovering from retention after permanent shutdown.
- [x] Document that caller-owned ResourceHandle/Arc values may retain resources,
  and that adapters must explicitly close streams and destroy sensitive session state
  during teardown rather than relying solely on Rust object destruction.
- [x] Test Drop/Weak observations for direct/pooled shutdown, replacement, callbacks
  and outstanding external handles. Verify awaited stop drains work before release.

Motivation: rocsteady's regression found setup state retained after stop because
the stopped framework could retain Session. Explicit state destruction during
disconnect fixed rocsteady; the generic retention contract still needs review.

Implementation evidence (2026-10-09): default and pool-enabled regression suites,
Clippy for all targets/features, Rust 1.88 compatibility, generated clients and
MQTT/Modbus/WebSocket recovery demos pass locally on Windows. New policy tests
cover failure/reset/stop races, partial-health pools, retained errors, replacement,
external handles, user captures and late callback retention. A temporary rocsteady
consumer removing its authentication latch/gate and nested-result workaround passes
all 43 tests on current Rust and 1.88. See the
[implementation record](LIFECYCLE_IMPROVEMENTS.md#implementation-record).
Cross-platform CI and package verification are recorded in [RELEASE.md](RELEASE.md).
Production rocsteady adoption remains pending.
Delivery: implement and document these as framework contracts, exercise an external
adapter and publish a new compatible/versioned release. Rocsteady adoption of the
published dependency is tracked downstream, separately from framework completion.
Protocol framing, typed codecs, heartbeat reader ownership and remote-handle
semantics remain in rocsteady.
