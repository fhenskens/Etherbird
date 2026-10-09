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

## Delivered framework work

Lifecycle failure policies, operation classification, checked configuration and
shutdown resource ownership shipped in 0.4.0. Bounded FIFO admission and
single-attempt call/readiness timeouts ship in 0.4.1. Protocol behavior and
adapter migration remain downstream responsibilities.

Use the [feature contracts](LIFECYCLE_POLICIES.md), [release verification](RELEASE.md)
and [changelog](../CHANGELOG.md) for behavior and evidence. The historical
[implementation plan](LIFECYCLE_IMPROVEMENTS.md) records the original scope decisions.