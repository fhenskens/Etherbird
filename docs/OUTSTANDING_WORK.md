# Outstanding work

## Reduce pooled operation dispatch overhead

Status: open. Scope: `Pool::execute` and the typed managed proxy's pooled call path.

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

- Extend the captured WSL stack profiles with allocation measurements and a
  comparison on another Linux environment; distinguish WSL effects from general behavior.
- Evaluate reducing per-operation task spawning and thread handoffs, including
  polling operation futures within the dispatcher rather than spawning each one.
  Preserve concurrency across pool slots and isolate individual operation panics.
- Examine redundant notifications, cancellation queue scans, boxed jobs, result
  channels, and repeated subscriptions before choosing an implementation.

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

No optimization has been implemented yet. Runtime selection is a diagnostic control,
not a universal workaround.
