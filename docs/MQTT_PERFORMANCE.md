# MQTT coordination performance

The managed proxy buys reusable coordination at a measurable runtime cost. In this
local saturated workload, that cost was substantial on Ubuntu under WSL. Direct
supervision performed in the same range as the manual implementation. On Windows,
the pooled proxy had higher throughput but higher tail latency. These results do
not establish a universal overhead percentage or a production MQTT capacity limit.

## Reproduce

```sh
cargo build --release --locked --example mqtt_without_etherbird --example mqtt_with_etherbird --example mqtt_with_supervisor
cargo run --release --locked --example mqtt_without_etherbird -- --benchmark
cargo run --release --locked --example mqtt_with_etherbird -- --benchmark
cargo run --release --locked --example mqtt_with_supervisor -- --benchmark
```

The [shared benchmark](../examples/mqtt/common/benchmark.rs) uses the actual example
clients and their shared rumqttc transport. A loopback MQTT wire fixture acknowledges
subscriptions and consumes publishes. Each process warms up with 500 publishes,
then runs five rounds each with 1, 3, and 16 concurrent callers. Each caller submits
2,000 QoS 0 publishes carrying 64 bytes. Calls use structured concurrency, as in
the application, within the examples' default multithreaded Tokio runtime.

The [direct Supervisor wrapper](../examples/mqtt/supervised/mod.rs) reuses the pooled
client's lifecycle hooks, timeout and backoff configuration, protocol, and application.
It calls `Supervisor::execute` without pool queueing or exclusive leases. All three
variants pass the shared readiness, cancellation, recovery, and shutdown checks.

Timing starts after setup. Call latency ends at local publish admission, not broker
acknowledgement or subscriber delivery. Each round subsequently verifies that the
fixture consumed every publish and that the connection generation stayed unchanged.
`drained_ms` includes that wait, whose one-millisecond polling can distort short
rounds. The benchmark includes network, fixture, allocation, and scheduler costs;
it does not isolate Etherbird instructions or measure heap allocation counts.

## Recorded results

Rerun on 2026-10-04 after the user stopped a background workload. These results
supersede the previous timing tables. Builds finished before measurements started;
platforms and benchmark processes ran sequentially, without overlapping benchmarks.
Stopping the known workload does not establish that the machine had no other activity.

The x86-64 host exposes 16 logical processors. Windows used Rust 1.99.0; Ubuntu
under WSL2 used Rust 1.96.0 and kernel 6.6.87.2. Both used the default Cargo release
profile. Compiler, OS, and scheduling differences mean these platform results
should not be compared as an OS ranking. No CPU affinity or scheduler tuning was applied.

Each platform ran manual/pooled/supervisor/supervisor/pooled/manual processes.
Values below are medians of ten rounds for each caller count; latency values are
medians of per-round percentiles, rather than percentiles of pooled observations.
The [180 raw rounds](benchmarks/mqtt-three-way.csv) retain admission and drain timings.

| Platform | Callers | Manual calls/sec | Direct Supervisor calls/sec | Pooled proxy calls/sec |
| --- | --- | --- | --- | --- |
| Windows | 1 | 137,033 | 130,299 | 143,350 |
| Windows | 3 | 133,595 | 131,402 | 154,144 |
| Windows | 16 | 135,353 | 134,471 | 153,760 |
| Ubuntu/WSL | 1 | 376,154 | 429,121 | 15,426 |
| Ubuntu/WSL | 3 | 556,470 | 478,162 | 38,731 |
| Ubuntu/WSL | 16 | 1,153,076 | 1,116,739 | 109,157 |

Latency medians, in microseconds, shown as p50 / p95:

| Platform | Callers | Manual | Direct Supervisor | Pooled proxy |
| --- | --- | --- | --- | --- |
| Windows | 1 | 8.40 / 12.25 | 8.40 / 12.20 | 3.30 / 14.00 |
| Windows | 3 | 21.65 / 26.95 | 21.65 / 27.50 | 8.85 / 114.40 |
| Windows | 16 | 114.30 / 125.65 | 114.65 / 127.65 | 63.15 / 172.85 |
| Ubuntu/WSL | 1 | 0.30 / 9.21 | 0.34 / 4.17 | 62.64 / 84.51 |
| Ubuntu/WSL | 3 | 0.93 / 23.06 | 0.89 / 28.25 | 62.79 / 126.18 |
| Ubuntu/WSL | 16 | 10.35 / 46.13 | 9.96 / 50.61 | 126.32 / 250.02 |

Ubuntu process CPU time (`/usr/bin/time -p`, user plus system):

| Variant | First suite | Second suite |
| --- | --- | --- |
| Manual | 0.64 seconds | 0.65 seconds |
| Direct Supervisor | 0.63 seconds | 0.65 seconds |
| Pooled proxy | 7.15 seconds | 7.15 seconds |

The [raw CPU measurements](benchmarks/mqtt-cpu.csv) also record wall, user, and system
time. Each suite admitted 200,500 publishes including warmup. These measurements
include the fixture, startup, and result reporting; they are not isolated library
CPU costs. The pooled suites used approximately eleven times the process CPU of
the other variants. Windows process CPU time was not measured.

## Executable footprint

Rebuilt executable sizes:

| Platform | Manual | Direct Supervisor | Pooled proxy |
| --- | --- | --- | --- |
| Windows | 2,067,456 bytes | 2,330,624 bytes | 2,579,456 bytes |
| Ubuntu/WSL | 2,660,704 bytes | 3,032,248 bytes | 3,357,472 bytes |

The pooled executables are 24.8% and 26.2% larger than their manual counterparts.
Windows PE `.text` sections previously measured 1,477,870 and 1,808,638 bytes
respectively (22.4% more for pooled). Linux GNU `size` reported `text` values of
1,882,394 for manual, 2,093,290 for direct supervision, and 2,295,798 for pooled;
that category includes read-only data as well as code.

These are whole example executables, including demos and harnesses. The manual
executable also contains the native-only probe. This is not a measurement of the
library's incremental size in a minimal consumer or of instruction-cache misses.

## What this means

The pooled example queues and dispatches a Tokio task for each publish and holds
an exclusive lease in a one-slot pool. The manual and directly supervised examples
permit concurrent admission. Equivalent application contracts therefore need not
have equivalent execution costs.

The rerun still points to queue/task dispatch and exclusive pool scheduling as the
main source of the observed runtime cost in this workload. Direct supervision
retains lifecycle management without that dispatch path. Profiling would be needed
to attribute costs more precisely. Differences between manual and direct supervision
vary with caller count; these runs do not establish a consistent performance advantage
for either implementation.

At the application's five- and seven-second publishing intervals, these measured
latencies are small compared with its deadlines. At sustained high rates or on
constrained hardware, the pooled proxy's overhead deserves attention. The result
supports maintenance benefits, not a zero-cost claim.

## Checking scheduler handoffs

An additional experiment compares current-thread, two-worker, and default
multithreaded runtimes. The default uses 16 workers on this host. Reproduce with
any of the three comparison executables:

```sh
cargo run --release --locked --example mqtt_with_etherbird -- --benchmark --runtime current
cargo run --release --locked --example mqtt_with_etherbird -- --benchmark --runtime two
cargo run --release --locked --example mqtt_with_etherbird -- --benchmark --runtime default
```

On each platform, each runtime ran manual/pooled/supervisor/supervisor/pooled/manual
processes sequentially after builds completed. The same warmup and publish workload
was used. The benchmark now records stable Tokio worker park counts and worker busy
duration at the start and end of each round, including the drain wait. Counters cover
all runtime tasks, including the fixture, and are not per-operation CPU measurements.
Snapshots are taken outside the call latency measurements. Worker busy-duration
counters may not yet include a currently active interval, so they are diagnostic
counters rather than a substitute for process CPU time.

The [540 measured rounds](benchmarks/mqtt-scheduler.csv) and
[18 Ubuntu CPU/context-switch suites](benchmarks/mqtt-scheduler-cpu.csv) preserve
the observations. Throughput medians for three concurrent callers:

| Platform | Runtime | Manual calls/sec | Direct Supervisor calls/sec | Pooled proxy calls/sec |
| --- | --- | --- | --- | --- |
| Ubuntu/WSL | Current-thread | 576,189 | 612,437 | 200,671 |
| Ubuntu/WSL | Two workers | 358,945 | 538,393 | 43,516 |
| Ubuntu/WSL | Default (16 workers) | 423,787 | 485,486 | 39,524 |
| Windows | Current-thread | 133,660 | 132,956 | 96,194 |
| Windows | Two workers | 126,800 | 129,653 | 148,081 |
| Windows | Default (16 workers) | 131,381 | 131,062 | 150,970 |

Ubuntu pooled process statistics over the entire 200,500-publish suite:

| Runtime | CPU seconds, first / second suite | Voluntary context switches, first / second suite |
| --- | --- | --- |
| Current-thread | 0.83 / 0.80 | 117 / 117 |
| Two workers | 4.73 / 4.79 | 112,727 / 117,060 |
| Default (16 workers) | 7.33 / 7.14 | 192,174 / 192,196 |

For comparison, the current-thread manual suites used 0.33 / 0.33 CPU seconds;
default-runtime manual suites used 0.68 / 0.65 seconds and recorded 14,001 / 13,103
voluntary context switches. At three callers, median worker parks per round were
2 for the current-thread pooled path, 5,231 with two workers, and 7,756 with the
default runtime. The default manual path recorded 329 parks per round.

This strongly supports inter-thread scheduling and wakeups as a major contributor
to the Ubuntu slowdown. Removing cross-thread execution improved pooled throughput
roughly fivefold at three callers and reduced pooled suite CPU by nearly ninefold.
Even on current-thread, the pooled path remained below manual throughput and used
about 2.5 times its process CPU. Queue bookkeeping, allocations, task scheduling,
and exclusive lease admission remain possible contributors to that residual cost.

Reducing the multithreaded runtime from sixteen workers to two did not remove the
Ubuntu penalty. Windows showed a different throughput response: current-thread
pooled throughput decreased. Changing an application's runtime is therefore not
a universal optimization recommendation.

`perf` and `strace` were unavailable in this WSL environment. These are controlled
runtime comparisons, Tokio counters, and `/usr/bin/time -v` process statistics,
not sampled stack profiles. They support the handoff hypothesis but cannot assign
exact cost to a particular notification, lock, allocator, or Tokio function.
No library scheduling implementation was changed for this experiment.

Reducing this overhead is recorded as [outstanding work](OUTSTANDING_WORK.md#reduce-pooled-operation-dispatch-overhead).
