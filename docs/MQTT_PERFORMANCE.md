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

The [direct supervised proxy](../examples/mqtt/supervised/mod.rs) reuses the pooled
client's generated methods, lifecycle hooks, timeout and backoff configuration,
protocol, and application. It uses `Client::from_supervisor` without pool queueing
or exclusive leases. Earlier captures used a handwritten `Supervisor::execute`
wrapper; the new proxy measurements appear below. All three variants pass the
shared application readiness, cancellation, recovery, and shutdown checks. The
direct and pooled paths have different operation ownership and shutdown-drain
guarantees, described in the [feature guide](FEATURES.md).

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

By default, the pooled example queues and dispatches a Tokio task for each publish and holds
an exclusive lease in a one-slot pool. The manual and directly supervised examples
permit concurrent admission. Equivalent application contracts therefore need not
have equivalent execution costs.

The rerun still points to queue/task dispatch and exclusive pool scheduling as the
main source of the observed runtime cost in this workload. Direct supervision
retains lifecycle management without that dispatch path. The CPU profiles below
give further evidence of scheduling costs. Differences between manual and direct supervision
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

`perf` and `strace` were unavailable during that experiment. Those results are controlled
runtime comparisons, Tokio counters, and `/usr/bin/time -v` process statistics,
not sampled stack profiles. They support the handoff hypothesis but cannot assign
exact cost to a particular notification, lock, allocator, or Tokio function.
No library scheduling implementation was changed for this experiment.

## WSL CPU profiles

On 2026-10-04, profiling succeeded in WSL 2 without a separate VM or kernel
configuration changes. Ubuntu's `perf` 6.8.0-138 executable and its missing
`libtraceevent1` and `libbabeltrace1` dependencies were downloaded and extracted
into `target/mqtt-profile/tools`, rather than installed system-wide. Running the
tool as root provided hardware counters and kernel symbols on the existing
6.6.87.2 Microsoft kernel. The distribution's usual `perf` wrapper is not needed;
the extracted executable is under `usr/lib/linux-tools-6.8.0-138/perf`, with
`LD_LIBRARY_PATH` pointing to the extracted `usr/lib/x86_64-linux-gnu` directory.

The same three executables were rebuilt with release optimization, debug symbols,
and frame pointers. Each runtime ran manual, direct supervisor, and pooled suites
sequentially. `perf stat` and `perf record` ran separate executions; sampling used
`cpu-clock` at 499 Hz with frame-pointer call graphs. All executions passed publish
count and generation checks. These are diagnostic captures, one stat run and one
sampled run per configuration, rather than repeated performance estimates.
They include the broker fixture, warmup, all caller counts, draining, and shutdown.

| Runtime | Implementation | CPU seconds (`task-clock`) | Context switches | Instructions (billions) |
| --- | --- | --- | --- | --- |
| Default (16 workers) | Manual | 0.705 | 10,745 | 3.933 |
| Default (16 workers) | Direct Supervisor | 0.696 | 10,620 | 3.918 |
| Default (16 workers) | Pooled proxy | 6.086 | 139,143 | 9.747 |
| Current-thread | Manual | 0.346 | 105 | 3.510 |
| Current-thread | Direct Supervisor | 0.336 | 110 | 3.522 |
| Current-thread | Pooled proxy | 0.837 | 117 | 7.791 |

In the default-runtime pooled profile, approximately 32% of samples include
`__x64_sys_futex` across worker and main threads. Approximately 34% land directly
in the kernel's `_raw_spin_unlock_irqrestore`; its stacks include both futex
wakeups and loopback socket wakeups. These percentages overlap. The sampled
futex wakeup stacks lead through Tokio worker scheduling and the pool's spawned
operation future. This connects the earlier context-switch observations to actual
CPU stacks, and makes reducing per-operation task handoffs a sensible first prototype.
It does not prove that all wakeups originate from task creation: result delivery,
lease release, dispatcher notifications, and MQTT I/O also wake tasks.

Current-thread removes most context switches but the pooled suite still executes
about 2.2 times the manual suite's instructions. Its profile includes substantial
dispatcher and spawned-operation polling. Allocation counts have not been measured,
and these captures do not assign exact costs to each pool mechanism. The comparator
profiles contain only about 170–300 samples and the current-thread pooled profile
418, so small percentage differences should not guide an optimization. The default
pooled profile contains about 3,000 samples; no profile reported lost samples.
WSL-specific wakeup costs still need comparison against another Linux environment
before making claims about their absolute size on Linux generally.

The [counter outputs and compact stack reports](benchmarks/mqtt-profile.txt)
preserve the captures. Reproduce with an available Linux `perf` executable:

```sh
bash docs/benchmarks/profile-mqtt.sh
# For a locally extracted tool, export PERF and LD_LIBRARY_PATH first.
```

The [script](benchmarks/profile-mqtt.sh) builds as the ordinary user and uses sudo
only for profiling. It writes binaries, counter outputs, benchmark CSVs, and raw
profiles under `target/mqtt-profile`. No library optimization was made as part
of the profiling capture; the subsequent implementation experiment follows.

## Inline dispatch experiment

Status: discarded. The polling implementation, `inline-pool-dispatch` Cargo feature,
and optional runtime dependency have been removed. Measurements are retained as
evidence about scheduling costs, not as a supported configuration.

The spike replaced the per-operation `JoinSet` with `FuturesUnordered`, polled by
the pool dispatcher. It retained the queue, leases, result channels, supervision,
and retry policy, and contained polling/destructor panics. Pending operations stayed
concurrent across slots, but all operation polls shared the dispatcher's task.
CPU work was therefore serialized and a long poll could delay the dispatcher itself.
The Linux gains and Windows regressions below show why eliminating task spawning
alone is not a portable solution. The library retains Tokio's per-operation tasks.

The final prototype was compared against preserved pre-change binaries from
`b2a7c48` on 2026-10-04. Linux builds used the same release/debug/frame-pointer
settings as the profiles; Windows used standard release builds. Builds and tests
finished before measuring, and Linux and Windows suites ran sequentially. For each
runtime, the first pass ran baseline manual/supervisor/pooled, then candidate
pooled/supervisor/manual; the second reversed the phase order. Each process ran
five rounds at each of 1, 3, and 16 callers, using the same 200,500-publish suite.
Every process verified exact received publish counts and an unchanged generation.

The [1,080 measured rounds](benchmarks/mqtt-dispatch.csv) include all three
implementations and both builds. Medians of pooled throughput and per-round p95
call admission latency at three callers:

| Platform | Runtime | Spawned calls/sec | Inline calls/sec | Spawned p95 µs | Inline p95 µs |
| --- | --- | --- | --- | --- | --- |
| Ubuntu/WSL | Current-thread | 194,619 | 215,495 | 17.83 | 15.76 |
| Ubuntu/WSL | Two workers | 44,360 | 51,506 | 96.82 | 75.18 |
| Ubuntu/WSL | Default (16 workers) | 38,136 | 52,949 | 127.04 | 87.19 |
| Windows | Current-thread | 95,960 | 99,694 | 31.90 | 33.45 |
| Windows | Two workers | 148,657 | 147,720 | 113.60 | 112.10 |
| Windows | Default (16 workers) | 153,813 | 146,880 | 114.65 | 113.40 |

At sixteen callers, the default-runtime pooled median improved from 107,605 to
150,528 calls/sec on WSL (about 40%), while Windows fell from 154,370 to 133,495
(about 14%). Windows's two-worker configuration also fell about 12% at sixteen
callers. These regressions are why the prototype is opt-in. One-caller default
runtime throughput on WSL changed little (15,380 to 15,420 calls/sec).

The [36 Linux process measurements](benchmarks/mqtt-dispatch-cpu.csv) include
manual and direct supervision. Pooled process statistics for the full suite:

| Runtime | Spawned CPU seconds, first / second | Inline CPU seconds, first / second | Spawned voluntary switches, first / second | Inline voluntary switches, first / second |
| --- | --- | --- | --- | --- |
| Current-thread | 0.82 / 0.81 | 0.73 / 0.71 | 121 / 120 | 113 / 116 |
| Two workers | 4.77 / 4.72 | 3.47 / 3.46 | 114,788 / 116,233 | 87,062 / 83,570 |
| Default | 7.33 / 7.13 | 4.55 / 4.62 | 196,746 / 189,678 | 118,770 / 115,721 |

Default-runtime pooled CPU falls about 37%, and voluntary context switches about
39%. This supports reducing task handoffs, but leaves substantial overhead. The
pooled path still trails manual/direct supervision on WSL. Comparator throughput
also varied between phases (for example, default-runtime manual medians were
620,203 versus 471,887 calls/sec), so these few process runs are indicative rather
than precise estimates of a universal speedup. Windows CPU counters and allocation
counts were not collected.

To compare preserved binaries on Linux, use identical compiler settings and run
[the alternating comparison script](benchmarks/compare-dispatch.sh):

```sh
bash docs/benchmarks/compare-dispatch.sh \
  target/baseline/release/examples target/inline/release/examples
```

During the experiment, both modes passed the pool regression tests, including exclusive capacity, FIFO,
priority/custom queues, cancellation, retries, and shutdown. New regressions cover
constructor/poll panics alongside a slow sibling, reuse of the panicked slot, and
destructor panics during shutdown. Existing gated multi-slot tests establish that
slow operations do not prevent other slots from progressing. The full all-target
suite passed with the feature on Windows and WSL, and live Modbus/WebSocket recovery
checks passed on both. The panic regressions remain useful with the existing Tokio
dispatcher and have been retained; feature-specific CI duplication was removed.
Slower workload performance, allocation counts, macOS, and non-WSL Linux measurements
remain outstanding; the optimization work is still open.

## Removing redundant completion notifications

After discarding inline polling, the dispatcher continues to spawn operation tasks
through Tokio. The retained change disarms a call's cancellation guard after its
result arrives. Previously every completed call set its cancellation flag and sent
another pool-change notification, even though the job was already dequeued and
lease release/task completion supplied the necessary wakeups. Abandoned queued or
running calls still send cancellation notifications. No runtime dependency or Cargo
feature was added.

Caller-side execution was considered but not implemented. Its operation future
would belong to the caller, so an unpolled caller could retain running work through
shutdown. The existing pool cancels and drains operations independently of caller
polling; a new regression explicitly checks this behavior. Any future execution
redesign must preserve that contract, rather than simply moving the future.

The same alternating two-pass comparison was repeated with the notification
cleanup, after all builds/tests completed and with platforms run sequentially.
The [1,080 rounds](benchmarks/mqtt-notifications.csv) and
[36 Linux CPU/context-switch suites](benchmarks/mqtt-notifications-cpu.csv) are
separate from the discarded inline experiment. Pooled medians at three callers:

| Platform | Runtime | Original calls/sec | Cleanup calls/sec | Original p95 µs | Cleanup p95 µs |
| --- | --- | --- | --- | --- | --- |
| Ubuntu/WSL | Current-thread | 200,511 | 200,259 | 16.60 | 17.07 |
| Ubuntu/WSL | Two workers | 44,550 | 45,259 | 97.01 | 99.62 |
| Ubuntu/WSL | Default | 38,921 | 38,773 | 125.22 | 128.42 |
| Windows | Current-thread | 94,879 | 96,216 | 32.50 | 32.15 |
| Windows | Two workers | 150,044 | 150,216 | 112.85 | 111.40 |
| Windows | Default | 153,264 | 152,316 | 112.50 | 113.35 |

Linux default-runtime pooled CPU was 7.32 / 7.27 seconds before and 7.33 / 7.25
after; voluntary context switches were 193,182 / 192,668 before and
197,483 / 194,945 after. These captures do not establish a meaningful performance
improvement. The change removes unnecessary coordination but does not address the
major scheduling cost. Queue scans, allocations, shared notifications, and remaining
task/result handoffs still need investigation within the existing Tokio model.

The full suite passes on Windows and WSL. Existing cancellation, queue ordering,
retry, panic isolation, and shutdown regressions remain in place, including the
new unpolled-caller shutdown check. There is no custom operation-polling dispatcher
in the shipping implementation.

## Targeted dispatcher wakeups

The next retained change separates dispatcher work from public readiness/capacity
notifications. Queue insertion, cancellation, and waiter-count changes use Tokio's
`Notify::notify_one` for the dispatcher. Lifecycle changes and lease returns still
broadcast through the existing watch channel and wake the dispatcher. Shutdown
continues to use its own watch signal and independently cancels/drains Tokio
operation tasks.

The dispatcher also checks queued capacity directly, sharing the reservation
logic with public borrowing. This removes its extra capacity-watch receiver and
the boxed acquisition future previously created for each queued call. Public
borrowers still subscribe before checking capacity, preserving wakeup ordering.
There is no custom operation-polling scheduler or additional dependency.
`Pool::subscribe` now explicitly documents state/capacity changes; queue-only
events no longer trigger it. A regression verifies that enqueue and cancellation
do not notify that observer, cancelled queue entries are still pruned, and lease
release still notifies it and allows subsequent work to complete.

This combined change, including the earlier cancellation-guard cleanup, was
compared with the original `b2a7c48` implementation using the same alternating
two-pass method, runtime configurations, and compiler settings. Platforms ran
sequentially after builds/tests finished. The [1,080 rounds](benchmarks/mqtt-targeted-wakeups.csv)
include all three implementations and all caller counts; every process verified
exact publish counts and an unchanged generation. Pooled medians at three callers:

| Platform | Runtime | Original calls/sec | Candidate calls/sec | Original p95 µs | Candidate p95 µs |
| --- | --- | --- | --- | --- | --- |
| Ubuntu/WSL | Current-thread | 197,177 | 210,359 | 16.80 | 15.76 |
| Ubuntu/WSL | Two workers | 42,860 | 47,653 | 100.10 | 96.78 |
| Ubuntu/WSL | Default | 39,050 | 39,037 | 124.16 | 125.91 |
| Windows | Current-thread | 95,433 | 100,365 | 32.65 | 30.55 |
| Windows | Two workers | 151,740 | 153,410 | 113.25 | 111.10 |
| Windows | Default | 152,882 | 152,273 | 114.25 | 114.45 |

At sixteen callers, default-runtime pooled throughput rose from 107,765 to
127,355 calls/sec on WSL (about 18%), while Windows changed from 156,049 to
155,081 (less than 1%). WSL default-runtime median worker parks per sixteen-caller
round fell from 16,394 to 13,770. Worker parks include fixture activity and do not
identify which individual notification caused a wakeup.

The [36 Linux process measurements](benchmarks/mqtt-targeted-wakeups-cpu.csv)
include all comparison clients. For pooled default-runtime suites, CPU seconds
were 7.23 / 7.12 before and 6.38 / 6.48 after (about 10% lower on average).
Voluntary context switches were 194,243 / 191,263 before and 175,233 / 178,048
after (about 8% lower). Current-thread CPU fell from 0.83 / 0.83 to 0.74 / 0.73;
two-worker CPU fell from 4.76 / 4.85 to 4.54 / 4.46.

These limited runs suggest a modest improvement without the inline spike's
Windows regression. They do not resolve the large WSL default-runtime penalty
at three callers or separate notification effects from removing the acquisition
allocation/subscriptions. Broad capacity broadcasts for public borrowers remain;
these captures do not measure a pool with many blocked public borrowers. Windows
CPU, allocation counts, macOS, and non-WSL Linux measurements remain outstanding.
The full test suite and Clippy pass on Windows and WSL, including ordering,
custom queues, cancellation, independent shutdown, growth, retirement, and recovery.

## Uncontended admission and allocation measurements

The next [pool admission comparison](POOL_DISPATCH_PERFORMANCE.md) adds process-wide
allocation counts, four-slot workloads, delayed operations, and blocked public
borrowers. Ready built-in pools can now spawn directly into the tracked Tokio task
set without allocating queue storage. Custom queues and contended calls retain
normal admission; shutdown still independently cancels and drains operations.

Allocation events fell from about six to three per uncontended call, while
requested bytes fell only 3.7%. WSL default-runtime MQTT throughput improved about
19% with one caller and 15% with three, but fell 2.5% with sixteen. Windows default
MQTT throughput changed little. Some instrumented contended cases slowed by up to
about 11%, so this is a limited improvement with documented tradeoffs, not a fix
for the major scheduling penalty. The linked report includes all raw rounds,
controls, CPU measurements, and a separate readiness race discovered by the runs.

## Generated direct-supervision proxy

The direct example now uses the same `managed_client!` method definitions as the
pooled example. `Client::from_supervisor(supervisor)` selects concurrent execution
and `Client::new(pool)` selects exclusive pool admission. The generic backend is
chosen at construction and dispatch is static; direct operations allocate no
pool queue storage, result channels, or operation tasks. This is a distinct mode
for concurrency-safe resources, not an automatic change to single-slot pools.

The final proxy was compared against manual and pooled clients on 2026-10-04,
using the same release settings, compilers, fixture, runtime choices, and workloads
as the admission experiment. Each runtime ran manual/direct/pooled, then
pooled/direct/manual. Platforms and processes ran sequentially after builds
finished. Every process verified exact publish consumption and an unchanged
generation. The [540 raw rounds](benchmarks/mqtt-supervised-proxy.csv) include all
caller counts; these are medians of ten rounds with three callers:

| Platform | Runtime | Manual calls/sec | Direct proxy calls/sec | Pooled proxy calls/sec |
| --- | --- | --- | --- | --- |
| Ubuntu/WSL | Current-thread | 599,165 | 579,451 | 217,315 |
| Ubuntu/WSL | Two workers | 491,032 | 515,642 | 52,999 |
| Ubuntu/WSL | Default | 605,718 | 702,256 | 44,351 |
| Windows | Current-thread | 133,513 | 132,250 | 99,725 |
| Windows | Two workers | 134,574 | 131,259 | 149,565 |
| Windows | Default | 132,954 | 134,250 | 152,170 |

At three callers, Windows direct-proxy throughput is within about 3% of manual,
and WSL current-thread is about 3% lower. WSL two-worker and default throughput
are about 5% and 16% higher respectively. Control measurements vary between
captures, particularly on multithreaded WSL, so these results support comparable
performance rather than a general speedup claim. WSL current-thread p95 latency
is 28.42 / 28.55 microseconds for manual/direct; Windows default is 27.10 / 27.60.

The [18 Linux process measurements](benchmarks/mqtt-supervised-proxy-cpu.csv)
show default-runtime suite CPU of 0.62 / 0.67 seconds for manual,
0.58 / 0.60 for direct, and 6.17 / 6.29 for pooled. These include fixture/runtime
work and do not isolate library instructions. The direct path avoids the major
pool scheduling cost in this MQTT workload, while pool optimisation remains
necessary for resources requiring exclusive operation ownership. Full tests and
Clippy pass on Windows and WSL; the shared MQTT contract exercises both proxy modes.

Reducing pooled overhead is recorded as [outstanding work](OUTSTANDING_WORK.md#reduce-pooled-operation-dispatch-overhead).
