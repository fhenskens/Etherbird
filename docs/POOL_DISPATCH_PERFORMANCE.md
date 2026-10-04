# Pool admission measurements

The allocation benchmark complements the [MQTT comparison](MQTT_PERFORMANCE.md):
it measures coordination around a minimal resource, while the MQTT benchmark
measures the complete application path without a counting allocator.

Run the standalone benchmark with:

```sh
cargo bench --bench pool_dispatch -- --runtime current
cargo bench --bench pool_dispatch -- --runtime two
cargo bench --bench pool_dispatch -- --runtime default
```

It runs immediate operations with one, three, and sixteen callers, using one or
four fixed pool slots. It also compares explicitly supplied FIFO queues, operations
that sleep for five milliseconds, and sixteen public borrowers waiting for leases.
Callers are concurrent futures in a `join_all`, rather than independently spawned
Tokio tasks. Each round checks exact successful operation counts, exclusive access,
and capacity limits. Delayed rounds must reach the configured concurrency. Shutdown
checks that every created resource was destroyed. Readiness, rounds, and shutdown
have deadlines so a stalled run fails rather than producing misleading results.

Allocation events and requested bytes are process-wide deltas after warmup. The
allocator counts allocation, zeroed allocation, and reallocation requests on all
threads; it delegates to the system allocator. These numbers include runtime and
fixture activity. They are neither live heap usage nor allocations attributable
exclusively to Etherbird. The shared atomic counters also perturb performance;
use the separately measured MQTT path to assess uninstrumented throughput.

## Uncontended admission

Previously, every pooled call allocated queue storage even when a resource was
immediately available. The candidate reserves a ready slot and spawns a tracked
Tokio operation directly when a built-in queue is empty and no public borrower is
waiting. Otherwise it uses normal queue admission. Explicit custom queue factories
always receive admission, including their rejection and ordering decisions.

This removes the cancellation flag allocation, boxed queued closure, and boxed
operation future from the immediate path. The result channel and independently
tracked Tokio task remain. Operations still run independently of caller polling,
and shutdown cancels and drains them before resource teardown. The task registry
is shared under a mutex; registration holds the admission lock so shutdown cannot
finish its drain while a previously accepted operation is still being registered.
This introduces another coordination cost for queued work, so contended cases
must be measured alongside the immediate path.

The benchmark exposed a separate readiness race: a connection could become ready
before its state relay first ran, causing the relay to consume readiness without
forwarding it to pool waiters. The relay now observes and forwards the current
state before waiting for another change. A deterministic regression publishes
readiness before the relay's first poll. Both comparison builds include this fix.

## Comparison method

The control is commit `bd9b0cb` with the readiness fix, compared with the same
source plus direct admission. Both builds use the same benchmark source and normal
release settings. Windows uses Rust 1.99; Ubuntu under WSL 2 uses Rust 1.96. Default
runtimes have sixteen workers on this machine. Platforms run sequentially, after
compilation finishes, with baseline/candidate order reversed on the second pass.
Each configuration has six allocation-benchmark rounds and ten MQTT rounds.

To compare two separately built release directories:

```sh
bash docs/benchmarks/compare-admission.sh baseline/release candidate/release
```

```powershell
./docs/benchmarks/compare-admission.ps1 -Baseline baseline/release -Candidate candidate/release
```

The existing [MQTT comparison script](benchmarks/compare-dispatch.sh) compares
all three clients, runtime configurations, and caller counts. Linux process CPU
and context switches include fixture and runtime activity. Allocation results
cannot be used to assign those costs to any particular notification or task.

## Results

The [1,152 allocation rounds](benchmarks/pool-admission.csv) and
[1,080 MQTT rounds](benchmarks/mqtt-fast-admission.csv) were captured on 2026-10-04.
Linux process measurements are retained for the
[allocation suite](benchmarks/pool-admission-cpu.csv) and
[MQTT suite](benchmarks/mqtt-fast-admission-cpu.csv).

For one caller, process allocation events per call fell from 6.006 to 3.006 on
both platforms and every runtime. Requested bytes fell from approximately
1,081.5 to 1,041.5 per call: halving allocation events is only a 3.7% reduction in
requested bytes, because the remaining task allocation is larger. Explicit custom
queues still use about six events per call. With sixteen callers contending for
one slot, almost every call still queues, so allocation savings are small.

Uninstrumented MQTT pooled medians:

| Platform | Runtime | Callers | Control calls/sec | Candidate calls/sec | Change |
| --- | --- | --- | --- | --- | --- |
| Ubuntu/WSL | Current-thread | 3 | 217,469 | 214,998 | -1.1% |
| Ubuntu/WSL | Two workers | 1 | 13,964 | 17,984 | +28.8% |
| Ubuntu/WSL | Two workers | 3 | 46,989 | 51,752 | +10.1% |
| Ubuntu/WSL | Default | 1 | 14,910 | 17,698 | +18.7% |
| Ubuntu/WSL | Default | 3 | 38,668 | 44,500 | +15.1% |
| Ubuntu/WSL | Default | 16 | 128,591 | 125,412 | -2.5% |
| Windows | Current-thread | 3 | 99,798 | 99,072 | -0.7% |
| Windows | Two workers | 3 | 151,141 | 152,638 | +1.0% |
| Windows | Default | 1 | 145,087 | 145,296 | +0.1% |
| Windows | Default | 3 | 154,167 | 156,558 | +1.6% |
| Windows | Default | 16 | 155,970 | 153,642 | -1.5% |

WSL default-runtime three-caller p95 latency fell from 126.82 to 106.79 microseconds.
At sixteen callers it rose from 226.01 to 231.95 microseconds. Pooled default-runtime
suite CPU was 6.29 / 6.39 seconds before and 6.17 / 6.28 after, roughly 2% lower
on average. Two-worker CPU was 4.46 / 4.50 before and 4.19 / 4.17 after, roughly
7% lower. Windows MQTT results generally changed little; the one-caller two-worker
case rose from 119,711 to 155,115 calls/sec, but that isolated result should not be
generalised to other configurations. These runs do not establish statistical
confidence or eliminate scheduling and timer variation.

Immediate instrumented operations show both gains and regressions. With four
slots and three callers, WSL default throughput rose from 57,489 to 65,615 calls/sec.
With one slot and sixteen callers, Windows two-worker throughput fell from 398,482
to 355,952 calls/sec, about 11%; WSL current-thread four-slot sixteen-caller
throughput fell about 6%. Custom FIFO cases also sometimes slowed, consistent with
additional registry coordination. The raw data includes these cases; the retained
change is an improvement for immediate admission, not a universal speedup.

Five-millisecond operations and blocked public borrowers preserved four-slot
concurrency. Median throughput changed by at most about 1.4% across those tested
configurations. These workloads demonstrate that the candidate preserves useful
concurrency, but do not substitute for a realistic slow transport workload or many
independently scheduled borrower tasks. The Windows MQTT pooled executable grew
from 2,546,688 to 2,560,000 bytes (about 0.5%); that is a whole example executable
comparison, not the library's incremental instruction footprint.

The full tests, Clippy, and live Modbus/WebSocket recovery demo validate ordering,
custom queue rejection, cancellation, retries, shutdown, and recovery on Windows
and WSL. New checks cover immediate admission racing shutdown and shutdown when
the immediate caller is no longer polled. macOS and Linux outside WSL remain
unmeasured. The large scheduling penalty remains
[outstanding work](OUTSTANDING_WORK.md#reduce-pooled-operation-dispatch-overhead).
