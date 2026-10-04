//! Process-wide allocation and latency measurements; run with cargo bench.
use etherbird::{Config, FifoQueue, Lifecycle, Pool, PoolConfig, Supervisor, async_trait};
use futures_util::future::join_all;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

struct CountingAllocator;
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
// Delegate every allocation to System. Counts include reallocations and all
// runtime/fixture threads, not just allocations attributable to Etherbird.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(size, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, size) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[derive(Default)]
struct Counts {
    calls: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
    created: AtomicUsize,
    destroyed: AtomicUsize,
}
struct Resource {
    busy: AtomicBool,
    counts: Arc<Counts>,
}
struct Busy(Arc<Resource>);
impl Drop for Busy {
    fn drop(&mut self) {
        self.0.counts.active.fetch_sub(1, Ordering::SeqCst);
        self.0.busy.store(false, Ordering::SeqCst);
    }
}
async fn operation(resource: Arc<Resource>, delay: Duration) -> Result<(), Infallible> {
    assert!(
        !resource.busy.swap(true, Ordering::SeqCst),
        "overlapping exclusive operations"
    );
    let active = resource.counts.active.fetch_add(1, Ordering::SeqCst) + 1;
    resource.counts.peak.fetch_max(active, Ordering::SeqCst);
    let guard = Busy(resource);
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    guard.0.counts.calls.fetch_add(1, Ordering::SeqCst);
    Ok(())
}
struct Hooks(Arc<Counts>);
#[async_trait]
impl Lifecycle for Hooks {
    type Resource = Resource;
    type Error = Infallible;
    async fn create(&self) -> Result<Resource, Infallible> {
        self.0.created.fetch_add(1, Ordering::SeqCst);
        Ok(Resource {
            busy: AtomicBool::new(false),
            counts: self.0.clone(),
        })
    }
    async fn connect(&self, _: &Resource) -> Result<(), Infallible> {
        Ok(())
    }
    async fn disconnect(&self, _: &Resource) -> Result<(), Infallible> {
        Ok(())
    }
    async fn destroy(&self, _: &Resource) -> Result<(), Infallible> {
        self.0.destroyed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

async fn run(runtime: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "runtime,scenario,slots,callers,round,calls,elapsed_ms,calls_per_second,p50_us,p95_us,p99_us,allocations,allocated_bytes,peak_active,worker_parks"
    );
    for scenario in ["ready", "custom_fifo", "delayed", "borrowers"] {
        for slots in [1, 4] {
            let counts = Arc::new(Counts::default());
            let control = counts.clone();
            let config = PoolConfig {
                min_size: slots,
                max_size: slots,
                ..PoolConfig::default()
            };
            let factory = move || Supervisor::new(Hooks(control.clone()), Config::default());
            let pool = if scenario == "custom_fifo" {
                Pool::start_with_queue_factory(factory, config, FifoQueue::default)
            } else {
                Pool::start(factory, config)
            };
            tokio::time::timeout(Duration::from_secs(10), pool.connected())
                .await
                .map_err(|_| {
                    format!(
                        "readiness timeout: {scenario}, slots={slots}, created={}",
                        counts.created.load(Ordering::SeqCst)
                    )
                })??;
            for _ in 0..100 {
                tokio::time::timeout(
                    Duration::from_secs(10),
                    pool.execute(|r| operation(r, Duration::ZERO)),
                )
                .await
                .map_err(|_| format!("warmup timeout: {scenario}, slots={slots}"))??;
            }
            let callers: &[usize] = if matches!(scenario, "ready" | "custom_fifo") {
                &[1, 3, 16]
            } else {
                &[16]
            };
            for &callers in callers {
                for round in 1..=3 {
                    let delayed = matches!(scenario, "delayed" | "borrowers");
                    let iterations = if delayed { 3 } else { 500 };
                    let delay = if delayed {
                        Duration::from_millis(5)
                    } else {
                        Duration::ZERO
                    };
                    counts.peak.store(0, Ordering::SeqCst);
                    let before_calls = counts.calls.load(Ordering::SeqCst);
                    let metrics = tokio::runtime::Handle::current().metrics();
                    let parks = || {
                        (0..metrics.num_workers())
                            .map(|i| metrics.worker_park_count(i))
                            .sum::<u64>()
                    };
                    let before_parks = parks();
                    let before_allocations = ALLOCATIONS.load(Ordering::Relaxed);
                    let before_bytes = BYTES.load(Ordering::Relaxed);
                    let start = Instant::now();
                    let results = tokio::time::timeout(Duration::from_secs(20), join_all((0..callers).map(|_| {
                        let pool = &pool;
                        async move {
                            let mut latencies = Vec::with_capacity(iterations);
                            for _ in 0..iterations {
                                let start = Instant::now();
                                if scenario == "borrowers" {
                                    let lease = pool.borrow().await.unwrap();
                                    lease.execute(|r| operation(r, delay)).await.unwrap();
                                } else {
                                    pool.execute(move |r| operation(r, delay)).await.unwrap();
                                }
                                latencies.push(start.elapsed().as_nanos() as u64);
                            }
                            latencies
                        }
                    })))
                    .await.map_err(|_| format!("round timeout: {scenario}, slots={slots}, callers={callers}, round={round}, calls={}, active={}", counts.calls.load(Ordering::SeqCst), counts.active.load(Ordering::SeqCst)))?;
                    let elapsed = start.elapsed();
                    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before_allocations;
                    let allocated_bytes = BYTES.load(Ordering::Relaxed) - before_bytes;
                    let worker_parks = parks() - before_parks;
                    let calls = callers * iterations;
                    assert_eq!(counts.calls.load(Ordering::SeqCst) - before_calls, calls);
                    assert_eq!(counts.active.load(Ordering::SeqCst), 0);
                    let peak = counts.peak.load(Ordering::SeqCst);
                    assert!(peak <= slots);
                    if delayed {
                        assert_eq!(peak, slots.min(callers));
                    }
                    let mut latencies: Vec<_> = results.into_iter().flatten().collect();
                    latencies.sort_unstable();
                    let percentile =
                        |p: usize| latencies[(calls * p).div_ceil(100) - 1] as f64 / 1000.0;
                    println!(
                        "{runtime},{scenario},{slots},{callers},{round},{calls},{:.3},{:.0},{:.3},{:.3},{:.3},{allocations},{allocated_bytes},{peak},{worker_parks}",
                        elapsed.as_secs_f64() * 1000.0,
                        calls as f64 / elapsed.as_secs_f64(),
                        percentile(50),
                        percentile(95),
                        percentile(99)
                    );
                }
            }
            tokio::time::timeout(Duration::from_secs(10), pool.stop())
                .await
                .map_err(|_| {
                    format!(
                        "shutdown timeout: {scenario}, slots={slots}, active={}, destroyed={}",
                        counts.active.load(Ordering::SeqCst),
                        counts.destroyed.load(Ordering::SeqCst)
                    )
                })?;
            assert_eq!(counts.created.load(Ordering::SeqCst), slots);
            assert_eq!(counts.destroyed.load(Ordering::SeqCst), slots);
        }
    }
    Ok(())
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    // cargo test --all-targets builds this harness but does not run benchmarks.
    if !args
        .iter()
        .any(|arg| matches!(arg.as_str(), "--bench" | "--run"))
    {
        return Ok(());
    }
    let mode = args
        .windows(2)
        .find(|pair| pair[0] == "--runtime")
        .map(|pair| pair[1].as_str())
        .unwrap_or("default");
    let mut builder = match mode {
        "current" => tokio::runtime::Builder::new_current_thread(),
        "two" | "default" => tokio::runtime::Builder::new_multi_thread(),
        _ => return Err("runtime must be current, two, or default".into()),
    };
    if mode == "two" {
        builder.worker_threads(2);
    }
    builder
        .enable_all()
        .build()?
        .block_on(async { tokio::time::timeout(Duration::from_secs(120), run(mode)).await })??;
    Ok(())
}
