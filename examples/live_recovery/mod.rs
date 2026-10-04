//! Modbus TCP and WebSocket recovery: cargo run --example live_recovery
//! Modbus TCP alone: cargo run --example live_recovery -- --modbus-only
mod adapter;
mod fixtures;

use adapter::{Endpoint, Hooks};
use etherbird::{Config, Error, Pool, PoolConfig, RetryPolicy, Supervisor};
use fixtures::{ModbusServer, WebSocketServer};
use std::{
    fs::File,
    io::{self, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::watch,
    task::JoinSet,
    time::{sleep, timeout},
};

const LIMIT: Duration = Duration::from_secs(15);

fn read_policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: std::num::NonZeroUsize::new(3).unwrap(),
        timeout: Duration::from_secs(10),
    }
}
fn retryable_read_error(error: &io::Error) -> bool {
    // Invalid data and device/protocol exceptions are not transient transport loss.
    matches!(
        error.kind(),
        io::ErrorKind::NotConnected
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::UnexpectedEof
            | io::ErrorKind::TimedOut
    ) || error.raw_os_error() == Some(0) // tokio-modbus 0.17 can report EOF this way.
}
async fn read_sample(
    pool: &Pool<Hooks>,
    retry_modbus: bool,
) -> Result<(usize, u64), Error<io::Error>> {
    if retry_modbus {
        pool.execute_with_retry(
            read_policy(),
            |r| async move { Ok((r.id, r.sample().await?)) },
            retryable_read_error,
        )
        .await
    } else {
        pool.execute(|r| async move { Ok((r.id, r.sample().await?)) })
            .await
    }
}
#[derive(Default)]
struct Stats {
    successes: AtomicUsize,
    failures: AtomicUsize,
}
struct Managed {
    pool: Pool<Hooks>,
    hooks: Hooks,
    stats: Arc<Stats>,
}
impl Managed {
    async fn start(endpoint: Endpoint) -> io::Result<Self> {
        let hooks = Hooks::new(endpoint);
        let factory = hooks.clone();
        let pool = Pool::start(
            move || {
                Supervisor::new(
                    factory.clone(),
                    Config {
                        resource_name: factory.endpoint.name().to_owned(),
                        connect_timeout: Duration::from_secs(2),
                        setup_timeout: Duration::from_secs(2),
                        cleanup_timeout: Duration::from_secs(1),
                        disconnect_timeout: Duration::from_secs(1),
                        retry_delay: Duration::from_millis(200),
                        max_retry_delay: Duration::from_millis(800),
                        ..Config::default()
                    },
                )
            },
            PoolConfig {
                min_size: 1,
                max_size: 3,
                idle_timeout: Duration::from_secs(60),
                ..PoolConfig::default()
            },
        );
        timeout(LIMIT, pool.connected())
            .await?
            .map_err(io::Error::other)?;
        // Reserve all three concurrently to prove real demand grows the pool.
        let leases = timeout(LIMIT, async {
            tokio::try_join!(pool.borrow(), pool.borrow(), pool.borrow())
        })
        .await?
        .map_err(io::Error::other)?;
        drop(leases);
        until(|| pool.resources().len() == 3).await?;
        tracing::info!(
            resource = hooks.endpoint.name(),
            slots = 3,
            "pool grown under demand"
        );
        Ok(Self {
            pool,
            hooks,
            stats: Arc::default(),
        })
    }
    fn traffic(&self, tasks: &mut JoinSet<()>, stopping: watch::Receiver<bool>) {
        for worker in 0..3 {
            let pool = self.pool.clone();
            let stats = self.stats.clone();
            let name = self.hooks.endpoint.name();
            let retry_modbus = matches!(self.hooks.endpoint, Endpoint::Modbus(_));
            let mut stopping = stopping.clone();
            tasks.spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = stopping.changed() => break,
                        result = read_sample(&pool, retry_modbus) => match result {
                            Ok((id, sequence)) => {
                                stats.successes.fetch_add(1, SeqCst);
                                tracing::debug!(resource = name, worker, id, sequence, "sample received");
                            }
                            Err(error) => {
                                stats.failures.fetch_add(1, SeqCst);
                                tracing::warn!(resource = name, worker, %error, "operation returned to caller");
                            }
                        }
                    }
                    tokio::select! {
                        _ = stopping.changed() => break,
                        _ = sleep(Duration::from_millis(100)) => {}
                    }
                }
            });
        }
    }
    fn observe(&self, tasks: &mut JoinSet<()>, mut stopping: watch::Receiver<bool>) {
        let pool = self.pool.clone();
        let name = self.hooks.endpoint.name();
        tasks.spawn(async move {
            let mut changed = pool.subscribe();
            let mut previous = None;
            loop {
                tokio::select! {
                    _ = stopping.changed() => break,
                    result = changed.changed() => {
                        if result.is_err() { break; }
                        let ids: Vec<_> = pool.resources().iter().map(|r| r.resource().id).collect();
                        let state = pool.state();
                        if previous.as_ref() != Some(&(state, ids.clone())) {
                            tracing::info!(resource = name, ?state, ?ids, "pool state changed");
                            previous = Some((state, ids));
                        }
                    }
                }
            }
        });
    }
    fn successes(&self) -> usize {
        self.stats.successes.load(SeqCst)
    }
    fn attempts(&self) -> usize {
        self.hooks.attempts.load(SeqCst)
    }
    async fn recovered(&self, old_id: usize) -> io::Result<()> {
        timeout(LIMIT, self.pool.connected())
            .await?
            .map_err(io::Error::other)?;
        let (id, value) = timeout(
            LIMIT,
            read_sample(
                &self.pool,
                matches!(self.hooks.endpoint, Endpoint::Modbus(_)),
            ),
        )
        .await?
        .map_err(io::Error::other)?;
        assert!(id > old_id, "old transport was reused");
        tracing::info!(
            resource = self.hooks.endpoint.name(),
            old_id,
            id,
            value,
            "PASS replacement serves real requests"
        );
        Ok(())
    }
    fn latest_id(&self) -> usize {
        self.pool.with_latest(|r| r.id).unwrap()
    }
    async fn stop(&self) -> io::Result<()> {
        // Shutdown must also close a resource still held by a caller.
        let lease = timeout(LIMIT, self.pool.borrow())
            .await?
            .map_err(io::Error::other)?;
        let resource = timeout(LIMIT, lease.acquire())
            .await?
            .map_err(io::Error::other)?;
        timeout(LIMIT, self.pool.stop()).await?;
        assert!(!etherbird::Lifecycle::is_connected(
            &self.hooks,
            resource.resource()
        ));
        assert_eq!(
            self.hooks.created.load(SeqCst),
            self.hooks.destroyed.load(SeqCst)
        );
        tracing::info!(
            resource = self.hooks.endpoint.name(),
            created = self.hooks.created.load(SeqCst),
            "PASS shutdown destroyed every created resource, including held lease"
        );
        Ok(())
    }
}
async fn until(mut predicate: impl FnMut() -> bool) -> io::Result<()> {
    timeout(LIMIT, async {
        while !predicate() {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    Ok(())
}
fn operation_failed<T>(result: Result<T, Error<io::Error>>) {
    match result {
        Err(Error::Operation(error)) => {
            tracing::info!(%error, "PASS in-flight operation failed once and returned to caller")
        }
        _ => panic!("expected an operation error after socket interruption"),
    }
}
async fn exercise(
    modbus: &Managed,
    device: &mut ModbusServer,
    mut websocket: Option<(&Managed, &mut WebSocketServer)>,
) -> io::Result<()> {
    let (stop, stopping) = watch::channel(false);
    let mut traffic = JoinSet::new();
    let mut observers = JoinSet::new();
    modbus.traffic(&mut traffic, stopping.clone());
    modbus.observe(&mut observers, stopping.clone());
    if let Some((websocket, _)) = websocket.as_ref() {
        websocket.traffic(&mut traffic, stopping.clone());
        websocket.observe(&mut observers, stopping.clone());
    }
    until(|| {
        modbus.successes() >= 12 && websocket.as_ref().is_none_or(|(r, _)| r.successes() >= 12)
    })
    .await?;
    tracing::info!(
        phase = "baseline",
        "PASS both pools communicate concurrently"
    );

    if let Some((websocket, server)) = websocket.as_mut() {
        let old_id = websocket.latest_id();
        let attempts = websocket.attempts();
        let subscriptions = server.state.subscriptions.load(SeqCst);
        let peer = modbus.successes();
        let received = server.state.blocked.load(SeqCst);
        let calls = Arc::new(AtomicUsize::new(0));
        let invoked = calls.clone();
        let pool = websocket.pool.clone();
        let blocked = tokio::spawn(async move {
            pool.execute(move |r| async move {
                invoked.fetch_add(1, SeqCst);
                r.blocking_websocket().await
            })
            .await
        });
        until(|| server.state.blocked.load(SeqCst) > received).await?;
        tracing::info!(
            phase = "websocket_down",
            "stopping WebSocket with a confirmed BLOCK in flight"
        );
        server.down().await?;
        operation_failed(timeout(LIMIT, blocked).await?.map_err(io::Error::other)?);
        until(|| websocket.attempts() >= attempts + 6 && modbus.successes() >= peer + 12).await?;
        assert!(!websocket.pool.is_connected());
        tracing::info!(
            phase = "websocket_down",
            "PASS retries while Modbus continues communicating"
        );
        server.up().await?;
        websocket.recovered(old_id).await?;
        assert!(server.state.subscriptions.load(SeqCst) > subscriptions);
        assert_eq!(calls.load(SeqCst), 1, "failed BLOCK was replayed");
    }

    let old_id = modbus.latest_id();
    let attempts = modbus.attempts();
    let peer = websocket.as_ref().map(|(r, _)| r.successes());
    let received = device.state.blocked.load(SeqCst);
    device.state.block_probe.store(true, SeqCst);
    let calls = Arc::new(AtomicUsize::new(0));
    let invoked = calls.clone();
    let pool = modbus.pool.clone();
    let blocked = tokio::spawn(async move {
        pool.execute(move |r| async move {
            invoked.fetch_add(1, SeqCst);
            r.blocking_modbus().await
        })
        .await
    });
    // The device acknowledges this special request before withholding its reply.
    until(|| device.state.blocked.load(SeqCst) > received).await?;
    tracing::info!(
        phase = "modbus_down",
        "closing device sockets during a request"
    );
    device.down().await?;
    operation_failed(timeout(LIMIT, blocked).await?.map_err(io::Error::other)?);
    until(|| {
        modbus.attempts() >= attempts + 6
            && websocket
                .as_ref()
                .is_none_or(|(r, _)| r.successes() >= peer.unwrap() + 12)
    })
    .await?;
    assert!(!modbus.pool.is_connected());
    tracing::info!(
        phase = "modbus_down",
        "PASS retries while WebSocket continues communicating"
    );
    device.up().await?;
    modbus.recovered(old_id).await?;
    assert_eq!(
        calls.load(SeqCst),
        1,
        "failed Modbus operation was replayed"
    );

    stop.send_replace(true);
    while let Some(result) = traffic.join_next().await {
        result.map_err(io::Error::other)?;
    }
    while let Some(result) = observers.join_next().await {
        result.map_err(io::Error::other)?;
    }
    // This register read is explicitly safe to repeat. Unlike the run-once probe
    // above, the caller receives a successful result after the device returns.
    let received = device.state.blocked.load(SeqCst);
    let attempts = modbus.attempts();
    device.state.block_probe.store(true, SeqCst);
    let calls = Arc::new(AtomicUsize::new(0));
    let generations = Arc::new(Mutex::new(Vec::new()));
    let invoked = calls.clone();
    let seen = generations.clone();
    let pool = modbus.pool.clone();
    let retrying = tokio::spawn(async move {
        pool.execute_with_retry(
            read_policy(),
            move |r| {
                let invoked = invoked.clone();
                let seen = seen.clone();
                async move {
                    invoked.fetch_add(1, SeqCst);
                    seen.lock().unwrap().push(r.id);
                    r.blocking_modbus().await?;
                    Ok(r.id)
                }
            },
            retryable_read_error,
        )
        .await
    });
    until(|| device.state.blocked.load(SeqCst) > received).await?;
    device.down().await?;
    until(|| modbus.attempts() >= attempts + 3).await?;
    assert!(!retrying.is_finished(), "read did not wait for recovery");
    assert_eq!(
        calls.load(SeqCst),
        1,
        "read ran before replacement was ready"
    );
    device.up().await?;
    let id = timeout(LIMIT, retrying)
        .await?
        .map_err(io::Error::other)?
        .map_err(io::Error::other)?;
    assert_eq!(calls.load(SeqCst), 2);
    let seen = generations.lock().unwrap().clone();
    assert!(seen[1] > seen[0]);
    assert_eq!(id, seen[1]);
    tracing::info!(id, "PASS opt-in Modbus read retries on a ready replacement");

    // With no application operations, only the protocol watchdog can detect loss.
    if let Some((websocket, server)) = websocket.as_mut() {
        let old_id = websocket.latest_id();
        let attempts = websocket.attempts();
        let subscriptions = server.state.subscriptions.load(SeqCst);
        tracing::info!(
            phase = "websocket_idle_down",
            "stopping WebSocket without application traffic"
        );
        server.down().await?;
        until(|| !websocket.pool.is_connected() && websocket.attempts() > attempts).await?;
        server.up().await?;
        websocket.recovered(old_id).await?;
        assert!(server.state.subscriptions.load(SeqCst) > subscriptions);
    }
    let old_id = modbus.latest_id();
    let attempts = modbus.attempts();
    tracing::info!(
        phase = "modbus_idle_down",
        "stopping device without application traffic"
    );
    device.down().await?;
    until(|| !modbus.pool.is_connected() && modbus.attempts() > attempts).await?;
    device.up().await?;
    modbus.recovered(old_id).await?;
    tracing::info!("PASS idle watchdog recovery");

    let old_id = modbus.latest_id();
    let attempts = modbus.attempts();
    tracing::info!(
        phase = "modbus_silent",
        "device accepts TCP but stops responding"
    );
    device.state.paused.store(true, SeqCst);
    until(|| !modbus.pool.is_connected() && modbus.attempts() >= attempts + 3).await?;
    device.down().await?;
    device.up().await?;
    modbus.recovered(old_id).await?;
    tracing::info!("PASS silent peer hits response deadline and recovers");
    Ok(())
}

struct Tee {
    file: Arc<Mutex<File>>,
}
impl Write for Tee {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.file.lock().unwrap().write_all(bytes)?;
        io::stdout().write_all(bytes)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.lock().unwrap().flush()?;
        io::stdout().flush()
    }
}
pub(crate) async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let modbus_only = match args.as_slice() {
        [] => false,
        [flag] if flag == "--modbus-only" => true,
        _ => return Err("usage: live_recovery [--modbus-only]".into()),
    };
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let output = std::path::PathBuf::from(format!(
        "target/live-recovery/{stamp}-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&output)?;
    let file = Arc::new(Mutex::new(File::create(output.join("events.log"))?));
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || Tee { file: file.clone() })
        .init();
    tracing::info!(path = %output.display(), "live recovery artifacts");
    let mut device = ModbusServer::start("127.0.0.1:0".parse()?, Arc::default()).await?;
    let modbus = Managed::start(Endpoint::Modbus(device.address)).await?;
    let mut server = if modbus_only {
        None
    } else {
        Some(WebSocketServer::start("127.0.0.1:0".parse()?, Arc::default()).await?)
    };
    let websocket = if let Some(server) = &server {
        Some(Managed::start(Endpoint::WebSocket(server.url())).await?)
    } else {
        None
    };
    let result = exercise(
        &modbus,
        &mut device,
        websocket.as_ref().zip(server.as_mut()),
    )
    .await;
    // Stop pools even when a scenario returns an error.
    if result.is_ok() {
        modbus.stop().await?;
        if let Some(websocket) = &websocket {
            websocket.stop().await?;
        }
    } else {
        modbus.pool.stop().await;
        if let Some(websocket) = &websocket {
            websocket.pool.stop().await;
        }
    }
    device.down().await?;
    if let Some(server) = &mut server {
        server.down().await?;
    }
    let records: Vec<_> = std::iter::once(&modbus).chain(websocket.iter()).map(|managed| {
        format!("  {{\"resource\":\"{}\",\"successes\":{},\"operation_errors\":{},\"connect_attempts\":{},\"created\":{},\"destroyed\":{}}}",
            managed.hooks.endpoint.name(), managed.successes(), managed.stats.failures.load(SeqCst),
            managed.attempts(), managed.hooks.created.load(SeqCst), managed.hooks.destroyed.load(SeqCst))
    }).collect();
    std::fs::write(
        output.join("summary.json"),
        format!(
            "{{\"passed\":{},\"resources\":[\n{}\n]}}\n",
            result.is_ok(),
            records.join(",\n")
        ),
    )?;
    result?;
    tracing::info!(path = %output.display(), "PASS live recovery checks completed");
    Ok(())
}
