//! Serial example command handling; protocol and fixtures are separate modules.
mod adapter;
#[cfg(any(unix, test))]
mod demo;
use adapter::{Hooks, config};
use etherbird::Supervisor;
use std::time::Duration;
use tokio::time::{sleep, timeout};

pub(crate) async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.is_empty() || args == ["--help"] {
        println!(
            "usage: serial_device <port> [baud=115200] [samples=10]\n       serial_device --demo (Unix pseudo-terminal outage check)"
        );
        return Ok(());
    }
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    if args == ["--demo"] {
        #[cfg(unix)]
        return demo::run().await.map_err(Into::into);
        #[cfg(not(unix))]
        return Err("--demo requires Unix pseudo-terminals; use a COM port on Windows".into());
    }
    if args.len() > 3 {
        return Err("usage: serial_device <port> [baud] [samples]".into());
    }
    let baud = args
        .get(1)
        .map(|v| v.parse())
        .transpose()?
        .unwrap_or(115200);
    let samples = args
        .get(2)
        .map(|v| v.parse::<usize>())
        .transpose()?
        .unwrap_or(10);
    let supervisor = Supervisor::new(Hooks::serial(args[0].clone(), baud), config());
    // A single physical port has one supervisor, rather than a growing pool.
    let result = timeout(Duration::from_secs(60), async {
        for _ in 0..samples {
            match supervisor
                .execute(|r| async move { r.sample().await })
                .await
            {
                Ok(value) => println!("sample: {value}"),
                Err(error) => {
                    eprintln!("sample failed: {error}; subsequent samples await recovery")
                }
            }
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await;
    supervisor.stop().await;
    result?;
    Ok(())
}
