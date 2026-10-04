//! Serial example command handling; protocol and fixtures are separate modules.
mod adapter;
#[cfg(any(unix, test))]
mod demo;
use adapter::{Hooks, client};
use std::time::Duration;
use tokio::time::{sleep, timeout};

pub(crate) async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.is_empty() || args == ["--help"] {
        println!(
            "usage: serial_device <port> [baud=115200] [samples=10] [gain=1]\n       serial_device --demo (Unix pseudo-terminal outage check)"
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
    if args.len() > 4 {
        return Err("usage: serial_device <port> [baud] [samples] [gain]".into());
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
    let gain = args
        .get(3)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(1);
    let client = client(Hooks::serial(args[0].clone(), baud), gain);
    // A one-slot proxy keeps one physical port and restores its desired gain.
    let result = timeout(Duration::from_secs(60), async {
        for _ in 0..samples {
            match client.sample().await {
                Ok(value) => println!("sample: {value}"),
                Err(error) => {
                    eprintln!("sample failed: {error}; subsequent samples await recovery")
                }
            }
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await;
    client.managed.stop().await;
    result?;
    Ok(())
}
