//! Serial fixtures and outage checks; no physical hardware needed by the tests.
#[cfg(test)]
use super::adapter::Instrument;
use super::adapter::{Hooks, Port, client};
#[cfg(test)]
use std::sync::atomic::AtomicU32;
use std::sync::{
    Mutex as StdMutex,
    atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
};
use std::{io, sync::Arc, time::Duration};
use tokio::io::{AsyncBufReadExt, BufReader};
#[cfg(test)]
use tokio::sync::{Mutex, watch};
use tokio::{io::AsyncWriteExt, time::timeout};

async fn serve(
    port: Port,
    handshakes: Arc<AtomicUsize>,
    outage: Arc<AtomicBool>,
    requested: Arc<AtomicBool>,
) {
    let mut reader = BufReader::new(port);
    let mut ready = false;
    let mut gain = None;
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        let reply = match line.trim() {
            "HELLO" => {
                handshakes.fetch_add(1, SeqCst);
                ready = true;
                "READY\n".to_owned()
            }
            "PING" if ready => "PONG\n".to_owned(),
            command if ready && command.starts_with("GAIN ") => {
                gain = command[5..].parse::<u32>().ok();
                if gain.is_none() {
                    break;
                }
                "OK\n".to_owned()
            }
            "SAMPLE" if ready && gain.is_some() => {
                requested.store(true, SeqCst);
                if outage.swap(false, SeqCst) {
                    break;
                }
                format!("{}\n", 42 * u64::from(gain.unwrap()))
            }
            _ => break,
        };
        if reader.get_mut().write_all(reply.as_bytes()).await.is_err() {
            break;
        }
    }
}

#[cfg(unix)]
pub async fn run() -> io::Result<()> {
    run_with(Arc::new(|| {
        let (device, instrument) = tokio_serial::SerialStream::pair().map_err(io::Error::other)?;
        Ok((Box::new(device), Box::new(instrument)))
    }))
    .await
}

type PairFactory = Arc<dyn Fn() -> io::Result<(Port, Port)> + Send + Sync>;
async fn run_with(pair: PairFactory) -> io::Result<()> {
    let handshakes = Arc::new(AtomicUsize::new(0));
    let outage = Arc::new(AtomicBool::new(true));
    let requested = Arc::new(AtomicBool::new(false));
    let tasks = Arc::new(StdMutex::new(Vec::new()));
    let hooks = Hooks {
        open: {
            let handshakes = handshakes.clone();
            let outage = outage.clone();
            let requested = requested.clone();
            let tasks = tasks.clone();
            Arc::new(move || {
                let (device, instrument) = pair()?;
                tasks.lock().unwrap().push(tokio::spawn(serve(
                    device,
                    handshakes.clone(),
                    outage.clone(),
                    requested.clone(),
                )));
                Ok(instrument)
            })
        },
    };
    let client = client(hooks, 7);
    let calls = Arc::new(AtomicUsize::new(0));
    let result = timeout(Duration::from_secs(10), async {
        let lease = client
            .managed
            .pool
            .borrow()
            .await
            .map_err(io::Error::other)?;
        let old = lease.acquire().await.map_err(io::Error::other)?;
        drop(lease);
        let invoked = calls.clone();
        let failed = client
            .managed
            .pool
            .execute(move |r| async move {
                invoked.fetch_add(1, SeqCst);
                r.sample().await
            })
            .await;
        assert!(matches!(failed, Err(etherbird::Error::Operation(_))));
        assert!(requested.load(SeqCst));
        assert_eq!(calls.load(SeqCst), 1, "failed operation was replayed");
        let lease = client
            .managed
            .pool
            .borrow()
            .await
            .map_err(io::Error::other)?;
        let replacement = lease.acquire().await.map_err(io::Error::other)?;
        drop(lease);
        assert!(replacement.generation() > old.generation());
        assert!(handshakes.load(SeqCst) >= 2, "setup was not repeated");
        assert_eq!(client.sample().await.map_err(io::Error::other)?, 294);
        Ok::<_, io::Error>(())
    })
    .await;
    client.managed.stop().await;
    let pending: Vec<_> = tasks.lock().unwrap().drain(..).collect();
    for task in pending {
        task.abort();
        let _ = task.await;
    }
    result??;
    println!("PASS serial replacement restores gain=7 before samples; failed sample runs once");
    Ok(())
}

#[tokio::test]
async fn byte_stream_disconnect_repeats_setup_without_replaying_sample() {
    run_with(Arc::new(|| {
        let (device, instrument) = tokio::io::duplex(256);
        Ok((Box::new(device), Box::new(instrument)))
    }))
    .await
    .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn real_serial_pseudo_terminal_recovers() {
    run().await.unwrap();
}

#[tokio::test]
async fn cancelled_exchange_closes_port_with_pending_reply() {
    let (peer, port) = tokio::io::duplex(256);
    let instrument = Instrument {
        port: Mutex::new(Some(Box::new(port))),
        connected: watch::channel(true).0,
        gain: AtomicU32::new(1),
    };
    let mut peer = BufReader::new(peer);
    let mut line = String::new();
    {
        let request = instrument.request(b"SAMPLE\n");
        tokio::pin!(request);
        tokio::select! {
            result = &mut request => panic!("request completed before reply: {result:?}"),
            result = peer.read_line(&mut line) => { result.unwrap(); }
        }
        assert_eq!(line, "SAMPLE\n");
    }
    assert!(instrument.port.lock().await.is_none());
    assert!(!*instrument.connected.borrow());
    assert!(peer.get_mut().write_all(b"42\n").await.is_err());
}
