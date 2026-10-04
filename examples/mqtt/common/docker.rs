//! Isolated Docker broker used by the MQTT examples.
use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::TcpStream,
    process::Command,
    time::{sleep, timeout},
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const LIMIT: Duration = Duration::from_secs(15);
pub(crate) const IMAGE: &str = "eclipse-mosquitto:2.0.22";

pub(crate) async fn docker(args: &[&str]) -> Result<String> {
    let output = timeout(
        Duration::from_secs(30),
        Command::new("docker")
            .args(args)
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    if !output.status.success() {
        return Err(format!(
            "docker {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

pub(crate) struct Broker {
    pub(crate) name: String,
    pub(crate) artifacts: PathBuf,
    present: bool,
}

impl Drop for Broker {
    fn drop(&mut self) {
        // Fallback for cancellation or unwinding. Ordinary exits await removal.
        if self.present {
            let _ = std::process::Command::new("docker")
                .args(["rm", "--force", &self.name])
                .output();
        }
    }
}

impl Broker {
    pub(crate) async fn start() -> Result<(Self, u16)> {
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let name = format!("etherbird-mqtt-{}-{stamp}", std::process::id());
        let artifacts = std::env::current_dir()?
            .join("target/mqtt-broker")
            .join(&name);
        std::fs::create_dir_all(&artifacts)?;
        let config = artifacts.join("mosquitto.conf");
        std::fs::write(
            &config,
            "listener 1883\nallow_anonymous true\npersistence false\nlog_dest stdout\nlog_type all\n",
        )?;
        let mount = format!("{}:/mosquitto/config/mosquitto.conf:ro", config.display());
        let mut broker = Self {
            name,
            artifacts,
            present: true,
        };
        // Choose a free host port, then pin it for the container's entire life.
        // Docker's automatic port allocation can change when a container starts
        // again, which would test endpoint migration rather than reconnecting.
        let reservation = std::net::TcpListener::bind("127.0.0.1:0")?;
        let port = reservation.local_addr()?.port();
        let binding = format!("127.0.0.1:{port}:1883");
        drop(reservation);
        docker(&[
            "run",
            "--detach",
            "--name",
            &broker.name,
            "--publish",
            &binding,
            "--volume",
            &mount,
            IMAGE,
        ])
        .await?;
        if let Err(error) = broker.wait(port).await {
            broker.finish().await?;
            return Err(error);
        }
        Ok((broker, port))
    }

    pub(crate) async fn wait(&self, port: u16) -> Result<()> {
        timeout(LIMIT, async {
            loop {
                if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                    break;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await?;
        Ok(())
    }

    pub(crate) async fn finish(&mut self) -> Result<()> {
        // Capture stdout and stderr, because Mosquitto may use either.
        let logs = timeout(
            Duration::from_secs(30),
            Command::new("docker")
                .args(["logs", &self.name])
                .kill_on_drop(true)
                .output(),
        )
        .await;
        let removed = docker(&["rm", "--force", &self.name]).await;
        if removed.is_ok() {
            self.present = false;
        }
        let logs = logs??;
        let mut bytes = logs.stdout;
        bytes.extend_from_slice(&logs.stderr);
        std::fs::write(self.artifacts.join("broker.log"), bytes)?;
        removed?;
        Ok(())
    }
}

pub(crate) async fn pull() -> Result<()> {
    // Pull explicitly, so an image download does not consume a recovery deadline.
    let output = timeout(
        Duration::from_secs(180),
        Command::new("docker")
            .args(["pull", IMAGE])
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    if !output.status.success() {
        return Err(format!("docker pull: {}", String::from_utf8_lossy(&output.stderr)).into());
    }

    Ok(())
}
