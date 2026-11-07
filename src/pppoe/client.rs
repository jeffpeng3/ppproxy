use chrono::Utc;
use log::{error, info};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

use crate::pppoe::manager::{ClientCommand, PpmsEvent};

pub struct PPPoEClient {
    username: String,
    password: String,
    pub interface: String,
    pppd: Option<Child>,
    event_sender: mpsc::Sender<PpmsEvent>,
    command_receiver: mpsc::Receiver<ClientCommand>,
    should_be_connected: bool,
    reconnect_attempts: u32,
    max_reconnect_attempts: u32,
}

impl PPPoEClient {
    pub fn new(
        username: String,
        password: String,
        interface: String,
        event_sender: mpsc::Sender<PpmsEvent>,
        command_receiver: mpsc::Receiver<ClientCommand>,
    ) -> Self {
        Self {
            username,
            password,
            interface,
            pppd: None,
            event_sender,
            command_receiver,
            should_be_connected: false,
            reconnect_attempts: 0,
            max_reconnect_attempts: 0,
        }
    }

    pub async fn run(mut self) {
        info!("PPPoE Client {} started", self.interface);

        self.should_be_connected = true;
        self.connect().await;

        loop {
            tokio::select! {
                Some(cmd) = self.command_receiver.recv() => {
                    match cmd {
                        ClientCommand::Connect => {
                            self.should_be_connected = true;
                            if self.pppd.is_none() {
                                self.reconnect_attempts = 0;
                                self.connect().await;
                            }
                        }
                        ClientCommand::Disconnect => {
                            self.should_be_connected = false;
                            self.reconnect_attempts = 0;
                            self.disconnect().await;
                        }
                        ClientCommand::Reconnect => {
                            self.should_be_connected = true;
                            self.reconnect_attempts = 0;
                            self.disconnect().await;
                            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                            self.connect().await;
                        }
                    }
                }
                Some(result) = async {
                    if let Some(ref mut child) = self.pppd {
                        child.wait().await.ok()
                    } else {
                        None
                    }
                } => {
                    info!("{}: pppd process exited with {:?}", self.interface, result);
                    self.pppd = None;

                    let _ = self.event_sender.send(PpmsEvent::Disconnected {
                        interface: self.interface.clone(),
                    }).await;

                    if self.should_be_connected {
                        if self.max_reconnect_attempts == 0 || self.reconnect_attempts < self.max_reconnect_attempts {
                            self.reconnect_attempts += 1;

                            // 5s, 10s, 20s, 40s, then 60s, with up to 3s of jitter.
                            // A session rejected by the BRAS needs time to be released,
                            // and the jitter keeps the sessions from retrying in lockstep.
                            let exp = std::cmp::min(self.reconnect_attempts.saturating_sub(1), 4);
                            let backoff = std::cmp::min(5u64 << exp, 60);
                            let delay_ms = backoff * 1000 + jitter_ms();

                            info!(
                                "{}: Auto-reconnecting in {:.1} seconds (attempt {}/{})",
                                self.interface,
                                delay_ms as f64 / 1000.0,
                                self.reconnect_attempts,
                                if self.max_reconnect_attempts == 0 { "∞".to_string() } else { self.max_reconnect_attempts.to_string() }
                            );

                            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                            self.connect().await;
                        } else {
                            error!(
                                "{}: Max reconnection attempts ({}) reached, giving up",
                                self.interface,
                                self.max_reconnect_attempts
                            );
                            self.should_be_connected = false;
                        }
                    } else {
                        info!("{}: Manual disconnect, not auto-reconnecting", self.interface);
                    }
                }
            }
        }
    }

    async fn connect(&mut self) {
        info!("Connecting {}", self.interface);

        let cmd = vec![
            "pppd".to_string(),
            "pty".to_string(),
            "pppoe".to_string(),
            "noauth".to_string(),
            "nodetach".to_string(),
            "usepeerdns".to_string(),
            "+ipv6".to_string(),
            "ifname".to_string(),
            self.interface.clone(),
            "user".to_string(),
            self.username.clone(),
            "password".to_string(),
            self.password.clone(),
        ];

        match Command::new("pppd")
            .args(&cmd[1..])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
        {
            Ok(mut child) => {
                let stdout = child.stdout.take().unwrap();
                self.pppd = Some(child);

                let interface = self.interface.clone();
                let event_sender = self.event_sender.clone();

                tokio::spawn(async move {
                    let mut reader = BufReader::new(stdout);
                    let mut line = String::new();
                    let mut ip_obtained = false;
                    while let Ok(n) = reader.read_line(&mut line).await {
                        if n == 0 {
                            break;
                        }
                        let trimmed = line.trim();
                        if trimmed.contains("local  IP address") {
                            let parts: Vec<&str> = trimmed.split_whitespace().collect();
                            if parts.len() >= 4 {
                                let local_ip = parts[3].to_string();
                                ip_obtained = true;
                                let _ = event_sender
                                    .send(PpmsEvent::IpUpdated {
                                        interface: interface.clone(),
                                        local_ip: Some(local_ip),
                                        connected_at: Some(Utc::now()),
                                    })
                                    .await;
                            }
                        }
                        line.clear();
                    }
                    if ip_obtained {
                        info!("{}: pppd stdout closed, connection likely lost", interface);
                    }
                });
            }
            Err(e) => {
                error!("Failed to start pppd for {}: {}", self.interface, e);
            }
        }
    }

    async fn disconnect(&mut self) {
        if let Some(mut child) = self.pppd.take() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }

        let _ = self
            .event_sender
            .send(PpmsEvent::Disconnected {
                interface: self.interface.clone(),
            })
            .await;
    }
}


/// A small pseudo-random delay used to spread out reconnects.
fn jitter_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| u64::from(elapsed.subsec_nanos()) % 3000)
        .unwrap_or(0)
}
