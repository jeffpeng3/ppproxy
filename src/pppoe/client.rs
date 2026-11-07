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

                            let delay = std::cmp::min(
                                5 * self.reconnect_attempts as u64,
                                30
                            );

                            info!(
                                "{}: Auto-reconnecting in {} seconds (attempt {}/{})",
                                self.interface,
                                delay,
                                self.reconnect_attempts,
                                if self.max_reconnect_attempts == 0 { "∞".to_string() } else { self.max_reconnect_attempts.to_string() }
                            );

                            tokio::time::sleep(tokio::time::Duration::from_secs(delay)).await;
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

                tokio::spawn(supervise_ipv6(self.interface.clone()));

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

/// Wait for IPv6CP on the PPP link, then run a DHCPv6 client requesting a
/// delegated prefix for as long as the interface exists.
///
/// The prefix cannot be assigned to the requesting interface, so a dummy
/// interface (pdN) is used as the delegation target and the dhcpcd hook
/// mirrors the resulting address onto pppN for NAT66.
async fn supervise_ipv6(interface: String) {
    let mut waited = 0;
    while !has_ipv6_linklocal(&interface).await {
        if waited >= 120 {
            return;
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        waited += 2;
    }

    let pd = format!("pd{}", interface.trim_start_matches("ppp"));

    let _ = Command::new("ip")
        .args(["link", "add", &pd, "type", "dummy"])
        .output()
        .await;
    let _ = Command::new("ip")
        .args(["link", "set", &pd, "up"])
        .output()
        .await;

    // Terminate a client left over from a previous session on this interface.
    if let Ok(pid) = tokio::fs::read_to_string(format!("/run/dhcpcd/{}-6.pid", interface)).await {
        if let Ok(pid) = pid.trim().parse::<u32>() {
            let _ = Command::new("kill").arg(pid.to_string()).output().await;
        }
    }

    let config_path = format!("/tmp/dhcpcd-{}.conf", interface);
    let config = format!(
        "noipv6rs\ninterface {}\n  dhcp6\n  ia_pd 1/::/64 {}/0/64\n",
        interface, pd
    );
    if tokio::fs::write(&config_path, config).await.is_err() {
        return;
    }

    let mut child = match Command::new("dhcpcd")
        .args([
            "-6",
            "-B",
            "-G",
            "-f",
            &config_path,
            "--nohook",
            "resolv.conf",
            "--nohook",
            "hostname",
            &interface,
        ])
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => {
            error!("{}: failed to start dhcpcd: {}", interface, e);
            return;
        }
    };

    info!("{}: dhcpcd started, delegating prefix to {}", interface, pd);

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        if !interface_exists(&interface).await {
            break;
        }
        if let Ok(Some(_)) = child.try_wait() {
            break;
        }
    }

    let _ = child.kill().await;
    let _ = child.wait().await;
}

async fn has_ipv6_linklocal(interface: &str) -> bool {
    Command::new("ip")
        .args(["-6", "-o", "addr", "show", "dev", interface, "scope", "link"])
        .output()
        .await
        .map(|output| output.status.success() && !output.stdout.is_empty())
        .unwrap_or(false)
}

async fn interface_exists(interface: &str) -> bool {
    Command::new("ip")
        .args(["-o", "link", "show", "dev", interface])
        .output()
        .await
        .map(|output| output.status.success())
        .unwrap_or(false)
}
