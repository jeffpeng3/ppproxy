use log::{debug, info};
use serde::Serialize;
use std::collections::HashMap;
use std::env;
use std::process::Stdio;
use std::sync::Arc;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

#[derive(Serialize)]
struct GostConfig {
    services: Vec<Service>,
    bypasses: Vec<Bypass>,
    api: ApiConfig,
    metrics: MetricsConfig,
    log: LogConfig,
}

#[derive(Serialize)]
struct Service {
    name: String,
    addr: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    bypass: Option<String>,
    handler: Handler,
    listener: Listener,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<HashMap<String, String>>,
}

#[derive(Serialize)]
struct Handler {
    #[serde(rename = "type")]
    handler_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Serialize)]
struct Listener {
    #[serde(rename = "type")]
    listener_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<HashMap<String, String>>,
}

#[derive(Serialize)]
struct Bypass {
    name: String,
    matchers: Vec<String>,
}

#[derive(Serialize)]
struct ApiConfig {
    addr: String,
}

#[derive(Serialize)]
struct MetricsConfig {
    addr: String,
}

#[derive(Serialize)]
struct LogConfig {
    format: String,
    level: String,
}

pub struct ProxyServer {
    process: Option<Child>,
    config_json: String,
    guard_task: Option<JoinHandle<()>>,
}

fn respawn_proxy_server(proxy: Arc<Mutex<ProxyServer>>) {
    tokio::spawn(Box::pin(ProxyServer::start(proxy)));
}

fn proxy_service(index: u16, interface: &str) -> Service {
    let mut handler_metadata = HashMap::new();
    handler_metadata.insert("udp".to_string(), serde_json::json!("true"));
    handler_metadata.insert("udpBufferSize".to_string(), serde_json::json!("65565"));

    let mut service_metadata = HashMap::new();
    service_metadata.insert("interface".to_string(), interface.to_string());

    Service {
        name: format!("if{}-proxy", index),
        addr: format!(":{}", 8080 + index),
        bypass: Some("local-bypass".to_string()),
        handler: Handler {
            handler_type: "auto".to_string(),
            metadata: Some(handler_metadata),
        },
        listener: Listener {
            listener_type: "tcp".to_string(),
            metadata: None,
        },
        metadata: Some(service_metadata),
    }
}

fn tun_service(index: u16, interface: &str) -> Service {
    let mut handler_metadata = HashMap::new();
    handler_metadata.insert("bufferSize".to_string(), serde_json::json!(65535));

    let mut listener_metadata = HashMap::new();
    listener_metadata.insert("name".to_string(), interface.to_string());
    listener_metadata.insert(
        "net".to_string(),
        format!("192.168.{}.1/24,fd00:{}::1/64", 100 + index, 100 + index),
    );

    Service {
        name: format!("if{}-tun", index),
        addr: format!(":{}", 8880 + index),
        bypass: Some("local-bypass".to_string()),
        handler: Handler {
            handler_type: "tun".to_string(),
            metadata: Some(handler_metadata),
        },
        listener: Listener {
            listener_type: "tun".to_string(),
            metadata: Some(listener_metadata),
        },
        metadata: None,
    }
}

impl ProxyServer {
    pub fn new(session_count: u16, logger_level: String) -> Arc<Mutex<Self>> {
        let bypass = Bypass {
            name: "local-bypass".to_string(),
            matchers: vec![
                "127.0.0.1/8".to_string(),
                "10.0.0.0/8".to_string(),
                "172.16.0.0/12".to_string(),
                "192.168.0.0/16".to_string(),
                "::1/128".to_string(),
                "fc00::/7".to_string(),
            ],
        };

        let mut services = Vec::new();

        services.push(proxy_service(0, "eth0"));
        services.push(tun_service(0, "tun0"));

        for i in 0..session_count {
            services.push(proxy_service(i + 1, &format!("ppp{}", i)));
            services.push(tun_service(i + 1, &format!("tun{}", i + 1)));
        }

        let config = GostConfig {
            services,
            bypasses: vec![bypass],
            api: ApiConfig {
                addr: env::var("GOST_API_ADDR")
                    .unwrap_or_else(|_| "127.0.0.1:18080".to_string()),
            },
            metrics: MetricsConfig {
                addr: ":9000".to_string(),
            },
            log: LogConfig {
                format: "text".to_string(),
                level: logger_level,
            },
        };
        let config_json = serde_json::to_string(&config).expect("Failed to serialize config");

        Arc::new(Mutex::new(Self {
            process: None,
            config_json,
            guard_task: None,
        }))
    }

    pub async fn start(proxy: Arc<Mutex<Self>>) {
        let guard_proxy = Arc::clone(&proxy);
        let mut p = proxy.lock().await;
        debug!("Starting proxy service with JSON config: {}", p.config_json);
        let verbose = env::var("PROXY_VERBOSE").unwrap_or_else(|_| "false".to_string()) == "true"
            || env::var("PROXY_VERBOSE").unwrap_or_else(|_| "0".to_string()) == "1";

        let stdio = if verbose { Stdio::inherit } else { Stdio::null };

        let child = Command::new("./gost")
            .arg("-C")
            .arg(&p.config_json)
            .stdout(stdio())
            .stderr(stdio())
            .spawn()
            .expect("Failed to start proxy");

        p.process = Some(child);

        let guard = tokio::spawn(async move {
            ProxyServer::guard(guard_proxy).await;
        });
        p.guard_task = Some(guard);
    }

    async fn guard(mutex_proxy: Arc<Mutex<Self>>) {
        let child_to_wait = {
            let mut proxy = mutex_proxy.lock().await;
            proxy.process.take()
        };
        info!("Proxy service guard started");
        if let Some(mut child) = child_to_wait {
            let _exit_status = child.wait().await.expect("Failed to wait for child");
            info!("Proxy service exited abnormally, restarting in 5 seconds...");
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            respawn_proxy_server(mutex_proxy);
        }
    }

    pub async fn stop(proxy: Arc<Mutex<Self>>) {
        let mut p = proxy.lock().await;
        if let Some(guard) = p.guard_task.take() {
            guard.abort();
        }
        if let Some(mut child) = p.process.take() {
            debug!("Stopping proxy service...");
            let _ = child.kill().await;
            debug!("Proxy service stopped");
        }
    }
}
