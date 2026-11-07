use anyhow::Result;
use log::error;
use tokio::process::Command;

pub async fn init_route(gateway: &str) -> Result<()> {
    for i in 0..8 {
        let table_id = 100 + i;
        Command::new("ip")
            .args([
                "rule",
                "add",
                "iif",
                format!("tun{i}").as_str(),
                "table",
                &table_id.to_string(),
                "prio",
                &table_id.to_string(),
            ])
            .output()
            .await
            .map_err(|e| {
                error!("Failed to add rule: {}", e);
                e
            })?;

        Command::new("ip")
            .args([
                "-6",
                "rule",
                "add",
                "iif",
                format!("tun{i}").as_str(),
                "table",
                &table_id.to_string(),
                "prio",
                &table_id.to_string(),
            ])
            .output()
            .await
            .map_err(|e| {
                error!("Failed to add IPv6 rule: {}", e);
                e
            })?;
    }

    Command::new("ip")
        .args([
            "route", "add", "default", "via", gateway, "dev", "eth0", "table", "100",
        ])
        .output()
        .await
        .map_err(|e| {
            error!("Failed to add default route: {}", e);
            e
        })?;

    Ok(())
}

/// Keep the container's own IPv6 traffic and service 0 on eth0.
///
/// The LAN router is only learned from a router advertisement, so the ppp
/// links receive an RA default route as well. Pin a low-metric default on
/// eth0, add an IPv6 default to table 100 (service 0), and drop the RA
/// defaults from the ppp links so they never take over the main table.
pub async fn pin_eth0_ipv6_default() {
    loop {
        if let Some(gateway) = eth0_ra_gateway().await {
            for args in [
                vec![
                    "-6", "route", "replace", "default", "via", gateway.as_str(), "dev", "eth0",
                    "metric", "100",
                ],
                vec![
                    "-6", "route", "replace", "default", "via", gateway.as_str(), "dev", "eth0",
                    "table", "100",
                ],
            ] {
                let _ = Command::new("ip").args(args).output().await;
            }
        }

        for i in 0..8u16 {
            let ppp = format!("ppp{i}");
            let _ = Command::new("ip")
                .args(["-6", "route", "del", "default", "dev", &ppp, "proto", "ra"])
                .output()
                .await;
        }

        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    }
}

async fn eth0_ra_gateway() -> Option<String> {
    let output = Command::new("ip")
        .args(["-6", "route", "show", "default", "dev", "eth0", "proto", "ra"])
        .output()
        .await
        .ok()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout.lines().next()?;
    let tokens: Vec<&str> = line.split_whitespace().collect();
    let pos = tokens.iter().position(|token| *token == "via")?;
    tokens.get(pos + 1).map(|gateway| gateway.to_string())
}
