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
