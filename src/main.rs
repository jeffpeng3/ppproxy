use log::{error, info};
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::mpsc;

mod bot;
mod core;
mod network;
mod pppoe;
mod proxy;

use crate::core::config::AppConfig;
use crate::core::logger;
use crate::network::route::{init_route, pin_eth0_ipv6_default};
use crate::pppoe::manager::PPPoEManager;
use crate::proxy::server::ProxyServer;
use anyhow::{Context, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Load config first to get dry_run flag
    let config = AppConfig::load()?;

    setup_nft().await?;

    logger::init();

    let _ = init_route(&config.gateway)
        .await
        .map_err(|x| error!("{x:?}"));

    tokio::spawn(pin_eth0_ipv6_default());

    let (event_tx, event_rx) = mpsc::channel(100);

    let pppoe_manager = PPPoEManager::new(config.ip_rotation.clone());
    pppoe_manager.set_event_receiver(event_rx).await;
    PPPoEManager::start_stats_task(Arc::clone(&pppoe_manager)).await;

    pppoe_manager
        .start_clients(
            config.username.clone(),
            config.password.clone(),
            config.session_count,
            event_tx,
        )
        .await;

    let pppoe_manager_clone = Arc::clone(&pppoe_manager);
    tokio::spawn(async move {
        pppoe_manager_clone.run_event_loop().await;
    });

    let pppoe_manager_clone = Arc::clone(&pppoe_manager);
    tokio::spawn(async move {
        pppoe_manager_clone.serve().await;
    });

    let pppoe_manager_clone = Arc::clone(&pppoe_manager);
    let discord_token = config.discord_token.clone();
    let discord_guild_id = config.discord_guild_id;
    tokio::spawn(async move {
        if let Err(e) = bot::start_bot(discord_token, discord_guild_id, pppoe_manager_clone).await {
            error!("Discord bot error: {:?}", e);
        }
    });

    let proxy = ProxyServer::new(config.session_count, config.logger_level.clone());
    ProxyServer::start(Arc::clone(&proxy)).await;

    info!("Service started. Press Ctrl+C to stop.");

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Shutting down...");
                break;
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                // Optional: Log stats periodically if needed, or rely on events
            }
        }
    }

    info!("Stopping services...");
    pppoe_manager.stop_all().await;
    ProxyServer::stop(proxy).await;
    info!("Goodbye!");

    Ok(())
}

async fn setup_nft() -> Result<()> {
    Command::new("nft")
        .arg("-f")
        .arg("/etc/nftables.conf")
        .status()
        .await
        .context("Failed to execute nft command")?;
    Ok(())
}
