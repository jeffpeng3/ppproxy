use crate::pppoe::manager::PPPoEManager;
use anyhow::{Error, Result};
use poise::serenity_prelude as serenity;
use std::sync::Arc;

pub struct Data {
    pub manager: Arc<PPPoEManager>,
}

pub type Context<'a> = poise::Context<'a, Data, Error>;

/// Autocomplete function for interface names
async fn autocomplete_interface<'a>(ctx: Context<'a>, partial: &'a str) -> Vec<String> {
    let manager = &ctx.data().manager;
    let stats = manager.get_all_stats().await;

    stats
        .keys()
        .filter(|name| name.starts_with(partial))
        .map(|s| s.to_string())
        .collect()
}

/// Get the status of all PPPoE interfaces
#[poise::command(slash_command)]
pub async fn status(ctx: Context<'_>) -> Result<()> {
    let manager = &ctx.data().manager;
    let stats = manager.get_all_stats().await;

    let mut embed = serenity::CreateEmbed::default()
        .title("PPPoE Connection Status")
        .timestamp(chrono::Utc::now());

    let mut all_healthy = true;
    let mut any_connected = false;

    for (interface, info) in stats {
        let status_emoji = if info.local_ip.is_some() {
            any_connected = true;
            if info.is_healthy {
                "✅"
            } else {
                all_healthy = false;
                "⚠️"
            }
        } else {
            "🔴"
        };

        let mut value = String::new();
        if let Some(ip) = info.local_ip {
            value.push_str(&format!("**IP:** {}\n", ip));
            if let Some(ip6) = manager.ipv6_address(&interface).await {
                value.push_str(&format!("**IPv6:** {}\n", ip6));
            }
            if let Some(connected_at) = info.connected_at {
                let duration = chrono::Utc::now() - connected_at;
                let hours = duration.num_hours();
                let minutes = duration.num_minutes() % 60;
                let seconds = duration.num_seconds() % 60;
                value.push_str(&format!(
                    "**Uptime:** {:02}:{:02}:{:02}\n",
                    hours, minutes, seconds
                ));
            }
            if !info.is_healthy {
                value.push_str(&format!("**Failures:** {}\n", info.consecutive_failures));
            }
            if let Some(last_check) = info.last_health_check {
                let since_check = chrono::Utc::now() - last_check;
                value.push_str(&format!(
                    "**Last Check:** {}s ago\n",
                    since_check.num_seconds()
                ));
            }
        } else {
            value.push_str("Disconnected");
        }

        embed = embed.field(format!("{} {}", status_emoji, interface), value, false);
    }

    if all_healthy && any_connected {
        embed = embed.color(0x00FF00); // Green
    } else if any_connected {
        embed = embed.color(0xFFA500); // Orange
    } else {
        embed = embed.color(0xFF0000); // Red
    }

    ctx.send(poise::CreateReply::default().embed(embed)).await?;
    Ok(())
}

/// Reconnect a specific PPPoE interface
#[poise::command(slash_command)]
pub async fn reconnect(
    ctx: Context<'_>,
    #[description = "Interface name (e.g., ppp0)"]
    #[autocomplete = "autocomplete_interface"]
    interface: String,
) -> Result<()> {
    let manager = &ctx.data().manager;
    match manager.reconnect_client(&interface).await {
        Ok(_) => {
            ctx.say(format!("Reconnecting {}...", interface)).await?;
        }
        Err(e) => {
            ctx.say(format!("Failed to reconnect {}: {}", interface, e))
                .await?;
        }
    }
    Ok(())
}

/// Disconnect a specific PPPoE interface
#[poise::command(slash_command)]
pub async fn disconnect(
    ctx: Context<'_>,
    #[description = "Interface name (e.g., ppp0)"]
    #[autocomplete = "autocomplete_interface"]
    interface: String,
) -> Result<()> {
    let manager = &ctx.data().manager;
    match manager.disconnect_client(&interface).await {
        Ok(_) => {
            ctx.say(format!("Disconnecting {}...", interface)).await?;
        }
        Err(e) => {
            ctx.say(format!("Failed to disconnect {}: {}", interface, e))
                .await?;
        }
    }
    Ok(())
}

/// Connect a specific PPPoE interface
#[poise::command(slash_command)]
pub async fn connect(
    ctx: Context<'_>,
    #[description = "Interface name (e.g., ppp0)"]
    #[autocomplete = "autocomplete_interface"]
    interface: String,
) -> Result<()> {
    let manager = &ctx.data().manager;
    match manager.connect_client(&interface).await {
        Ok(_) => {
            ctx.say(format!("Connecting {}...", interface)).await?;
        }
        Err(e) => {
            ctx.say(format!("Failed to connect {}: {}", interface, e))
                .await?;
        }
    }
    Ok(())
}

/// Trigger a health check for a specific PPPoE interface
#[poise::command(slash_command)]
pub async fn healthcheck(
    ctx: Context<'_>,
    #[description = "Interface name (e.g., ppp0)"]
    #[autocomplete = "autocomplete_interface"]
    interface: String,
) -> Result<()> {
    let manager = &ctx.data().manager;
    ctx.say(format!("Running health check for {}...", interface))
        .await?;

    let is_healthy = manager.check_health(&interface).await;
    manager.update_health_status(&interface, is_healthy).await;

    if is_healthy {
        ctx.say(format!("✅ {} is healthy", interface)).await?;
    } else {
        ctx.say(format!("⚠️ {} is unhealthy", interface)).await?;
    }
    Ok(())
}

pub async fn start_bot(
    token: String,
    guild_id: Option<u64>,
    manager: Arc<PPPoEManager>,
) -> Result<()> {
    let intents = serenity::GatewayIntents::non_privileged();

    let framework = poise::Framework::builder()
        .options(poise::FrameworkOptions {
            commands: vec![
                status(),
                reconnect(),
                disconnect(),
                connect(),
                healthcheck(),
            ],
            ..Default::default()
        })
        .setup(move |ctx, _ready, framework| {
            Box::pin(async move {
                if let Some(guild_id) = guild_id {
                    poise::builtins::register_in_guild(
                        ctx,
                        &framework.options().commands,
                        serenity::GuildId::new(guild_id),
                    )
                    .await?;
                } else {
                    poise::builtins::register_globally(ctx, &framework.options().commands).await?;
                }
                Ok(Data { manager })
            })
        })
        .build();

    let client = serenity::ClientBuilder::new(token, intents)
        .framework(framework)
        .await;
    client?.start().await?;
    Ok(())
}
