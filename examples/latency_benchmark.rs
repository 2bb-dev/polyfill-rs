//! Latency Benchmark - Logs event timestamps to file for comparison
//! 
//! Run this on both old and new versions simultaneously to compare latencies.
//! Each writes to a separate log file, then compare the timestamps for matching event IDs.
//!
//! Usage:
//!   RECONNECT: cd /tmp/polyfill-rs-reconnect && cargo run --example latency_benchmark -- reconnect
//!   OPTIMIZED: cd polyfill-rs && cargo run --example latency_benchmark -- optimized

use polyfill_rs::stream::WebSocketStream;
use polyfill_rs::auth::create_wss_auth;
use polyfill_rs::types::StreamMessage;
use polyfill_rs::ClobClient;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::io::Write;
use std::fs::OpenOptions;
use futures::StreamExt;
use tokio::time::interval;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get version tag from args
    let args: Vec<String> = std::env::args().collect();
    let version = args.get(1).map(|s| s.as_str()).unwrap_or("unknown");
    
    let log_file = format!("/tmp/latency_{}.log", version);
    println!("📊 Latency Benchmark - Version: {}", version);
    println!("📁 Logging to: {}", log_file);
    
    // Load private key
    dotenvy::dotenv().ok();
    let private_key = std::env::var("PRIVATE_KEY")?;
    let private_key = if private_key.starts_with("0x") {
        private_key
    } else {
        format!("0x{}", private_key)
    };
    
    // Create client with L1 headers (required for API key derivation)
    let mut client = ClobClient::with_l1_headers(
        "https://clob.polymarket.com",
        &private_key,
        137,
    );
    let api_creds = client.create_or_derive_api_key(None).await?;
    client.set_api_creds(api_creds.clone());
    println!("✅ API credentials obtained");
    
    // Create WebSocket
    let wss_auth = create_wss_auth(&api_creds);
    let mut ws = WebSocketStream::new("wss://ws-subscriptions-clob.polymarket.com/ws/user")
        .with_auth(wss_auth);
    
    // Subscribe to current BTC 15-min window
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let window_start = (now / 900) * 900;
    let slug = format!("btc-updown-15m-{}", window_start);
    
    // Fetch condition_id from Gamma API
    let gamma_url = format!(
        "https://gamma-api.polymarket.com/events?slug={}&closed=false",
        slug
    );
    let response: serde_json::Value = reqwest::get(&gamma_url).await?.json().await?;
    let condition_id = response[0]["markets"][0]["conditionId"]
        .as_str()
        .ok_or("No condition_id found")?
        .to_string();
    
    println!("📡 Subscribing to market: {}", condition_id);
    ws.subscribe_user_channel(vec![condition_id]).await?;
    println!("✅ Subscribed - waiting for events...\n");
    
    // Open log file
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file)?;
    
    // Write header
    writeln!(file, "# Latency benchmark - Version: {} - Started: {}", version, now)?;
    writeln!(file, "# Format: event_type,event_id,receive_timestamp_us,event_timestamp_ms")?;
    
    let mut poll_interval = interval(Duration::from_millis(100));
    poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    
    let mut event_count = 0u64;
    
    loop {
        let result = tokio::select! {
            biased;
            msg = ws.next() => msg,
            _ = poll_interval.tick() => continue,
        };
        
        let Some(result) = result else { break };
        
        // Capture receive timestamp immediately
        let receive_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();
        
        if let Ok(msg) = result {
            event_count += 1;
            
            let (event_type, event_id, event_ts) = match &msg {
                StreamMessage::UserTrade(trade) => {
                    let ts = trade.timestamp.as_ref()
                        .and_then(|t| t.parse::<u64>().ok())
                        .unwrap_or(0);
                    ("TRADE", trade.id.clone(), ts)
                },
                StreamMessage::UserOrderPlacement(order) => {
                    let ts = order.timestamp.as_ref()
                        .and_then(|t| t.parse::<u64>().ok())
                        .unwrap_or(0);
                    ("PLACEMENT", order.id.clone(), ts)
                },
                StreamMessage::UserOrderUpdate(order) => {
                    let ts = order.timestamp.as_ref()
                        .and_then(|t| t.parse::<u64>().ok())
                        .unwrap_or(0);
                    ("UPDATE", order.id.clone(), ts)
                },
                StreamMessage::UserOrderCancellation(order) => {
                    let ts = order.timestamp.as_ref()
                        .and_then(|t| t.parse::<u64>().ok())
                        .unwrap_or(0);
                    ("CANCEL", order.id.clone(), ts)
                },
                _ => continue,
            };
            
            // Log to file
            writeln!(file, "{},{},{},{}", event_type, event_id, receive_ts, event_ts)?;
            file.flush()?;
            
            // Print summary
            let latency_ms = if event_ts > 0 {
                (receive_ts as i64 / 1000) - (event_ts as i64)
            } else {
                0
            };
            println!("[{}] #{} {} id={:.20}... latency={}ms", 
                version, event_count, event_type, event_id, latency_ms);
        }
    }
    
    Ok(())
}

