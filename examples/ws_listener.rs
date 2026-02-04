//! WebSocket Listener for Manual Fill Testing
//!
//! This example connects to the User WebSocket channel
//! and logs ALL messages with full details (no truncation) for debugging.
//!
//! Usage:
//!   cargo run --example ws_listener [condition_id]
//!
//! If no condition_id is provided, automatically fetches the current active
//! BTC 15-min window market from Polymarket.

use polyfill_rs::stream::WebSocketStream;
use polyfill_rs::auth::create_wss_auth;
use polyfill_rs::types::{StreamMessage, UserTradeMessage};
use polyfill_rs::ClobClient;
use futures::StreamExt;
use std::time::Duration;
use serde::Deserialize;

/// Market entry from Gamma API (nested inside event)
#[derive(Debug, Deserialize)]
struct GammaMarket {
    #[serde(rename = "conditionId")]
    condition_id: Option<String>,
    question: Option<String>,
}

/// Response from Gamma API for events  
#[derive(Debug, Deserialize)]
struct GammaEvent {
    title: Option<String>,
    markets: Option<Vec<GammaMarket>>,
}

/// Window info including condition_id and timing
#[derive(Debug, Clone)]
struct WindowInfo {
    condition_id: String,
    title: String,
    window_start: u64,
    window_end: u64,
}

const WINDOW_SECONDS: u64 = 900; // 15 minutes

/// Get current and next window boundary timestamps
fn get_window_boundaries() -> (u64, u64) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    
    let current_window = (now / WINDOW_SECONDS) * WINDOW_SECONDS;
    let next_window = current_window + WINDOW_SECONDS;
    (current_window, next_window)
}

/// Fetch the current active BTC 15-min window with timing info
async fn fetch_current_btc_15m_window() -> Result<WindowInfo, Box<dyn std::error::Error>> {
    let (current_window, next_window) = get_window_boundaries();
    
    // Construct slug: btc-updown-15m-{timestamp}
    let slug = format!("btc-updown-15m-{}", current_window);
    println!("🔍 Searching for market with slug: {}", slug);
    
    // Try to fetch market with current window
    if let Some((condition_id, title)) = fetch_market_by_slug(&slug).await? {
        return Ok(WindowInfo {
            condition_id,
            title,
            window_start: current_window,
            window_end: next_window,
        });
    }
    
    // Try next window (might be between windows or market not yet created)
    let next_slug = format!("btc-updown-15m-{}", next_window);
    println!("⏭️  Current window not found, trying next: {}", next_slug);
    
    if let Some((condition_id, title)) = fetch_market_by_slug(&next_slug).await? {
        return Ok(WindowInfo {
            condition_id,
            title,
            window_start: next_window,
            window_end: next_window + WINDOW_SECONDS,
        });
    }
    
    Err(format!("Could not find active BTC 15-min market for slug {} or {}", slug, next_slug).into())
}


/// Fetch market condition_id and title by slug
async fn fetch_market_by_slug(slug: &str) -> Result<Option<(String, String)>, Box<dyn std::error::Error>> {
    let url = format!("https://gamma-api.polymarket.com/events?slug={}", slug);
    let response = reqwest::get(&url).await?;
    
    if !response.status().is_success() {
        return Ok(None);
    }
    
    let events: Vec<GammaEvent> = response.json().await?;
    
    if let Some(event) = events.first() {
        if let Some(markets) = &event.markets {
            if let Some(market) = markets.first() {
                if let Some(ref condition_id) = market.condition_id {
                    let title = event.title.clone().unwrap_or_else(|| "Unknown".to_string());
                    println!("✅ Found market: {}", title);
                    println!("   Condition ID: {}", condition_id);
                    return Ok(Some((condition_id.clone(), title)));
                }
            }
        }
    }
    
    Ok(None)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Enable FULL debug logging
    std::env::set_var("RUST_LOG", "debug,polyfill_rs=debug");
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(true)
        .with_ansi(true)
        .init();
    
    // Load private key from env
    dotenvy::from_path("../.env").ok();
    dotenvy::dotenv().ok();
    let private_key = std::env::var("PRIVATE_KEY")
        .expect("PRIVATE_KEY must be set in .env");
    
    let private_key = if private_key.starts_with("0x") {
        private_key
    } else {
        format!("0x{}", private_key)
    };
    
    // Create CLOB client and derive API credentials
    let mut client = ClobClient::with_l1_headers(
        "https://clob.polymarket.com",
        &private_key,
        137,
    );
    
    println!("📝 Deriving API credentials...");
    let api_creds = client.create_or_derive_api_key(None).await?;
    client.set_api_creds(api_creds.clone());
    println!("✅ API credentials obtained");
    println!("🔑 Your API Key (owner_id): {}", api_creds.api_key);
    
    // Set owner_id from API key immediately - this is YOUR identity
    let known_owner_id = api_creds.api_key.clone();
    
    // Create WebSocket auth
    let wss_auth = create_wss_auth(&api_creds);

    
    // Create User WebSocket stream
    let mut ws = WebSocketStream::new("wss://ws-subscriptions-clob.polymarket.com/ws/user")
        .with_auth(wss_auth);
    
    // Check if user provided a specific condition_id (disables auto-transition)
    let args: Vec<String> = std::env::args().collect();
    let auto_transition = args.get(1).is_none();
    
    // Get initial window
    let mut current_window = if let Some(arg) = args.get(1) {
        println!("📌 Using provided condition_id (auto-transition disabled): {}", arg);
        WindowInfo {
            condition_id: arg.clone(),
            title: "Manual market".to_string(),
            window_start: 0,
            window_end: u64::MAX, // Never expires
        }
    } else {
        println!("🔄 Fetching current BTC 15-min window (auto-transition enabled)...");
        fetch_current_btc_15m_window().await?
    };
    
    // Subscribe to initial market
    println!("📡 Subscribing to User channel...");
    ws.subscribe_user_channel(vec![current_window.condition_id.clone()]).await?;
    println!("✅ Subscribed to: {}", current_window.title);
    
    println!();
    println!("═══════════════════════════════════════════════════════════════════");
    println!("                    WebSocket Listener (Verbose Mode)              ");
    println!("═══════════════════════════════════════════════════════════════════");
    println!("Market: {}", current_window.condition_id);
    if auto_transition {
        let time_remaining = current_window.window_end.saturating_sub(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );
        println!("Auto-transition: ON (next window in {}s)", time_remaining);
    }
    println!();
    
    let mut msg_count = 0u64;
    let mut trade_count = 0u64;
    let mut reconnect_count = 0u32;
    let mut window_count = 1u32;
    const MAX_RECONNECTS: u32 = 10;
    let start = std::time::Instant::now();
    
    // Aggregate share tracking
    let mut total_up_shares: f64 = 0.0;
    let mut total_down_shares: f64 = 0.0;
    
    // Our owner ID - set from API credentials (this is YOUR identity)
    let our_owner_id: Option<String> = Some(known_owner_id);

    
    // Outer reconnection loop
    'reconnect: loop {
        // Create interval for frequent polling (100ms)
        let mut poll_interval = tokio::time::interval(Duration::from_millis(100));
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        

        // Inner message processing loop
        loop {
            // Check if current window has expired and we need to transition
            if auto_transition {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                
                if now >= current_window.window_end {
                    println!();
                    println!("═══════════════════════════════════════════════════════════════════");
                    println!("🔄 WINDOW TRANSITION: {} ended", current_window.title);
                    println!("═══════════════════════════════════════════════════════════════════");
                    
                    // Fetch next window
                    match fetch_current_btc_15m_window().await {
                        Ok(next_window) => {
                            window_count += 1;
                            
                            // Unsubscribe from old market (best effort)
                            let _ = ws.unsubscribe_user_channel(vec![current_window.condition_id.clone()]).await;
                            
                            // Subscribe to new market
                            if let Err(e) = ws.subscribe_user_channel(vec![next_window.condition_id.clone()]).await {
                                eprintln!("❌ Failed to subscribe to new window: {}", e);
                                // Continue with old subscription
                            } else {
                                println!("✅ Switched to window #{}: {}", window_count, next_window.title);
                                println!("   Condition ID: {}", next_window.condition_id);
                                let time_remaining = next_window.window_end.saturating_sub(now);
                                println!("   Next transition in: {}s", time_remaining);
                                println!();
                                
                                // Reset per-window stats
                                total_up_shares = 0.0;
                                total_down_shares = 0.0;
                                trade_count = 0;
                                
                                current_window = next_window;
                            }
                        }
                        Err(e) => {
                            eprintln!("❌ Failed to fetch next window: {}", e);
                            eprintln!("   Will retry on next poll cycle");
                        }
                    }
                }
            }
            
            let result = tokio::select! {
                biased;  // Prefer first branch (messages) over second (timer)
                msg = ws.next() => msg,
                _ = poll_interval.tick() => {
                    // Periodic tick - just continue to poll stream again  
                    continue;
                }
            };
            
            let Some(result) = result else {
                // Stream ended
                break;
            };
            
            msg_count += 1;
            let elapsed = start.elapsed();
            
            match result {
                Ok(msg) => {
                    match &msg {
                        StreamMessage::UserTrade(trade) => {
                            trade_count += 1;
                            
                            // Count YOUR fills - check both taker and maker positions
                            let mut my_up_shares: f64 = 0.0;
                            let mut my_down_shares: f64 = 0.0;
                            
                            if let Some(ref owner_id) = our_owner_id {

                                // Check if we're the taker
                                if &trade.owner == owner_id {
                                    let size: f64 = trade.size.parse().unwrap_or(0.0);
                                    if trade.outcome.to_lowercase() == "up" {
                                        my_up_shares += size;
                                    } else {
                                        my_down_shares += size;
                                    }
                                }
                                
                                // Also check maker_orders for our fills
                                for maker in &trade.maker_orders {
                                    if &maker.owner == owner_id {
                                        let size: f64 = maker.matched_amount.parse().unwrap_or(0.0);
                                        if maker.outcome.to_lowercase() == "up" {
                                            my_up_shares += size;
                                        } else {
                                            my_down_shares += size;
                                        }
                                    }
                                }
                            }
                            
                            // Update totals
                            total_up_shares += my_up_shares;
                            total_down_shares += my_down_shares;
                            
                            // Only log if WE got filled
                            let my_total = my_up_shares + my_down_shares;
                            if my_total > 0.0 {
                                let price: f64 = trade.price.parse().unwrap_or(0.0);
                                println!("\n╔══════════════════════════════════════════════════════════════════╗");
                                println!("║ ⚡ MY FILL #{:<55}║", trade_count);
                                println!("╠══════════════════════════════════════════════════════════════════╣");
                                if my_up_shares > 0.0 {
                                    println!("║   UP:   +{:<10.2} shares @ {:<6} {:>30}║", my_up_shares, price, "");
                                }
                                if my_down_shares > 0.0 {
                                    println!("║   DOWN: +{:<10.2} shares @ {:<6} {:>30}║", my_down_shares, price, "");
                                }
                                println!("╠══════════════════════════════════════════════════════════════════╣");
                                println!("║ TOTALS: Up={:<12.2} Down={:<12.2} {:>19}║", total_up_shares, total_down_shares, "");
                                println!("╚══════════════════════════════════════════════════════════════════╝");
                            }
                            
                            // Verbose logging for all trades (for debugging)
                            print_trade_verbose(trade, trade_count, elapsed);
                        },

                        StreamMessage::UserOrderPlacement(order) => {
                            println!("\n╔══════════════════════════════════════════════════════════════════╗");
                            println!("║ 📋 ORDER PLACEMENT #{:<52}║", msg_count);

                            println!("╠══════════════════════════════════════════════════════════════════╣");
                            println!("║ Time:   {:>56} ║", format!("{:?}", elapsed));
                            println!("║ ID:     {:>56} ║", &order.id[..order.id.len().min(56)]);
                            println!("║ Side:   {:>56} ║", order.side);
                            println!("║ Price:  {:>56} ║", order.price);
                            println!("║ Size:   {:>56} ║", order.original_size);
                            println!("╚══════════════════════════════════════════════════════════════════╝");
                        },
                        StreamMessage::UserOrderUpdate(order) => {
                            println!("\n🔄 ORDER UPDATE #{} at {:?}", msg_count, elapsed);
                            println!("   ID: {}", order.id);
                            println!("   Type: {}", order.message_type);
                            println!("   Price: {} | Size Matched: {}", order.price, order.size_matched);
                        },
                        StreamMessage::UserOrderCancellation(order) => {
                            println!("\n❌ ORDER CANCELLED #{} at {:?}", msg_count, elapsed);
                            println!("   ID: {}", order.id);
                        },
                        StreamMessage::Heartbeat { timestamp } => {
                            // Only print heartbeats every 10 to reduce noise
                            if msg_count % 10 == 1 {
                                println!("💓 Heartbeat #{} at {} (trades: {})", msg_count, timestamp, trade_count);
                            }
                        },
                        other => {
                            println!("\n📨 OTHER MESSAGE #{} at {:?}", msg_count, elapsed);
                            println!("   Full content: {:#?}", other);
                        }
                    }
                },
                Err(e) => {
                    println!("\n❌ ERROR #{} at {:?}: {}", msg_count, elapsed, e);
                }
            }
        }
        
        // Stream ended - check if we should reconnect
        if ws.needs_reconnect() {
            reconnect_count += 1;
            if reconnect_count > MAX_RECONNECTS {
                println!("\n🚫 Max reconnection attempts ({}) exceeded, giving up", MAX_RECONNECTS);
                break 'reconnect;
            }
            
            println!("\n⚠️  Connection lost! Attempting reconnect ({}/{})...", reconnect_count, MAX_RECONNECTS);
            
            match ws.reconnect().await {
                Ok(()) => {
                    println!("✅ Reconnected successfully! Resuming message processing...\n");
                    continue 'reconnect;
                }
                Err(e) => {
                    println!("❌ Reconnect failed: {}", e);
                    // reconnect() already has its own retry logic with backoff
                    // If it returns Err, all retries failed
                    break 'reconnect;
                }
            }
        } else {
            // Clean shutdown (no reconnect needed)
            break 'reconnect;
        }
    }
    
    println!("\n🔌 WebSocket disconnected after {} messages ({} trades, {} reconnects)", 
             msg_count, trade_count, reconnect_count);
    Ok(())
}

/// Print a trade message with FULL details (no truncation)
fn print_trade_verbose(trade: &UserTradeMessage, count: u64, elapsed: Duration) {
    println!();
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║ 💰 TRADE #{:<58}║", count);
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║ Elapsed:        {:>50} ║", format!("{:?}", elapsed));
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║ CORE FIELDS                                                      ║");
    println!("║ ────────────────────────────────────────────────────────────────╢");
    println!("║ ID:             {:>50} ║", trade.id);
    println!("║ Status:         {:>50} ║", trade.status);
    println!("║ Side:           {:>50} ║", trade.side);
    println!("║ Price:          {:>50} ║", trade.price);
    println!("║ Size:           {:>50} ║", trade.size);
    println!("║ Outcome:        {:>50} ║", trade.outcome);
    
    // Determine if we are TAKER or MAKER for this trade
    // Use trader_side field from Polymarket API if available
    let role = match trade.trader_side.as_deref() {
        Some("TAKER") => "🟢 TAKER (we initiated)",
        Some("MAKER") => "🔵 MAKER (we provided liquidity)",
        _ => {
            // Fallback: compare owner with trade_owner
            if trade.trade_owner.as_ref() == Some(&trade.owner) {
                "🟢 TAKER (we initiated)"
            } else {
                "🔵 MAKER (we provided liquidity)"
            }
        }
    };
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║ ROLE:           {:>50} ║", role);
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║ IDENTIFIERS                                                      ║");
    println!("║ ────────────────────────────────────────────────────────────────╢");
    println!("║ Asset ID:       {} ║", &trade.asset_id[..trade.asset_id.len().min(50)]);
    println!("║ Market:         {:>50} ║", &trade.market[..trade.market.len().min(50)]);
    println!("║ Owner:          {:>50} ║", trade.owner);
    
    // Taker order ID detection
    if let Some(ref taker_id) = trade.taker_order_id {
        println!("║ Taker Order ID: {:>50} ║", &taker_id[..taker_id.len().min(50)]);
    } else {
        println!("║ Taker Order ID: {:>50} ║", "(none)");
    }
    
    // Trade owner detection (who initiated?)
    if let Some(ref trade_owner) = trade.trade_owner {
        println!("║ Trade Owner:    {:>50} ║", trade_owner);
    }
    
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║ MAKER ORDERS ({} total)                                          ║", trade.maker_orders.len());
    println!("║ ────────────────────────────────────────────────────────────────╢");
    
    for (i, maker) in trade.maker_orders.iter().enumerate() {
        println!("║ [{}] Order ID:   {:>48} ║", i, &maker.order_id[..maker.order_id.len().min(48)]);
        println!("║     Owner:      {:>48} ║", maker.owner);
        println!("║     Price:      {:>48} ║", maker.price);
        println!("║     Matched:    {:>48} ║", maker.matched_amount);
        if i < trade.maker_orders.len() - 1 {
            println!("║     ─────────────────────────────────────────────────────────── ║");
        }
    }
    
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║ TIMING                                                           ║");
    println!("║ ────────────────────────────────────────────────────────────────╢");
    println!("║ Timestamp:      {:>50} ║", trade.timestamp.as_deref().unwrap_or("(none)"));
    println!("║ Matchtime:      {:>50} ║", trade.matchtime.as_deref().unwrap_or("(none)"));
    println!("║ Last Update:    {:>50} ║", trade.last_update.as_deref().unwrap_or("(none)"));
    println!("╚══════════════════════════════════════════════════════════════════╝");
    
    // Also print raw JSON for comparison
    println!("\n📋 RAW JSON (for debugging):");
    println!("{}", serde_json::to_string_pretty(&trade).unwrap_or_else(|_| format!("{:?}", trade)));
}
