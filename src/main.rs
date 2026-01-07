mod btc;
mod bucket;
mod config;
mod db;
mod display;
mod gamma;
mod orderbook;
mod websocket;

use anyhow::{Context, Result};
use crossterm::{cursor, execute, terminal};
use serde_json::Value;
use std::env;
use std::io::stdout;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};

use btc::{BinanceKlineClient, BinanceWsClient, BtcTicker, BINANCE_KLINE_WS_URL, BINANCE_WS_URL};
use bucket::BucketTime;
use config::AppConfig;
use db::DbLogger;
use display::{BtcDisplay, OrderbookDisplay};
use gamma::GammaClient;
use orderbook::OrderbookState;
use websocket::WebSocketClient;
use websocket::{UserEvent, UserOrderMessage, UserTradeMessage, UserWebSocketClient};

// Polymarket CLOB websocket base URL (market channel is /ws/market).
const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com";
const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";
const WATCHER_USER_ADDRESS: &str = "0xB6Ca6c5a5092A0f91babDC92FceFC200D031C084";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RunMode {
    Polymarket,
    Btc,
    Logging,
    Watcher,
}

#[derive(Clone, Debug)]
struct LoggingCounters {
    polymarket: Arc<AtomicU64>,
    btc: Arc<AtomicU64>,
}

impl LoggingCounters {
    fn new() -> Self {
        Self {
            polymarket: Arc::new(AtomicU64::new(0)),
            btc: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[derive(Clone, Debug)]
struct WatcherCounters {
    orderbook: Arc<AtomicU64>,
    user_events: Arc<AtomicU64>,
    btc_klines: Arc<AtomicU64>,
}

impl WatcherCounters {
    fn new() -> Self {
        Self {
            orderbook: Arc::new(AtomicU64::new(0)),
            user_events: Arc::new(AtomicU64::new(0)),
            btc_klines: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from a .env file if present (non-fatal on missing file).
    let _ = dotenvy::dotenv();
    let config = AppConfig::from_env()?;
    let mode = parse_mode_from_args();
    match mode {
        RunMode::Polymarket => run_polymarket(config).await,
        RunMode::Btc => run_btc(config).await,
        RunMode::Logging => run_logging(config).await,
        RunMode::Watcher => run_watcher(config).await,
    }
}

fn parse_mode_from_args() -> RunMode {
    env::args()
        .nth(1)
        .map(|s| s.to_lowercase())
        .map(|s| match s.as_str() {
            "btc" | "binance" => RunMode::Btc,
            "log" | "logging" | "db" => RunMode::Logging,
            "watch" | "watcher" => RunMode::Watcher,
            _ => RunMode::Polymarket,
        })
        .unwrap_or(RunMode::Polymarket)
}

async fn run_polymarket(config: AppConfig) -> Result<()> {
    let orderbook_state = Arc::new(RwLock::new(OrderbookState::new()));
    let status = Arc::new(RwLock::new(String::new()));
    let db_logger = DbLogger::new(&config.database).await?;
    *status.write().await = "Starting…".to_string();
    let gamma_client = Arc::new(GammaClient::new(GAMMA_API_BASE.to_string()));

    // Initial bucket calculation
    let current_bucket = BucketTime::current();
    *status.write().await = format!("Current bucket slug: {}", current_bucket.slug());

    // Resolve initial market
    let (market_info, used_slug) =
        match resolve_market_with_fallback(&gamma_client, &current_bucket).await {
            Ok((info, slug)) => {
                *status.write().await = format!(
                    "Resolved via slug: {} -> market {} ({} tokens)",
                    slug,
                    info.market_id,
                    info.token_ids.len()
                );
                {
                    let mut ob = orderbook_state.write().await;
                    ob.set_binary_labels(&info.token_ids);
                }
                (info, slug)
            }
            Err(e) => {
                eprintln!("❌ Failed to resolve market: {}", e);
                return Err(e);
            }
        };
    // Seed slug cache for DB logger to avoid re-fetch churn.
    db_logger.cache_market_slug(&market_info.market_id, &used_slug);

    // Create WebSocket client
    let ws_client = Arc::new(WebSocketClient::new(WS_URL.to_string()));

    // Connect + initial subscription (market channel)
    if let Err(e) = ws_client.connect_market(&market_info.token_ids).await {
        eprintln!("❌ Failed to subscribe: {}", e);
        return Err(e);
    }

    // Enter an alternate screen so the UI refreshes in place instead of appending into scrollback.
    // (PowerShell + Cursor terminals handle this well.)
    {
        let mut out = stdout();
        let _ = execute!(
            out,
            terminal::EnterAlternateScreen,
            cursor::Hide,
            terminal::Clear(terminal::ClearType::All),
            cursor::MoveTo(0, 0),
        );
    }

    // Start orderbook message handler
    let orderbook_state_clone = orderbook_state.clone();
    let ws_client_clone = ws_client.clone();

    tokio::spawn(async move {
        if let Err(e) =
            handle_websocket_messages(ws_client_clone, orderbook_state_clone, db_logger, None).await
        {
            eprintln!("❌ WebSocket handler error: {}", e);
        }
    });

    // Start display updater
    let orderbook_state_display = orderbook_state.clone();
    let status_display = status.clone();
    tokio::spawn(async move {
        let mut display = OrderbookDisplay::new();
        let mut interval = interval(Duration::from_millis(200));
        loop {
            interval.tick().await;
            display
                .update(&orderbook_state_display, &status_display)
                .await;
        }
    });

    // Monitor for bucket rollovers in a task so main can wait for Ctrl+C and restore the terminal.
    let gamma_client_roll = gamma_client.clone();
    let ws_client_roll = ws_client.clone();
    let orderbook_state_roll = orderbook_state.clone();
    let status_roll = status.clone();
    tokio::spawn(async move {
        let mut rollover_check = interval(Duration::from_secs(5));
        let mut current_market_info = market_info;
        let mut current_bucket_ts = current_bucket.timestamp();
        let mut current_slug = used_slug;

        loop {
            rollover_check.tick().await;

            let now_bucket = BucketTime::current();
            let now_ts = now_bucket.timestamp();

            if now_ts != current_bucket_ts {
                *status_roll.write().await = format!(
                    "Bucket rollover: {} -> {} (next slug: {})",
                    current_bucket_ts,
                    now_ts,
                    BucketTime::from_timestamp(now_ts).slug()
                );

                let next_bucket = BucketTime::from_timestamp(now_ts);
                match resolve_market_with_fallback(&gamma_client_roll, &next_bucket).await {
                    Ok((new_info, new_slug)) => {
                        *status_roll.write().await = format!(
                            "Resolved via slug: {} -> market {} ({} tokens)",
                            new_slug,
                            new_info.market_id,
                            new_info.token_ids.len()
                        );

                        if let Err(e) = ws_client_roll
                            .unsubscribe_assets(&current_market_info.token_ids)
                            .await
                        {
                            eprintln!("⚠️  Failed to unsubscribe old: {}", e);
                        }

                        if let Err(e) = ws_client_roll.subscribe_assets(&new_info.token_ids).await {
                            eprintln!("❌ Failed to subscribe new: {}", e);
                        } else {
                            current_market_info = new_info;
                            current_bucket_ts = now_ts;
                            current_slug = new_slug;
                            let mut ob_state = orderbook_state_roll.write().await;
                            ob_state.set_binary_labels(&current_market_info.token_ids);
                            ob_state.clear();
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "⚠️  Failed to resolve new market (prev slug was {}): {}",
                            current_slug, e
                        );
                    }
                }
            }
        }
    });

    // Wait for Ctrl+C, then restore the terminal state.
    let _ = tokio::signal::ctrl_c().await;
    {
        let mut out = stdout();
        let _ = execute!(out, cursor::Show, terminal::LeaveAlternateScreen);
    }

    Ok(())
}

async fn run_btc(config: AppConfig) -> Result<()> {
    let status = Arc::new(RwLock::new(String::from(
        "Starting Binance BTCUSDT ticker…",
    )));
    let ticker_state: Arc<RwLock<Option<BtcTicker>>> = Arc::new(RwLock::new(None));
    let ws_client = Arc::new(BinanceWsClient::new(BINANCE_WS_URL.to_string()));
    let db_logger = DbLogger::new(&config.database).await?;

    if let Err(e) = ws_client.connect().await {
        eprintln!("❌ Failed to connect to Binance websocket: {}", e);
        return Err(e);
    }
    *status.write().await = "Connected to Binance, waiting for first tick…".to_string();

    {
        let mut out = stdout();
        let _ = execute!(
            out,
            terminal::EnterAlternateScreen,
            cursor::Hide,
            terminal::Clear(terminal::ClearType::All),
            cursor::MoveTo(0, 0),
        );
    }

    // Stream handler
    let ws_client_clone = ws_client.clone();
    let ticker_state_clone = ticker_state.clone();
    let status_clone = status.clone();
    tokio::spawn(async move {
        loop {
            match ws_client_clone.next_ticker().await {
                Ok(Some(ticker)) => {
                    *ticker_state_clone.write().await = Some(ticker.clone());
                    db_logger.log_btc_ticker(&ticker);
                    *status_clone.write().await = format!("Last tick @ {}", ticker.last_price);
                }
                Ok(None) => {
                    if !ws_client_clone.is_connected().await {
                        if let Err(e) = ws_client_clone.reconnect().await {
                            eprintln!("❌ Binance reconnect failed: {}", e);
                        } else {
                            *status_clone.write().await =
                                "Reconnected to Binance, waiting for tick…".to_string();
                        }
                    }
                }
                Err(e) => {
                    eprintln!("❌ Binance WS error: {}", e);
                    *status_clone.write().await = "Binance WS error, retrying…".to_string();
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let _ = ws_client_clone.reconnect().await;
                }
            }
        }
    });

    // Display loop
    let ticker_state_display = ticker_state.clone();
    let status_display = status.clone();
    tokio::spawn(async move {
        let mut display = BtcDisplay::new();
        let mut tick = interval(Duration::from_millis(500));
        loop {
            tick.tick().await;
            display.update(&ticker_state_display, &status_display).await;
        }
    });

    // Wait for Ctrl+C, then restore the terminal state.
    let _ = tokio::signal::ctrl_c().await;
    {
        let mut out = stdout();
        let _ = execute!(out, cursor::Show, terminal::LeaveAlternateScreen);
    }

    Ok(())
}

async fn run_logging(config: AppConfig) -> Result<()> {
    let counters = Arc::new(LoggingCounters::new());
    let db_logger = DbLogger::new(&config.database).await?;

    // --- Polymarket setup (no UI) ---
    let orderbook_state = Arc::new(RwLock::new(OrderbookState::new()));
    let gamma_client = Arc::new(GammaClient::new(GAMMA_API_BASE.to_string()));
    let current_bucket = BucketTime::current();
    let (market_info, used_slug) =
        resolve_market_with_fallback(&gamma_client, &current_bucket).await?;
    {
        let mut ob = orderbook_state.write().await;
        ob.set_binary_labels(&market_info.token_ids);
    }
    // Seed slug cache for DB logger to avoid re-fetch churn.
    db_logger.cache_market_slug(&market_info.market_id, &used_slug);
    let ws_client = Arc::new(WebSocketClient::new(WS_URL.to_string()));
    ws_client.connect_market(&market_info.token_ids).await?;

    let poly_counter = counters.polymarket.clone();
    let orderbook_state_clone = orderbook_state.clone();
    let ws_client_clone = ws_client.clone();
    let db_clone = db_logger.clone();
    tokio::spawn(async move {
        if let Err(e) = handle_websocket_messages(
            ws_client_clone,
            orderbook_state_clone,
            db_clone,
            Some(poly_counter),
        )
        .await
        {
            eprintln!("❌ Polymarket handler error: {e}");
        }
    });

    // Bucket rollover (logging only, but ensure we stay on the correct market)
    let gamma_client_roll = gamma_client.clone();
    let ws_client_roll = ws_client.clone();
    let orderbook_state_roll = orderbook_state.clone();
    tokio::spawn(async move {
        let mut rollover_check = interval(Duration::from_secs(5));
        let mut current_market_info = market_info;
        let mut current_bucket_ts = current_bucket.timestamp();
        let mut current_slug = used_slug;

        loop {
            rollover_check.tick().await;
            let now_bucket = BucketTime::current();
            let now_ts = now_bucket.timestamp();

            if now_ts != current_bucket_ts {
                let next_bucket = BucketTime::from_timestamp(now_ts);
                match resolve_market_with_fallback(&gamma_client_roll, &next_bucket).await {
                    Ok((new_info, new_slug)) => {
                        if let Err(e) = ws_client_roll
                            .unsubscribe_assets(&current_market_info.token_ids)
                            .await
                        {
                            eprintln!("⚠️  Failed to unsubscribe old: {}", e);
                        }

                        if let Err(e) = ws_client_roll.subscribe_assets(&new_info.token_ids).await {
                            eprintln!("❌ Failed to subscribe new: {}", e);
                        } else {
                            current_market_info = new_info;
                            current_bucket_ts = now_ts;
                            current_slug = new_slug;
                            let mut ob_state = orderbook_state_roll.write().await;
                            ob_state.set_binary_labels(&current_market_info.token_ids);
                            ob_state.clear();
                            println!(
                                "✅ Rollover to bucket {} (slug {})",
                                current_bucket_ts, current_slug
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "⚠️  Failed to resolve new market (prev slug was {}): {}",
                            current_slug, e
                        );
                    }
                }
            }
        }
    });

    // --- BTC setup (no UI) ---
    let ws_btc = Arc::new(BinanceWsClient::new(BINANCE_WS_URL.to_string()));
    ws_btc.connect().await?;
    let btc_counter = counters.btc.clone();
    let ws_btc_clone = ws_btc.clone();
    let db_clone = db_logger.clone();
    tokio::spawn(async move {
        loop {
            match ws_btc_clone.next_ticker().await {
                Ok(Some(ticker)) => {
                    db_clone.log_btc_ticker(&ticker);
                    btc_counter.fetch_add(1, Ordering::Relaxed);
                }
                Ok(None) => {
                    if !ws_btc_clone.is_connected().await {
                        if let Err(e) = ws_btc_clone.reconnect().await {
                            eprintln!("❌ Binance reconnect failed: {}", e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("❌ Binance WS error: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let _ = ws_btc_clone.reconnect().await;
                }
            }
        }
    });

    // --- Status printer ---
    let counters_print = counters.clone();
    tokio::spawn(async move {
        let mut tick = interval(Duration::from_secs(5));
        loop {
            tick.tick().await;
            let poly = counters_print.polymarket.load(Ordering::Relaxed);
            let btc = counters_print.btc.load(Ordering::Relaxed);
            println!(
                "LOGGING MODE | polymarket events logged: {} | btc ticks logged: {}",
                poly, btc
            );
        }
    });

    println!("Logging mode running. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;
    println!("Stopping logging mode…");
    Ok(())
}

async fn run_watcher(config: AppConfig) -> Result<()> {
    let api_key = config
        .polymarket_api_key
        .clone()
        .context("POLYMARKET_API_KEY is required for watcher mode")?;
    let counters = Arc::new(WatcherCounters::new());
    let db_logger = DbLogger::new(&config.database).await?;

    // --- Polymarket orderbook setup (no UI) ---
    let orderbook_state = Arc::new(RwLock::new(OrderbookState::new()));
    let gamma_client = Arc::new(GammaClient::new(GAMMA_API_BASE.to_string()));
    let current_bucket = BucketTime::current();
    let (market_info, used_slug) =
        resolve_market_with_fallback(&gamma_client, &current_bucket).await?;
    {
        let mut ob = orderbook_state.write().await;
        ob.set_binary_labels(&market_info.token_ids);
    }
    db_logger.cache_market_slug(&market_info.market_id, &used_slug);

    let ws_client = Arc::new(WebSocketClient::new(WS_URL.to_string()));
    ws_client.connect_market(&market_info.token_ids).await?;
    let orderbook_counter = counters.orderbook.clone();
    let orderbook_state_clone = orderbook_state.clone();
    let db_clone = db_logger.clone();
    let ws_client_clone = ws_client.clone();
    tokio::spawn(async move {
        if let Err(e) = handle_watcher_orderbook_messages(
            ws_client_clone,
            orderbook_state_clone,
            db_clone,
            orderbook_counter,
        )
        .await
        {
            eprintln!("❌ Watcher orderbook handler error: {e}");
        }
    });

    // Bucket rollover (watcher mode)
    let gamma_client_roll = gamma_client.clone();
    let ws_client_roll = ws_client.clone();
    let orderbook_state_roll = orderbook_state.clone();
    tokio::spawn(async move {
        let mut rollover_check = interval(Duration::from_secs(5));
        let mut current_market_info = market_info;
        let mut current_bucket_ts = current_bucket.timestamp();
        let mut current_slug = used_slug;

        loop {
            rollover_check.tick().await;
            let now_bucket = BucketTime::current();
            let now_ts = now_bucket.timestamp();

            if now_ts != current_bucket_ts {
                let next_bucket = BucketTime::from_timestamp(now_ts);
                match resolve_market_with_fallback(&gamma_client_roll, &next_bucket).await {
                    Ok((new_info, new_slug)) => {
                        if let Err(e) = ws_client_roll
                            .unsubscribe_assets(&current_market_info.token_ids)
                            .await
                        {
                            eprintln!("⚠️  Failed to unsubscribe old: {}", e);
                        }

                        if let Err(e) = ws_client_roll.subscribe_assets(&new_info.token_ids).await {
                            eprintln!("❌ Failed to subscribe new: {}", e);
                        } else {
                            current_market_info = new_info;
                            current_bucket_ts = now_ts;
                            current_slug = new_slug;
                            let mut ob_state = orderbook_state_roll.write().await;
                            ob_state.set_binary_labels(&current_market_info.token_ids);
                            ob_state.clear();
                            println!(
                                "✅ Watcher rollover to bucket {} (slug {})",
                                current_bucket_ts, current_slug
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "⚠️  Failed to resolve new market (prev slug was {}): {}",
                            current_slug, e
                        );
                    }
                }
            }
        }
    });

    // --- User channel setup ---
    let user_ws = Arc::new(UserWebSocketClient::new(WS_URL.to_string(), api_key));
    user_ws.connect().await?;
    let user_counter = counters.user_events.clone();
    let db_clone = db_logger.clone();
    let user_ws_clone = user_ws.clone();
    tokio::spawn(async move {
        loop {
            match user_ws_clone.receive_message().await {
                Ok(Some(message)) => {
                    if let Some(event) = websocket::parse_user_message(&message) {
                        if process_user_event(WATCHER_USER_ADDRESS, event, &db_clone, &user_counter)
                        {
                            println!("✅ User event stored for {WATCHER_USER_ADDRESS}");
                        }
                    }
                }
                Ok(None) => {
                    if let Err(e) = user_ws_clone.reconnect().await {
                        eprintln!("❌ User channel reconnect failed: {e}");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
                Err(e) => {
                    eprintln!("❌ User channel error: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let _ = user_ws_clone.reconnect().await;
                }
            }
        }
    });

    // --- BTC kline setup ---
    let ws_kline = Arc::new(BinanceKlineClient::new(BINANCE_KLINE_WS_URL.to_string()));
    ws_kline.connect().await?;
    let kline_counter = counters.btc_klines.clone();
    let db_clone = db_logger.clone();
    let ws_kline_clone = ws_kline.clone();
    tokio::spawn(async move {
        loop {
            match ws_kline_clone.next_kline().await {
                Ok(Some(kline)) => {
                    db_clone.log_btc_kline(&kline);
                    kline_counter.fetch_add(1, Ordering::Relaxed);
                    println!(
                        "BTC KLINE 1m | o={} h={} l={} c={} v={} (final={})",
                        kline.open_price,
                        kline.high_price,
                        kline.low_price,
                        kline.close_price,
                        kline.volume,
                        kline.is_final
                    );
                }
                Ok(None) => {
                    if !ws_kline_clone.is_connected().await {
                        if let Err(e) = ws_kline_clone.reconnect().await {
                            eprintln!("❌ Binance kline reconnect failed: {e}");
                        }
                    }
                }
                Err(e) => {
                    eprintln!("❌ Binance kline WS error: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let _ = ws_kline_clone.reconnect().await;
                }
            }
        }
    });

    // --- Status printer ---
    let counters_print = counters.clone();
    tokio::spawn(async move {
        let mut tick = interval(Duration::from_secs(5));
        loop {
            tick.tick().await;
            let orderbook = counters_print.orderbook.load(Ordering::Relaxed);
            let user = counters_print.user_events.load(Ordering::Relaxed);
            let klines = counters_print.btc_klines.load(Ordering::Relaxed);
            println!(
                "WATCHER MODE | orderbook events: {} | user events: {} | btc klines: {}",
                orderbook, user, klines
            );
        }
    });

    println!(
        "Watcher mode running for address {}. Press Ctrl+C to stop.",
        WATCHER_USER_ADDRESS
    );
    tokio::signal::ctrl_c().await?;
    println!("Stopping watcher mode…");
    Ok(())
}

async fn resolve_market_with_fallback(
    gamma_client: &GammaClient,
    bucket: &BucketTime,
) -> Result<(gamma::MarketInfo, String)> {
    // Try current, previous, next buckets
    let slugs = vec![
        bucket.slug(),
        BucketTime::from_timestamp(bucket.timestamp() - 900).slug(),
        BucketTime::from_timestamp(bucket.timestamp() + 900).slug(),
    ];

    for slug in slugs.clone() {
        match gamma_client.get_event_by_slug(&slug).await {
            Ok(event) => {
                if let Some(market) = find_valid_market(&event) {
                    return Ok((market, slug));
                }
            }
            Err(_e) => {
                // Continue to next slug
                continue;
            }
        }
    }

    anyhow::bail!(
        "Failed to resolve market for any bucket variant. Tried slugs: {:?}",
        slugs
    )
}

fn find_valid_market(event: &gamma::Event) -> Option<gamma::MarketInfo> {
    event
        .markets
        .iter()
        .find(|m| {
            m.enable_order_book.unwrap_or(false)
                && m.accepting_orders.unwrap_or(false)
                && m.active.unwrap_or(false)
        })
        .and_then(|m| {
            parse_token_ids(&m.clob_token_ids).map(|ids| gamma::MarketInfo {
                market_id: m.id.clone(),
                token_ids: ids,
            })
        })
}

fn parse_token_ids(clob_token_ids: &str) -> Option<Vec<String>> {
    // Try parsing as JSON array first
    if let Ok(ids) = serde_json::from_str::<Vec<String>>(clob_token_ids) {
        if ids.len() == 2 {
            return Some(ids);
        }
    }

    // Try parsing as comma-separated string
    let ids: Vec<String> = clob_token_ids
        .split(',')
        .map(|s| {
            s.trim()
                .trim_matches('"')
                .trim_matches('[')
                .trim_matches(']')
                .to_string()
        })
        .filter(|s| !s.is_empty())
        .collect();

    if ids.len() == 2 {
        Some(ids)
    } else {
        None
    }
}

fn normalize_timestamp_ms(ts: i64) -> i64 {
    if ts < 1_000_000_000_000 {
        ts.saturating_mul(1000)
    } else {
        ts
    }
}

fn current_millis_i64() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn parse_timestamp_ms(raw: &str) -> i64 {
    raw.parse::<i64>().map(normalize_timestamp_ms).unwrap_or(0)
}

fn normalize_outcome_label(label: &str) -> Option<&'static str> {
    match label.to_ascii_lowercase().as_str() {
        "yes" | "up" => Some("YES"),
        "no" | "down" => Some("NO"),
        _ => None,
    }
}

fn matches_address(address: &str, candidate: &str) -> bool {
    address.eq_ignore_ascii_case(candidate)
}

fn trade_matches_user(address: &str, trade: &UserTradeMessage) -> bool {
    matches_address(address, &trade.owner)
        || matches_address(address, &trade.trade_owner)
        || trade
            .maker_orders
            .iter()
            .any(|order| matches_address(address, &order.owner))
}

fn order_matches_user(address: &str, order: &UserOrderMessage) -> bool {
    matches_address(address, &order.owner) || matches_address(address, &order.order_owner)
}

fn format_price_level(price: Option<f64>, qty: Option<f64>) -> String {
    match (price, qty) {
        (Some(p), Some(q)) => format!("{p} @ {q}"),
        _ => "N/A".to_string(),
    }
}

fn process_user_event(
    address: &str,
    event: UserEvent,
    db_logger: &DbLogger,
    counter: &Arc<AtomicU64>,
) -> bool {
    let received_at_ms = current_millis_i64();
    match event {
        UserEvent::Trade(trade) => {
            if !trade_matches_user(address, &trade) {
                return false;
            }
            let maker_orders = serde_json::to_value(&trade.maker_orders).unwrap_or(Value::Null);
            db_logger.log_user_trade(
                address,
                &trade.id,
                &trade.asset_id,
                &trade.market,
                &trade.outcome,
                &trade.side,
                trade.price.parse::<f64>().ok(),
                trade.size.parse::<f64>().ok(),
                &trade.status,
                &trade.taker_order_id,
                parse_timestamp_ms(&trade.matchtime),
                parse_timestamp_ms(&trade.last_update),
                parse_timestamp_ms(&trade.timestamp),
                &trade.owner,
                &trade.trade_owner,
                maker_orders,
                received_at_ms,
            );
            println!(
                "USER TRADE | id={} market={} outcome={} side={} price={} size={} status={}",
                trade.id,
                trade.market,
                trade.outcome,
                trade.side,
                trade.price,
                trade.size,
                trade.status
            );
            counter.fetch_add(1, Ordering::Relaxed);
            true
        }
        UserEvent::Order(order) => {
            if !order_matches_user(address, &order) {
                return false;
            }
            let associate_trades =
                serde_json::to_value(&order.associate_trades).unwrap_or(Value::Null);
            db_logger.log_user_order(
                address,
                &order.id,
                &order.asset_id,
                &order.market,
                &order.outcome,
                &order.side,
                order.price.parse::<f64>().ok(),
                order.original_size.parse::<f64>().ok(),
                order.size_matched.parse::<f64>().ok(),
                &order.order_owner,
                &order.owner,
                &order.order_type,
                parse_timestamp_ms(&order.timestamp),
                associate_trades,
                received_at_ms,
            );
            println!(
                "USER ORDER | id={} market={} outcome={} side={} price={} matched={}/{} type={}",
                order.id,
                order.market,
                order.outcome,
                order.side,
                order.price,
                order.size_matched,
                order.original_size,
                order.order_type
            );
            counter.fetch_add(1, Ordering::Relaxed);
            true
        }
    }
}

async fn handle_watcher_orderbook_messages(
    ws_client: Arc<WebSocketClient>,
    orderbook_state: Arc<RwLock<OrderbookState>>,
    db_logger: DbLogger,
    counter: Arc<AtomicU64>,
) -> Result<()> {
    loop {
        match ws_client.receive_message().await {
            Ok(Some(message)) => {
                if let Some(orderbook) = websocket::parse_orderbook_message(&message) {
                    let receive_time = current_millis_i64();
                    let mut ob = orderbook;
                    ob.received_at_ms = receive_time;
                    let asset_label = orderbook_state
                        .read()
                        .await
                        .label_for(&ob.asset_id)
                        .map(|s| s.to_string());

                    db_logger.log_polymarket_orderbook(&ob, asset_label.as_deref());
                    db_logger.log_polymarket_best_price(&ob, asset_label.as_deref());

                    let best_bid = ob
                        .bids
                        .last()
                        .and_then(|l| l.price.parse::<f64>().ok().zip(l.size.parse::<f64>().ok()));
                    let best_ask = ob
                        .asks
                        .last()
                        .and_then(|l| l.price.parse::<f64>().ok().zip(l.size.parse::<f64>().ok()));
                    let (best_bid_price, best_bid_qty) = best_bid
                        .map(|(price, qty)| (Some(price), Some(qty)))
                        .unwrap_or((None, None));
                    let (best_ask_price, best_ask_qty) = best_ask
                        .map(|(price, qty)| (Some(price), Some(qty)))
                        .unwrap_or((None, None));

                    if let Some(label) = asset_label.as_deref().and_then(normalize_outcome_label) {
                        println!(
                            "ORDERBOOK BEST {} | bid={} ask={} market={}",
                            label,
                            format_price_level(best_bid_price, best_bid_qty),
                            format_price_level(best_ask_price, best_ask_qty),
                            ob.market
                        );
                    }

                    orderbook_state.write().await.update(ob);
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok(None) => {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if let Err(e) = ws_client.reconnect().await {
                    eprintln!("❌ Reconnect failed: {}", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
            Err(e) => {
                eprintln!("❌ WebSocket error: {}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
                if ws_client.reconnect().await.is_err() {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}

async fn handle_websocket_messages(
    ws_client: Arc<WebSocketClient>,
    orderbook_state: Arc<RwLock<OrderbookState>>,
    db_logger: DbLogger,
    event_counter: Option<Arc<AtomicU64>>,
) -> Result<()> {
    loop {
        match ws_client.receive_message().await {
            Ok(Some(message)) => {
                if let Some(orderbook) = websocket::parse_orderbook_message(&message) {
                    let receive_time = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64;
                    let mut ob = orderbook;
                    ob.received_at_ms = receive_time;
                    db_logger.log_polymarket_orderbook(
                        &ob,
                        orderbook_state.read().await.label_for(&ob.asset_id),
                    );
                    orderbook_state.write().await.update(ob);
                    if let Some(counter) = &event_counter {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Ok(None) => {
                // Connection closed, attempt reconnect
                tokio::time::sleep(Duration::from_secs(1)).await;
                if let Err(e) = ws_client.reconnect().await {
                    eprintln!("❌ Reconnect failed: {}", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
            Err(e) => {
                eprintln!("❌ WebSocket error: {}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
                // Try to reconnect
                if ws_client.reconnect().await.is_err() {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}
