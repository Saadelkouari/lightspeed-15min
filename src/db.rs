use crate::btc::{BtcKline, BtcMarkPrice, BtcTicker};
use crate::config::DatabaseConfig;
use crate::orderbook::Orderbook;
use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{self, Value};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_postgres::{Client, NoTls};
use ureq;

#[derive(Clone, Debug)]
pub struct DbLogger {
    cfg: DatabaseConfig,
    tx: mpsc::UnboundedSender<DbEvent>,
}

#[derive(Debug)]
enum DbEvent {
    Polymarket(PolymarketEvent),
    PolymarketBestPrice(PolymarketBestPriceEvent),
    UserTrade(UserTradeEvent),
    UserOrder(UserOrderEvent),
    Btc(BtcEvent),
    BtcKline(BtcKlineEvent),
    BtcMarkPrice(BtcMarkPriceEvent),
    CacheSlug { market_id: String, slug: String },
}

#[derive(Debug)]
struct PolymarketEvent {
    asset_label: String,
    side: Side,
    asset_id: String,
    market_id: String,
    best_bid_price: Option<f64>,
    best_bid_qty: Option<f64>,
    best_ask_price: Option<f64>,
    best_ask_qty: Option<f64>,
    event_ts_ms: i64,
    received_at_ms: i64,
}

#[derive(Debug)]
struct PolymarketBestPriceEvent {
    outcome: Outcome,
    asset_id: String,
    market_id: String,
    best_bid_price: Option<f64>,
    best_bid_qty: Option<f64>,
    best_ask_price: Option<f64>,
    best_ask_qty: Option<f64>,
    event_ts_ms: i64,
    received_at_ms: i64,
}

#[derive(Debug)]
struct BtcEvent {
    price: f64,
    volume: f64,
    event_ts_ms: i64,
    received_at_ms: i64,
}

#[derive(Debug)]
struct UserTradeEvent {
    user_address: String,
    trade_id: String,
    asset_id: String,
    market: String,
    outcome: String,
    side: String,
    price: Option<f64>,
    size: Option<f64>,
    status: String,
    taker_order_id: String,
    matchtime_ms: i64,
    last_update_ms: i64,
    timestamp_ms: i64,
    owner: String,
    trade_owner: String,
    maker_orders: Value,
    received_at_ms: i64,
}

#[derive(Debug)]
struct UserOrderEvent {
    user_address: String,
    order_id: String,
    asset_id: String,
    market: String,
    outcome: String,
    side: String,
    price: Option<f64>,
    original_size: Option<f64>,
    size_matched: Option<f64>,
    order_owner: String,
    owner: String,
    order_type: String,
    timestamp_ms: i64,
    associate_trades: Value,
    received_at_ms: i64,
}

#[derive(Debug)]
struct BtcKlineEvent {
    symbol: String,
    interval: String,
    open_time: u64,
    close_time: u64,
    open_price: f64,
    close_price: f64,
    high_price: f64,
    low_price: f64,
    base_volume: f64,
    quote_volume: f64,
    number_of_trades: u64,
    is_closed: bool,
    taker_buy_base_volume: f64,
    taker_buy_quote_volume: f64,
    event_ts_ms: i64,
    received_at_ms: i64,
}

#[derive(Debug)]
struct BtcMarkPriceEvent {
    symbol: String,
    mark_price: f64,
    index_price: f64,
    estimated_settle_price: f64,
    funding_rate: f64,
    next_funding_time: u64,
    event_ts_ms: i64,
    received_at_ms: i64,
}

#[derive(Debug, Clone)]
enum Side {
    Up,
    Down,
}

#[derive(Debug, Clone)]
enum Outcome {
    Yes,
    No,
}

impl Outcome {
    fn as_str(&self) -> &'static str {
        match self {
            Outcome::Yes => "YES",
            Outcome::No => "NO",
        }
    }

    fn from_label(label: &str) -> Option<Self> {
        match label.to_ascii_lowercase().as_str() {
            "yes" | "up" => Some(Outcome::Yes),
            "no" | "down" => Some(Outcome::No),
            _ => None,
        }
    }
}

impl Side {
    fn as_str(&self) -> &'static str {
        match self {
            Side::Up => "UP",
            Side::Down => "DOWN",
        }
    }

    fn from_label(label: &str) -> Option<Self> {
        match label.to_ascii_lowercase().as_str() {
            "up" => Some(Side::Up),
            "down" => Some(Side::Down),
            _ => None,
        }
    }
}

impl DbLogger {
    pub async fn new(cfg: &DatabaseConfig) -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();

        let logger = Self {
            cfg: cfg.clone(),
            tx,
        };

        if cfg.logging_enabled {
            let url = cfg
                .url
                .clone()
                .context("DATABASE_URL is required when DB logging is enabled")?;
            let auto_create = cfg.auto_create_schema;
            tokio::spawn(async move {
                if let Err(e) = run_worker(url, auto_create, rx).await {
                    eprintln!("❌ DB logger terminated: {e}");
                }
            });
        }

        Ok(logger)
    }

    pub fn log_polymarket_orderbook(&self, ob: &Orderbook, asset_label: Option<&str>) {
        if !self.cfg.logging_enabled {
            return;
        }

        let side = match asset_label.and_then(Side::from_label) {
            Some(s) => s,
            None => {
                eprintln!(
                    "⚠️  Skipping polymarket log: missing/unknown side for asset_id={}",
                    ob.asset_id
                );
                return;
            }
        };

        let best_bid = ob
            .bids
            .last()
            .and_then(|l| l.price.parse::<f64>().ok().zip(l.size.parse::<f64>().ok()));
        let best_ask = ob
            .asks
            .last()
            .and_then(|l| l.price.parse::<f64>().ok().zip(l.size.parse::<f64>().ok()));

        let event = PolymarketEvent {
            asset_label: asset_label.unwrap_or("UNKNOWN").to_string(),
            side,
            asset_id: ob.asset_id.clone(),
            market_id: ob.market.clone(), // websocket field is the market id
            best_bid_price: best_bid.map(|(p, _)| p),
            best_bid_qty: best_bid.map(|(_, q)| q),
            best_ask_price: best_ask.map(|(p, _)| p),
            best_ask_qty: best_ask.map(|(_, q)| q),
            event_ts_ms: normalize_timestamp_ms(ob.timestamp),
            received_at_ms: ob.received_at_ms,
        };

        if let Err(e) = self.tx.send(DbEvent::Polymarket(event)) {
            eprintln!("⚠️  Failed to enqueue polymarket event for DB logging: {e}");
        }
    }

    pub fn log_polymarket_best_price(&self, ob: &Orderbook, asset_label: Option<&str>) {
        if !self.cfg.logging_enabled {
            return;
        }

        let outcome = match asset_label.and_then(Outcome::from_label) {
            Some(outcome) => outcome,
            None => {
                eprintln!(
                    "⚠️  Skipping best price log: missing/unknown outcome for asset_id={}",
                    ob.asset_id
                );
                return;
            }
        };

        let best_bid = ob
            .bids
            .last()
            .and_then(|l| l.price.parse::<f64>().ok().zip(l.size.parse::<f64>().ok()));
        let best_ask = ob
            .asks
            .last()
            .and_then(|l| l.price.parse::<f64>().ok().zip(l.size.parse::<f64>().ok()));

        let event = PolymarketBestPriceEvent {
            outcome,
            asset_id: ob.asset_id.clone(),
            market_id: ob.market.clone(),
            best_bid_price: best_bid.map(|(p, _)| p),
            best_bid_qty: best_bid.map(|(_, q)| q),
            best_ask_price: best_ask.map(|(p, _)| p),
            best_ask_qty: best_ask.map(|(_, q)| q),
            event_ts_ms: normalize_timestamp_ms(ob.timestamp),
            received_at_ms: ob.received_at_ms,
        };

        if let Err(e) = self.tx.send(DbEvent::PolymarketBestPrice(event)) {
            eprintln!("⚠️  Failed to enqueue best price event for DB logging: {e}");
        }
    }

    pub fn log_user_trade(
        &self,
        user_address: &str,
        trade_id: &str,
        asset_id: &str,
        market: &str,
        outcome: &str,
        side: &str,
        price: Option<f64>,
        size: Option<f64>,
        status: &str,
        taker_order_id: &str,
        matchtime_ms: i64,
        last_update_ms: i64,
        timestamp_ms: i64,
        owner: &str,
        trade_owner: &str,
        maker_orders: Value,
        received_at_ms: i64,
    ) {
        if !self.cfg.logging_enabled {
            return;
        }

        let event = UserTradeEvent {
            user_address: user_address.to_string(),
            trade_id: trade_id.to_string(),
            asset_id: asset_id.to_string(),
            market: market.to_string(),
            outcome: outcome.to_string(),
            side: side.to_string(),
            price,
            size,
            status: status.to_string(),
            taker_order_id: taker_order_id.to_string(),
            matchtime_ms,
            last_update_ms,
            timestamp_ms,
            owner: owner.to_string(),
            trade_owner: trade_owner.to_string(),
            maker_orders,
            received_at_ms,
        };

        if let Err(e) = self.tx.send(DbEvent::UserTrade(event)) {
            eprintln!("⚠️  Failed to enqueue user trade for DB logging: {e}");
        }
    }

    pub fn log_user_order(
        &self,
        user_address: &str,
        order_id: &str,
        asset_id: &str,
        market: &str,
        outcome: &str,
        side: &str,
        price: Option<f64>,
        original_size: Option<f64>,
        size_matched: Option<f64>,
        order_owner: &str,
        owner: &str,
        order_type: &str,
        timestamp_ms: i64,
        associate_trades: Value,
        received_at_ms: i64,
    ) {
        if !self.cfg.logging_enabled {
            return;
        }

        let event = UserOrderEvent {
            user_address: user_address.to_string(),
            order_id: order_id.to_string(),
            asset_id: asset_id.to_string(),
            market: market.to_string(),
            outcome: outcome.to_string(),
            side: side.to_string(),
            price,
            original_size,
            size_matched,
            order_owner: order_owner.to_string(),
            owner: owner.to_string(),
            order_type: order_type.to_string(),
            timestamp_ms,
            associate_trades,
            received_at_ms,
        };

        if let Err(e) = self.tx.send(DbEvent::UserOrder(event)) {
            eprintln!("⚠️  Failed to enqueue user order for DB logging: {e}");
        }
    }

    /// Seed the market slug cache so the worker doesn't have to re-fetch.
    pub fn cache_market_slug(&self, market_id: &str, slug: &str) {
        if !self.cfg.logging_enabled {
            return;
        }
        if let Err(e) = self.tx.send(DbEvent::CacheSlug {
            market_id: market_id.to_string(),
            slug: slug.to_string(),
        }) {
            eprintln!(
                "⚠️  Failed to enqueue cache seed for market_id={} slug={}: {e}",
                market_id, slug
            );
        }
    }

    pub fn log_btc_ticker(&self, tick: &BtcTicker) {
        if !self.cfg.logging_enabled {
            return;
        }

        let event = BtcEvent {
            price: tick.last_price,
            volume: tick.volume,
            event_ts_ms: normalize_timestamp_ms(tick.event_time as i64),
            received_at_ms: tick.received_at_ms as i64,
        };

        if let Err(e) = self.tx.send(DbEvent::Btc(event)) {
            eprintln!("⚠️  Failed to enqueue BTC tick for DB logging: {e}");
        }
    }

    pub fn log_btc_kline(&self, kline: &BtcKline) {
        if !self.cfg.logging_enabled {
            return;
        }

        let event = BtcKlineEvent {
            symbol: kline.symbol.clone(),
            interval: kline.interval.clone(),
            open_time: kline.open_time,
            close_time: kline.close_time,
            open_price: kline.open_price,
            close_price: kline.close_price,
            high_price: kline.high_price,
            low_price: kline.low_price,
            base_volume: kline.base_volume,
            quote_volume: kline.quote_volume,
            number_of_trades: kline.number_of_trades,
            is_closed: kline.is_closed,
            taker_buy_base_volume: kline.taker_buy_base_volume,
            taker_buy_quote_volume: kline.taker_buy_quote_volume,
            event_ts_ms: normalize_timestamp_ms(kline.event_time as i64),
            received_at_ms: kline.received_at_ms as i64,
        };

        if let Err(e) = self.tx.send(DbEvent::BtcKline(event)) {
            eprintln!("⚠️  Failed to enqueue BTC kline for DB logging: {e}");
        }
    }

    pub fn log_btc_mark_price(&self, mark_price: &BtcMarkPrice) {
        if !self.cfg.logging_enabled {
            return;
        }

        let event = BtcMarkPriceEvent {
            symbol: mark_price.symbol.clone(),
            mark_price: mark_price.mark_price,
            index_price: mark_price.index_price,
            estimated_settle_price: mark_price.estimated_settle_price,
            funding_rate: mark_price.funding_rate,
            next_funding_time: mark_price.next_funding_time,
            event_ts_ms: normalize_timestamp_ms(mark_price.event_time as i64),
            received_at_ms: mark_price.received_at_ms as i64,
        };

        if let Err(e) = self.tx.send(DbEvent::BtcMarkPrice(event)) {
            eprintln!("⚠️  Failed to enqueue BTC mark price for DB logging: {e}");
        }
    }
}

async fn run_worker(url: String, auto_create: bool, mut rx: mpsc::UnboundedReceiver<DbEvent>) -> Result<()> {
    let mut pending: Option<DbEvent> = None;
    let mut market_slug_cache: HashMap<String, Option<String>> = HashMap::new();

    loop {
        let (client, connection) = match tokio_postgres::connect(&url, NoTls).await {
            Ok(res) => res,
            Err(e) => {
                eprintln!("⚠️  DB connect failed, retrying in 2s: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        };

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("⚠️  DB connection task ended: {e}");
            }
        });

        if auto_create {
            if let Err(e) = ensure_schema(&client).await {
                eprintln!("⚠️  Failed to ensure DB schema (will retry): {e}");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        }

        let mut client = client;
        loop {
            let event = match pending.take() {
                Some(e) => e,
                None => match rx.recv().await {
                    Some(e) => e,
                    None => return Ok(()),
                },
            };

            let result = match &event {
                DbEvent::Polymarket(ev) => {
                    // Resolve slug once per market id and cache
                    let slug = resolve_market_slug(&ev.market_id, &mut market_slug_cache).await;
                    if slug.is_none() {
                        eprintln!(
                            "⚠️  No market_instance_id resolved; market_id={} asset_label={} asset_id={} ts_ms={} (storing NULL)",
                            ev.market_id, ev.asset_label, ev.asset_id, ev.event_ts_ms
                        );
                    }
                    insert_polymarket(&mut client, ev, slug.as_deref()).await
                }
                DbEvent::PolymarketBestPrice(ev) => {
                    insert_polymarket_best_price(&mut client, ev).await
                }
                DbEvent::UserTrade(ev) => insert_user_trade(&mut client, ev).await,
                DbEvent::UserOrder(ev) => insert_user_order(&mut client, ev).await,
                DbEvent::Btc(ev) => insert_btc(&mut client, ev).await,
                DbEvent::BtcKline(ev) => insert_btc_kline(&mut client, ev).await,
                DbEvent::BtcMarkPrice(ev) => insert_btc_mark_price(&mut client, ev).await,
                DbEvent::CacheSlug { market_id, slug } => {
                    market_slug_cache.insert(market_id.clone(), Some(slug.clone()));
                    eprintln!(
                        "ℹ️  Seeded slug cache; market_id={} slug={}",
                        market_id, slug
                    );
                    Ok(())
                }
            };

            if let Err(e) = result {
                eprintln!("⚠️  DB insert failed, will reconnect and retry: {e}");
                pending = Some(event);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                break;
            }
        }
    }
}

fn normalize_timestamp_ms(ts: i64) -> i64 {
    if ts < 1_000_000_000_000 {
        ts.saturating_mul(1000)
    } else {
        ts
    }
}

async fn ensure_schema(client: &Client) -> Result<()> {
    client
        .batch_execute(
            r#"
            CREATE TABLE IF NOT EXISTS polymarket_orderbook_events (
                id BIGSERIAL PRIMARY KEY,
                asset_label TEXT NOT NULL,
                side TEXT NOT NULL CHECK (side IN ('UP','DOWN')),
                asset_id TEXT NOT NULL,
                market TEXT NOT NULL,
                market_instance_id TEXT,
                best_bid_price DOUBLE PRECISION,
                best_bid_qty DOUBLE PRECISION,
                best_ask_price DOUBLE PRECISION,
                best_ask_qty DOUBLE PRECISION,
                event_timestamp_ms BIGINT NOT NULL,
                received_at_ms BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            ALTER TABLE polymarket_orderbook_events
            ADD COLUMN IF NOT EXISTS market_instance_id TEXT;
            ALTER TABLE polymarket_orderbook_events
            ADD COLUMN IF NOT EXISTS side TEXT;

            -- Postgres does not support IF NOT EXISTS for constraints; add defensively.
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname = 'polymarket_orderbook_events_side_check'
                ) THEN
                    ALTER TABLE polymarket_orderbook_events
                    ADD CONSTRAINT polymarket_orderbook_events_side_check CHECK (side IN ('UP','DOWN'));
                END IF;
            END$$;

            CREATE TABLE IF NOT EXISTS polymarket_best_prices (
                id BIGSERIAL PRIMARY KEY,
                outcome TEXT NOT NULL CHECK (outcome IN ('YES','NO')),
                asset_id TEXT NOT NULL,
                market TEXT NOT NULL,
                best_bid_price DOUBLE PRECISION,
                best_bid_qty DOUBLE PRECISION,
                best_ask_price DOUBLE PRECISION,
                best_ask_qty DOUBLE PRECISION,
                event_timestamp_ms BIGINT NOT NULL,
                received_at_ms BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS btc_ticks (
                id BIGSERIAL PRIMARY KEY,
                price DOUBLE PRECISION NOT NULL,
                volume DOUBLE PRECISION NOT NULL,
                event_timestamp_ms BIGINT NOT NULL,
                received_at_ms BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS polymarket_user_trades (
                id BIGSERIAL PRIMARY KEY,
                user_address TEXT NOT NULL,
                trade_id TEXT NOT NULL,
                asset_id TEXT NOT NULL,
                market TEXT NOT NULL,
                outcome TEXT NOT NULL,
                side TEXT NOT NULL,
                price DOUBLE PRECISION,
                size DOUBLE PRECISION,
                status TEXT NOT NULL,
                taker_order_id TEXT NOT NULL,
                matchtime_ms BIGINT NOT NULL,
                last_update_ms BIGINT NOT NULL,
                timestamp_ms BIGINT NOT NULL,
                owner TEXT NOT NULL,
                trade_owner TEXT NOT NULL,
                maker_orders JSONB NOT NULL,
                received_at_ms BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS polymarket_user_orders (
                id BIGSERIAL PRIMARY KEY,
                user_address TEXT NOT NULL,
                order_id TEXT NOT NULL,
                asset_id TEXT NOT NULL,
                market TEXT NOT NULL,
                outcome TEXT NOT NULL,
                side TEXT NOT NULL,
                price DOUBLE PRECISION,
                original_size DOUBLE PRECISION,
                size_matched DOUBLE PRECISION,
                order_owner TEXT NOT NULL,
                owner TEXT NOT NULL,
                order_type TEXT NOT NULL,
                timestamp_ms BIGINT NOT NULL,
                associate_trades JSONB NOT NULL,
                received_at_ms BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS btc_klines (
                id BIGSERIAL PRIMARY KEY,
                symbol TEXT NOT NULL,
                interval TEXT NOT NULL,
                open_time BIGINT NOT NULL,
                close_time BIGINT NOT NULL,
                open_price DOUBLE PRECISION NOT NULL,
                close_price DOUBLE PRECISION NOT NULL,
                high_price DOUBLE PRECISION NOT NULL,
                low_price DOUBLE PRECISION NOT NULL,
                base_volume DOUBLE PRECISION NOT NULL,
                quote_volume DOUBLE PRECISION NOT NULL,
                number_of_trades BIGINT NOT NULL,
                is_closed BOOLEAN NOT NULL,
                taker_buy_base_volume DOUBLE PRECISION NOT NULL,
                taker_buy_quote_volume DOUBLE PRECISION NOT NULL,
                event_timestamp_ms BIGINT NOT NULL,
                received_at_ms BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS btc_mark_prices (
                id BIGSERIAL PRIMARY KEY,
                symbol TEXT NOT NULL,
                mark_price DOUBLE PRECISION NOT NULL,
                index_price DOUBLE PRECISION NOT NULL,
                estimated_settle_price DOUBLE PRECISION NOT NULL,
                funding_rate DOUBLE PRECISION NOT NULL,
                next_funding_time BIGINT NOT NULL,
                event_timestamp_ms BIGINT NOT NULL,
                received_at_ms BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        "#,
        )
        .await?;
    Ok(())
}

async fn insert_polymarket(
    client: &mut Client,
    ev: &PolymarketEvent,
    market_slug: Option<&str>,
) -> Result<()> {
    client
        .execute(
            r#"
            INSERT INTO polymarket_orderbook_events (
                asset_label,
                side,
                asset_id,
                market,
                market_instance_id,
                best_bid_price,
                best_bid_qty,
                best_ask_price,
                best_ask_qty,
                event_timestamp_ms,
                received_at_ms
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        "#,
            &[
                &ev.asset_label,
                &ev.side.as_str(),
                &ev.asset_id,
                &ev.market_id,
                &market_slug,
                &ev.best_bid_price,
                &ev.best_bid_qty,
                &ev.best_ask_price,
                &ev.best_ask_qty,
                &ev.event_ts_ms,
                &ev.received_at_ms,
            ],
        )
        .await
        .map(|_| ())
        .map_err(|e| e.into())
}

async fn insert_polymarket_best_price(
    client: &mut Client,
    ev: &PolymarketBestPriceEvent,
) -> Result<()> {
    client
        .execute(
            r#"
            INSERT INTO polymarket_best_prices (
                outcome,
                asset_id,
                market,
                best_bid_price,
                best_bid_qty,
                best_ask_price,
                best_ask_qty,
                event_timestamp_ms,
                received_at_ms
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        "#,
            &[
                &ev.outcome.as_str(),
                &ev.asset_id,
                &ev.market_id,
                &ev.best_bid_price,
                &ev.best_bid_qty,
                &ev.best_ask_price,
                &ev.best_ask_qty,
                &ev.event_ts_ms,
                &ev.received_at_ms,
            ],
        )
        .await
        .map(|_| ())
        .map_err(|e| e.into())
}

async fn insert_user_trade(client: &mut Client, ev: &UserTradeEvent) -> Result<()> {
    client
        .execute(
            r#"
            INSERT INTO polymarket_user_trades (
                user_address,
                trade_id,
                asset_id,
                market,
                outcome,
                side,
                price,
                size,
                status,
                taker_order_id,
                matchtime_ms,
                last_update_ms,
                timestamp_ms,
                owner,
                trade_owner,
                maker_orders,
                received_at_ms
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
        "#,
            &[
                &ev.user_address,
                &ev.trade_id,
                &ev.asset_id,
                &ev.market,
                &ev.outcome,
                &ev.side,
                &ev.price,
                &ev.size,
                &ev.status,
                &ev.taker_order_id,
                &ev.matchtime_ms,
                &ev.last_update_ms,
                &ev.timestamp_ms,
                &ev.owner,
                &ev.trade_owner,
                &ev.maker_orders,
                &ev.received_at_ms,
            ],
        )
        .await
        .map(|_| ())
        .map_err(|e| e.into())
}

async fn insert_user_order(client: &mut Client, ev: &UserOrderEvent) -> Result<()> {
    client
        .execute(
            r#"
            INSERT INTO polymarket_user_orders (
                user_address,
                order_id,
                asset_id,
                market,
                outcome,
                side,
                price,
                original_size,
                size_matched,
                order_owner,
                owner,
                order_type,
                timestamp_ms,
                associate_trades,
                received_at_ms
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
        "#,
            &[
                &ev.user_address,
                &ev.order_id,
                &ev.asset_id,
                &ev.market,
                &ev.outcome,
                &ev.side,
                &ev.price,
                &ev.original_size,
                &ev.size_matched,
                &ev.order_owner,
                &ev.owner,
                &ev.order_type,
                &ev.timestamp_ms,
                &ev.associate_trades,
                &ev.received_at_ms,
            ],
        )
        .await
        .map(|_| ())
        .map_err(|e| e.into())
}

const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";

#[derive(Deserialize)]
struct MarketDetail {
    slug: Option<String>,
}

async fn resolve_market_slug(
    market_id: &str,
    cache: &mut HashMap<String, Option<String>>,
) -> Option<String> {
    if let Some(cached) = cache.get(market_id) {
        match cached {
            Some(slug) => {
                return Some(slug.clone());
            }
            None => {
                eprintln!(
                    "ℹ️  Cached NULL market_instance_id; market_id={} (will re-fetch to inspect body)",
                    market_id
                );
                // fall through to re-fetch so we can log the latest body
            }
        }
    }

    let fetched = match fetch_market_slug(market_id).await {
        Ok(res) => res,
        Err(e) => {
            eprintln!(
                "⚠️  Failed to fetch market slug for market_id={}: {}; storing NULL",
                market_id, e
            );
            None
        }
    };
    cache.insert(market_id.to_string(), fetched.clone());
    fetched
}

async fn fetch_market_slug(market_id: &str) -> Result<Option<String>> {
    let market_id = market_id.to_string();
    let res = tokio::task::spawn_blocking(move || -> Result<Option<String>> {
        // Try multiple endpoints and id variants to reduce NULLs. Order matters.
        // The Gamma API appears to require the `0x` prefix; calling it without
        // the prefix can return 422 "id is invalid". To avoid noisy warnings,
        // only try the stripped variant when the original id lacks the prefix.
        let mut id_variants = Vec::new();
        if market_id.starts_with("0x") {
            id_variants.push(market_id.clone());
        } else {
            id_variants.push(format!("0x{market_id}"));
            id_variants.push(market_id.clone());
        }

        id_variants.sort();
        id_variants.dedup();

        let mut endpoints: Vec<String> = Vec::new();
        for id in &id_variants {
            // The Polymarket websocket "market id" corresponds to the condition_id.
            // Querying via condition_ids works even when /markets/{id} rejects.
            endpoints.push(format!("{}/markets?condition_ids={}", GAMMA_API_BASE, id));
            endpoints.push(format!("{}/markets/{}", GAMMA_API_BASE, id));
            endpoints.push(format!("{}/marketInstances/{}", GAMMA_API_BASE, id));
        }

        for url in endpoints {
            match fetch_and_extract_slug(&url, &market_id) {
                Ok(Some(slug)) => {
                    eprintln!(
                        "✅  Resolved market_instance_id; market_id={} url={} slug={}",
                        market_id, url, slug
                    );
                    return Ok(Some(slug));
                }
                Ok(None) => {
                    eprintln!(
                        "ℹ️  No slug at endpoint; market_id={} url={} (will try next endpoint)",
                        market_id, url
                    );
                    continue;
                }
                Err(e) => {
                    eprintln!(
                        "⚠️  Slug lookup failed market_id={} url={}: {}",
                        market_id, url, e
                    );
                    continue;
                }
            }
        }

        Ok(None)
    })
    .await
    .map_err(|e| anyhow::anyhow!("JoinError fetching market slug: {e}"))??;

    Ok(res)
}

fn fetch_and_extract_slug(url: &str, market_id: &str) -> Result<Option<String>> {
    let response = match ureq::get(url).timeout(Duration::from_secs(10)).call() {
        Ok(resp) => resp,
        Err(ureq::Error::Status(status, resp)) => {
            let body = resp
                .into_string()
                .unwrap_or_else(|e| format!("<<failed to read body: {e}>>"));
            eprintln!(
                "⚠️  Gamma status error market_id={} url={} status={} body_snip={}",
                market_id,
                url,
                status,
                snippet(&body)
            );
            return Ok(None);
        }
        Err(e) => {
            return Err(anyhow::anyhow!(
                "HTTP error market_id={} url={}: {e}",
                market_id,
                url
            ))
        }
    };

    let status = response.status();
    let status_text = response.status_text().to_string();
    let body = response
        .into_string()
        .map_err(|e| anyhow::anyhow!("Read body error market_id={} url={}: {e}", market_id, url))?;

    if status == 404 {
        eprintln!(
            "ℹ️  Gamma returned 404 for market_id={} url={} body_snip={}",
            market_id,
            url,
            snippet(&body)
        );
        return Ok(None);
    }

    if status != 200 {
        eprintln!(
            "⚠️  Gamma non-200 status={} {} market_id={} url={} body_snip={}",
            status,
            status_text,
            market_id,
            url,
            snippet(&body)
        );
        return Ok(None);
    }

    // First, try the typed MarketDetail; fall back to generic traversal.
    if let Ok(detail) = serde_json::from_str::<MarketDetail>(&body) {
        if detail.slug.is_none() {
            eprintln!(
                "⚠️  Gamma response missing slug; market_id={} url={} status={} body_snip={}",
                market_id,
                url,
                status,
                snippet(&body)
            );
        }
        if let Some(s) = detail.slug {
            eprintln!(
                "✅  Found slug via MarketDetail; market_id={} url={} slug={}",
                market_id, url, s
            );
            return Ok(Some(s));
        }
        return Ok(None);
    }

    let val: Value = serde_json::from_str(&body).map_err(|e| {
        anyhow::anyhow!(
            "Parse error market_id={} url={} status={} body_snip={} err={}",
            market_id,
            url,
            status,
            snippet(&body),
            e
        )
    })?;

    let slug = find_slug_recursive(&val);
    if let Some(s) = slug.clone() {
        eprintln!(
            "✅  Found slug via generic traversal; market_id={} url={} slug={}",
            market_id, url, s
        );
        return Ok(Some(s));
    } else {
        eprintln!(
            "⚠️  No slug found in Gamma body; market_id={} url={} status={} body_snip={}",
            market_id,
            url,
            status,
            snippet(&body)
        );
        return Ok(None);
    }
}

fn snippet(body: &str) -> String {
    const MAX: usize = 800;
    if body.len() > MAX {
        format!("{}...[truncated]", &body[..MAX])
    } else {
        body.to_string()
    }
}

fn find_slug_recursive(value: &Value) -> Option<String> {
    match value {
        Value::String(_s) => None,
        Value::Number(_) | Value::Bool(_) | Value::Null => None,
        Value::Array(arr) => {
            for v in arr {
                if let Some(found) = find_slug_recursive(v) {
                    return Some(found);
                }
            }
            None
        }
        Value::Object(map) => {
            if let Some(Value::String(s)) = map.get("slug") {
                return Some(s.clone());
            }
            for v in map.values() {
                if let Some(found) = find_slug_recursive(v) {
                    return Some(found);
                }
            }
            None
        }
    }
}

async fn insert_btc(client: &mut Client, ev: &BtcEvent) -> Result<()> {
    client
        .execute(
            r#"
            INSERT INTO btc_ticks (
                price,
                volume,
                event_timestamp_ms,
                received_at_ms
            ) VALUES ($1,$2,$3,$4)
        "#,
            &[&ev.price, &ev.volume, &ev.event_ts_ms, &ev.received_at_ms],
        )
        .await
        .map(|_| ())
        .map_err(|e| e.into())
}

async fn insert_btc_kline(client: &mut Client, ev: &BtcKlineEvent) -> Result<()> {
    client
        .execute(
            r#"
            INSERT INTO btc_klines (
                symbol,
                interval,
                open_time,
                close_time,
                open_price,
                close_price,
                high_price,
                low_price,
                base_volume,
                quote_volume,
                number_of_trades,
                is_closed,
                taker_buy_base_volume,
                taker_buy_quote_volume,
                event_timestamp_ms,
                received_at_ms
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
        "#,
            &[
                &ev.symbol,
                &ev.interval,
                &(ev.open_time as i64),
                &(ev.close_time as i64),
                &ev.open_price,
                &ev.close_price,
                &ev.high_price,
                &ev.low_price,
                &ev.base_volume,
                &ev.quote_volume,
                &(ev.number_of_trades as i64),
                &ev.is_closed,
                &ev.taker_buy_base_volume,
                &ev.taker_buy_quote_volume,
                &ev.event_ts_ms,
                &ev.received_at_ms,
            ],
        )
        .await
        .map(|_| ())
        .map_err(|e| e.into())
}

async fn insert_btc_mark_price(client: &mut Client, ev: &BtcMarkPriceEvent) -> Result<()> {
    client
        .execute(
            r#"
            INSERT INTO btc_mark_prices (
                symbol,
                mark_price,
                index_price,
                estimated_settle_price,
                funding_rate,
                next_funding_time,
                event_timestamp_ms,
                received_at_ms
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        "#,
            &[
                &ev.symbol,
                &ev.mark_price,
                &ev.index_price,
                &ev.estimated_settle_price,
                &ev.funding_rate,
                &(ev.next_funding_time as i64),
                &ev.event_ts_ms,
                &ev.received_at_ms,
            ],
        )
        .await
        .map(|_| ())
        .map_err(|e| e.into())
}
