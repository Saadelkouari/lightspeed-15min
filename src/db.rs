use crate::config::DatabaseConfig;
use crate::orderbook::Orderbook;
use crate::btc::BtcTicker;
use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tokio_postgres::{Client, NoTls};

#[derive(Clone, Debug)]
pub struct DbLogger {
    cfg: DatabaseConfig,
    tx: mpsc::UnboundedSender<DbEvent>,
}

#[derive(Debug)]
enum DbEvent {
    Polymarket(PolymarketEvent),
    Btc(BtcEvent),
}

#[derive(Debug)]
struct PolymarketEvent {
    asset_label: String,
    asset_id: String,
    market: String,
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
            asset_id: ob.asset_id.clone(),
            market: ob.market.clone(),
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
}

async fn run_worker(url: String, auto_create: bool, mut rx: mpsc::UnboundedReceiver<DbEvent>) -> Result<()> {
    let mut pending: Option<DbEvent> = None;

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
                DbEvent::Polymarket(ev) => insert_polymarket(&mut client, ev).await,
                DbEvent::Btc(ev) => insert_btc(&mut client, ev).await,
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
        "#,
        )
        .await?;
    Ok(())
}

async fn insert_polymarket(client: &mut Client, ev: &PolymarketEvent) -> Result<()> {
    client
        .execute(
            r#"
            INSERT INTO polymarket_orderbook_events (
                asset_label,
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
                &ev.asset_label,
                &ev.asset_id,
                &ev.market,
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

