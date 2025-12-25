use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookLevel {
    pub price: String,
    pub size: String,
}

#[derive(Debug, Clone)]
pub struct Orderbook {
    pub asset_id: String,
    pub market: String,
    pub bids: Vec<OrderbookLevel>,
    pub asks: Vec<OrderbookLevel>,
    pub timestamp: i64,
    pub received_at_ms: i64,
    pub hash: String,
    pub tick_size: String,
    pub min_order_size: String,
}

pub struct OrderbookState {
    orderbooks: HashMap<String, Orderbook>,
    labels: HashMap<String, String>,
}

impl OrderbookState {
    pub fn new() -> Self {
        Self {
            orderbooks: HashMap::new(),
            labels: HashMap::new(),
        }
    }

    pub fn update(&mut self, orderbook: Orderbook) {
        self.orderbooks.insert(orderbook.asset_id.clone(), orderbook);
    }

    pub fn get_best_levels(&self) -> Option<(Option<&OrderbookLevel>, Option<&OrderbookLevel>)> {
        // Get the first orderbook (we expect 2, one for YES, one for NO)
        // For display, we'll show the one with the most liquidity
        let best_orderbook = self.orderbooks.values()
            .max_by_key(|ob| {
                let bid_size: f64 = ob.bids.last().and_then(|b| b.size.parse().ok()).unwrap_or(0.0);
                let ask_size: f64 = ob.asks.last().and_then(|a| a.size.parse().ok()).unwrap_or(0.0);
                (bid_size + ask_size) as i64
            })?;

        // API delivers bids ascending (low->high); best bid is last.
        // Asks have been observed descending (high->low); best ask is last.
        let best_bid = best_orderbook.bids.last();
        let best_ask = best_orderbook.asks.last();
        
        Some((best_bid, best_ask))
    }

    /// Map the two CLOB token ids to friendly labels.
    /// Convention: first = UP, second = DOWN (Polymarket binary markets).
    pub fn set_binary_labels(&mut self, assets_ids: &[String]) {
        self.labels.clear();
        if let Some(id) = assets_ids.get(0) {
            self.labels.insert(id.clone(), "UP".to_string());
        }
        if let Some(id) = assets_ids.get(1) {
            self.labels.insert(id.clone(), "DOWN".to_string());
        }
    }

    pub fn label_for(&self, asset_id: &str) -> Option<&str> {
        self.labels.get(asset_id).map(|s| s.as_str())
    }

    pub fn get_full_orderbook(&self, asset_id: &str) -> Option<&Orderbook> {
        self.orderbooks.get(asset_id)
    }

    pub fn get_all_orderbooks(&self) -> &HashMap<String, Orderbook> {
        &self.orderbooks
    }

    pub fn clear(&mut self) {
        self.orderbooks.clear();
    }
}

