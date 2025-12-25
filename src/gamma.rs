use anyhow::Result;
use serde::Deserialize;
use std::time::{Duration, Instant};

pub struct GammaClient {
    base_url: String,
    last_call: std::sync::Mutex<Option<Instant>>,
}

#[derive(Debug, Clone)]
pub struct MarketInfo {
    pub market_id: String,
    pub token_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct Event {
    pub id: String,
    pub slug: String,
    pub markets: Vec<Market>,
}

#[derive(Debug, Deserialize)]
pub struct Market {
    pub id: String,
    #[serde(rename = "enableOrderBook")]
    pub enable_order_book: Option<bool>,
    #[serde(rename = "acceptingOrders")]
    pub accepting_orders: Option<bool>,
    pub active: Option<bool>,
    #[serde(rename = "clobTokenIds")]
    pub clob_token_ids: String,
}

impl GammaClient {
    pub fn new(base_url: String) -> Self {
        Self {
            base_url,
            last_call: std::sync::Mutex::new(None),
        }
    }

    pub async fn get_event_by_slug(&self, slug: &str) -> Result<Event> {
        // Simple rate limiting (do not hold a MutexGuard across an `.await`)
        loop {
            let sleep_for = {
                let last_call = self.last_call.lock().unwrap();
                last_call.and_then(|last| {
                    let elapsed = last.elapsed();
                    (elapsed < Duration::from_secs(1)).then(|| Duration::from_secs(1) - elapsed)
                })
            };

            if let Some(d) = sleep_for {
                tokio::time::sleep(d).await;
                continue;
            }

            *self.last_call.lock().unwrap() = Some(Instant::now());
            break;
        }

        let url = format!("{}/events/slug/{}", self.base_url, slug);
        let response = ureq::get(&url)
            .timeout(Duration::from_secs(10))
            .call()?;
        
        if response.status() == 404 {
            anyhow::bail!("Event not found: {}", slug);
        }
        
        let event: Event = response.into_json()?;
        Ok(event)
    }
}

