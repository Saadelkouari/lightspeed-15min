use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{HeaderValue, ORIGIN, USER_AGENT};
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, WebSocketStream};

use crate::orderbook::Orderbook;

pub struct WebSocketClient {
    url: String,
    stream: Arc<RwLock<Option<WebSocketStream<MaybeTlsStream<TcpStream>>>>>,
    subscribed_assets: Arc<RwLock<Vec<String>>>,
}

#[derive(Debug, Serialize)]
struct MarketSubscribeMessage {
    #[serde(rename = "type")]
    channel_type: String,
    #[serde(rename = "assets_ids")]
    assets_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct MarketOperationMessage {
    #[serde(rename = "assets_ids")]
    assets_ids: Vec<String>,
    operation: String, // "subscribe" | "unsubscribe"
}

#[derive(Debug, Deserialize)]
struct MarketBookMessage {
    #[serde(rename = "event_type")]
    event_type: String, // "book"
    #[serde(rename = "asset_id")]
    asset_id: String,
    market: String,
    bids: Vec<OrderbookLevel>,
    asks: Vec<OrderbookLevel>,
    timestamp: String,
    hash: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct UserTradeMessage {
    pub asset_id: String,
    pub event_type: String,
    pub id: String,
    pub last_update: String,
    pub maker_orders: Vec<UserMakerOrder>,
    pub market: String,
    pub matchtime: String,
    pub outcome: String,
    pub owner: String,
    pub price: String,
    pub side: String,
    pub size: String,
    pub status: String,
    pub taker_order_id: String,
    pub timestamp: String,
    pub trade_owner: String,
    #[serde(rename = "type")]
    pub trade_type: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct UserMakerOrder {
    pub asset_id: String,
    pub matched_amount: String,
    pub order_id: String,
    pub outcome: String,
    pub owner: String,
    pub price: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct UserOrderMessage {
    pub asset_id: String,
    pub associate_trades: Option<Vec<String>>,
    pub event_type: String,
    pub id: String,
    pub market: String,
    pub order_owner: String,
    pub original_size: String,
    pub outcome: String,
    pub owner: String,
    pub price: String,
    pub side: String,
    pub size_matched: String,
    pub timestamp: String,
    #[serde(rename = "type")]
    pub order_type: String,
}

#[derive(Debug, Clone)]
pub enum UserEvent {
    Trade(UserTradeMessage),
    Order(UserOrderMessage),
}

#[derive(Debug, Serialize, Clone)]
pub struct UserAuth {
    #[serde(rename = "apiKey")]
    api_key: String,
    secret: String,
    passphrase: String,
}

impl UserAuth {
    pub fn new(api_key: String, secret: String, passphrase: String) -> Self {
        Self {
            api_key,
            secret,
            passphrase,
        }
    }
}

#[derive(Debug, Serialize)]
struct UserSubscribeMessage {
    #[serde(rename = "type")]
    channel_type: String,
    auth: UserAuth,
    markets: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct OrderbookLevel {
    price: String,
    size: String,
}

impl WebSocketClient {
    pub fn new(url: String) -> Self {
        Self {
            url,
            stream: Arc::new(RwLock::new(None)),
            subscribed_assets: Arc::new(RwLock::new(Vec::new())),
        }
    }

    fn market_ws_url(&self) -> String {
        // CLOB market channel uses /ws/market
        format!("{}/ws/market", self.url.trim_end_matches('/'))
    }

    pub async fn connect_market(&self, assets_ids: &[String]) -> Result<()> {
        {
            let mut sub = self.subscribed_assets.write().await;
            *sub = assets_ids.to_vec();
        }

        let ws_url = self.market_ws_url();

        // Cloudflare often expects a browser-like Origin header.
        let mut req = ws_url.into_client_request()?;
        req.headers_mut()
            .insert(ORIGIN, HeaderValue::from_static("https://polymarket.com"));
        req.headers_mut()
            .insert(USER_AGENT, HeaderValue::from_static("lightspeed-15min/0.1"));

        let (ws_stream, _) = match connect_async(req).await {
            Ok(ok) => ok,
            Err(e) => {
                // If the server replies with a non-101 status, log headers to help debug required path/headers.
                if let tokio_tungstenite::tungstenite::Error::Http(resp) = &e {
                    eprintln!(
                        "WS handshake rejected: status={} headers={:?}",
                        resp.status(),
                        resp.headers()
                    );
                }
                return Err(e.into());
            }
        };
        *self.stream.write().await = Some(ws_stream);

        // Initial subscription message on open
        self.send_initial_market_subscribe().await?;

        // Keepalive ping loop (server expects text "PING")
        self.spawn_ping_loop();

        Ok(())
    }

    pub async fn reconnect(&self) -> Result<()> {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        let assets = self.subscribed_assets.read().await.clone();
        self.connect_market(&assets).await
    }

    pub async fn subscribe_assets(&self, assets_ids: &[String]) -> Result<()> {
        self.send_market_operation("subscribe", assets_ids).await?;
        let mut sub = self.subscribed_assets.write().await;
        for id in assets_ids {
            if !sub.contains(id) {
                sub.push(id.clone());
            }
        }
        Ok(())
    }

    pub async fn unsubscribe_assets(&self, assets_ids: &[String]) -> Result<()> {
        self.send_market_operation("unsubscribe", assets_ids)
            .await?;
        let mut sub = self.subscribed_assets.write().await;
        sub.retain(|x| !assets_ids.contains(x));
        Ok(())
    }

    async fn send_initial_market_subscribe(&self) -> Result<()> {
        let assets_ids = self.subscribed_assets.read().await.clone();
        let msg = MarketSubscribeMessage {
            channel_type: "market".to_string(),
            assets_ids,
        };
        self.send_json(&msg).await
    }

    async fn send_market_operation(&self, operation: &str, assets_ids: &[String]) -> Result<()> {
        let msg = MarketOperationMessage {
            assets_ids: assets_ids.to_vec(),
            operation: operation.to_string(),
        };
        self.send_json(&msg).await
    }

    async fn send_json<T: Serialize>(&self, msg: &T) -> Result<()> {
        let json = serde_json::to_string(msg)?;
        if let Some(ref mut stream) = *self.stream.write().await {
            stream.send(Message::Text(json)).await?;
        }
        Ok(())
    }

    fn spawn_ping_loop(&self) {
        let stream = self.stream.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(10));
            loop {
                ticker.tick().await;
                if let Some(ref mut ws) = *stream.write().await {
                    // Spec says clients should send PING text periodically
                    let _ = ws.send(Message::Text("PING".to_string())).await;
                } else {
                    break;
                }
            }
        });
    }

    pub async fn receive_message(&self) -> Result<Option<String>> {
        let mut stream_guard = self.stream.write().await;

        if let Some(ref mut stream) = *stream_guard {
            match stream.next().await {
                Some(Ok(Message::Text(text))) => Ok(Some(text)),
                Some(Ok(Message::Ping(_))) => {
                    // ignore
                    Ok(None)
                }
                Some(Ok(Message::Pong(_))) => {
                    // ignore
                    Ok(None)
                }
                Some(Ok(Message::Close(_))) => {
                    *stream_guard = None;
                    Ok(None)
                }
                Some(Err(e)) => {
                    *stream_guard = None;
                    Err(anyhow::anyhow!("WebSocket error: {}", e))
                }
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }
}

pub struct UserWebSocketClient {
    url: String,
    auth: UserAuth,
    markets: Vec<String>,
    stream: Arc<RwLock<Option<WebSocketStream<MaybeTlsStream<TcpStream>>>>>,
}

impl UserWebSocketClient {
    pub fn new(url: String, auth: UserAuth, markets: Vec<String>) -> Self {
        Self {
            url,
            auth,
            markets,
            stream: Arc::new(RwLock::new(None)),
        }
    }

    fn user_ws_url(&self) -> String {
        format!("{}/ws/user", self.url.trim_end_matches('/'))
    }

    pub async fn connect(&self) -> Result<()> {
        let ws_url = self.user_ws_url();

        let mut req = ws_url.into_client_request()?;
        req.headers_mut()
            .insert(ORIGIN, HeaderValue::from_static("https://polymarket.com"));
        req.headers_mut()
            .insert(USER_AGENT, HeaderValue::from_static("lightspeed-15min/0.1"));
        req.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {}", self.auth.api_key))?,
        );
        req.headers_mut()
            .insert("X-Api-Key", HeaderValue::from_str(&self.auth.api_key)?);

        let (ws_stream, _) = match connect_async(req).await {
            Ok(ok) => ok,
            Err(e) => {
                if let tokio_tungstenite::tungstenite::Error::Http(resp) = &e {
                    eprintln!(
                        "WS handshake rejected: status={} headers={:?}",
                        resp.status(),
                        resp.headers()
                    );
                }
                return Err(e.into());
            }
        };
        *self.stream.write().await = Some(ws_stream);

        self.send_user_subscribe().await?;
        self.spawn_ping_loop();
        Ok(())
    }

    pub async fn reconnect(&self) -> Result<()> {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        self.connect().await
    }

    async fn send_user_subscribe(&self) -> Result<()> {
        let msg = UserSubscribeMessage {
            channel_type: "user".to_string(),
            auth: self.auth.clone(),
            markets: self.markets.clone(),
        };
        self.send_json(&msg).await
    }

    async fn send_json<T: Serialize>(&self, msg: &T) -> Result<()> {
        let json = serde_json::to_string(msg)?;
        if let Some(ref mut stream) = *self.stream.write().await {
            stream.send(Message::Text(json)).await?;
        }
        Ok(())
    }

    fn spawn_ping_loop(&self) {
        let stream = self.stream.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(10));
            loop {
                ticker.tick().await;
                if let Some(ref mut ws) = *stream.write().await {
                    let _ = ws.send(Message::Text("PING".to_string())).await;
                } else {
                    break;
                }
            }
        });
    }

    pub async fn receive_message(&self) -> Result<Option<String>> {
        let mut stream_guard = self.stream.write().await;

        if let Some(ref mut stream) = *stream_guard {
            match stream.next().await {
                Some(Ok(Message::Text(text))) => Ok(Some(text)),
                Some(Ok(Message::Ping(_))) => Ok(None),
                Some(Ok(Message::Pong(_))) => Ok(None),
                Some(Ok(Message::Close(_))) => {
                    *stream_guard = None;
                    Ok(None)
                }
                Some(Err(e)) => {
                    *stream_guard = None;
                    Err(anyhow::anyhow!("WebSocket error: {}", e))
                }
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }
}

pub fn parse_orderbook_message(message: &str) -> Option<Orderbook> {
    // Market channel emits messages directly like: { event_type: "book", bids, asks, ... }
    let book: MarketBookMessage = serde_json::from_str(message).ok()?;

    if book.event_type != "book" {
        return None;
    }

    let timestamp = book.timestamp.parse::<i64>().ok()?;

    Some(Orderbook {
        asset_id: book.asset_id,
        market: book.market,
        bids: book
            .bids
            .into_iter()
            .map(|l| crate::orderbook::OrderbookLevel {
                price: l.price,
                size: l.size,
            })
            .collect(),
        asks: book
            .asks
            .into_iter()
            .map(|l| crate::orderbook::OrderbookLevel {
                price: l.price,
                size: l.size,
            })
            .collect(),
        timestamp,
        received_at_ms: 0,
        hash: book.hash.unwrap_or_default(),
        tick_size: String::new(),
        min_order_size: String::new(),
    })
}

pub fn parse_user_message(message: &str) -> Option<UserEvent> {
    let value: serde_json::Value = serde_json::from_str(message).ok()?;
    let event_type = value.get("event_type")?.as_str()?;
    match event_type {
        "trade" => serde_json::from_value::<UserTradeMessage>(value)
            .ok()
            .map(UserEvent::Trade),
        "order" => serde_json::from_value::<UserOrderMessage>(value)
            .ok()
            .map(UserEvent::Order),
        _ => None,
    }
}
