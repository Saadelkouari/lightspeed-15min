use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

pub const BINANCE_WS_URL: &str = "wss://fstream.binance.com/ws/btcusdt@ticker";

fn current_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Debug, Clone)]
pub struct BtcTicker {
    pub symbol: String,
    pub last_price: f64,
    pub price_change: f64,
    pub price_change_percent: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub event_time: u64,
    pub received_at_ms: u64,
}

#[derive(Debug, Deserialize)]
struct RawTicker {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "c")]
    last_price: String,
    #[serde(rename = "p")]
    price_change: String,
    #[serde(rename = "P")]
    price_change_percent: String,
    #[serde(rename = "h")]
    high_price: String,
    #[serde(rename = "l")]
    low_price: String,
    #[serde(rename = "v")]
    volume: String,
    #[serde(rename = "q")]
    quote_volume: String,
}

impl TryFrom<RawTicker> for BtcTicker {
    type Error = anyhow::Error;

    fn try_from(raw: RawTicker) -> Result<Self> {
        Ok(Self {
            symbol: raw.symbol,
            last_price: raw.last_price.parse::<f64>().unwrap_or(0.0),
            price_change: raw.price_change.parse::<f64>().unwrap_or(0.0),
            price_change_percent: raw.price_change_percent.parse::<f64>().unwrap_or(0.0),
            high_price: raw.high_price.parse::<f64>().unwrap_or(0.0),
            low_price: raw.low_price.parse::<f64>().unwrap_or(0.0),
            volume: raw.volume.parse::<f64>().unwrap_or(0.0),
            quote_volume: raw.quote_volume.parse::<f64>().unwrap_or(0.0),
            event_time: raw.event_time,
            received_at_ms: 0,
        })
    }
}

pub struct BinanceWsClient {
    url: String,
    stream: Arc<RwLock<Option<WebSocketStream<MaybeTlsStream<TcpStream>>>>>,
}

impl BinanceWsClient {
    pub fn new(url: String) -> Self {
        Self {
            url,
            stream: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn connect(&self) -> Result<()> {
        let (ws_stream, _) = connect_async(&self.url).await?;
        *self.stream.write().await = Some(ws_stream);
        Ok(())
    }

    pub async fn reconnect(&self) -> Result<()> {
        tokio::time::sleep(Duration::from_secs(1)).await;
        self.connect().await
    }

    pub async fn is_connected(&self) -> bool {
        self.stream.read().await.is_some()
    }

    pub async fn next_ticker(&self) -> Result<Option<BtcTicker>> {
        let mut stream_guard = self.stream.write().await;

        if let Some(ref mut stream) = *stream_guard {
            match stream.next().await {
                Some(Ok(Message::Text(text))) => {
                    match serde_json::from_str::<RawTicker>(&text) {
                        Ok(raw) => {
                            let mut ticker = BtcTicker::try_from(raw)?;
                            ticker.received_at_ms = current_millis();
                            Ok(Some(ticker))
                        },
                        Err(_) => Ok(None), // ignore unknown payloads
                    }
                }
                Some(Ok(Message::Ping(payload))) => {
                    let _ = stream.send(Message::Pong(payload)).await;
                    Ok(None)
                }
                Some(Ok(Message::Pong(_))) => Ok(None),
                Some(Ok(Message::Binary(_))) => Ok(None),
                Some(Ok(Message::Frame(_))) => Ok(None),
                Some(Ok(Message::Close(_))) => {
                    *stream_guard = None;
                    Ok(None)
                }
                Some(Err(e)) => {
                    *stream_guard = None;
                    Err(anyhow::anyhow!("Binance WS error: {}", e))
                }
                None => {
                    *stream_guard = None;
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }
}

