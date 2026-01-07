use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

pub const BINANCE_WS_URL: &str = "wss://fstream.binance.com/ws/btcusdt@ticker";
pub const BINANCE_WS_URL_KLINE: &str = "wss://fstream.binance.com/ws/btcusdt@kline_15m";
pub const BINANCE_WS_URL_MARK_PRICE: &str = "wss://fstream.binance.com/ws/btcusdt@markPrice@1s";
pub const BINANCE_KLINE_WS_URL: &str = "wss://fstream.binance.com/ws/btcusdt@kline_1m";

fn current_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub struct BinanceKlineClient {
    url: String,
    stream: Arc<RwLock<Option<WebSocketStream<MaybeTlsStream<TcpStream>>>>>,
}

impl BinanceKlineClient {
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

    pub async fn next_kline(&self) -> Result<Option<BtcKline>> {
        let mut stream_guard = self.stream.write().await;

        if let Some(ref mut stream) = *stream_guard {
            match stream.next().await {
                Some(Ok(Message::Text(text))) => {
                    match serde_json::from_str::<RawKline>(&text) {
                        Ok(raw) => {
                            let mut kline = BtcKline::try_from(raw)?;
                            kline.received_at_ms = current_millis();
                            Ok(Some(kline))
                        }
                        Err(_) => Ok(None),
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

#[derive(Debug, Clone)]
pub struct BtcKline {
    pub symbol: String,
    pub interval: String,
    pub open_time: u64,
    pub close_time: u64,
    pub open_price: f64,
    pub close_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub base_volume: f64,
    pub quote_volume: f64,
    pub number_of_trades: u64,
    pub is_closed: bool,
    pub taker_buy_base_volume: f64,
    pub taker_buy_quote_volume: f64,
    pub event_time: u64,
    pub received_at_ms: u64,
}

#[derive(Debug, Deserialize)]
struct RawKline {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "k")]
    kline: KlineData,
}

#[derive(Debug, Deserialize)]
struct KlineData {
    #[serde(rename = "t")]
    open_time: u64,
    #[serde(rename = "T")]
    close_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "i")]
    interval: String,
    #[serde(rename = "f")]
    first_trade_id: u64,
    #[serde(rename = "L")]
    last_trade_id: u64,
    #[serde(rename = "o")]
    open_price: String,
    #[serde(rename = "c")]
    close_price: String,
    #[serde(rename = "h")]
    high_price: String,
    #[serde(rename = "l")]
    low_price: String,
    #[serde(rename = "v")]
    base_volume: String,
    #[serde(rename = "n")]
    number_of_trades: u64,
    #[serde(rename = "x")]
    is_closed: bool,
    #[serde(rename = "q")]
    quote_volume: String,
    #[serde(rename = "V")]
    taker_buy_base_volume: String,
    #[serde(rename = "Q")]
    taker_buy_quote_volume: String,
}

impl TryFrom<RawKline> for BtcKline {
    type Error = anyhow::Error;

    fn try_from(raw: RawKline) -> Result<Self> {
        Ok(Self {
            symbol: raw.kline.symbol,
            interval: raw.kline.interval,
            open_time: raw.kline.open_time,
            close_time: raw.kline.close_time,
            open_price: raw.kline.open_price.parse::<f64>().unwrap_or(0.0),
            close_price: raw.kline.close_price.parse::<f64>().unwrap_or(0.0),
            high_price: raw.kline.high_price.parse::<f64>().unwrap_or(0.0),
            low_price: raw.kline.low_price.parse::<f64>().unwrap_or(0.0),
            base_volume: raw.kline.base_volume.parse::<f64>().unwrap_or(0.0),
            quote_volume: raw.kline.quote_volume.parse::<f64>().unwrap_or(0.0),
            number_of_trades: raw.kline.number_of_trades,
            is_closed: raw.kline.is_closed,
            taker_buy_base_volume: raw.kline.taker_buy_base_volume.parse::<f64>().unwrap_or(0.0),
            taker_buy_quote_volume: raw.kline.taker_buy_quote_volume.parse::<f64>().unwrap_or(0.0),
            event_time: raw.event_time,
            received_at_ms: 0,
        })
    }
}

#[derive(Debug, Clone)]
pub struct BtcMarkPrice {
    pub symbol: String,
    pub mark_price: f64,
    pub index_price: f64,
    pub estimated_settle_price: f64,
    pub funding_rate: f64,
    pub next_funding_time: u64,
    pub event_time: u64,
    pub received_at_ms: u64,
}

#[derive(Debug, Deserialize)]
struct RawMarkPrice {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "p")]
    mark_price: String,
    #[serde(rename = "i")]
    index_price: String,
    #[serde(rename = "P")]
    estimated_settle_price: String,
    #[serde(rename = "r")]
    funding_rate: String,
    #[serde(rename = "T")]
    next_funding_time: u64,
}

impl TryFrom<RawMarkPrice> for BtcMarkPrice {
    type Error = anyhow::Error;

    fn try_from(raw: RawMarkPrice) -> Result<Self> {
        Ok(Self {
            symbol: raw.symbol,
            mark_price: raw.mark_price.parse::<f64>().unwrap_or(0.0),
            index_price: raw.index_price.parse::<f64>().unwrap_or(0.0),
            estimated_settle_price: raw.estimated_settle_price.parse::<f64>().unwrap_or(0.0),
            funding_rate: raw.funding_rate.parse::<f64>().unwrap_or(0.0),
            next_funding_time: raw.next_funding_time,
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
                        }
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

    pub async fn next_kline(&self) -> Result<Option<BtcKline>> {
        let mut stream_guard = self.stream.write().await;

        if let Some(ref mut stream) = *stream_guard {
            match stream.next().await {
                Some(Ok(Message::Text(text))) => {
                    match serde_json::from_str::<RawKline>(&text) {
                        Ok(raw) => {
                            let mut kline = BtcKline::try_from(raw)?;
                            kline.received_at_ms = current_millis();
                            Ok(Some(kline))
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

    pub async fn next_mark_price(&self) -> Result<Option<BtcMarkPrice>> {
        let mut stream_guard = self.stream.write().await;

        if let Some(ref mut stream) = *stream_guard {
            match stream.next().await {
                Some(Ok(Message::Text(text))) => {
                    match serde_json::from_str::<RawMarkPrice>(&text) {
                        Ok(raw) => {
                            let mut mark_price = BtcMarkPrice::try_from(raw)?;
                            mark_price.received_at_ms = current_millis();
                            Ok(Some(mark_price))
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
