use colored::*;
use crossterm::{cursor, terminal, ExecutableCommand};
use std::io::{stdout, Write};
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::orderbook::OrderbookState;
use crate::btc::BtcTicker;

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn normalize_timestamp_to_ms(ts: i64) -> i64 {
    if ts < 1_000_000_000_000 {
        // If server ever sends seconds precision, lift to ms.
        ts.saturating_mul(1000)
    } else {
        ts
    }
}

fn compute_latency_ms(feed_timestamp: i64, now_ms: i64) -> i64 {
    let ts_ms = normalize_timestamp_to_ms(feed_timestamp);
    now_ms.saturating_sub(ts_ms).max(0)
}

fn compute_latency_ms_opt(feed_timestamp: i64, end_ms: Option<i64>) -> Option<i64> {
    match end_ms {
        Some(end) if end > 0 => Some(compute_latency_ms(feed_timestamp, end)),
        _ => None,
    }
}

fn render_latency(latency_ms: i64) -> colored::ColoredString {
    match latency_ms {
        l if l <= 120 => format!("{l} ms").bright_green(),
        l if l <= 400 => format!("{l} ms").bright_yellow(),
        l => format!("{l} ms").bright_red().bold(),
    }
}

fn render_latency_opt(latency_ms: Option<i64>) -> colored::ColoredString {
    latency_ms
        .map(render_latency)
        .unwrap_or_else(|| "N/A".bright_black())
}

pub struct OrderbookDisplay {
    last_update: std::time::Instant,
    last_frame: String,
}

impl OrderbookDisplay {
    pub fn new() -> Self {
        Self {
            last_update: std::time::Instant::now(),
            last_frame: String::new(),
        }
    }

    pub async fn update(&mut self, state: &Arc<RwLock<OrderbookState>>, status: &Arc<RwLock<String>>) {
        let state = state.read().await;
        let orderbooks = state.get_all_orderbooks();
        let now_ms = now_millis();

        // Build one full frame and write it in a single flush. This avoids "appended log" feel.
        // We also cap rendering to terminal height to prevent scrolling (which pushes old frames into scrollback).
        let (cols, rows) = terminal::size().unwrap_or((80, 24));
        let cols_usize = cols as usize;
        let rows_usize = rows.max(1) as usize;

        // Layout budgeting (rough but effective)
        // Header: 4 lines, Summary: up to 8 lines, Footer: 2 lines, Per-orderbook fixed: 5 lines.
        let orderbook_count = orderbooks.len().max(1);
        let header_lines = 4usize;
        let summary_lines = 8usize;
        let footer_lines = 2usize;
        let per_ob_fixed_lines = 5usize; // asset+sep+asks label+blank+bids label (levels added separately)
        let fixed = header_lines + summary_lines + footer_lines + (per_ob_fixed_lines * orderbook_count);

        // Remaining lines for levels across all orderbooks (asks+bids)
        let remaining = rows_usize.saturating_sub(fixed).max(2); // ensure at least 1 per side
        let mut levels_per_side = remaining / (orderbook_count * 2);
        if levels_per_side == 0 {
            levels_per_side = 1;
        }
        levels_per_side = levels_per_side.min(10);

        let mut lines: Vec<String> = Vec::with_capacity(rows_usize);

        let rule = "═".repeat(cols_usize.saturating_sub(2).max(40));
        lines.push(format!(" {}", rule).bright_cyan().to_string());
        lines.push("  POLYMARKET BTC 15-MIN UP/DOWN - LIVE ORDERBOOK STREAM".bright_cyan().bold().to_string());
        lines.push(format!(" {}", rule).bright_cyan().to_string());
        lines.push(String::new());

        if orderbooks.is_empty() {
            lines.push("  Waiting for first orderbook update...".bright_black().to_string());
            lines.push(String::new());
        } else {
            for (asset_id, orderbook) in orderbooks.iter() {
                let label = state.label_for(asset_id).unwrap_or(asset_id);
                self.display_orderbook_lines(&mut lines, label, orderbook, levels_per_side, now_ms);
                lines.push(String::new());
            }
        }

        // Best levels now shown inline with each asset header; no separate summary block.

        lines.push(String::new());

        let now_seconds = (now_ms / 1000).max(0) as u64;
        let hours = (now_seconds / 3600) % 24;
        let minutes = (now_seconds / 60) % 60;
        let seconds = now_seconds % 60;
        let ts = format!("{:02}:{:02}:{:02}", hours, minutes, seconds).bright_black();
        let market_str = orderbooks
            .values()
            .next()
            .map(|ob| ob.market.as_str())
            .unwrap_or("N/A");
        let market = market_str.bright_black();
        lines.push(format!("  Last Update: {} | Market: {}", ts, market));

        let status_line = status.read().await.clone();
        if !status_line.is_empty() {
            lines.push(format!("  Status: {}", status_line.bright_black()));
        } else {
            lines.push(String::new());
        }

        lines.push(format!(" {}", rule).bright_cyan().to_string());

        // Hard cap to terminal height to avoid scrolling.
        if lines.len() > rows_usize {
            lines.truncate(rows_usize);
        } else {
            // pad to full height so old content doesn't remain on screen on some terminals
            while lines.len() < rows_usize {
                lines.push(String::new());
            }
        }

        // Normalize each line width so shorter rows fully overwrite prior content.
        for line in &mut lines {
            let char_count = line.chars().count();
            if char_count > cols_usize {
                *line = line.chars().take(cols_usize).collect();
            } else if char_count < cols_usize {
                let pad = " ".repeat(cols_usize - char_count);
                line.push_str(&pad);
            }
        }

        let frame = lines.join("\n"); // no trailing newline to reduce chance of scroll
        if frame == self.last_frame {
            return;
        }
        self.last_frame = frame.clone();

        let mut out = stdout();
        let _ = out.execute(cursor::MoveTo(0, 0));
        let _ = out.execute(terminal::Clear(terminal::ClearType::All));
        let _ = write!(out, "{frame}");
        let _ = out.flush();
    }

    fn display_orderbook_lines(
        &self,
        lines: &mut Vec<String>,
        asset_label: &str,
        orderbook: &crate::orderbook::Orderbook,
        levels_per_side: usize,
        now_ms: i64,
    ) {
        let best_bid = orderbook.bids.last();
        let best_ask = orderbook.asks.last();

        let bid_str = if let Some(bid) = best_bid {
            let bid_price: f64 = bid.price.parse().unwrap_or(0.0);
            let bid_size: f64 = bid.size.parse().unwrap_or(0.0);
            format!(
                "{} @ {}",
                format!("{:>6.4}", bid_price).bright_green(),
                format!("{:>7.2}", bid_size).bright_white()
            )
        } else {
            "N/A".bright_black().to_string()
        };

        let ask_str = if let Some(ask) = best_ask {
            let ask_price: f64 = ask.price.parse().unwrap_or(0.0);
            let ask_size: f64 = ask.size.parse().unwrap_or(0.0);
            format!(
                "{} @ {}",
                format!("{:>6.4}", ask_price).bright_red(),
                format!("{:>7.2}", ask_size).bright_white()
            )
        } else {
            "N/A".bright_black().to_string()
        };

        lines.push(format!(
            "  ASSET: {} | BID {}   ASK {}",
            asset_label.bright_cyan().bold(),
            bid_str,
            ask_str
        ));
        let display_latency = compute_latency_ms(orderbook.timestamp, now_ms);
        let network_latency = compute_latency_ms_opt(orderbook.timestamp, Some(orderbook.received_at_ms));
        lines.push(format!(
            "  LATENCY: NET {}   DISP {}",
            render_latency_opt(network_latency),
            render_latency(display_latency)
        ));
        let thin_rule = "─".repeat(60);
        lines.push(format!(" {}", thin_rule).bright_black().to_string());
        let max_size = self.max_level_size(orderbook);
        lines.push("      PRICE      SIZE        DEPTH".bright_black().to_string());
        lines.push(String::new());
        
        // Display asks (sell side) best-first (API observed high->low, best = last)
        lines.push("  ASKS (SELL)".bright_red().bold().to_string());
        for ask in orderbook.asks.iter().rev().take(levels_per_side) {
            let price: f64 = ask.price.parse().unwrap_or(0.0);
            let size: f64 = ask.size.parse().unwrap_or(0.0);
            lines.push(format!("    {} │ {} │ {}", 
                format!("{:>10.4}", price).bright_red(),
                format!("{:>12.2}", size).bright_white(),
                self.size_bar(size, max_size, 40)
            ));
        }
        
        lines.push(String::new());
        
        // Display bids (buy side) best-first (API delivers low->high, best = last)
        lines.push("  BIDS (BUY)".bright_green().bold().to_string());
        for bid in orderbook.bids.iter().rev().take(levels_per_side) {
            let price: f64 = bid.price.parse().unwrap_or(0.0);
            let size: f64 = bid.size.parse().unwrap_or(0.0);
            lines.push(format!("    {} │ {} │ {}", 
                format!("{:>10.4}", price).bright_green(),
                format!("{:>12.2}", size).bright_white(),
                self.size_bar(size, max_size, 40)
            ));
        }
    }

    fn max_level_size(&self, orderbook: &crate::orderbook::Orderbook) -> f64 {
        orderbook.asks.iter()
            .chain(orderbook.bids.iter())
            .filter_map(|l| l.size.parse::<f64>().ok())
            .fold(1.0, f64::max)
    }

    fn size_bar(&self, size: f64, max_size: f64, max_width: usize) -> String {
        let width = ((size / max_size) * max_width as f64) as usize;
        let width = width.min(max_width);
        "█".repeat(width).bright_black().to_string()
    }
}

pub struct BtcDisplay {
    last_frame: String,
}

impl BtcDisplay {
    pub fn new() -> Self {
        Self {
            last_frame: String::new(),
        }
    }

    pub async fn update(&mut self, ticker: &Arc<RwLock<Option<BtcTicker>>>, status: &Arc<RwLock<String>>) {
        let ticker = ticker.read().await.clone();
        let now_ms = now_millis();

        let (cols, rows) = terminal::size().unwrap_or((80, 24));
        let cols_usize = cols as usize;
        let rows_usize = rows.max(1) as usize;

        let mut lines: Vec<String> = Vec::with_capacity(rows_usize);
        let rule = "═".repeat(cols_usize.saturating_sub(2).max(40));
        lines.push(format!(" {}", rule).bright_cyan().to_string());
        lines.push("  BINANCE BTCUSDT TICKER".bright_cyan().bold().to_string());
        lines.push(format!(" {}", rule).bright_cyan().to_string());
        lines.push(String::new());

        if let Some(t) = ticker {
            let change_is_up = t.price_change >= 0.0;
            let last_price = format!("{:.2}", t.last_price);
            let change = format!("{:+.2}", t.price_change);
            let change_pct = format!("{:+.2}%", t.price_change_percent);
            let high = format!("{:.2}", t.high_price);
            let low = format!("{:.2}", t.low_price);
            let volume = format!("{:.2}", t.volume);
            let quote_volume = format!("{:.2}", t.quote_volume);
            let display_latency = compute_latency_ms(t.event_time as i64, now_ms);
            let network_latency = compute_latency_ms_opt(
                t.event_time as i64,
                Some(t.received_at_ms as i64)
            );

            lines.push(format!("  SYMBOL: {}", t.symbol.bright_cyan().bold()));
            lines.push(format!("  PRICE: {}", last_price.bright_white().bold()));
            lines.push(format!("  CHANGE: {} ({})",
                if change_is_up { change.bright_green() } else { change.bright_red() },
                if change_is_up { change_pct.bright_green() } else { change_pct.bright_red() },
            ));
            lines.push(String::new());
            lines.push(format!("  HIGH: {}   LOW: {}", high.bright_white(), low.bright_white()));
            lines.push(format!("  VOL: {} BTC   QUOTE VOL: {} USDT", volume.bright_white(), quote_volume.bright_white()));

            let seconds = (t.event_time / 1000) % 60;
            let minutes = (t.event_time / 1000 / 60) % 60;
            let hours = (t.event_time / 1000 / 3600) % 24;
            lines.push(String::new());
            lines.push(format!("  Last Update (UTC): {:02}:{:02}:{:02}", hours, minutes, seconds).bright_black().to_string());
            lines.push(format!(
                "  Latency: NET {}   DISP {}",
                render_latency_opt(network_latency),
                render_latency(display_latency)
            ));
        } else {
            lines.push("  Waiting for first BTCUSDT tick…".bright_black().to_string());
        }

        lines.push(String::new());
        let status_line = status.read().await.clone();
        if !status_line.is_empty() {
            lines.push(format!("  Status: {}", status_line.bright_black()));
        }
        lines.push(format!(" {}", rule).bright_cyan().to_string());

        if lines.len() > rows_usize {
            lines.truncate(rows_usize);
        } else {
            while lines.len() < rows_usize {
                lines.push(String::new());
            }
        }

        for line in &mut lines {
            let char_count = line.chars().count();
            if char_count > cols_usize {
                *line = line.chars().take(cols_usize).collect();
            } else if char_count < cols_usize {
                let pad = " ".repeat(cols_usize - char_count);
                line.push_str(&pad);
            }
        }

        let frame = lines.join("\n");
        if frame == self.last_frame {
            return;
        }
        self.last_frame = frame.clone();

        let mut out = stdout();
        let _ = out.execute(cursor::MoveTo(0, 0));
        let _ = out.execute(terminal::Clear(terminal::ClearType::All));
        let _ = write!(out, "{frame}");
        let _ = out.flush();
    }
}

