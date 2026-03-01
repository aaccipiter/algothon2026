use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use futures_util::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const EXCHANGE_URL:             &str = "http://ec2-52-19-74-159.eu-west-1.compute.amazonaws.com";
const USERNAME:                 &str = "BERT_trading";
const PASSWORD:                 &str = "bertbertbert";

const MIN_EDGE_ABS:             f64  = 5.0;  // absolute price difference threshold
const ORDER_SIZE:               i64  = 1;
const MAX_POSITION:             i64  = 500;
const LOOP_INTERVAL_MS:         u64  = 2_000;
const MIN_REQUEST_INTERVAL_MS:  u64  = 1_100;

const ETF:  &str = "LON_ETF";
const LEGS: [&str; 3] = ["TIDE_SPOT", "WX_SPOT", "LHR_COUNT"];


#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
enum Side { Buy, Sell }

#[derive(Debug, Clone, Deserialize)]
struct SseVolumeEntry {
    #[serde(rename = "marketVolume")]
    market_volume: f64,   // exchange sends 2.0 not 2
    #[serde(rename = "userVolume")]
    user_volume:   f64,
}

#[derive(Debug, Clone, Deserialize)]
struct SseOrderEvent {
    #[serde(rename = "productsymbol")]
    product:     String,
    #[serde(rename = "buyOrders")]
    buy_orders:  HashMap<String, SseVolumeEntry>,
    #[serde(rename = "sellOrders")]
    sell_orders: HashMap<String, SseVolumeEntry>,
}

#[derive(Debug, Clone, Deserialize)]
struct TradeEvent {
    product: String,
    price:   f64,
    volume:  i64,
    buyer:   String,
    seller:  String,
}

#[derive(Debug, Clone, Serialize)]
struct OrderRequest {
    product: String,
    price:   f64,
    side:    Side,
    volume:  i64,
}

#[derive(Debug, Clone, Deserialize)]
struct OrderResponse {
    id:     String,
    volume: i64,
    filled: i64,
}

#[derive(Debug, Clone, Deserialize)]
struct ActiveOrder {
    id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct PositionEntry {
    product:      String,
    #[serde(rename = "netPosition")]
    net_position: i64,
}


#[derive(Debug, Clone, Default)]
struct BookLevel { price: f64, volume: i64, own_volume: i64 }

#[derive(Debug, Clone, Default)]
struct Book { bids: Vec<BookLevel>, asks: Vec<BookLevel> }

impl Book {
    fn best_bid(&self) -> Option<(f64, i64)> {
        self.bids.iter().find(|l| l.volume - l.own_volume > 0).map(|l| (l.price, l.volume - l.own_volume))
    }
    fn best_ask(&self) -> Option<(f64, i64)> {
        self.asks.iter().find(|l| l.volume - l.own_volume > 0).map(|l| (l.price, l.volume - l.own_volume))
    }
    fn mid(&self) -> Option<f64> { Some((self.best_bid()?.0 + self.best_ask()?.0) / 2.0) }

    fn from_sse(ev: SseOrderEvent) -> Self {
        let mut bids: Vec<BookLevel> = ev.buy_orders.iter()
            .filter_map(|(p, v)| p.parse::<f64>().ok().map(|price| BookLevel {
                price, volume: v.market_volume as i64, own_volume: v.user_volume as i64,
            })).collect();
        bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());

        let mut asks: Vec<BookLevel> = ev.sell_orders.iter()
            .filter_map(|(p, v)| p.parse::<f64>().ok().map(|price| BookLevel {
                price, volume: v.market_volume as i64, own_volume: v.user_volume as i64,
            })).collect();
        asks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());

        Self { bids, asks }
    }
}

// ---------------------------------------------------------------------------
// Rate limiter
// ---------------------------------------------------------------------------

struct RateLimiter {
    min_interval: Duration,
    last_call:    Mutex<Instant>,
}
impl RateLimiter {
    fn new(min_ms: u64) -> Self {
        Self { min_interval: Duration::from_millis(min_ms), last_call: Mutex::new(Instant::now() - Duration::from_secs(60)) }
    }
    async fn wait(&self) {
        let mut last = self.last_call.lock().await;
        let elapsed = last.elapsed();
        if elapsed < self.min_interval { tokio::time::sleep(self.min_interval - elapsed).await; }
        *last = Instant::now();
    }
}

// ---------------------------------------------------------------------------
// Arb monitor
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ArbRecord {
    direction: &'static str, target_size: i64, etf_filled: i64, leg_fills: HashMap<String, i64>,
}
impl ArbRecord {
    fn is_full    (&self) -> bool { self.etf_filled == self.target_size && self.leg_fills.values().all(|&v| v == self.etf_filled) }
    fn is_etf_miss(&self) -> bool { self.etf_filled == 0 }
    fn residuals  (&self) -> HashMap<String, i64> {
        let sign: i64 = if self.direction == "SELL_ETF" { -1 } else { 1 };
        let mut res = HashMap::new();
        if self.etf_filled != 0 { res.insert(ETF.to_string(), sign * self.etf_filled); }
        for (leg, &filled) in &self.leg_fills {
            let shortfall = self.etf_filled - filled;
            if shortfall != 0 { res.insert(leg.clone(), -sign * shortfall); }
        }
        res
    }
}

#[derive(Default)]
struct ArbMonitor { records: Vec<ArbRecord> }
impl ArbMonitor {
    fn record(&mut self, arb: ArbRecord) {
        let status = if arb.is_full() { "✓ FULL" } else if arb.is_etf_miss() { "✗ ETF MISS" } else { "~ PARTIAL" };
        println!("\n  [MONITOR] {} | {} | target={}", status, arb.direction, arb.target_size);
        println!("    ETF : {}/{}", arb.etf_filled, arb.target_size);
        for leg in &LEGS {
            let filled = arb.leg_fills.get(*leg).copied().unwrap_or(0);
            let gap = arb.etf_filled - filled;
            let flag = if gap > 0 { format!("  ← SHORT {}", gap) } else { String::new() };
            println!("    {:<15}: {}/{}{}", leg, filled, arb.etf_filled, flag);
        }
        let res = arb.residuals();
        if !res.is_empty() { println!("    Residual exposure: {:?}", res); }
        self.records.push(arb);
        let total = self.records.len();
        let full    = self.records.iter().filter(|r| r.is_full()).count();
        let partial = self.records.iter().filter(|r| !r.is_full() && !r.is_etf_miss()).count();
        let misses  = self.records.iter().filter(|r| r.is_etf_miss()).count();
        println!("  [MONITOR] Attempts: {} | Full: {} ({:.0}%) | Partial: {} ({:.0}%) | ETF miss: {} ({:.0}%)",
            total, full, 100.0*full as f64/total as f64, partial, 100.0*partial as f64/total as f64,
            misses, 100.0*misses as f64/total as f64);
    }
}

// ---------------------------------------------------------------------------
// Exchange client
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct ExchangeClient {
    base_url: String, client: Client, token: String, limiter: Arc<RateLimiter>,
}

impl ExchangeClient {
    async fn login(base_url: &str, username: &str, password: &str) -> Result<Self> {
        let client = Client::new();
        let resp = client
            .post(format!("{}/api/user/authenticate", base_url))
            .header("Content-Type", "application/json; charset=utf-8")
            .json(&serde_json::json!({ "username": username, "password": password }))
            .send().await.context("Login request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body   = resp.text().await.unwrap_or_default();
            bail!("Login HTTP {} — body: {:?}", status, body);
        }

        // Token is in the response *header*, not the body
        let token = resp.headers()
            .get("Authorization")
            .context("No Authorization header in login response")?
            .to_str().context("Authorization header not valid UTF-8")?
            .to_string();

        info!("Logged in as '{}'", username);
        Ok(Self { base_url: base_url.to_string(), client, token, limiter: Arc::new(RateLimiter::new(MIN_REQUEST_INTERVAL_MS)) })
    }

    fn hdrs(&self) -> reqwest::header::HeaderMap {
        let mut m = reqwest::header::HeaderMap::new();
        m.insert("Authorization", self.token.parse().unwrap());
        m.insert("Content-Type",  "application/json; charset=utf-8".parse().unwrap());
        m
    }

    async fn get_positions(&self) -> Result<HashMap<String, i64>> {
        self.limiter.wait().await;
        let entries: Vec<PositionEntry> = self.client
            .get(format!("{}/api/position/current-user", self.base_url))
            .headers(self.hdrs()).send().await?.error_for_status()?.json().await?;
        Ok(entries.into_iter().map(|e| (e.product, e.net_position)).collect())
    }

    async fn send_order(&self, order: &OrderRequest) -> Result<OrderResponse> {
        self.limiter.wait().await;
        Ok(self.client.post(format!("{}/api/order", self.base_url))
            .headers(self.hdrs()).json(order).send().await?.error_for_status()?.json().await?)
    }

    async fn cancel_order(&self, id: &str) -> Result<()> {
        self.limiter.wait().await;
        self.client.delete(format!("{}/api/order/{}", self.base_url, id))
            .headers(self.hdrs()).send().await?.error_for_status()?;
        Ok(())
    }

    async fn cancel_all_orders(&self) -> Result<()> {
        self.limiter.wait().await;
        let orders: Vec<ActiveOrder> = self.client
            .get(format!("{}/api/order/current-user", self.base_url))
            .headers(self.hdrs()).send().await?.error_for_status()?.json().await?;
        for o in orders {
            if let Err(e) = self.cancel_order(&o.id).await { warn!("cancel {} failed: {}", o.id, e); }
        }
        Ok(())
    }

    async fn send_ioc(&self, order: &OrderRequest) -> i64 {
        match self.send_order(order).await {
            Err(e) => { error!("send_order failed: {}", e); 0 }
            Ok(r)  => {
                let filled = r.filled;
                if r.filled < r.volume {
                    if let Err(e) = self.cancel_order(&r.id).await { warn!("cancel remainder failed: {}", e); }
                }
                filled
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Bot state
// ---------------------------------------------------------------------------

#[derive(Default)]
struct BotState {
    books: HashMap<String, Book>, positions: HashMap<String, i64>,
    realized_pnl: f64, monitor: ArbMonitor,
}
impl BotState {
    fn best_bid(&self, p: &str) -> Option<(f64, i64)> { self.books.get(p)?.best_bid() }
    fn best_ask(&self, p: &str) -> Option<(f64, i64)> { self.books.get(p)?.best_ask() }
    fn mid     (&self, p: &str) -> Option<f64>         { self.books.get(p)?.mid() }
    fn can_buy (&self, p: &str) -> bool { self.positions.get(p).copied().unwrap_or(0) <  MAX_POSITION }
    fn can_sell(&self, p: &str) -> bool { self.positions.get(p).copied().unwrap_or(0) > -MAX_POSITION }
}

// ---------------------------------------------------------------------------
// SSE listener — robust line-buffered parser
// ---------------------------------------------------------------------------

async fn run_sse(base_url: String, token: String, state: Arc<RwLock<BotState>>, username: String) {
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();

    loop {
        info!("SSE connecting...");
        match sse_connect(&client, &base_url, &token, &state, &username).await {
            Ok(())  => info!("SSE stream ended, reconnecting"),
            Err(e)  => error!("SSE error: {} — reconnecting in 3s", e),
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn sse_connect(
    client:   &Client,
    base_url: &str,
    token:    &str,
    state:    &Arc<RwLock<BotState>>,
    username: &str,
) -> Result<()> {
    let resp = client
        .get(format!("{}/api/market/stream", base_url))
        .header("Authorization", token)
        .header("Accept", "text/event-stream; charset=utf-8")
        // No .timeout() here — SSE is a long-lived stream
        .send().await
        .context("SSE GET failed")?;

    info!("SSE connected, status={}", resp.status());

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        bail!("SSE bad status — body: {}", body);
    }

    let mut stream = resp.bytes_stream();

    // Accumulate raw bytes into a string buffer; parse complete SSE messages
    let mut buf = String::new();

    while let Some(item) = stream.next().await {
        match item {
            Err(e) => {
                // Log but don't crash — just return so we reconnect
                warn!("SSE chunk error: {}", e);
                return Ok(());
            }
            Ok(bytes) => {
                let text = String::from_utf8_lossy(&bytes);
                debug!("SSE raw chunk: {:?}", text);
                buf.push_str(&text);

                // A complete SSE message ends with "\n\n"
                while let Some(pos) = buf.find("\n\n") {
                    let msg = buf[..pos].to_string();
                    buf = buf[pos + 2..].to_string();
                    if !msg.trim().is_empty() {
                        dispatch_sse_message(&msg, state, username).await;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn dispatch_sse_message(msg: &str, state: &Arc<RwLock<BotState>>, username: &str) {
    let mut event_type = String::new();
    let mut data_parts: Vec<&str> = Vec::new();

    for line in msg.lines() {
        if let Some(v) = line.strip_prefix("event:") { event_type = v.trim().to_string(); }
        else if let Some(v) = line.strip_prefix("data:") { data_parts.push(v.trim()); }
    }

    // data: lines can be split across multiple lines in SSE — join them
    let data = data_parts.join("");
    if data.is_empty() { return; }

    debug!("SSE event='{}' data={}", event_type, &data[..data.len().min(120)]);

    match event_type.as_str() {
        "order" => {
            match serde_json::from_str::<SseOrderEvent>(&data) {
                Ok(ev) => {
                    let product = ev.product.clone();
                    let book = Book::from_sse(ev);
                    debug!("Book update: {} bids={} asks={}", product, book.bids.len(), book.asks.len());
                    state.write().await.books.insert(product, book);
                }
                Err(e) => warn!("Failed to parse order event: {} | data: {}", e, &data[..data.len().min(200)]),
            }
        }
        "trade" => {
            let trades: Vec<TradeEvent> =
                serde_json::from_str::<Vec<TradeEvent>>(&data)
                    .or_else(|_| serde_json::from_str::<TradeEvent>(&data).map(|t| vec![t]))
                    .unwrap_or_default();

            let mut s = state.write().await;
            for t in trades {
                let is_buyer  = t.buyer  == username;
                let is_seller = t.seller == username;
                if !is_buyer && !is_seller { continue; }
                let sign: i64 = if is_buyer { 1 } else { -1 };
                *s.positions.entry(t.product.clone()).or_default() += sign * t.volume;
                s.realized_pnl += if is_buyer { -t.price * t.volume as f64 } else { t.price * t.volume as f64 };
                println!("  [FILL] {} {} {} @ {} | pos={} | pnl={:.0}",
                    if is_buyer { "BOT" } else { "SLD" }, t.volume, t.product, t.price,
                    s.positions[&t.product], s.realized_pnl);
            }
        }
        other => { debug!("Unknown SSE event type: '{}'", other); }
    }
}

// ---------------------------------------------------------------------------
// Arb execution
// ---------------------------------------------------------------------------

async fn execute_arb(direction: &'static str, size: i64, state: &Arc<RwLock<BotState>>, client: &ExchangeClient) {
    println!("\n[ARB] Attempting {} | size={}", direction, size);

    let (etf_price, leg_prices, etf_side, leg_side) = {
        let s = state.read().await;
        if direction == "SELL_ETF" {
            let Some((ep, _)) = s.best_bid(ETF) else { println!("[ARB] No ETF bid"); return; };
            let mut lp = HashMap::new();
            for leg in &LEGS {
                let Some((p, _)) = s.best_ask(leg) else { println!("[ARB] No ask for {}", leg); return; };
                lp.insert(leg.to_string(), p);
            }
            // Re-validate edge with fresh prices
            let leg_sum: f64 = lp.values().sum();
            if ep - leg_sum < MIN_EDGE_ABS {
                println!("[ARB] Edge gone on re-check: ETF_bid={} leg_asks={:.1} edge={:.1}", ep, leg_sum, ep - leg_sum);
                return;
            }
            (ep, lp, Side::Sell, Side::Buy)
        } else {
            let Some((ep, _)) = s.best_ask(ETF) else { println!("[ARB] No ETF ask"); return; };
            let mut lp = HashMap::new();
            for leg in &LEGS {
                let Some((p, _)) = s.best_bid(leg) else { println!("[ARB] No bid for {}", leg); return; };
                lp.insert(leg.to_string(), p);
            }
            // Re-validate edge with fresh prices
            let leg_sum: f64 = lp.values().sum();
            if leg_sum - ep < MIN_EDGE_ABS {
                println!("[ARB] Edge gone on re-check: ETF_ask={} leg_bids={:.1} edge={:.1}", ep, leg_sum, leg_sum - ep);
                return;
            }
            (ep, lp, Side::Buy, Side::Sell)
        }
    };

    let etf_filled = client.send_ioc(&OrderRequest {
        product: ETF.to_string(), price: etf_price, side: etf_side, volume: size,
    }).await;

    let mut leg_fills = HashMap::new();
    for leg in &LEGS {
        let filled = if etf_filled > 0 {
            client.send_ioc(&OrderRequest {
                product: leg.to_string(), price: leg_prices[*leg], side: leg_side, volume: etf_filled,
            }).await
        } else { 0 };
        leg_fills.insert(leg.to_string(), filled);
    }

    state.write().await.monitor.record(ArbRecord { direction, target_size: size, etf_filled, leg_fills });
}

// ---------------------------------------------------------------------------
// Scan
// ---------------------------------------------------------------------------

async fn scan(state: &Arc<RwLock<BotState>>, client: &ExchangeClient) {
    let (sell_edge, buy_edge, etf_mid, basket) = {
        let s = state.read().await;

        // Debug: show which books we have
        let have: Vec<&str> = LEGS.iter().copied().filter(|l| s.books.contains_key(*l)).collect();
        let leg_mids: Vec<f64> = LEGS.iter().filter_map(|l| s.mid(l)).collect();

        if leg_mids.len() != LEGS.len() {
            println!("[SCAN] Waiting for books — have: {:?} (need all of {:?})", have, LEGS);
            return;
        }
        let basket: f64 = leg_mids.iter().sum();

        // Use executable prices for edge: sell_edge uses leg asks, buy_edge uses leg bids
        let leg_ask_sum: Option<f64> = LEGS.iter().map(|l| s.best_ask(l).map(|(p, _)| p)).collect::<Option<Vec<_>>>().map(|v| v.iter().sum());
        let leg_bid_sum: Option<f64> = LEGS.iter().map(|l| s.best_bid(l).map(|(p, _)| p)).collect::<Option<Vec<_>>>().map(|v| v.iter().sum());

        (
            // SELL_ETF edge: sell ETF at bid, buy legs at asks
            s.best_bid(ETF).and_then(|(p, _)| leg_ask_sum.map(|la| p - la)),
            // BUY_ETF edge: buy ETF at ask, sell legs at bids
            s.best_ask(ETF).and_then(|(p, _)| leg_bid_sum.map(|lb| lb - p)),
            s.mid(ETF),
            basket,
        )
    };

    println!("[SCAN] etf_mid={:?} basket={:.1} sell_edge={:?} buy_edge={:?}", etf_mid, basket, sell_edge, buy_edge);

    if let Some(edge) = sell_edge {
        if edge >= MIN_EDGE_ABS {
            let size = {
                let s = state.read().await;
                if !s.can_sell(ETF) || LEGS.iter().any(|l| !s.can_buy(l)) { println!("[ARB] Position limit — skipping SELL_ETF"); return; }
                let evol = s.best_bid(ETF).map(|(_, v)| v).unwrap_or(0);
                let lvols: Vec<i64> = LEGS.iter().map(|l| s.best_ask(l).map(|(_, v)| v).unwrap_or(0)).collect();
                [ORDER_SIZE, evol].iter().chain(lvols.iter()).copied().min().unwrap_or(0)
            };
            if size > 0 { execute_arb("SELL_ETF", size, state, client).await; }
            return;
        }
    }

    if let Some(edge) = buy_edge {
        if edge >= MIN_EDGE_ABS {
            let size = {
                let s = state.read().await;
                if !s.can_buy(ETF) || LEGS.iter().any(|l| !s.can_sell(l)) { println!("[ARB] Position limit — skipping BUY_ETF"); return; }
                let evol = s.best_ask(ETF).map(|(_, v)| v).unwrap_or(0);
                let lvols: Vec<i64> = LEGS.iter().map(|l| s.best_bid(l).map(|(_, v)| v).unwrap_or(0)).collect();
                [ORDER_SIZE, evol].iter().chain(lvols.iter()).copied().min().unwrap_or(0)
            };
            if size > 0 { execute_arb("BUY_ETF", size, state, client).await; }
        }
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                // Default: info. Use RUST_LOG=debug to see raw SSE chunks.
                .unwrap_or_else(|_| "etf_arb_bot=info".parse().unwrap()),
        )
        .init();

    let client = ExchangeClient::login(EXCHANGE_URL, USERNAME, PASSWORD)
        .await.context("Login failed")?;

    let state: Arc<RwLock<BotState>> = Arc::new(RwLock::new(BotState::default()));

    match client.get_positions().await {
        Ok(pos) => { info!("Initial positions: {:?}", pos); state.write().await.positions = pos; }
        Err(e)  => warn!("Position sync failed: {}", e),
    }

    let (sse_state, sse_url, sse_token) = (Arc::clone(&state), EXCHANGE_URL.to_string(), client.token.clone());
    tokio::spawn(async move { run_sse(sse_url, sse_token, sse_state, USERNAME.to_string()).await; });

    // Give SSE time to populate books before scanning
    info!("Waiting for books...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    info!("ETF Arb bot started | min_edge={} size={} max_pos={}", MIN_EDGE_ABS, ORDER_SIZE, MAX_POSITION);

    let shutdown = Arc::new(tokio::sync::Notify::new());
    let sd2 = Arc::clone(&shutdown);
    tokio::spawn(async move { tokio::signal::ctrl_c().await.ok(); sd2.notify_one(); });

    let interval = Duration::from_millis(LOOP_INTERVAL_MS);
    loop {
        tokio::select! {
            _ = shutdown.notified() => break,
            _ = tokio::time::sleep(interval) => { scan(&state, &client).await; }
        }
    }

    println!("\nShutting down...");

    let s = state.read().await;
    println!("\n{}", "=".repeat(55));
    println!("  Arb attempts : {}", s.monitor.records.len());
    println!("  Full fills   : {}", s.monitor.records.iter().filter(|r| r.is_full()).count());
    println!("  Partial fills: {}", s.monitor.records.iter().filter(|r| !r.is_full() && !r.is_etf_miss()).count());
    println!("  ETF misses   : {}", s.monitor.records.iter().filter(|r| r.is_etf_miss()).count());
    println!("  Realized PnL : {:.2}", s.realized_pnl);
    println!("  Positions    : {:?}", s.positions);
    println!("{}", "=".repeat(55));
    Ok(())
}