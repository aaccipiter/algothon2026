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

const ETF_STRIKE:              f64  = 7000.0; // pivot point in the relationship
const RISK_PREMIUM:            f64  = 50.0;   // extra edge required for the ETF-long leg
const MIN_EDGE_LONG_FLY:       f64  = 5.0;    // min edge required for the FLY-long leg
const ORDER_SIZE:              i64  = 1;
const MAX_POSITION:            i64  = 50;     // per-product
const LOOP_INTERVAL_MS:        u64  = 2_000;
const MIN_REQUEST_INTERVAL_MS: u64  = 1_100;

const ETF: &str = "LON_ETF";
const FLY: &str = "LON_FLY";


#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
enum Side { Buy, Sell }

#[derive(Debug, Clone, Deserialize)]
struct SseVolumeEntry {
    #[serde(rename = "marketVolume")] market_volume: f64,
    #[serde(rename = "userVolume")]   user_volume:   f64,
}

#[derive(Debug, Clone, Deserialize)]
struct SseOrderEvent {
    #[serde(rename = "productsymbol")] product:     String,
    #[serde(rename = "buyOrders")]     buy_orders:  HashMap<String, SseVolumeEntry>,
    #[serde(rename = "sellOrders")]    sell_orders: HashMap<String, SseVolumeEntry>,
}

#[derive(Debug, Clone, Deserialize)]
struct TradeEvent {
    product: String, price: f64, volume: i64, buyer: String, seller: String,
}

#[derive(Debug, Clone, Serialize)]
struct OrderRequest { product: String, price: f64, side: Side, volume: i64 }

#[derive(Debug, Clone, Deserialize)]
struct OrderResponse { id: String, volume: i64, filled: i64 }

#[derive(Debug, Clone, Deserialize)]
struct ActiveOrder { id: String }

#[derive(Debug, Clone, Deserialize)]
struct PositionEntry {
    product: String,
    #[serde(rename = "netPosition")] net_position: i64,
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


struct RateLimiter { min_interval: Duration, last_call: Mutex<Instant> }
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


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Signal { LongFlyShortEtf, LongEtfShortFly }

impl Signal {
    fn label(&self) -> &'static str {
        match self { Signal::LongFlyShortEtf => "LONG_FLY/SHORT_ETF", Signal::LongEtfShortFly => "LONG_ETF/SHORT_FLY" }
    }
}

#[derive(Debug, Clone)]
struct TradeRecord {
    signal:       Signal,
    target_size:  i64,
    etf_filled:   i64,
    fly_filled:   i64,
    edge_at_entry: f64,   // theoretical edge when we fired
}

impl TradeRecord {
    fn etf_target(&self) -> i64 { self.target_size * 2 } // 2:1 hedge ratio
    fn is_full   (&self) -> bool { self.etf_filled == self.etf_target() && self.fly_filled == self.target_size }
    fn is_miss   (&self) -> bool { self.etf_filled == 0 && self.fly_filled == 0 }
    fn is_partial(&self) -> bool { !self.is_full() && !self.is_miss() }
}

#[derive(Default)]
struct TradeMonitor { records: Vec<TradeRecord> }
impl TradeMonitor {
    fn record(&mut self, r: TradeRecord) {
        let status = if r.is_full() { "✓ FULL" } else if r.is_miss() { "✗ MISS" } else { "~ PARTIAL" };
        println!("\n  [MONITOR] {} | {} | fly_target={} etf_target={}", status, r.signal.label(), r.target_size, r.etf_target());
        println!("    ETF : {}/{}", r.etf_filled, r.etf_target());
        println!("    FLY : {}/{}", r.fly_filled, r.target_size);
        println!("    Edge at entry: {:.1}", r.edge_at_entry);

        // Warn about unhedged exposure (2:1 ratio: etf_filled should be 2×fly_filled)
        let etf_residual = r.etf_filled - r.fly_filled * 2;
        if etf_residual != 0 {
            println!("    ⚠ Unhedged ETF residual: {}", etf_residual);
        }

        self.records.push(r);
        self.print_summary();
    }

    fn print_summary(&self) {
        let total = self.records.len();
        if total == 0 { return; }
        let full    = self.records.iter().filter(|r| r.is_full()).count();
        let partial = self.records.iter().filter(|r| r.is_partial()).count();
        let misses  = self.records.iter().filter(|r| r.is_miss()).count();
        let long_fly = self.records.iter().filter(|r| r.signal == Signal::LongFlyShortEtf).count();
        let long_etf = self.records.iter().filter(|r| r.signal == Signal::LongEtfShortFly).count();
        println!(
            "  [MONITOR] Trades: {} | Full: {} ({:.0}%) | Partial: {} | Miss: {} | LongFly: {} LongETF: {}",
            total, full, 100.0*full as f64/total as f64, partial, misses, long_fly, long_etf,
        );
    }
}


#[derive(Clone)]
struct ExchangeClient { base_url: String, client: Client, token: String, limiter: Arc<RateLimiter> }

impl ExchangeClient {
    async fn login(base_url: &str, username: &str, password: &str) -> Result<Self> {
        let client = Client::new();
        let resp = client
            .post(format!("{}/api/user/authenticate", base_url))
            .header("Content-Type", "application/json; charset=utf-8")
            .json(&serde_json::json!({ "username": username, "password": password }))
            .send().await.context("Login request failed")?;

        if !resp.status().is_success() {
            bail!("Login HTTP {} — {}", resp.status(), resp.text().await.unwrap_or_default());
        }
        let token = resp.headers().get("Authorization")
            .context("No Authorization header")?.to_str()?.to_string();
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


#[derive(Default)]
struct BotState {
    books:        HashMap<String, Book>,
    positions:    HashMap<String, i64>,
    realized_pnl: f64,
    monitor:      TradeMonitor,
}

impl BotState {
    fn best_bid(&self, p: &str) -> Option<(f64, i64)> { self.books.get(p)?.best_bid() }
    fn best_ask(&self, p: &str) -> Option<(f64, i64)> { self.books.get(p)?.best_ask() }
    fn mid     (&self, p: &str) -> Option<f64>         { self.books.get(p)?.mid() }
    fn pos     (&self, p: &str) -> i64                 { self.positions.get(p).copied().unwrap_or(0) }
    fn can_buy (&self, p: &str) -> bool { self.pos(p) <  MAX_POSITION }
    fn can_sell(&self, p: &str) -> bool { self.pos(p) > -MAX_POSITION }
}


async fn run_sse(base_url: String, token: String, state: Arc<RwLock<BotState>>, username: String) {
    let client = Client::builder().build().unwrap();
    loop {
        info!("SSE connecting...");
        match sse_connect(&client, &base_url, &token, &state, &username).await {
            Ok(())  => info!("SSE stream ended, reconnecting"),
            Err(e)  => error!("SSE error: {} — reconnecting in 3s", e),
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn sse_connect(client: &Client, base_url: &str, token: &str, state: &Arc<RwLock<BotState>>, username: &str) -> Result<()> {
    let resp = client
        .get(format!("{}/api/market/stream", base_url))
        .header("Authorization", token)
        .header("Accept", "text/event-stream; charset=utf-8")
        .send().await.context("SSE GET failed")?;

    info!("SSE connected, status={}", resp.status());
    if !resp.status().is_success() { bail!("SSE bad status"); }

    let mut stream = resp.bytes_stream();
    let mut buf = String::new();

    while let Some(item) = stream.next().await {
        match item {
            Err(e) => { warn!("SSE chunk error: {}", e); return Ok(()); }
            Ok(bytes) => {
                buf.push_str(&String::from_utf8_lossy(&bytes));
                while let Some(pos) = buf.find("\n\n") {
                    let msg = buf[..pos].to_string();
                    buf = buf[pos + 2..].to_string();
                    if !msg.trim().is_empty() { dispatch_sse(&msg, state, username).await; }
                }
            }
        }
    }
    Ok(())
}

async fn dispatch_sse(msg: &str, state: &Arc<RwLock<BotState>>, username: &str) {
    let mut event_type = String::new();
    let mut data_parts: Vec<&str> = Vec::new();
    for line in msg.lines() {
        if let Some(v) = line.strip_prefix("event:") { event_type = v.trim().to_string(); }
        else if let Some(v) = line.strip_prefix("data:") { data_parts.push(v.trim()); }
    }
    let data = data_parts.join("");
    if data.is_empty() { return; }

    match event_type.as_str() {
        "order" => {
            match serde_json::from_str::<SseOrderEvent>(&data) {
                Ok(ev) => {
                    let product = ev.product.clone();
                    let book = Book::from_sse(ev);
                    state.write().await.books.insert(product, book);
                }
                Err(e) => warn!("Failed to parse order event: {} | {}", e, &data[..data.len().min(120)]),
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
        _ => {}
    }
}


fn evaluate_signals(s: &BotState) -> Option<(Signal, f64, f64, f64, i64)> {
    let etf_bid = s.best_bid(ETF)?;
    let etf_ask = s.best_ask(ETF)?;
    let fly_bid = s.best_bid(FLY)?;
    let fly_ask = s.best_ask(FLY)?;

    // Signal 1: LONG FLY / SHORT ETF
    //   Condition: ETF_bid - 7000 > FLY_ask / 2
    //   Edge = (ETF_bid - 7000) - (FLY_ask / 2)
    //   We sell 2×ETF at bid, buy 1×FLY at ask (2:1 hedge ratio, since dFLY ≈ 2×dETF)
    let edge_long_fly = (etf_bid.0 - ETF_STRIKE) - (fly_ask.0 / 2.0);
    if edge_long_fly >= MIN_EDGE_LONG_FLY && s.can_sell(ETF) && s.can_buy(FLY) {
        // size = number of FLY units; ETF volume will be 2×size
        let size = [ORDER_SIZE, etf_bid.1 / 2, fly_ask.1].iter().copied().min().unwrap();
        if size > 0 {
            return Some((Signal::LongFlyShortEtf, edge_long_fly, etf_bid.0, fly_ask.0, size));
        }
    }

    // Signal 2: LONG ETF / SHORT FLY
    //   Condition: FLY_bid / 2 > ETF_ask - 7000 + RISK_PREMIUM
    //   Edge = (FLY_bid / 2) - (ETF_ask - 7000)
    //   We buy 2×ETF at ask, sell 1×FLY at bid (2:1 hedge ratio)
    let edge_long_etf = (fly_bid.0 / 2.0) - (etf_ask.0 - ETF_STRIKE);
    if edge_long_etf > RISK_PREMIUM && s.can_buy(ETF) && s.can_sell(FLY) {
        // size = number of FLY units; ETF volume will be 2×size
        let size = [ORDER_SIZE, etf_ask.1 / 2, fly_bid.1].iter().copied().min().unwrap();
        if size > 0 {
            return Some((Signal::LongEtfShortFly, edge_long_etf, etf_ask.0, fly_bid.0, size));
        }
    }

    None
}


async fn execute(signal: Signal, _edge: f64, _etf_price: f64, _fly_price: f64, size: i64,
                 state: &Arc<RwLock<BotState>>, client: &ExchangeClient) {

    let (etf_side, fly_side) = match signal {
        Signal::LongFlyShortEtf => (Side::Sell, Side::Buy),
        Signal::LongEtfShortFly => (Side::Buy,  Side::Sell),
    };

    // Re-read book for fresh prices and re-validate edge before firing
    let (etf_price, fly_price, edge) = {
        let s = state.read().await;
        let result = evaluate_signals(&s);
        match result {
            Some((sig, e, ep, fp, _)) if sig == signal => (ep, fp, e),
            _ => {
                println!("[TRADE] Edge gone on re-check for {} — skipping", signal.label());
                return;
            }
        }
    };

    let etf_size = size * 2; // 2:1 hedge ratio (dFLY ≈ 2×dETF)
    println!("\n[TRADE] {} | fly_size={} etf_size={} | edge={:.1} | etf={} fly={}",
        signal.label(), size, etf_size, edge, etf_price, fly_price);

    // ETF first (more liquid anchor leg) — 2× volume for hedge ratio
    let etf_filled = client.send_ioc(&OrderRequest {
        product: ETF.to_string(), price: etf_price, side: etf_side, volume: etf_size,
    }).await;

    // FLY leg — sized to half of ETF fill to maintain 2:1 ratio
    let fly_target = etf_filled / 2;
    let fly_filled = if fly_target > 0 {
        client.send_ioc(&OrderRequest {
            product: FLY.to_string(), price: fly_price, side: fly_side, volume: fly_target,
        }).await
    } else { 0 };

    state.write().await.monitor.record(TradeRecord {
        signal, target_size: size, etf_filled, fly_filled, edge_at_entry: edge,
    });
}


async fn scan(state: &Arc<RwLock<BotState>>, client: &ExchangeClient) {
    let result = {
        let s = state.read().await;

        if !s.books.contains_key(ETF) || !s.books.contains_key(FLY) {
            println!("[SCAN] Waiting for books — have: ETF={} FLY={}",
                s.books.contains_key(ETF), s.books.contains_key(FLY));
            return;
        }

        let etf_mid = s.mid(ETF);
        let fly_mid = s.mid(FLY);

        // Log current state every scan for visibility
        println!("[SCAN] ETF_mid={:?}  FLY_mid={:?}  ETF_pos={}  FLY_pos={}  pnl={:.0}",
            etf_mid, fly_mid, s.pos(ETF), s.pos(FLY), s.realized_pnl);

        if let (Some(em), Some(fm)) = (etf_mid, fly_mid) {
            println!("       implied_fly={:.1}  fly_mid={:.1}  diff={:.1}",
                (em - ETF_STRIKE) * 2.0, fm, fm - (em - ETF_STRIKE) * 2.0);
        }

        evaluate_signals(&s).map(|(sig, edge, ep, fp, sz)| (sig, edge, ep, fp, sz))
    };

    if let Some((signal, edge, etf_price, fly_price, size)) = result {
        execute(signal, edge, etf_price, fly_price, size, state, client).await;
    }
}


#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "fly_arb=info".parse().unwrap()),
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

    info!("Waiting for books...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    info!("FLY/ETF RV bot | strike={} risk_premium={} size={} max_pos={}",
        ETF_STRIKE, RISK_PREMIUM, ORDER_SIZE, MAX_POSITION);

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
    let records = &s.monitor.records;
    println!("\n{}", "=".repeat(55));
    println!("  SESSION SUMMARY");
    println!("{}", "=".repeat(55));
    println!("  Trades       : {}", records.len());
    println!("  Full fills   : {}", records.iter().filter(|r| r.is_full()).count());
    println!("  Partial fills: {}", records.iter().filter(|r| r.is_partial()).count());
    println!("  Misses       : {}", records.iter().filter(|r| r.is_miss()).count());
    println!("  Long FLY     : {}", records.iter().filter(|r| r.signal == Signal::LongFlyShortEtf).count());
    println!("  Long ETF     : {}", records.iter().filter(|r| r.signal == Signal::LongEtfShortFly).count());
    println!("  Realized PnL : {:.2}", s.realized_pnl);
    println!("  ETF position : {}", s.pos(ETF));
    println!("  FLY position : {}", s.pos(FLY));
    println!("{}", "=".repeat(55));
    Ok(())
}