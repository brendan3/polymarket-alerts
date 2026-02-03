#!/usr/bin/env python3
"""
Polymarket +EV Signal Scanner

Strategies:
1. Complement Arbitrage: Markets that should sum to 100% but don't
2. Early Sweep Detection: Alert at START of aggressive flow (not after)
3. Stale Quote Hunting: Wide spreads after volume spikes = opportunity
4. Whale Order Detection: Large resting orders in the book
"""

from __future__ import annotations

import argparse
import json
import math
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import requests
from websocket import WebSocketApp

CLOB_WS_BASE = "wss://ws-subscriptions-clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"


# -------------------------
# Helpers
# -------------------------

def env_truthy(v: Optional[str]) -> bool:
    return v is not None and v.strip().lower() in {"1", "true", "yes", "y", "on"}


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def safe_float(x: Any) -> Optional[float]:
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def alpha_score(notional: float, pct_of_depth: float) -> float:
    """Calculate alpha score: (pct_of_depth / 100.0) * log10(notional + 1.0) * 100.0"""
    return (pct_of_depth / 100.0) * math.log10(notional + 1.0) * 100.0


def send_telegram(body: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print(f"[ALERT] {body}")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": body, "disable_web_page_preview": True},
            timeout=10,
        )
    except Exception as e:
        print(f"[telegram error] {e}")


# -------------------------
# Data Models
# -------------------------

@dataclass
class Market:
    condition_id: str
    question: str
    event_slug: Optional[str]
    outcomes: List[str]
    clob_token_ids: List[str]  # [YES_token, NO_token] for binary
    volume: Optional[float] = None
    liquidity: Optional[float] = None

    def url(self) -> str:
        if self.event_slug:
            return f"https://polymarket.com/event/{self.event_slug}"
        return ""

    def outcome_for_token(self, token_id: str) -> str:
        try:
            idx = self.clob_token_ids.index(token_id)
            return self.outcomes[idx] if idx < len(self.outcomes) else "?"
        except ValueError:
            return "?"


@dataclass
class EventGroup:
    """Markets that belong to the same event (e.g., 'Who wins election?' with multiple candidates)"""
    event_slug: str
    event_title: str
    markets: List[Market] = field(default_factory=list)


# -------------------------
# Gamma Fetch
# -------------------------

def fetch_markets(per_page: int = 100, pages: int = 50) -> Tuple[Dict[str, Market], Dict[str, EventGroup]]:
    """Fetch markets and group by event for complement detection"""
    markets: Dict[str, Market] = {}  # token_id -> Market
    events: Dict[str, EventGroup] = {}  # event_slug -> EventGroup

    for p in range(pages):
        try:
            r = requests.get(
                f"{GAMMA_BASE}/markets",
                params={"limit": per_page, "offset": p * per_page, "closed": "false", "active": "true"},
                timeout=20,
            )
            r.raise_for_status()
            data = r.json()
            items = data if isinstance(data, list) else data.get("markets", [])
        except Exception as e:
            print(f"[fetch error] page {p}: {e}")
            continue

        for m in items:
            clob_ids = m.get("clobTokenIds") or m.get("clob_token_ids") or []
            if not clob_ids:
                continue

            if isinstance(clob_ids, str):
                try:
                    clob_ids = json.loads(clob_ids)
                except:
                    clob_ids = [clob_ids]

            outcomes = m.get("outcomes") or []
            if isinstance(outcomes, str):
                try:
                    outcomes = json.loads(outcomes)
                except:
                    outcomes = [outcomes]

            market = Market(
                condition_id=m.get("conditionId") or m.get("condition_id") or "",
                question=m.get("question") or "?",
                event_slug=m.get("eventSlug") or m.get("slug"),
                outcomes=list(outcomes),
                clob_token_ids=list(clob_ids),
                volume=safe_float(m.get("volume")),
                liquidity=safe_float(m.get("liquidity")),
            )

            for tid in clob_ids:
                markets[tid] = market

            # Group by event for complement detection
            slug = market.event_slug
            if slug:
                if slug not in events:
                    events[slug] = EventGroup(
                        event_slug=slug,
                        event_title=m.get("eventTitle") or m.get("title") or "?",
                    )
                events[slug].markets.append(market)

        time.sleep(0.15)

    return markets, events


# -------------------------
# Strategy 1: Complement Arbitrage
# -------------------------

class ComplementArbitrageDetector:
    """
    Detects when mutually exclusive outcomes don't sum to ~100%.
    Modified to alert immediately if current known prices > 100%.
    """

    def __init__(self, events: Dict[str, EventGroup], min_edge_pct: float = 3.0, min_liquidity: float = 5000):
        self.events = events
        self.min_edge_pct = min_edge_pct  
        self.min_liquidity = min_liquidity
        self.prices: Dict[str, float] = {}  # token_id -> last price
        self.last_alert: Dict[str, float] = {}  # event_slug -> timestamp
        self.alert_cooldown = 300  

    def update_price(self, token_id: str, price: float) -> None:
        self.prices[token_id] = price

    def check_event(self, event_slug: str) -> Optional[str]:
        if event_slug not in self.events:
            return None

        now = time.time()
        if event_slug in self.last_alert and (now - self.last_alert[event_slug]) < self.alert_cooldown:
            return None

        event = self.events[event_slug]
        if len(event.markets) < 2:
            return None

        total_prob = 0.0
        outcome_prices: List[Tuple[str, float]] = []
        missing_any = False

        for market in event.markets:
            if not market.clob_token_ids:
                continue
            yes_token = market.clob_token_ids[0]
            if yes_token not in self.prices:
                missing_any = True
                continue
            
            price = self.prices[yes_token]
            total_prob += price
            outcome_name = market.outcomes[0] if market.outcomes else market.question[:30]
            outcome_prices.append((outcome_name, price))

        # --- ALPHA LOGIC ---
        # 1. OVERPRICED: If sum > 100%, it's an alert even if tokens are missing.
        # 2. UNDERPRICED: If sum < 100%, we ONLY alert if we have ALL prices.
        
        is_arb = False
        direction = ""
        
        if total_prob > (1.0 + (self.min_edge_pct / 100)):
            is_arb = True
            direction = "OVERPRICED (short all)"
        elif not missing_any and total_prob < (1.0 - (self.min_edge_pct / 100)):
            is_arb = True
            direction = "UNDERPRICED (long all)"

        if is_arb:
            self.last_alert[event_slug] = now
            deviation = abs(total_prob - 1.0) * 100
            
            lines = [
                f"⚡ COMPLEMENT ARB: {event.event_title}",
                f"Total implied prob: {total_prob*100:.1f}% ({direction})",
                f"Edge: {deviation:.1f}%",
                f"Status: {'Partial Data' if missing_any else 'Full Event Data'}",
                "",
            ]
            for name, price in outcome_prices:
                lines.append(f"  • {name}: {price*100:.1f}%")
            lines.append(f"\nhttps://polymarket.com/event/{event_slug}")
            
            return "\n".join(lines)

        return None


# -------------------------
# Strategy 2: Early Sweep Detection
# -------------------------

@dataclass
class TradeRecord:
    ts: float
    price: float
    size: float
    direction: str  # BUY or SELL


class EarlySweepDetector:
    """
    Detect aggressive flow AS IT HAPPENS - not after confirmation.
    
    Alert when:
    - N+ trades in same direction within T seconds
    - Total notional exceeds threshold
    - Average trade size is large (not retail noise)
    
    This is actionable because you get the alert DURING the sweep, not after.
    """

    def __init__(
        self,
        markets: Dict[str, Market],
        window_sec: int = 10,  # Shorter window = earlier detection
        min_trades: int = 2,
        min_notional: float = 1000,
        min_avg_size: float = 500,  # Filter out small retail trades
    ):
        self.markets = markets
        self.window_sec = window_sec
        self.min_trades = min_trades
        self.min_notional = min_notional
        self.min_avg_size = min_avg_size

        self.trades: Dict[str, Deque[TradeRecord]] = defaultdict(lambda: deque(maxlen=500))
        self.last_alert: Dict[str, float] = {}
        self.alert_cooldown = 120  # 2 minutes
        self.last_quote: Dict[str, Tuple[float, float]] = {}  # token -> (bid, ask)

    def update_quote(self, token_id: str, bid: float, ask: float) -> None:
        self.last_quote[token_id] = (bid, ask)

    def add_trade(self, token_id: str, price: float, size: float) -> Optional[str]:
        if token_id not in self.markets:
            return None

        now = time.time()
        
        # Infer direction from quote
        direction = "BUY"
        if token_id in self.last_quote:
            bid, ask = self.last_quote[token_id]
            mid = (bid + ask) / 2
            direction = "BUY" if price >= mid else "SELL"
        else:
            direction = "BUY" if price >= 0.5 else "SELL"

        self.trades[token_id].append(TradeRecord(ts=now, price=price, size=size, direction=direction))

        # Check for sweep pattern
        return self._check_sweep(token_id)

    def _check_sweep(self, token_id: str) -> Optional[str]:
        now = time.time()
        
        if token_id in self.last_alert and (now - self.last_alert[token_id]) < self.alert_cooldown:
            return None

        recent = [t for t in self.trades[token_id] if now - t.ts <= self.window_sec]
        if len(recent) < self.min_trades:
            return None

        # Count by direction
        buys = [t for t in recent if t.direction == "BUY"]
        sells = [t for t in recent if t.direction == "SELL"]

        # Determine dominant direction
        if len(buys) >= self.min_trades:
            sweep_trades = buys
            direction = "BUY"
        elif len(sells) >= self.min_trades:
            sweep_trades = sells
            direction = "SELL"
        else:
            return None

        total_notional = sum(t.price * t.size for t in sweep_trades)
        if total_notional < self.min_notional:
            return None

        avg_size = sum(t.size for t in sweep_trades) / len(sweep_trades)
        if avg_size < self.min_avg_size:
            return None

        # Calculate aggression: how much price moved during sweep
        prices = [t.price for t in sweep_trades]
        price_range = max(prices) - min(prices)

        self.last_alert[token_id] = now
        market = self.markets[token_id]
        outcome = market.outcome_for_token(token_id)

        vwap = total_notional / sum(t.size for t in sweep_trades)

        return (
            f"🔥 SWEEP DETECTED: {market.question[:60]}\n"
            f"Outcome: {outcome} | Direction: {direction}\n"
            f"Trades: {len(sweep_trades)} in {self.window_sec}s\n"
            f"Notional: ${total_notional:,.0f} | Avg size: {avg_size:,.0f}\n"
            f"VWAP: {vwap:.3f} | Price range: {price_range:.3f}\n"
            f"{market.url()}"
        )


# -------------------------
# Strategy 3: Stale Quote / Wide Spread Hunting
# -------------------------

class StaleQuoteDetector:
    """
    Detect when spreads widen after volume activity.
    
    Wide spread + recent volume = market maker pulled quotes, potential opportunity
    to provide liquidity or take a position before spread normalizes.
    """

    def __init__(
        self,
        markets: Dict[str, Market],
        max_spread_normal: float = 0.02,  # 2¢ is normal
        min_spread_alert: float = 0.05,   # 5¢+ is wide
        volume_window_sec: int = 300,     # Look for volume in last 5 min
        min_recent_volume: float = 2000,  # Need recent activity
    ):
        self.markets = markets
        self.max_spread_normal = max_spread_normal
        self.min_spread_alert = min_spread_alert
        self.volume_window_sec = volume_window_sec
        self.min_recent_volume = min_recent_volume

        self.spread_history: Dict[str, Deque[Tuple[float, float]]] = defaultdict(lambda: deque(maxlen=100))
        self.volume_history: Dict[str, Deque[Tuple[float, float]]] = defaultdict(lambda: deque(maxlen=500))
        self.last_alert: Dict[str, float] = {}
        self.alert_cooldown = 600  # 10 minutes

    def update_spread(self, token_id: str, spread: float) -> Optional[str]:
        now = time.time()
        self.spread_history[token_id].append((now, spread))

        if token_id not in self.markets:
            return None

        if token_id in self.last_alert and (now - self.last_alert[token_id]) < self.alert_cooldown:
            return None

        if spread < self.min_spread_alert:
            return None

        # Was spread recently tighter?
        recent_spreads = [(ts, s) for ts, s in self.spread_history[token_id] if now - ts <= 300]
        if len(recent_spreads) < 5:
            return None

        min_recent_spread = min(s for _, s in recent_spreads[:-1])  # Exclude current
        if min_recent_spread > self.max_spread_normal:
            return None  # Spread was always wide

        # Check for recent volume
        recent_vol = sum(v for ts, v in self.volume_history[token_id] if now - ts <= self.volume_window_sec)
        if recent_vol < self.min_recent_volume:
            return None

        self.last_alert[token_id] = now
        market = self.markets[token_id]
        outcome = market.outcome_for_token(token_id)

        return (
            f"📊 WIDE SPREAD ALERT: {market.question[:60]}\n"
            f"Outcome: {outcome}\n"
            f"Current spread: {spread*100:.1f}¢ (was {min_recent_spread*100:.1f}¢)\n"
            f"Recent volume: ${recent_vol:,.0f}\n"
            f"Opportunity: Market maker pulled quotes - liquidity provision or direction?\n"
            f"{market.url()}"
        )

    def add_volume(self, token_id: str, notional: float) -> None:
        self.volume_history[token_id].append((time.time(), notional))


# -------------------------
# Strategy 4: Large Resting Order Detection
# -------------------------

class WhaleOrderDetector:
    """
    Tuned for Alpha: Detects significant market-defining orders.
    Ignores retail 'large' orders and passive liquidity reward farmers.
    """

    def __init__(
        self,
        markets: Dict[str, Market],
        min_order_size: float = 30000,  # Raised from $10k to $30k for higher signal
        min_size_vs_depth: float = 0.6,  # Raised from 30% to 50% of the book
    ):
        self.markets = markets
        self.min_order_size = min_order_size
        self.min_size_vs_depth = min_size_vs_depth
        self.last_alert: Dict[str, float] = {}
        self.alert_cooldown = 900 # Increased to 15 mins to prevent flapping

    def check_book(self, token_id: str, bids: List[Tuple[float, float]], asks: List[Tuple[float, float]]) -> Optional[str]:
        if token_id not in self.markets:
            return None

        now = time.time()
        if token_id in self.last_alert and (now - self.last_alert[token_id]) < self.alert_cooldown:
            return None

        alerts = []
        
        for side, levels in [("BID", bids), ("ASK", asks)]:
            # ALPHA FILTER: Only look at the "Meat" of the book (0.15 - 0.85)
            # This ignores the $100k+ orders sitting at 0.90/0.10 for rewards
            valid_levels = [ (p, s) for p, s in levels if 0.15 <= p <= 0.85 ]
            total_depth = sum(p * s for p, s in valid_levels)
            
            for price, size in valid_levels:
                notional = price * size
                if notional >= self.min_order_size:
                    # Is this order defining the book?
                    if total_depth > 0 and notional / total_depth >= self.min_size_vs_depth:
                        alerts.append((side, price, size, notional, total_depth))

        if not alerts:
            return None

        # Calculate alpha_score for each alert and filter to only high-signal alerts (>= 400)
        scored_alerts = []
        for side, price, size, notional, total_depth in alerts:
            pct_of_depth = (notional / total_depth) * 100 if total_depth > 0 else 0.0
            score = alpha_score(notional, pct_of_depth)
            if score >= 400:
                scored_alerts.append((side, price, size, notional, total_depth, pct_of_depth, score))

        if not scored_alerts:
            return None

        # Only show the best alert per market (highest alpha_score) to reduce spam
        best = max(scored_alerts, key=lambda t: t[6])  # t[6] is the score
        scored_alerts = [best]

        self.last_alert[token_id] = now
        market = self.markets[token_id]
        outcome = market.outcome_for_token(token_id)

        lines = [f"🐋 ALPHA WHALE: {market.question[:60]}", f"Outcome: {outcome}", ""]
        for side, price, size, notional, total_depth, pct_of_depth, score in scored_alerts:
            lines.append(
                f"  {side} @ {price:.3f}: ${notional:,.0f} ({pct_of_depth:.0f}% depth) | alpha_score={score:.1f}"
            )
        lines.append(f"\n{market.url()}")

        return "\n".join(lines)
# -------------------------
# Main Scanner
# -------------------------

class EVScanner:
    def __init__(
        self,
        markets: Dict[str, Market],
        events: Dict[str, EventGroup],
        telegram: bool = True,
    ):
        self.markets = markets
        self.events = events
        self.telegram = telegram

        self.token_ids = list(markets.keys())

        # Initialize detectors
        self.complement_detector = ComplementArbitrageDetector(events)
        self.sweep_detector = EarlySweepDetector(markets)
        self.stale_detector = StaleQuoteDetector(markets)
        self.whale_detector = WhaleOrderDetector(markets)

        self._last_msg_ts = time.time()
        self._trade_count = 0  # Track trade events received for verification
        self._last_trade_log_ts = time.time()
        self._start_time = time.time()  # Track start time for rate calculation
        self._event_type_counts: Dict[str, int] = defaultdict(int)  # Track event types for verification

    def _alert(self, msg: str) -> None:
        print(f"\n{'='*60}\n{msg}\n{'='*60}\n")
        if self.telegram:
            send_telegram(msg)

    def _parse_levels(self, levels: Any) -> List[Tuple[float, float]]:
        out = []
        if not isinstance(levels, list):
            return out
        for lv in levels:
            if isinstance(lv, (list, tuple)) and len(lv) >= 2:
                px, sz = safe_float(lv[0]), safe_float(lv[1])
            elif isinstance(lv, dict):
                px = safe_float(lv.get("price") or lv.get("p"))
                sz = safe_float(lv.get("size") or lv.get("s"))
            else:
                continue
            if px is not None and sz is not None:
                out.append((px, sz))
        return out

    def _handle_book(self, data: Dict[str, Any]) -> None:
        token_id = data.get("asset_id") or data.get("token_id") or data.get("market")
        if not token_id or token_id not in self.markets:
            return

        book = data.get("book") if isinstance(data.get("book"), dict) else data
        bids = self._parse_levels(book.get("bids") or book.get("bid") or [])
        asks = self._parse_levels(book.get("asks") or book.get("ask") or [])

        if not bids or not asks:
            return

        best_bid = max(p for p, _ in bids)
        best_ask = min(p for p, _ in asks)
        spread = best_ask - best_bid
        mid = (best_bid + best_ask) / 2

        # Update detectors
        self.complement_detector.update_price(token_id, mid)
        self.sweep_detector.update_quote(token_id, best_bid, best_ask)

        # Check for alerts
        if alert := self.stale_detector.update_spread(token_id, spread):
            self._alert(alert)

        if alert := self.whale_detector.check_book(token_id, bids, asks):
            self._alert(alert)

        # Check complement arbitrage for this market's event
        market = self.markets[token_id]
        if market.event_slug:
            if alert := self.complement_detector.check_event(market.event_slug):
                self._alert(alert)

    def _handle_trade(self, data: Dict[str, Any]) -> None:
        """
        Handle trade events. Supports multiple formats:
        - Single trade: {"event_type": "trade", "asset_id": "...", "price": ..., "size": ...}
        - Trades array: {"event_type": "trade", "trades": [{...}, {...}]}
        - Trade object: {"trades": {...}} (single object in trades field)
        """
        # Handle trades array format
        raw_trades = data.get("trades")
        trades: List[Dict[str, Any]] = []
        
        if isinstance(raw_trades, list):
            trades = [t for t in raw_trades if isinstance(t, dict)]
        elif isinstance(raw_trades, dict):
            trades = [raw_trades]
        else:
            # If no trades array, treat the payload itself as a single trade
            if any(k in data for k in ("asset_id", "token_id", "market", "assetId", "price", "p", "size", "s", "amount", "qty")):
                trades = [data]
        
        # Process each trade
        for trade in trades:
            token_id = (
                trade.get("asset_id") or 
                trade.get("token_id") or 
                trade.get("market") or 
                trade.get("assetId") or
                data.get("asset_id") or 
                data.get("token_id") or 
                data.get("market")
            )
            
            if not token_id or token_id not in self.markets:
                continue

            price = safe_float(
                trade.get("price") or 
                trade.get("p") or 
                trade.get("last_trade_price")
            )
            size = safe_float(
                trade.get("size") or 
                trade.get("s") or 
                trade.get("amount") or 
                trade.get("qty")
            )
            
            if price is None or size is None:
                continue

            # Track trade events for verification
            self._trade_count += 1
            
            # Log first few trades immediately for verification
            if self._trade_count <= 5:
                print(f"[trade] ✓ Trade #{self._trade_count}: token={token_id[:8]}... price={price:.4f} size={size:.2f}")
            
            # Log trade activity periodically (every 60 seconds)
            now = time.time()
            if now - self._last_trade_log_ts >= 60:
                elapsed_min = (now - self._start_time) / 60.0
                rate = self._trade_count / max(1, elapsed_min)
                print(f"[trade] Processed {self._trade_count} trade events total (~{rate:.1f}/min)")
                self._last_trade_log_ts = now

            notional = price * size
            self.stale_detector.add_volume(token_id, notional)
            self.complement_detector.update_price(token_id, price)

            if alert := self.sweep_detector.add_trade(token_id, price, size):
                self._alert(alert)

    def _handle_price(self, token_id: str, price: float) -> None:
        if token_id not in self.markets:
            return
        self.complement_detector.update_price(token_id, price)

        market = self.markets[token_id]
        if market.event_slug:
            if alert := self.complement_detector.check_event(market.event_slug):
                self._alert(alert)

    def run(self) -> None:
        if not self.token_ids:
            print("[scanner] No markets to monitor")
            return

        # Limit to avoid subscription limits
        max_tokens = 1800
        subscribe_ids = self.token_ids[:max_tokens]

        ws_url = f"{CLOB_WS_BASE}/ws/market"

        def on_open(ws):
            time.sleep(0.25)
            ws.send(json.dumps({"type": "market", "assets_ids": subscribe_ids}))
            print(f"[ws] Subscribed to {len(subscribe_ids)} tokens")
            print(f"[scanner] Monitoring {len(self.events)} events for complement arb")
            print(f"[trade] Trade handling enabled - will log first 5 trades for verification")

        def on_message(ws, msg):
            self._last_msg_ts = time.time()
            try:
                payload = json.loads(msg)
            except:
                return

            items = payload if isinstance(payload, list) else [payload]
            for it in items:
                if not isinstance(it, dict):
                    continue

                event_type = it.get("event_type", "")
                event_type_key = event_type or "no_type"
                self._event_type_counts[event_type_key] += 1
                
                # Handle book updates
                if event_type == "book":
                    self._handle_book(it)
                # Handle trade events (explicit or inferred)
                elif event_type == "trade" or it.get("trades") is not None:
                    self._handle_trade(it)
                # Handle price updates
                elif event_type in ("price_change", "last_trade_price"):
                    token_id = it.get("asset_id") or it.get("token_id")
                    price = safe_float(it.get("price") or it.get("value"))
                    if token_id and price is not None:
                        self._handle_price(token_id, price)
                # Fallback: check if payload looks like a trade (has trade-like fields but no event_type)
                elif not event_type and any(k in it for k in ("asset_id", "token_id", "market", "assetId")) and any(k in it for k in ("price", "p", "size", "s", "amount", "qty")):
                    # Might be a trade event without explicit event_type
                    self._handle_trade(it)
            
            # Log event type summary periodically (every 5 minutes)
            if sum(self._event_type_counts.values()) % 1000 == 0 and sum(self._event_type_counts.values()) > 0:
                print(f"[ws] Event type summary: {dict(self._event_type_counts)} | Trades processed: {self._trade_count}")

        def on_error(ws, err):
            print(f"[ws error] {err}")

        def on_close(ws, code, reason):
            print(f"[ws close] {code}: {reason}")

        while True:
            print("[ws] Connecting...")
            ws = WebSocketApp(ws_url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
            ws.run_forever(ping_interval=20, ping_timeout=10)
            print("[ws] Reconnecting in 3s...")
            time.sleep(3)


def main():
    parser = argparse.ArgumentParser(description="Polymarket +EV Signal Scanner")
    parser.add_argument("--pages", type=int, default=50)
    parser.add_argument("--per-page", type=int, default=100)
    parser.add_argument("--telegram", action="store_true")
    args = parser.parse_args()

    print("[fetch] Loading markets from Gamma...")
    markets, events = fetch_markets(args.per_page, args.pages)
    print(f"[fetch] Loaded {len(markets)} tokens across {len(events)} events")

    scanner = EVScanner(markets, events, telegram=args.telegram)
    scanner.run()


if __name__ == "__main__":
    main()