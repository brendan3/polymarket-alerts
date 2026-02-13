#!/usr/bin/env python3
"""
Polymarket Alpha Engine v2 — Quantified Edge Alert System

Replaces the old reactive notification system with an actual alpha generator.
Every alert includes: entry price, target, stop, edge in cents, confidence score.

STRATEGIES (ranked by expected alpha):
1. CROSS-PLATFORM ARB   — Auto-discover Kalshi↔Polymarket pairs, monitor for locked-payout arb
2. BOOK IMBALANCE        — Track bid/ask depth ratio, signal when imbalance + momentum confirm
3. VWAP REVERSION/MOM    — Fade low-volume price deviations, follow high-volume deviations
4. TOXIC FLOW (VPIN)     — Detect informed order flow via volume-bucketed imbalance
5. LIQ VACUUM            — Detect book depletion after aggressive fills

REQUIRED ENV:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

OPTIONAL ENV (for cross-platform arb — highly recommended):
  KALSHI_API_KEY, KALSHI_PRIVATE_KEY_PEM (or KALSHI_PRIVATE_KEY_PATH)
  KALSHI_BASE_URL (default: https://api.elections.kalshi.com/trade-api/v2)

Usage:
  python polymarket_v1_alerts.py --telegram --pages 30
  python polymarket_v1_alerts.py --telegram --min-arb-edge 1.0 --imbalance-threshold 2.5
"""
from __future__ import annotations

import argparse
import base64
import json
import math
import os
import sqlite3
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import requests
from websocket import WebSocketApp

# Best-effort fuzzy matching: rapidfuzz > difflib fallback
try:
    from rapidfuzz import fuzz as _rfuzz

    def fuzzy_ratio(a: str, b: str) -> float:
        return _rfuzz.token_sort_ratio(a, b)
except ImportError:
    from difflib import SequenceMatcher

    def fuzzy_ratio(a: str, b: str) -> float:  # type: ignore[misc]
        return SequenceMatcher(None, a.lower().split(), b.lower().split()).ratio() * 100


# ═══════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════

CLOB_WS_BASE = "wss://ws-subscriptions-clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"
POLY_CLOB_BASE = "https://clob.polymarket.com"
KALSHI_BASE_DEFAULT = "https://api.elections.kalshi.com/trade-api/v2"


# ═══════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def safe_float(x: Any) -> Optional[float]:
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def as_money(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    if x >= 1_000_000:
        return f"${x / 1_000_000:.2f}M"
    if x >= 1_000:
        return f"${x / 1_000:.1f}K"
    return f"${x:.0f}"


def send_telegram(body: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat:
        print(f"[ALERT] {body}")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat, "text": body, "disable_web_page_preview": True},
            timeout=10,
        )
    except Exception as e:
        print(f"[telegram error] {e}")


def coerce_json_list(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw]
    if isinstance(raw, str):
        s = raw.strip()
        if s.startswith("["):
            try:
                return [str(x) for x in json.loads(s)]
            except Exception:
                return []
        return [s] if s else []
    return [str(raw)]


# ═══════════════════════════════════════════
# Data Models
# ═══════════════════════════════════════════

@dataclass
class PolyMarket:
    condition_id: str
    question: str
    event_slug: Optional[str]
    outcomes: List[str]
    clob_token_ids: List[str]
    volume: Optional[float] = None
    liquidity: Optional[float] = None

    def url(self) -> str:
        return f"https://polymarket.com/event/{self.event_slug}" if self.event_slug else ""

    def outcome_for_token(self, tid: str) -> str:
        try:
            idx = self.clob_token_ids.index(tid)
            return self.outcomes[idx] if idx < len(self.outcomes) else "?"
        except ValueError:
            return "?"


@dataclass
class KalshiMarket:
    ticker: str
    title: str
    event_ticker: str
    close_time: Optional[str] = None
    yes_bid: float = 0.0
    yes_ask: float = 1.0
    volume: float = 0.0
    open_interest: float = 0.0


@dataclass
class DiscoveredPair:
    name: str
    poly_market: PolyMarket
    kalshi_market: KalshiMarket
    match_score: float
    grade: str  # A / B / C
    poly_yes_token: str
    poly_no_token: str


@dataclass
class TradeRecord:
    ts: float
    price: float
    size: float
    direction: str  # BUY / SELL
    notional: float


# ─── Unified alert format: every signal is actionable ───

@dataclass
class AlertSignal:
    strategy: str
    market_name: str
    edge_cents: float          # quantified edge in cents per $1
    confidence: int            # 0-100
    entry_price: float
    entry_side: str            # "BUY YES", "SELL YES", etc.
    target_price: Optional[float] = None
    stop_price: Optional[float] = None
    depth_available: Optional[float] = None
    time_horizon: str = ""
    url: str = ""
    details: str = ""

    def score(self) -> float:
        """Composite alpha score for prioritization."""
        depth = max(self.depth_available or 100, 100)
        return self.edge_cents * (self.confidence / 100.0) * math.log10(depth)

    def format(self) -> str:
        lines = [
            f"🎯 [{self.strategy}] {self.market_name}",
            f"Edge: {self.edge_cents:.1f}¢/$1 | Confidence: {self.confidence}/100 | Score: {self.score():.1f}",
            f"Entry: {self.entry_side} @ {self.entry_price:.3f}",
        ]
        if self.target_price is not None:
            t = f"Target: {self.target_price:.3f}"
            if self.stop_price is not None:
                t += f" | Stop: {self.stop_price:.3f}"
            lines.append(t)
        if self.depth_available:
            lines.append(f"Depth: {as_money(self.depth_available)}")
        if self.time_horizon:
            lines.append(f"Horizon: {self.time_horizon}")
        if self.details:
            lines.append(self.details)
        if self.url:
            lines.append(self.url)
        return "\n".join(lines)


# ═══════════════════════════════════════════
# SQLite Alert Logger
# ═══════════════════════════════════════════

class AlertLogger:
    def __init__(self, path: str = "data/alpha_engine.db"):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        with sqlite3.connect(path) as c:
            c.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY,
                    ts TEXT,
                    strategy TEXT,
                    market TEXT,
                    edge_cents REAL,
                    confidence INTEGER,
                    entry_price REAL,
                    entry_side TEXT,
                    target_price REAL,
                    stop_price REAL,
                    depth_usd REAL,
                    score REAL,
                    details TEXT
                )
            """)
            c.commit()

    def log(self, signal: AlertSignal) -> None:
        try:
            with sqlite3.connect(self.path) as c:
                c.execute(
                    "INSERT INTO signals "
                    "(ts,strategy,market,edge_cents,confidence,entry_price,entry_side,"
                    "target_price,stop_price,depth_usd,score,details) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        now_utc_iso(), signal.strategy, signal.market_name,
                        signal.edge_cents, signal.confidence, signal.entry_price,
                        signal.entry_side, signal.target_price, signal.stop_price,
                        signal.depth_available, signal.score(), signal.details,
                    ),
                )
                c.commit()
        except Exception as e:
            print(f"[db error] {e}")


# ═══════════════════════════════════════════
# Kalshi API Client
# ═══════════════════════════════════════════

def _sign_kalshi(pem: str, ts_ms: str, method: str, path: str) -> str:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding as asym_padding

    key = serialization.load_pem_private_key(
        pem.encode() if isinstance(pem, str) else pem, password=None,
    )
    msg = (ts_ms + method + path.split("?")[0]).encode()
    sig = key.sign(
        msg,
        asym_padding.PSS(mgf=asym_padding.MGF1(hashes.SHA256()), salt_length=asym_padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode()


class KalshiClient:
    """Minimal Kalshi v2 REST client for market discovery + orderbook polling."""

    def __init__(self, base_url: str, api_key: Optional[str], private_key_pem: Optional[str]):
        self.base = base_url.rstrip("/")
        self.api_key = api_key
        self.pem = private_key_pem

    @property
    def enabled(self) -> bool:
        return bool(self.api_key and self.pem)

    def _headers(self, method: str, path: str) -> Dict[str, str]:
        if not self.enabled:
            return {}
        try:
            ts = str(int(time.time() * 1000))
            sig = _sign_kalshi(self.pem, ts, method, path)  # type: ignore[arg-type]
            return {
                "KALSHI-ACCESS-KEY": self.api_key,  # type: ignore[dict-item]
                "KALSHI-ACCESS-TIMESTAMP": ts,
                "KALSHI-ACCESS-SIGNATURE": sig,
            }
        except Exception:
            return {}

    # ── market discovery ──

    def fetch_markets(self, limit: int = 200, status: str = "open") -> List[KalshiMarket]:
        markets: List[KalshiMarket] = []
        cursor: Optional[str] = None

        for _ in range(20):
            path = "/trade-api/v2/markets"
            params: Dict[str, Any] = {"limit": limit, "status": status}
            if cursor:
                params["cursor"] = cursor
            try:
                r = requests.get(
                    f"{self.base}/markets", params=params,
                    headers=self._headers("GET", path), timeout=20,
                )
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                print(f"[kalshi] fetch error: {e}")
                break

            for m in data.get("markets", []):
                yes_bid_raw = safe_float(m.get("yes_bid"))
                yes_ask_raw = safe_float(m.get("yes_ask"))
                # Kalshi may return prices in cents (int) — normalise to 0-1
                yes_bid = (yes_bid_raw / 100.0 if yes_bid_raw and yes_bid_raw > 1.0 else yes_bid_raw) or 0.0
                yes_ask = (yes_ask_raw / 100.0 if yes_ask_raw and yes_ask_raw > 1.0 else yes_ask_raw) or 1.0

                markets.append(KalshiMarket(
                    ticker=m.get("ticker", ""),
                    title=m.get("title", ""),
                    event_ticker=m.get("event_ticker", ""),
                    close_time=m.get("close_time"),
                    yes_bid=yes_bid,
                    yes_ask=yes_ask,
                    volume=safe_float(m.get("volume")) or 0.0,
                    open_interest=safe_float(m.get("open_interest")) or 0.0,
                ))

            cursor = data.get("cursor")
            if not cursor or not data.get("markets"):
                break
            time.sleep(0.1)

        return markets

    # ── orderbook ──

    def fetch_orderbook(self, ticker: str, depth: int = 50) -> Dict[str, Any]:
        path = f"/trade-api/v2/markets/{ticker}/orderbook"
        try:
            r = requests.get(
                f"{self.base}/markets/{ticker}/orderbook",
                params={"depth": depth},
                headers=self._headers("GET", path), timeout=15,
            )
            r.raise_for_status()
            return r.json()
        except Exception:
            return {}

    def parse_orderbook_bba(
        self, ob: Dict[str, Any],
    ) -> Tuple[float, float, float, float]:
        """Return (yes_bid, yes_ask, bid_depth_$, ask_depth_$)."""
        orderbook = ob.get("orderbook") or {}

        def _parse(levels: Any) -> List[Tuple[float, float]]:
            out: List[Tuple[float, float]] = []
            for lvl in (levels or []):
                if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                    p = float(lvl[0])
                    if p > 1.0:
                        p /= 100.0
                    out.append((p, float(lvl[1])))
            return out

        yes = _parse(orderbook.get("yes"))
        no = _parse(orderbook.get("no"))
        if not yes or not no:
            return (0.0, 1.0, 0.0, 0.0)

        best_yes_bid = max(yes, key=lambda t: t[0])
        best_no_bid = max(no, key=lambda t: t[0])
        return (
            best_yes_bid[0],
            1.0 - best_no_bid[0],
            sum(p * q for p, q in yes),
            sum((1.0 - p) * q for p, q in no),
        )


# ═══════════════════════════════════════════
# Gamma Market Fetcher (Polymarket)
# ═══════════════════════════════════════════

def fetch_polymarket_markets(per_page: int = 100, pages: int = 30) -> Dict[str, PolyMarket]:
    """Return {token_id: PolyMarket} for all active markets."""
    markets: Dict[str, PolyMarket] = {}
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
            print(f"[gamma] page {p} error: {e}")
            continue
        if not items:
            break
        for m in items:
            clob_ids = coerce_json_list(m.get("clobTokenIds") or m.get("clob_token_ids"))
            if not clob_ids:
                continue
            outcomes = coerce_json_list(m.get("outcomes"))
            mkt = PolyMarket(
                condition_id=m.get("conditionId") or "",
                question=m.get("question") or "?",
                event_slug=m.get("eventSlug") or m.get("slug"),
                outcomes=outcomes,
                clob_token_ids=clob_ids,
                volume=safe_float(m.get("volume")),
                liquidity=safe_float(m.get("liquidity")),
            )
            for tid in clob_ids:
                markets[tid] = mkt
        time.sleep(0.15)
    return markets


# ═══════════════════════════════════════════
# STRATEGY 1 — Cross-Platform Auto-Arbitrage
# ═══════════════════════════════════════════

class CrossPlatformArbitrage:
    """
    Auto-discover equivalent markets between Kalshi and Polymarket via
    fuzzy name matching, then poll both orderbooks for locked-payout arb.

    Edge = $1.00 − (ask_A + ask_B) − fees.
    This is GUARANTEED PROFIT when edge > 0.
    """

    def __init__(
        self,
        kalshi: KalshiClient,
        poly_markets: Dict[str, PolyMarket],
        min_edge_cents: float = 1.5,
        min_match_score: float = 75.0,
        kalshi_fee_bps: float = 0.0,
        poly_fee_bps: float = 0.0,
    ):
        self.kalshi = kalshi
        self.poly_markets = poly_markets
        self.min_edge = min_edge_cents
        self.min_match = min_match_score
        self.k_fee = kalshi_fee_bps
        self.p_fee = poly_fee_bps

        self.pairs: List[DiscoveredPair] = []
        self._last_alert: Dict[str, float] = {}
        self._cooldown = 300.0

        # Deduplicated question → PolyMarket lookup
        self._poly_by_q: Dict[str, PolyMarket] = {}
        seen: Set[str] = set()
        for mkt in poly_markets.values():
            if mkt.question not in seen:
                self._poly_by_q[mkt.question] = mkt
                seen.add(mkt.question)

    # ── discovery ──

    def discover_pairs(self) -> List[DiscoveredPair]:
        print("[arb] Fetching Kalshi markets for auto-discovery...")
        kalshi_mkts = self.kalshi.fetch_markets()
        print(f"[arb] {len(kalshi_mkts)} Kalshi markets vs {len(self._poly_by_q)} Poly questions")

        poly_qs = list(self._poly_by_q.keys())
        pairs: List[DiscoveredPair] = []

        for km in kalshi_mkts:
            if km.volume < 50:
                continue
            best_score, best_q = 0.0, ""
            for pq in poly_qs:
                score = fuzzy_ratio(km.title, pq)
                if score > best_score:
                    best_score, best_q = score, pq
            if best_score < self.min_match or not best_q:
                continue
            pm = self._poly_by_q[best_q]
            if len(pm.clob_token_ids) < 2:
                continue
            grade = "A" if best_score >= 90 else ("B" if best_score >= 80 else "C")
            pairs.append(DiscoveredPair(
                name=km.title[:80],
                poly_market=pm, kalshi_market=km,
                match_score=best_score, grade=grade,
                poly_yes_token=pm.clob_token_ids[0],
                poly_no_token=pm.clob_token_ids[1],
            ))

        self.pairs = pairs
        print(f"[arb] Discovered {len(pairs)} cross-platform pairs:")
        for p in pairs[:15]:
            print(f"  [{p.grade}] {p.match_score:.0f}%  K:{p.kalshi_market.ticker}  ↔  P:{p.poly_market.question[:55]}")
        return pairs

    # ── real-time check ──

    def check_all_pairs(self) -> List[AlertSignal]:
        signals: List[AlertSignal] = []
        now = time.time()

        for pair in self.pairs:
            if pair.grade not in ("A", "B"):
                continue
            key = pair.kalshi_market.ticker
            if key in self._last_alert and (now - self._last_alert[key]) < self._cooldown:
                continue

            try:
                k_ob = self.kalshi.fetch_orderbook(pair.kalshi_market.ticker)
                ky_bid, ky_ask, k_bid_d, k_ask_d = self.kalshi.parse_orderbook_bba(k_ob)

                py_book = requests.get(
                    f"{POLY_CLOB_BASE}/book", params={"token_id": pair.poly_yes_token}, timeout=10,
                ).json()
                pn_book = requests.get(
                    f"{POLY_CLOB_BASE}/book", params={"token_id": pair.poly_no_token}, timeout=10,
                ).json()

                py_asks = py_book.get("asks", [])
                pn_asks = pn_book.get("asks", [])
                py_bids = py_book.get("bids", [])
                pn_bids = pn_book.get("bids", [])
                if not py_asks or not pn_asks:
                    continue

                py_ask = min(float(x["price"]) for x in py_asks)
                pn_ask = min(float(x["price"]) for x in pn_asks)
                py_bid = max(float(x["price"]) for x in py_bids) if py_bids else 0.0
                pn_bid = max(float(x["price"]) for x in pn_bids) if pn_bids else 0.0
            except Exception:
                continue

            # Direction 1: Buy YES Kalshi + Buy NO Polymarket
            cost1 = ky_ask + pn_ask + ky_ask * (self.k_fee / 1e4) + pn_ask * (self.p_fee / 1e4)
            edge1 = (1.0 - cost1) * 100.0

            # Direction 2: Buy YES Polymarket + Buy NO Kalshi
            k_no_ask = 1.0 - ky_bid if ky_bid > 0 else 1.0
            cost2 = py_ask + k_no_ask + py_ask * (self.p_fee / 1e4) + k_no_ask * (self.k_fee / 1e4)
            edge2 = (1.0 - cost2) * 100.0

            for edge, label, entry_px in [
                (edge1, "Buy YES Kalshi + Buy NO Poly", ky_ask),
                (edge2, "Buy YES Poly + Buy NO Kalshi", py_ask),
            ]:
                if edge >= self.min_edge:
                    self._last_alert[key] = now
                    conf = min(98, int(55 + pair.match_score / 2))
                    if pair.grade == "A":
                        conf = min(98, conf + 8)
                    signals.append(AlertSignal(
                        strategy="CROSS-PLATFORM ARB",
                        market_name=pair.name,
                        edge_cents=edge,
                        confidence=conf,
                        entry_price=entry_px,
                        entry_side=label,
                        target_price=None,
                        depth_available=min(k_ask_d, k_bid_d) if k_ask_d and k_bid_d else None,
                        time_horizon=f"Hold to settlement ({pair.kalshi_market.close_time or 'TBD'})",
                        url=pair.poly_market.url(),
                        details=(
                            f"Grade {pair.grade} ({pair.match_score:.0f}%) | Kalshi: {pair.kalshi_market.ticker}\n"
                            f"K YES {ky_bid:.3f}/{ky_ask:.3f} | P YES {py_bid:.3f}/{py_ask:.3f} | P NO {pn_bid:.3f}/{pn_ask:.3f}"
                        ),
                    ))
                    break  # alert best direction only

        return signals


# ═══════════════════════════════════════════
# STRATEGY 2 — Orderbook Imbalance Momentum
# ═══════════════════════════════════════════

class OrderbookImbalance:
    """
    Heavy bid-side depth + upward momentum → BUY signal (and vice-versa).

    Why this works: prediction markets are thin enough that resting depth
    reveals informed participant positioning. Momentum confirmation filters
    out passive/stale liquidity.
    """

    def __init__(
        self,
        markets: Dict[str, PolyMarket],
        imbalance_threshold: float = 3.0,
        momentum_window_sec: float = 60.0,
        min_momentum_bps: float = 50.0,
        min_total_depth: float = 1000.0,
        max_levels: int = 10,
    ):
        self.markets = markets
        self.threshold = imbalance_threshold
        self.mom_window = momentum_window_sec
        self.min_mom_bps = min_momentum_bps
        self.min_depth = min_total_depth
        self.max_levels = max_levels

        self._mid_hist: Dict[str, Deque[Tuple[float, float]]] = defaultdict(lambda: deque(maxlen=200))
        self._last_alert: Dict[str, float] = {}
        self._cooldown = 300.0

    def update(
        self, token_id: str,
        bids: List[Tuple[float, float]],
        asks: List[Tuple[float, float]],
    ) -> Optional[AlertSignal]:
        if token_id not in self.markets:
            return None
        now = time.time()
        if token_id in self._last_alert and (now - self._last_alert[token_id]) < self._cooldown:
            return None
        if not bids or not asks:
            return None

        best_bid = max(p for p, _ in bids)
        best_ask = min(p for p, _ in asks)
        mid = (best_bid + best_ask) / 2
        spread = best_ask - best_bid

        if mid < 0.05 or mid > 0.95:
            return None

        self._mid_hist[token_id].append((now, mid))

        bid_d = sum(p * s for p, s in bids[: self.max_levels] if 0.10 <= p <= 0.90)
        ask_d = sum(p * s for p, s in asks[: self.max_levels] if 0.10 <= p <= 0.90)
        total = bid_d + ask_d
        if total < self.min_depth or bid_d == 0 or ask_d == 0:
            return None

        ratio = bid_d / ask_d
        inv = ask_d / bid_d

        if ratio >= self.threshold:
            side, r = "BUY", ratio
        elif inv >= self.threshold:
            side, r = "SELL", inv
        else:
            return None

        # Momentum confirmation
        recent = [(t, m) for t, m in self._mid_hist[token_id] if now - t <= self.mom_window]
        if len(recent) < 3:
            return None
        mom_bps = ((mid - recent[0][1]) / recent[0][1]) * 10_000 if recent[0][1] > 0 else 0
        if side == "BUY" and mom_bps < self.min_mom_bps:
            return None
        if side == "SELL" and mom_bps > -self.min_mom_bps:
            return None

        self._last_alert[token_id] = now
        mkt = self.markets[token_id]
        outcome = mkt.outcome_for_token(token_id)

        est_move = spread * (r - 1) / r
        edge = max(est_move * 100, 1.0)
        entry = best_ask if side == "BUY" else best_bid
        target = entry + est_move if side == "BUY" else entry - est_move
        stop = entry - spread * 2 if side == "BUY" else entry + spread * 2

        return AlertSignal(
            strategy="BOOK IMBALANCE",
            market_name=f"{mkt.question[:60]} ({outcome})",
            edge_cents=edge,
            confidence=min(80, int(40 + r * 8)),
            entry_price=round(entry, 3),
            entry_side=f"{side} YES",
            target_price=round(target, 3),
            stop_price=round(stop, 3),
            depth_available=bid_d if side == "BUY" else ask_d,
            time_horizon="5-30 min",
            url=mkt.url(),
            details=(
                f"Bid depth: {as_money(bid_d)} | Ask depth: {as_money(ask_d)} | Ratio: {r:.1f}:1\n"
                f"Momentum: {mom_bps:.0f}bps/{self.mom_window:.0f}s | Spread: {spread * 100:.1f}¢"
            ),
        )


# ═══════════════════════════════════════════
# STRATEGY 3 — VWAP Mean Reversion / Momentum
# ═══════════════════════════════════════════

class VWAPDivergence:
    """
    Track 30-min VWAP per token.

    - Price deviated on LOW volume  → noise, FADE it  (mean reversion)
    - Price deviated on HIGH volume → signal, FOLLOW it (momentum)
    """

    def __init__(
        self,
        markets: Dict[str, PolyMarket],
        vwap_window_sec: float = 1800.0,
        min_deviation_pct: float = 8.0,
        min_trades: int = 10,
        low_vol_pctile: float = 0.3,
    ):
        self.markets = markets
        self.window = vwap_window_sec
        self.min_dev = min_deviation_pct
        self.min_trades = min_trades
        self.low_vol = low_vol_pctile

        self._trades: Dict[str, Deque[TradeRecord]] = defaultdict(lambda: deque(maxlen=2000))
        self._mid: Dict[str, float] = {}
        self._last_alert: Dict[str, float] = {}
        self._cooldown = 600.0

    def add_trade(self, token_id: str, price: float, size: float, direction: str = "BUY") -> None:
        if token_id in self.markets:
            self._trades[token_id].append(
                TradeRecord(ts=time.time(), price=price, size=size, direction=direction, notional=price * size)
            )

    def update_mid(self, token_id: str, mid: float) -> None:
        self._mid[token_id] = mid

    def check(self, token_id: str) -> Optional[AlertSignal]:
        if token_id not in self.markets or token_id not in self._mid:
            return None
        now = time.time()
        if token_id in self._last_alert and (now - self._last_alert[token_id]) < self._cooldown:
            return None

        recent = [t for t in self._trades[token_id] if now - t.ts <= self.window]
        if len(recent) < self.min_trades:
            return None

        total_pv = sum(t.price * t.size for t in recent)
        total_v = sum(t.size for t in recent)
        if total_v == 0:
            return None
        vwap = total_pv / total_v
        cur = self._mid[token_id]
        if vwap <= 0:
            return None

        dev_pct = ((cur - vwap) / vwap) * 100
        if abs(dev_pct) < self.min_dev:
            return None

        # Volume regime classification
        v5 = sum(t.notional for t in recent if now - t.ts <= 300)
        avg5 = (total_pv / max(self.window / 300, 1))
        low_vol = v5 < avg5 * self.low_vol if avg5 > 0 else True

        if low_vol:
            # FADE the noise
            side = "SELL YES" if dev_pct > 0 else "BUY YES"
            target = vwap
            edge = abs(cur - vwap) * 100
            conf = min(75, int(40 + abs(dev_pct) * 2))
            strat = "VWAP REVERSION"
            horizon = "15-60 min"
            stop = cur + (cur - vwap) * 0.5
        else:
            # FOLLOW the signal
            side = "BUY YES" if dev_pct > 0 else "SELL YES"
            target = cur + (cur - vwap) * 0.5
            edge = abs(cur - vwap) * 0.5 * 100
            conf = min(70, int(35 + abs(dev_pct) * 1.5))
            strat = "VWAP MOMENTUM"
            horizon = "30-120 min"
            stop = vwap

        self._last_alert[token_id] = now
        mkt = self.markets[token_id]
        outcome = mkt.outcome_for_token(token_id)

        return AlertSignal(
            strategy=strat,
            market_name=f"{mkt.question[:60]} ({outcome})",
            edge_cents=max(edge, 0.5),
            confidence=conf,
            entry_price=round(cur, 3),
            entry_side=side,
            target_price=round(target, 3),
            stop_price=round(stop, 3),
            depth_available=v5 if v5 > 0 else None,
            time_horizon=horizon,
            url=mkt.url(),
            details=(
                f"VWAP: {vwap:.3f} | Price: {cur:.3f} | Dev: {dev_pct:+.1f}%\n"
                f"Volume: {'LOW → fade' if low_vol else 'HIGH → follow'} | "
                f"5m vol: {as_money(v5)} vs avg: {as_money(avg5)}"
            ),
        )


# ═══════════════════════════════════════════
# STRATEGY 4 — Trade Flow Toxicity (VPIN)
# ═══════════════════════════════════════════

class FlowToxicity:
    """
    VPIN: Volume-synchronised Probability of INformed trading.

    Bucket trades by volume (not time). In each bucket measure buy/sell
    imbalance. Rolling average of absolute imbalance = VPIN.

    High VPIN + consistent direction = smart money is active → follow them.
    """

    def __init__(
        self,
        markets: Dict[str, PolyMarket],
        bucket_size_usd: float = 500.0,
        num_buckets: int = 20,
        toxicity_threshold: float = 0.65,
        min_directional_pct: float = 70.0,
    ):
        self.markets = markets
        self.bucket_sz = bucket_size_usd
        self.n_buckets = num_buckets
        self.threshold = toxicity_threshold
        self.min_dir = min_directional_pct / 100.0

        self._buy: Dict[str, float] = defaultdict(float)
        self._sell: Dict[str, float] = defaultdict(float)
        self._vol: Dict[str, float] = defaultdict(float)
        self._imb: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=60))
        self._dirs: Dict[str, Deque[str]] = defaultdict(lambda: deque(maxlen=60))
        self._last_alert: Dict[str, float] = {}
        self._cooldown = 600.0
        self._quote: Dict[str, Tuple[float, float]] = {}

    def update_quote(self, token_id: str, bid: float, ask: float) -> None:
        self._quote[token_id] = (bid, ask)

    def add_trade(self, token_id: str, price: float, size: float) -> Optional[AlertSignal]:
        if token_id not in self.markets:
            return None

        if token_id in self._quote:
            bid, ask = self._quote[token_id]
            direction = "BUY" if price >= (bid + ask) / 2 else "SELL"
        else:
            direction = "BUY" if price >= 0.5 else "SELL"

        notional = price * size
        if direction == "BUY":
            self._buy[token_id] += notional
        else:
            self._sell[token_id] += notional
        self._vol[token_id] += notional

        if self._vol[token_id] < self.bucket_sz:
            return None

        # Bucket full — compute imbalance
        b, s = self._buy[token_id], self._sell[token_id]
        total = b + s
        if total > 0:
            self._imb[token_id].append(abs(b - s) / total)
            self._dirs[token_id].append("BUY" if b > s else "SELL")

        self._buy[token_id] = 0.0
        self._sell[token_id] = 0.0
        self._vol[token_id] = 0.0

        return self._check(token_id)

    def _check(self, token_id: str) -> Optional[AlertSignal]:
        now = time.time()
        if token_id in self._last_alert and (now - self._last_alert[token_id]) < self._cooldown:
            return None

        imbs = list(self._imb[token_id])
        dirs = list(self._dirs[token_id])
        if len(imbs) < self.n_buckets:
            return None

        vpin = sum(imbs[-self.n_buckets :]) / self.n_buckets
        if vpin < self.threshold:
            return None

        buy_n = sum(1 for d in dirs[-self.n_buckets :] if d == "BUY")
        sell_n = self.n_buckets - buy_n
        dom_dir = "BUY" if buy_n > sell_n else "SELL"
        dom_pct = max(buy_n, sell_n) / self.n_buckets
        if dom_pct < self.min_dir:
            return None

        self._last_alert[token_id] = now
        mkt = self.markets[token_id]
        outcome = mkt.outcome_for_token(token_id)

        entry = 0.5
        if token_id in self._quote:
            bid, ask = self._quote[token_id]
            entry = ask if dom_dir == "BUY" else bid

        edge = vpin * 5.0
        target = entry + edge / 100 if dom_dir == "BUY" else entry - edge / 100
        stop = entry - 0.03 if dom_dir == "BUY" else entry + 0.03

        return AlertSignal(
            strategy="TOXIC FLOW",
            market_name=f"{mkt.question[:60]} ({outcome})",
            edge_cents=edge,
            confidence=min(80, int(vpin * 100)),
            entry_price=round(entry, 3),
            entry_side=f"{dom_dir} YES",
            target_price=round(target, 3),
            stop_price=round(stop, 3),
            time_horizon="10-60 min",
            url=mkt.url(),
            details=(
                f"VPIN: {vpin:.2f} | Direction: {dom_dir} ({dom_pct * 100:.0f}% of {self.n_buckets} buckets)\n"
                f"Informed flow detected — smart money aggressively {dom_dir.lower()}ing"
            ),
        )


# ═══════════════════════════════════════════
# STRATEGY 5 — Liquidity Vacuum Detection
# ═══════════════════════════════════════════

class LiquidityVacuum:
    """
    Detect when one side of the book empties after aggressive fills.
    A gap in the book means price MUST move to find the next resting order.
    """

    def __init__(
        self,
        markets: Dict[str, PolyMarket],
        min_gap_cents: float = 3.0,
        min_depth_before: float = 2000.0,
        depth_drop_pct: float = 60.0,
    ):
        self.markets = markets
        self.min_gap = min_gap_cents
        self.min_prev = min_depth_before
        self.drop_pct = depth_drop_pct

        self._prev: Dict[str, Tuple[float, float]] = {}
        self._last_alert: Dict[str, float] = {}
        self._cooldown = 600.0

    def update(
        self, token_id: str,
        bids: List[Tuple[float, float]],
        asks: List[Tuple[float, float]],
    ) -> Optional[AlertSignal]:
        if token_id not in self.markets:
            return None
        now = time.time()
        if token_id in self._last_alert and (now - self._last_alert[token_id]) < self._cooldown:
            return None
        if not bids or not asks:
            return None

        best_bid = max(p for p, _ in bids)
        best_ask = min(p for p, _ in asks)
        mid = (best_bid + best_ask) / 2
        if mid < 0.10 or mid > 0.90:
            return None

        bid_d = sum(p * s for p, s in bids if 0.10 <= p <= 0.90)
        ask_d = sum(p * s for p, s in asks if 0.10 <= p <= 0.90)

        # Find largest gap on each side
        ask_ps = sorted(p for p, _ in asks if 0.10 <= p <= 0.90)
        bid_ps = sorted((p for p, _ in bids if 0.10 <= p <= 0.90), reverse=True)
        ask_gap = max((ask_ps[i + 1] - ask_ps[i] for i in range(len(ask_ps) - 1)), default=0)
        bid_gap = max((bid_ps[i] - bid_ps[i + 1] for i in range(len(bid_ps) - 1)), default=0)

        prev = self._prev.get(token_id)
        self._prev[token_id] = (bid_d, ask_d)
        if prev is None:
            return None

        prev_bid, prev_ask = prev
        drop = 1.0 - self.drop_pct / 100.0

        signal = None
        mkt = self.markets[token_id]
        outcome = mkt.outcome_for_token(token_id)

        # Ask vacuum → price up
        if prev_ask > self.min_prev and ask_d < prev_ask * drop and ask_gap * 100 >= self.min_gap:
            self._last_alert[token_id] = now
            signal = AlertSignal(
                strategy="LIQ VACUUM",
                market_name=f"{mkt.question[:60]} ({outcome})",
                edge_cents=ask_gap * 50,
                confidence=min(75, int(40 + ask_gap * 200)),
                entry_price=best_ask,
                entry_side="BUY YES",
                target_price=round(best_ask + ask_gap * 0.5, 3),
                stop_price=round(best_bid - 0.01, 3),
                depth_available=ask_d,
                time_horizon="1-15 min",
                url=mkt.url(),
                details=(
                    f"Ask depth: {as_money(prev_ask)} → {as_money(ask_d)} "
                    f"({(1 - ask_d / prev_ask) * 100:.0f}% drop)\n"
                    f"Gap: {ask_gap * 100:.1f}¢ — price should move up to fill vacuum"
                ),
            )

        # Bid vacuum → price down
        elif prev_bid > self.min_prev and bid_d < prev_bid * drop and bid_gap * 100 >= self.min_gap:
            self._last_alert[token_id] = now
            signal = AlertSignal(
                strategy="LIQ VACUUM",
                market_name=f"{mkt.question[:60]} ({outcome})",
                edge_cents=bid_gap * 50,
                confidence=min(75, int(40 + bid_gap * 200)),
                entry_price=best_bid,
                entry_side="SELL YES",
                target_price=round(best_bid - bid_gap * 0.5, 3),
                stop_price=round(best_ask + 0.01, 3),
                depth_available=bid_d,
                time_horizon="1-15 min",
                url=mkt.url(),
                details=(
                    f"Bid depth: {as_money(prev_bid)} → {as_money(bid_d)} "
                    f"({(1 - bid_d / prev_bid) * 100:.0f}% drop)\n"
                    f"Gap: {bid_gap * 100:.1f}¢ — price should move down to fill vacuum"
                ),
            )

        return signal


# ═══════════════════════════════════════════
# Alpha Engine — Main Orchestrator
# ═══════════════════════════════════════════

class AlphaEngine:
    """
    Combines all strategies, manages the WebSocket feed,
    background threads, and alert dispatch.
    """

    def __init__(
        self,
        poly_markets: Dict[str, PolyMarket],
        kalshi: Optional[KalshiClient] = None,
        logger: Optional[AlertLogger] = None,
        telegram: bool = True,
        min_arb_edge: float = 1.5,
        imbalance_threshold: float = 3.0,
        vwap_deviation: float = 8.0,
        toxicity_threshold: float = 0.65,
        arb_poll_sec: float = 5.0,
    ):
        self.poly_markets = poly_markets
        self.token_ids = list(poly_markets.keys())
        self.logger = logger or AlertLogger()
        self.telegram = telegram
        self.arb_poll = arb_poll_sec

        # Strategies
        self.imbalance = OrderbookImbalance(poly_markets, imbalance_threshold=imbalance_threshold)
        self.vwap = VWAPDivergence(poly_markets, min_deviation_pct=vwap_deviation)
        self.toxicity = FlowToxicity(poly_markets, toxicity_threshold=toxicity_threshold)
        self.vacuum = LiquidityVacuum(poly_markets)

        self.arb: Optional[CrossPlatformArbitrage] = None
        if kalshi and kalshi.enabled:
            self.arb = CrossPlatformArbitrage(kalshi, poly_markets, min_edge_cents=min_arb_edge)

        # Stats
        self._t0 = time.time()
        self._msg_count = 0
        self._alert_count = 0
        self._trade_count = 0
        self._last_msg_ts = time.time()

    # ── emit ──

    def _emit(self, signal: AlertSignal) -> None:
        self._alert_count += 1
        msg = signal.format()
        print(f"\n{'=' * 60}\n{msg}\n{'=' * 60}\n")
        self.logger.log(signal)
        if self.telegram:
            send_telegram(msg)

    # ── level parser ──

    @staticmethod
    def _parse_levels(levels: Any) -> List[Tuple[float, float]]:
        out: List[Tuple[float, float]] = []
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

    # ── WS handlers ──

    def _handle_book(self, data: Dict[str, Any]) -> None:
        token_id = data.get("asset_id") or data.get("token_id") or data.get("market")
        if not token_id or token_id not in self.poly_markets:
            return

        book = data.get("book") if isinstance(data.get("book"), dict) else data
        bids = self._parse_levels(book.get("bids") or [])
        asks = self._parse_levels(book.get("asks") or [])
        if not bids or not asks:
            return

        best_bid = max(p for p, _ in bids)
        best_ask = min(p for p, _ in asks)
        mid = (best_bid + best_ask) / 2

        self.vwap.update_mid(token_id, mid)
        self.toxicity.update_quote(token_id, best_bid, best_ask)

        if sig := self.imbalance.update(token_id, bids, asks):
            self._emit(sig)
        if sig := self.vacuum.update(token_id, bids, asks):
            self._emit(sig)

    def _handle_trade(self, data: Dict[str, Any]) -> None:
        raw = data.get("trades")
        trades: List[Dict[str, Any]] = []
        if isinstance(raw, list):
            trades = [t for t in raw if isinstance(t, dict)]
        elif isinstance(raw, dict):
            trades = [raw]
        elif any(k in data for k in ("asset_id", "token_id", "price", "size")):
            trades = [data]

        for tr in trades:
            tid = (
                tr.get("asset_id") or tr.get("token_id")
                or tr.get("market") or data.get("asset_id") or data.get("token_id")
            )
            if not tid or tid not in self.poly_markets:
                continue
            price = safe_float(tr.get("price") or tr.get("p") or tr.get("last_trade_price"))
            size = safe_float(tr.get("size") or tr.get("s") or tr.get("amount") or tr.get("qty"))
            if price is None:
                continue
            if size is None:
                size = 1.0

            self._trade_count += 1
            self.vwap.add_trade(tid, price, size)

            if sig := self.toxicity.add_trade(tid, price, size):
                self._emit(sig)

            # Periodic VWAP check
            if self._trade_count % 50 == 0:
                if sig := self.vwap.check(tid):
                    self._emit(sig)

    def _handle_price_change(self, data: Dict[str, Any]) -> None:
        for pc in data.get("price_changes", []) or []:
            aid = pc.get("asset_id", "")
            bid = safe_float(pc.get("best_bid"))
            ask = safe_float(pc.get("best_ask"))
            if aid and bid and ask:
                self.vwap.update_mid(aid, (bid + ask) / 2)
                self.toxicity.update_quote(aid, bid, ask)

    # ── background threads ──

    def _arb_loop(self) -> None:
        if not self.arb:
            return
        time.sleep(5)
        try:
            self.arb.discover_pairs()
        except Exception as e:
            print(f"[arb] Discovery error: {e}")

        while True:
            try:
                for sig in self.arb.check_all_pairs():
                    self._emit(sig)
            except Exception as e:
                print(f"[arb] Poll error: {e}")
            time.sleep(self.arb_poll)

    def _rediscovery_loop(self) -> None:
        if not self.arb:
            return
        while True:
            time.sleep(1800)
            try:
                print("[arb] Re-discovering pairs...")
                self.arb.discover_pairs()
            except Exception as e:
                print(f"[arb] Rediscovery error: {e}")

    def _vwap_sweep_loop(self) -> None:
        """Periodically check VWAP divergence for all tracked tokens."""
        while True:
            time.sleep(120)
            for tid in list(self.vwap._mid.keys()):
                try:
                    if sig := self.vwap.check(tid):
                        self._emit(sig)
                except Exception:
                    pass

    def _heartbeat_loop(self) -> None:
        while True:
            time.sleep(60)
            elapsed = time.time() - self._t0
            rate = self._trade_count / max(1, elapsed / 60)
            arb_pairs = len(self.arb.pairs) if self.arb else 0
            print(
                f"[heartbeat] up={elapsed / 60:.0f}m | msgs={self._msg_count} | "
                f"trades={self._trade_count} ({rate:.1f}/m) | alerts={self._alert_count} | "
                f"arb_pairs={arb_pairs}"
            )

    # ── main run ──

    def run(self) -> None:
        if not self.token_ids:
            print("[engine] No markets to monitor")
            return

        # Background threads
        for fn in (self._arb_loop, self._rediscovery_loop, self._vwap_sweep_loop, self._heartbeat_loop):
            threading.Thread(target=fn, daemon=True).start()

        max_tokens = 1800
        sub_ids = self.token_ids[:max_tokens]

        strategies = ["BOOK IMBALANCE", "VWAP", "TOXIC FLOW", "LIQ VACUUM"]
        if self.arb:
            strategies.insert(0, "CROSS-PLATFORM ARB")

        def on_open(ws):
            time.sleep(0.25)
            ws.send(json.dumps({"type": "market", "assets_ids": sub_ids}))
            print(f"[engine] Subscribed to {len(sub_ids)} tokens")
            print(f"[engine] Active: {', '.join(strategies)}")

            if self.telegram:
                arb_info = f"{len(self.arb.pairs)} discovered" if self.arb else "disabled (no Kalshi key)"
                send_telegram(
                    f"🎯 Alpha Engine v2 Online\n\n"
                    f"Tokens: {len(sub_ids)}\n"
                    f"Strategies: {', '.join(strategies)}\n"
                    f"Cross-platform pairs: {arb_info}\n"
                    f"Every alert has quantified edge + entry/exit levels"
                )

        def on_message(ws, msg):
            self._last_msg_ts = time.time()
            self._msg_count += 1
            try:
                payload = json.loads(msg)
            except Exception:
                return
            items = payload if isinstance(payload, list) else [payload]
            for it in items:
                if not isinstance(it, dict):
                    continue
                et = it.get("event_type", "")
                if et == "book":
                    self._handle_book(it)
                elif et in ("trade", "last_trade_price") or it.get("trades") is not None:
                    self._handle_trade(it)
                elif et == "price_change":
                    self._handle_price_change(it)

        def on_error(ws, err):
            print(f"[ws error] {err}")

        def on_close(ws, code, reason):
            print(f"[ws close] {code}: {reason}")

        while True:
            print("[ws] Connecting...")
            ws = WebSocketApp(
                f"{CLOB_WS_BASE}/ws/market",
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
            print("[ws] Reconnecting in 3s...")
            time.sleep(3)


# ═══════════════════════════════════════════
# CLI + Entry Point
# ═══════════════════════════════════════════

def main() -> None:
    ap = argparse.ArgumentParser(description="Polymarket Alpha Engine v2 — Quantified Edge Alert System")
    ap.add_argument("--pages", type=int, default=30, help="Gamma pages to fetch")
    ap.add_argument("--per-page", type=int, default=100)
    ap.add_argument("--telegram", action="store_true", help="Send alerts via Telegram")
    # Backwards-compat flags so existing Railway/Nixpacks start commands keep working.
    # These are effectively no-ops in v2 but we accept them to avoid crashes.
    ap.add_argument("--gamma-active", action="store_true", help="(deprecated, ignored) kept for v1 CLI compatibility")
    ap.add_argument("--broad-test", action="store_true", help="(deprecated, ignored) kept for v1 CLI compatibility")
    ap.add_argument(
        "--whale-threshold",
        type=float,
        default=0.0,
        help="(deprecated, ignored) v2 uses per-strategy sizing instead of this threshold",
    )
    ap.add_argument("--min-arb-edge", type=float, default=1.5, help="Min cross-platform arb edge (cents)")
    ap.add_argument("--imbalance-threshold", type=float, default=3.0, help="Min bid/ask depth ratio for imbalance signal")
    ap.add_argument("--vwap-deviation", type=float, default=8.0, help="Min VWAP deviation percent")
    ap.add_argument("--toxicity-threshold", type=float, default=0.65, help="VPIN threshold (0-1)")
    ap.add_argument("--arb-poll-sec", type=float, default=5.0, help="Kalshi orderbook poll interval (seconds)")
    ap.add_argument("--db-path", type=str, default="data/alpha_engine.db", help="SQLite DB path for signal log")
    args = ap.parse_args()

    print("=" * 60)
    print("  Polymarket Alpha Engine v2")
    print("  Quantified edge on every alert")
    print("=" * 60)

    # Polymarket markets
    print("\n[fetch] Loading Polymarket markets from Gamma...")
    poly_markets = fetch_polymarket_markets(args.per_page, args.pages)
    print(f"[fetch] Loaded {len(poly_markets)} tokens")

    # Kalshi client
    kalshi_key = os.environ.get("KALSHI_API_KEY")
    kalshi_pem = os.environ.get("KALSHI_PRIVATE_KEY_PEM")
    if not kalshi_pem:
        pem_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH")
        if pem_path:
            p = Path(pem_path)
            if p.is_file():
                kalshi_pem = p.read_text()

    kalshi = KalshiClient(
        os.getenv("KALSHI_BASE_URL", KALSHI_BASE_DEFAULT),
        kalshi_key, kalshi_pem,
    )
    if kalshi.enabled:
        print("[kalshi] API credentials loaded — CROSS-PLATFORM ARB enabled")
    else:
        print("[kalshi] No credentials — cross-platform arb DISABLED")
        print("[kalshi] Set KALSHI_API_KEY + KALSHI_PRIVATE_KEY_PEM to enable")

    logger = AlertLogger(args.db_path)

    engine = AlphaEngine(
        poly_markets=poly_markets,
        kalshi=kalshi,
        logger=logger,
        telegram=args.telegram,
        min_arb_edge=args.min_arb_edge,
        imbalance_threshold=args.imbalance_threshold,
        vwap_deviation=args.vwap_deviation,
        toxicity_threshold=args.toxicity_threshold,
        arb_poll_sec=args.arb_poll_sec,
    )

    engine.run()


if __name__ == "__main__":
    main()
