#!/usr/bin/env python3
"""
Polymarket Alpha Engine v3 — Precision Alpha System

PHILOSOPHY: Fewer alerts, higher conviction, every alert tradeable.
- Every signal gated by MarketQualityGate (price, depth, liquidity, category, time-to-resolution)
- Market category classification: political/legal/economic/crypto >> sports longshots
- Composite signal confirmation: multi-strategy agreement boosts confidence
- Per-market rate limiting kills repeat-spam
- 12%+ monthly annualized target requires ~2-5 high-conviction alerts/day, not 50 noisy ones

STRATEGIES:
1. CROSS-PLATFORM ARB   — Auto-discover Kalshi↔Polymarket pairs, locked-payout arb
2. BOOK IMBALANCE        — Depth ratio + momentum, heavy quality gating
3. VWAP REVERSION/MOM    — Fade low-vol noise, follow high-vol signal
4. TOXIC FLOW (VPIN)     — Informed flow detection with throughput gate
5. LIQ VACUUM            — Book depletion in event-driven markets

ENV:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
  KALSHI_API_KEY, KALSHI_PRIVATE_KEY_PEM (optional, enables cross-platform arb)
"""
from __future__ import annotations

import argparse
import base64
import json
import math
import os
import re
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
# Market Category Classification
# ═══════════════════════════════════════════

# Category keywords — order matters (first match wins)
_CAT_RULES: List[Tuple[str, List[str]]] = [
    ("sports", [
        r"\b(nba|nfl|mlb|nhl|epl|premier league|champions league|world cup|"
        r"serie a|bundesliga|la liga|ligue 1|mls|wnba|ncaa|ufc|wwe|"
        r"super bowl|playoff|finals|mvp|copa|euro 202|afc|nfc|"
        r"cricket|rugby|tennis|golf|formula 1|f1 grand prix|"
        r"manchester|liverpool|chelsea|arsenal|barcelona|real madrid|"
        r"lakers|celtics|warriors|knicks|nets|76ers|sixers|bulls|heat|"
        r"mavericks|clippers|rockets|spurs|bucks|nuggets|pistons|"
        r"wizards|hornets|raptors|pacers|grizzlies|cavaliers|"
        r"suns|kings|hawks|magic|thunder|jazz|timberwolves|pelicans|"
        r"blazers|trail blazers|national football team|"
        r"yankees|dodgers|braves|astros|phillies|padres|mets|cubs|"
        r"red sox|chiefs|eagles|bills|lions|ravens|49ers|cowboys|"
        r"bengals|dolphins|vikings|packers|rams|saints|steelers|"
        r"chargers|jaguars|texans|colts|commanders|bears|"
        r"broncos|seahawks|panthers|falcons|giants|"
        r"win\s+(the\s+)?(championship|title|trophy|cup|ring|pennant|"
        r"world series|stanley cup|super bowl))\b",
    ]),
    ("crypto", [
        r"\b(bitcoin|btc|ethereum|eth|solana|sol|crypto|token|"
        r"dogecoin|doge|xrp|cardano|ada|polkadot|"
        r"defi|nft|blockchain|halving|memecoin|altcoin|stablecoin)\b",
    ]),
    ("political", [
        r"\b(trump|biden|congress|senate|house|election|vote|"
        r"democrat|republican|gop|potus|governor|mayor|"
        r"impeach|cabinet|confirm|nominate|executive order|"
        r"fed\s+chair|tariff|sanction|diplomatic|nato|un\b|"
        r"legislation|bill\s+pass|veto|filibuster|midterm|"
        r"presidential|primary|caucus|electoral|inaugurat|"
        r"supreme court|scotus|speaker of|majority leader|"
        r"government shutdown|debt ceiling|border|immigration|"
        r"pardon|indictment|classified|special counsel|"
        r"war\b|invasion|cease.?fire|peace\s+deal)\b",
    ]),
    ("legal", [
        r"\b(sentenc|verdict|trial|convict|acquit|guilty|"
        r"indict|prosecut|lawsuit|settlement|court|judge|"
        r"prison|jail|parole|extradite|plea\s+deal|"
        r"appeal|ruling|injunction|subpoena|testimony|"
        r"weinstein|bankman|sbf|epstein|diddy|combs)\b",
    ]),
    ("economic", [
        r"\b(recession|inflation|cpi|gdp|unemployment|fed\s+rate|"
        r"interest rate|rate\s+cut|rate\s+hike|fomc|"
        r"treasury|yield|bond|stock market|s&p|nasdaq|dow|"
        r"oil\s+price|gas\s+price|housing|real estate|"
        r"trade\s+deficit|jobs\s+report|nonfarm|"
        r"revenue|budget|fiscal|deficit|surplus|"
        r"stimulus|quantitative|taper)\b",
    ]),
]
_CAT_COMPILED = [(cat, [re.compile(p, re.IGNORECASE) for p in pats]) for cat, pats in _CAT_RULES]


def classify_market(question: str) -> str:
    """Return market category: sports|crypto|political|legal|economic|other."""
    for cat, patterns in _CAT_COMPILED:
        for pat in patterns:
            if pat.search(question):
                return cat
    return "other"


# Category-specific minimum thresholds
# Sports longshots get brutally high bars; political/legal/economic get lower
CAT_MIN_DEPTH: Dict[str, float] = {
    "sports": 10_000, "crypto": 3_000, "political": 1_500,
    "legal": 1_500, "economic": 2_000, "other": 3_000,
}
CAT_MIN_LIQUIDITY: Dict[str, float] = {
    "sports": 50_000, "crypto": 5_000, "political": 2_000,
    "legal": 2_000, "economic": 3_000, "other": 5_000,
}
# Confidence boost per category (political/legal are most actionable on microstructure signals)
CAT_CONF_BOOST: Dict[str, int] = {
    "sports": -15, "crypto": 0, "political": +10,
    "legal": +12, "economic": +5, "other": 0,
}
# Max alerts per hour per category
CAT_MAX_ALERTS_HOUR: Dict[str, int] = {
    "sports": 1, "crypto": 3, "political": 5,
    "legal": 5, "economic": 4, "other": 2,
}


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


def parse_end_date_to_days(raw: Any) -> Optional[float]:
    """Parse endDate field and return days until resolution, or None if unparseable."""
    if raw is None:
        return None
    now_ts = time.time()
    ts: Optional[float] = None
    if isinstance(raw, (int, float)):
        ts = float(raw)
    elif isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            elif "T" in s and "+" not in s and s.count("-") >= 2:
                s = s + "+00:00"
            dt = datetime.fromisoformat(s)
            ts = dt.timestamp()
        except Exception:
            try:
                ts = float(s)
            except Exception:
                return None
    if ts is None:
        return None
    days = (ts - now_ts) / 86400.0
    return days if days > 0 else 0.0


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
    category: str = "other"
    days_to_resolution: Optional[float] = None
    tags: List[str] = field(default_factory=list)

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
    grade: str
    poly_yes_token: str
    poly_no_token: str


@dataclass
class TradeRecord:
    ts: float
    price: float
    size: float
    direction: str
    notional: float


# ═══════════════════════════════════════════
# Unified Alert Signal
# ═══════════════════════════════════════════

@dataclass
class AlertSignal:
    strategy: str
    market_name: str
    edge_cents: float
    confidence: int
    entry_price: float
    entry_side: str
    target_price: Optional[float] = None
    stop_price: Optional[float] = None
    depth_available: Optional[float] = None
    time_horizon: str = ""
    url: str = ""
    details: str = ""
    category: str = "other"
    token_id: str = ""

    def score(self) -> float:
        depth = max(self.depth_available or 100, 100)
        return self.edge_cents * (self.confidence / 100.0) * math.log10(depth)

    def format(self) -> str:
        cat_emoji = {"political": "🏛", "legal": "⚖️", "economic": "📊",
                     "crypto": "₿", "sports": "⚽", "other": "🎯"}.get(self.category, "🎯")
        lines = [
            f"{cat_emoji} [{self.strategy}] {self.market_name}",
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
# Market Quality Gate
# ═══════════════════════════════════════════

class MarketQualityGate:
    """
    Centralised gatekeeper. Every signal must pass this BEFORE being emitted.
    Kills: extreme prices, zero depth, sports longshots, stale/far-dated markets,
    low-liquidity garbage, repeated spam.
    """

    def __init__(
        self,
        markets: Dict[str, PolyMarket],
        min_price: float = 0.05,
        max_price: float = 0.95,
        max_days_to_resolution: float = 270.0,
        min_market_volume: float = 5_000.0,
        global_max_alerts_hour: int = 15,
    ):
        self.markets = markets
        self.min_px = min_price
        self.max_px = max_price
        self.max_days = max_days_to_resolution
        self.min_vol = min_market_volume
        self.global_max = global_max_alerts_hour

        # Per-token rate limiter: token_id -> deque of alert timestamps
        self._token_alerts: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=50))
        # Global rate limiter
        self._global_alerts: Deque[float] = deque(maxlen=200)
        # Per-market duplicate suppressor: (token_id, strategy) -> last alert ts
        self._dedup: Dict[Tuple[str, str], float] = {}
        self._dedup_cooldown = 1800.0  # 30 min same market+strategy

    def check(self, signal: AlertSignal, mid_price: Optional[float] = None) -> Optional[str]:
        """
        Return None if signal passes, or a rejection reason string.
        """
        now = time.time()

        # 1. Basic quality: edge and confidence
        if signal.edge_cents < 0.5:
            return "edge < 0.5c"
        if signal.confidence < 25:
            return "confidence < 25"
        if signal.depth_available is not None and signal.depth_available < 50:
            return "depth < $50"

        # 2. Price extremity filter
        px = mid_price or signal.entry_price
        if px < self.min_px or px > self.max_px:
            return f"price {px:.3f} outside [{self.min_px}, {self.max_px}]"

        # 3. Market metadata filters
        mkt = self.markets.get(signal.token_id)
        if mkt:
            cat = mkt.category

            # Time to resolution
            if mkt.days_to_resolution is not None and mkt.days_to_resolution > self.max_days:
                return f"resolves in {mkt.days_to_resolution:.0f}d (max {self.max_days:.0f}d)"

            # Market volume gate
            vol = mkt.volume or 0
            if vol < self.min_vol:
                return f"market volume {as_money(vol)} < {as_money(self.min_vol)}"

            # Market liquidity gate (category-specific)
            liq = mkt.liquidity or 0
            min_liq = CAT_MIN_LIQUIDITY.get(cat, 5000)
            if liq < min_liq:
                return f"liquidity {as_money(liq)} < {as_money(min_liq)} ({cat})"

            # Depth gate (category-specific)
            if signal.depth_available is not None:
                min_d = CAT_MIN_DEPTH.get(cat, 3000)
                if signal.depth_available < min_d:
                    return f"depth {as_money(signal.depth_available)} < {as_money(min_d)} ({cat})"

            # Sports longshot: extra filter (price near extreme + sports = kill)
            if cat == "sports" and (px < 0.08 or px > 0.92):
                return "sports longshot near extreme"

            # Category rate limit
            cat_max = CAT_MAX_ALERTS_HOUR.get(cat, 3)
            # Count recent alerts for any token in this category
            cat_count = 0
            for tid, ts_q in self._token_alerts.items():
                t_mkt = self.markets.get(tid)
                if t_mkt and t_mkt.category == cat:
                    cat_count += sum(1 for t in ts_q if now - t < 3600)
            if cat_count >= cat_max:
                return f"category '{cat}' rate limit ({cat_max}/hr)"

        # 4. Per-market dedup: same (token, strategy) within cooldown
        dedup_key = (signal.token_id, signal.strategy)
        if dedup_key in self._dedup and (now - self._dedup[dedup_key]) < self._dedup_cooldown:
            return f"dedup: same market+strategy within {self._dedup_cooldown / 60:.0f}m"

        # 5. Per-token rate limit (max 2/hr per token regardless of strategy)
        recent_token = sum(1 for t in self._token_alerts.get(signal.token_id, []) if now - t < 3600)
        if recent_token >= 2:
            return f"token rate limit (2/hr)"

        # 6. Global rate limit
        recent_global = sum(1 for t in self._global_alerts if now - t < 3600)
        if recent_global >= self.global_max:
            return f"global rate limit ({self.global_max}/hr)"

        return None  # passes

    def record(self, signal: AlertSignal) -> None:
        """Record that this signal was emitted."""
        now = time.time()
        self._token_alerts[signal.token_id].append(now)
        self._global_alerts.append(now)
        self._dedup[(signal.token_id, signal.strategy)] = now

    def apply_category_boost(self, signal: AlertSignal) -> AlertSignal:
        """Apply category-based confidence adjustments."""
        boost = CAT_CONF_BOOST.get(signal.category, 0)
        signal.confidence = max(10, min(98, signal.confidence + boost))
        return signal


# ═══════════════════════════════════════════
# Composite Signal Tracker
# ═══════════════════════════════════════════

class CompositeTracker:
    """
    Track recent signals per token. When multiple strategies agree on
    the same direction within a window, boost confidence.
    When they conflict, suppress.
    """

    def __init__(self, window_sec: float = 600.0):
        self.window = window_sec
        # token_id -> deque of (ts, strategy, direction)
        self._recent: Dict[str, Deque[Tuple[float, str, str]]] = defaultdict(
            lambda: deque(maxlen=20)
        )

    def record(self, token_id: str, strategy: str, direction: str) -> None:
        self._recent[token_id].append((time.time(), strategy, direction))

    def agreement_score(self, token_id: str, direction: str) -> Tuple[int, int]:
        """Return (agreeing_strategies, conflicting_strategies) in recent window."""
        now = time.time()
        agree, conflict = set(), set()
        for ts, strat, d in self._recent.get(token_id, []):
            if now - ts > self.window:
                continue
            if d == direction:
                agree.add(strat)
            else:
                conflict.add(strat)
        return len(agree), len(conflict)

    def adjust_confidence(self, signal: AlertSignal) -> AlertSignal:
        """Boost if strategies agree, suppress if they conflict."""
        direction = "BUY" if "BUY" in signal.entry_side else "SELL"
        agree, conflict = self.agreement_score(signal.token_id, direction)

        if agree >= 2:
            signal.confidence = min(95, signal.confidence + 12)
            signal.details += f"\n✅ CONFIRMED by {agree} strategies"
        elif conflict >= 2 and agree == 0:
            signal.confidence = max(10, signal.confidence - 20)
            signal.details += f"\n⚠️ CONFLICTED: {conflict} strategies disagree"

        return signal


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
                    category TEXT,
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
                    "(ts,strategy,market,category,edge_cents,confidence,entry_price,entry_side,"
                    "target_price,stop_price,depth_usd,score,details) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        now_utc_iso(), signal.strategy, signal.market_name,
                        signal.category,
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
                yb = safe_float(m.get("yes_bid"))
                ya = safe_float(m.get("yes_ask"))
                yes_bid = (yb / 100.0 if yb and yb > 1.0 else yb) or 0.0
                yes_ask = (ya / 100.0 if ya and ya > 1.0 else ya) or 1.0
                markets.append(KalshiMarket(
                    ticker=m.get("ticker", ""), title=m.get("title", ""),
                    event_ticker=m.get("event_ticker", ""),
                    close_time=m.get("close_time"),
                    yes_bid=yes_bid, yes_ask=yes_ask,
                    volume=safe_float(m.get("volume")) or 0.0,
                    open_interest=safe_float(m.get("open_interest")) or 0.0,
                ))
            cursor = data.get("cursor")
            if not cursor or not data.get("markets"):
                break
            time.sleep(0.1)
        return markets

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

    def parse_orderbook_bba(self, ob: Dict[str, Any]) -> Tuple[float, float, float, float]:
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
            best_yes_bid[0], 1.0 - best_no_bid[0],
            sum(p * q for p, q in yes),
            sum((1.0 - p) * q for p, q in no),
        )


# ═══════════════════════════════════════════
# Gamma Market Fetcher (Polymarket)
# ═══════════════════════════════════════════

def fetch_polymarket_markets(per_page: int = 100, pages: int = 30) -> Dict[str, PolyMarket]:
    """Return {token_id: PolyMarket} for all active markets with enriched metadata."""
    markets: Dict[str, PolyMarket] = {}
    cat_counts: Dict[str, int] = defaultdict(int)

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
            question = m.get("question") or "?"

            # Parse endDate for time-to-resolution
            days = parse_end_date_to_days(m.get("endDate"))

            # Parse tags
            raw_tags = m.get("tags") or []
            if isinstance(raw_tags, str):
                try:
                    raw_tags = json.loads(raw_tags)
                except Exception:
                    raw_tags = []
            tags = [str(t.get("label") or t) if isinstance(t, dict) else str(t) for t in raw_tags]

            # Classify category
            cat = classify_market(question)
            cat_counts[cat] += 1

            mkt = PolyMarket(
                condition_id=m.get("conditionId") or "",
                question=question,
                event_slug=m.get("eventSlug") or m.get("slug"),
                outcomes=outcomes,
                clob_token_ids=clob_ids,
                volume=safe_float(m.get("volume")),
                liquidity=safe_float(m.get("liquidity")),
                category=cat,
                days_to_resolution=days,
                tags=tags,
            )
            for tid in clob_ids:
                markets[tid] = mkt
        time.sleep(0.15)

    print(f"[gamma] Category breakdown: {dict(cat_counts)}")
    return markets


# ═══════════════════════════════════════════
# STRATEGY 1 — Cross-Platform Arbitrage
# ═══════════════════════════════════════════

class CrossPlatformArbitrage:
    def __init__(
        self, kalshi: KalshiClient, poly_markets: Dict[str, PolyMarket],
        min_edge_cents: float = 1.5, min_match_score: float = 75.0,
        kalshi_fee_bps: float = 0.0, poly_fee_bps: float = 0.0,
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
        self._poly_by_q: Dict[str, PolyMarket] = {}
        seen: Set[str] = set()
        for mkt in poly_markets.values():
            if mkt.question not in seen:
                self._poly_by_q[mkt.question] = mkt
                seen.add(mkt.question)

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
                name=km.title[:80], poly_market=pm, kalshi_market=km,
                match_score=best_score, grade=grade,
                poly_yes_token=pm.clob_token_ids[0],
                poly_no_token=pm.clob_token_ids[1],
            ))
        self.pairs = pairs
        print(f"[arb] Discovered {len(pairs)} cross-platform pairs:")
        for p in pairs[:15]:
            print(f"  [{p.grade}] {p.match_score:.0f}%  K:{p.kalshi_market.ticker}  ↔  P:{p.poly_market.question[:55]}")
        return pairs

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
                    f"{POLY_CLOB_BASE}/book", params={"token_id": pair.poly_yes_token}, timeout=10).json()
                pn_book = requests.get(
                    f"{POLY_CLOB_BASE}/book", params={"token_id": pair.poly_no_token}, timeout=10).json()
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

            cost1 = ky_ask + pn_ask + ky_ask * (self.k_fee / 1e4) + pn_ask * (self.p_fee / 1e4)
            edge1 = (1.0 - cost1) * 100.0
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
                        edge_cents=edge, confidence=conf,
                        entry_price=entry_px, entry_side=label,
                        depth_available=min(k_ask_d, k_bid_d) if k_ask_d and k_bid_d else None,
                        time_horizon=f"Hold to settlement ({pair.kalshi_market.close_time or 'TBD'})",
                        url=pair.poly_market.url(),
                        category=pair.poly_market.category,
                        token_id=pair.poly_yes_token,
                        details=(
                            f"Grade {pair.grade} ({pair.match_score:.0f}%) | Kalshi: {pair.kalshi_market.ticker}\n"
                            f"K YES {ky_bid:.3f}/{ky_ask:.3f} | P YES {py_bid:.3f}/{py_ask:.3f} | P NO {pn_bid:.3f}/{pn_ask:.3f}"
                        ),
                    ))
                    break
        return signals


# ═══════════════════════════════════════════
# STRATEGY 2 — Orderbook Imbalance Momentum
# ═══════════════════════════════════════════

class OrderbookImbalance:
    def __init__(
        self, markets: Dict[str, PolyMarket],
        imbalance_threshold: float = 3.5,
        momentum_window_sec: float = 90.0,
        min_momentum_bps: float = 80.0,
        min_total_depth: float = 3_000.0,
        max_spread: float = 0.05,
        max_levels: int = 10,
    ):
        self.markets = markets
        self.threshold = imbalance_threshold
        self.mom_window = momentum_window_sec
        self.min_mom_bps = min_momentum_bps
        self.min_depth = min_total_depth
        self.max_spread = max_spread
        self.max_levels = max_levels
        self._mid_hist: Dict[str, Deque[Tuple[float, float]]] = defaultdict(lambda: deque(maxlen=200))
        self._last_alert: Dict[str, float] = {}
        self._cooldown = 600.0

    def update(
        self, token_id: str,
        bids: List[Tuple[float, float]],
        asks: List[Tuple[float, float]],
    ) -> Optional[AlertSignal]:
        if token_id not in self.markets:
            return None
        mkt = self.markets[token_id]

        now = time.time()
        if token_id in self._last_alert and (now - self._last_alert[token_id]) < self._cooldown:
            return None
        if not bids or not asks:
            return None

        best_bid = max(p for p, _ in bids)
        best_ask = min(p for p, _ in asks)
        mid = (best_bid + best_ask) / 2
        spread = best_ask - best_bid

        # Tight price range and spread filter
        if mid < 0.08 or mid > 0.92:
            return None
        if spread > self.max_spread:
            return None

        self._mid_hist[token_id].append((now, mid))

        bid_d = sum(p * s for p, s in bids[:self.max_levels] if 0.08 <= p <= 0.92)
        ask_d = sum(p * s for p, s in asks[:self.max_levels] if 0.08 <= p <= 0.92)
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

        # Momentum confirmation with tighter window
        recent = [(t, m) for t, m in self._mid_hist[token_id] if now - t <= self.mom_window]
        if len(recent) < 4:
            return None
        mom_bps = ((mid - recent[0][1]) / recent[0][1]) * 10_000 if recent[0][1] > 0 else 0
        if side == "BUY" and mom_bps < self.min_mom_bps:
            return None
        if side == "SELL" and mom_bps > -self.min_mom_bps:
            return None

        self._last_alert[token_id] = now
        outcome = mkt.outcome_for_token(token_id)
        est_move = spread * (r - 1) / r
        edge = max(est_move * 100, 1.0)
        entry = best_ask if side == "BUY" else best_bid
        target = entry + est_move if side == "BUY" else entry - est_move
        stop = entry - spread * 2 if side == "BUY" else entry + spread * 2

        return AlertSignal(
            strategy="BOOK IMBALANCE",
            market_name=f"{mkt.question[:60]} ({outcome})",
            edge_cents=edge, confidence=min(80, int(40 + r * 6)),
            entry_price=round(entry, 3), entry_side=f"{side} YES",
            target_price=round(target, 3), stop_price=round(stop, 3),
            depth_available=bid_d if side == "BUY" else ask_d,
            time_horizon="5-30 min", url=mkt.url(),
            category=mkt.category, token_id=token_id,
            details=(
                f"Bid depth: {as_money(bid_d)} | Ask depth: {as_money(ask_d)} | Ratio: {r:.1f}:1\n"
                f"Momentum: {mom_bps:.0f}bps/{self.mom_window:.0f}s | Spread: {spread * 100:.1f}¢ | Cat: {mkt.category}"
            ),
        )


# ═══════════════════════════════════════════
# STRATEGY 3 — VWAP Mean Reversion / Momentum
# ═══════════════════════════════════════════

class VWAPDivergence:
    def __init__(
        self, markets: Dict[str, PolyMarket],
        vwap_window_sec: float = 1800.0, min_deviation_pct: float = 10.0,
        min_trades: int = 15, min_notional: float = 2_000.0,
        low_vol_pctile: float = 0.3,
    ):
        self.markets = markets
        self.window = vwap_window_sec
        self.min_dev = min_deviation_pct
        self.min_trades = min_trades
        self.min_notional = min_notional
        self.low_vol = low_vol_pctile
        self._trades: Dict[str, Deque[TradeRecord]] = defaultdict(lambda: deque(maxlen=2000))
        self._mid: Dict[str, float] = {}
        self._last_alert: Dict[str, float] = {}
        self._cooldown = 900.0

    def add_trade(self, token_id: str, price: float, size: float, direction: str = "BUY") -> None:
        if token_id in self.markets:
            self._trades[token_id].append(
                TradeRecord(ts=time.time(), price=price, size=size, direction=direction, notional=price * size))

    def update_mid(self, token_id: str, mid: float) -> None:
        self._mid[token_id] = mid

    def check(self, token_id: str) -> Optional[AlertSignal]:
        if token_id not in self.markets or token_id not in self._mid:
            return None
        mkt = self.markets[token_id]
        now = time.time()
        if token_id in self._last_alert and (now - self._last_alert[token_id]) < self._cooldown:
            return None

        cur = self._mid[token_id]
        # Price range gate
        if cur < 0.08 or cur > 0.92:
            return None

        recent = [t for t in self._trades[token_id] if now - t.ts <= self.window]
        if len(recent) < self.min_trades:
            return None
        total_notional = sum(t.notional for t in recent)
        if total_notional < self.min_notional:
            return None

        total_pv = sum(t.price * t.size for t in recent)
        total_v = sum(t.size for t in recent)
        if total_v == 0:
            return None
        vwap = total_pv / total_v
        if vwap <= 0:
            return None

        dev_pct = ((cur - vwap) / vwap) * 100
        if abs(dev_pct) < self.min_dev:
            return None

        v5 = sum(t.notional for t in recent if now - t.ts <= 300)
        avg5 = total_notional / max(self.window / 300, 1)
        low_vol = v5 < avg5 * self.low_vol if avg5 > 0 else True

        if low_vol:
            side = "SELL YES" if dev_pct > 0 else "BUY YES"
            target = vwap
            edge = abs(cur - vwap) * 100
            conf = min(75, int(40 + abs(dev_pct) * 1.5))
            strat = "VWAP REVERSION"
            horizon = "15-60 min"
            stop = cur + (cur - vwap) * 0.5
        else:
            side = "BUY YES" if dev_pct > 0 else "SELL YES"
            target = cur + (cur - vwap) * 0.5
            edge = abs(cur - vwap) * 0.5 * 100
            conf = min(70, int(35 + abs(dev_pct) * 1.2))
            strat = "VWAP MOMENTUM"
            horizon = "30-120 min"
            stop = vwap

        self._last_alert[token_id] = now
        outcome = mkt.outcome_for_token(token_id)

        return AlertSignal(
            strategy=strat,
            market_name=f"{mkt.question[:60]} ({outcome})",
            edge_cents=max(edge, 0.5), confidence=conf,
            entry_price=round(cur, 3), entry_side=side,
            target_price=round(target, 3), stop_price=round(stop, 3),
            depth_available=v5 if v5 > 0 else None,
            time_horizon=horizon, url=mkt.url(),
            category=mkt.category, token_id=token_id,
            details=(
                f"VWAP: {vwap:.3f} | Price: {cur:.3f} | Dev: {dev_pct:+.1f}%\n"
                f"Volume: {'LOW → fade' if low_vol else 'HIGH → follow'} | "
                f"5m vol: {as_money(v5)} vs avg: {as_money(avg5)} | Cat: {mkt.category}"
            ),
        )


# ═══════════════════════════════════════════
# STRATEGY 4 — Trade Flow Toxicity (VPIN)
# ═══════════════════════════════════════════

class FlowToxicity:
    def __init__(
        self, markets: Dict[str, PolyMarket],
        bucket_size_usd: float = 1_000.0,
        num_buckets: int = 20,
        toxicity_threshold: float = 0.70,
        min_directional_pct: float = 75.0,
        min_throughput_usd: float = 5_000.0,
    ):
        self.markets = markets
        self.bucket_sz = bucket_size_usd
        self.n_buckets = num_buckets
        self.threshold = toxicity_threshold
        self.min_dir = min_directional_pct / 100.0
        self.min_throughput = min_throughput_usd

        self._buy: Dict[str, float] = defaultdict(float)
        self._sell: Dict[str, float] = defaultdict(float)
        self._vol: Dict[str, float] = defaultdict(float)
        self._total_vol: Dict[str, float] = defaultdict(float)
        self._imb: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=60))
        self._dirs: Dict[str, Deque[str]] = defaultdict(lambda: deque(maxlen=60))
        self._bucket_ts: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=60))
        self._last_alert: Dict[str, float] = {}
        self._cooldown = 900.0
        self._quote: Dict[str, Tuple[float, float]] = {}

    def update_quote(self, token_id: str, bid: float, ask: float) -> None:
        self._quote[token_id] = (bid, ask)

    def add_trade(self, token_id: str, price: float, size: float) -> Optional[AlertSignal]:
        if token_id not in self.markets:
            return None

        # Price range gate
        if token_id in self._quote:
            bid, ask = self._quote[token_id]
            mid = (bid + ask) / 2
            if mid < 0.08 or mid > 0.92:
                return None
            direction = "BUY" if price >= mid else "SELL"
        else:
            if price < 0.08 or price > 0.92:
                return None
            direction = "BUY" if price >= 0.5 else "SELL"

        notional = price * size
        if direction == "BUY":
            self._buy[token_id] += notional
        else:
            self._sell[token_id] += notional
        self._vol[token_id] += notional
        self._total_vol[token_id] += notional

        if self._vol[token_id] < self.bucket_sz:
            return None

        b, s = self._buy[token_id], self._sell[token_id]
        total = b + s
        if total > 0:
            self._imb[token_id].append(abs(b - s) / total)
            self._dirs[token_id].append("BUY" if b > s else "SELL")
            self._bucket_ts[token_id].append(time.time())

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

        # Min throughput gate: total volume through the buckets we're analysing
        throughput = self.n_buckets * self.bucket_sz
        if throughput < self.min_throughput:
            return None

        vpin = sum(imbs[-self.n_buckets:]) / self.n_buckets
        if vpin < self.threshold:
            return None

        buy_n = sum(1 for d in dirs[-self.n_buckets:] if d == "BUY")
        sell_n = self.n_buckets - buy_n
        dom_dir = "BUY" if buy_n > sell_n else "SELL"
        dom_pct = max(buy_n, sell_n) / self.n_buckets
        if dom_pct < self.min_dir:
            return None

        # Recency check: last N buckets should have filled within 30 min
        bucket_times = list(self._bucket_ts[token_id])
        if len(bucket_times) >= self.n_buckets:
            span = bucket_times[-1] - bucket_times[-self.n_buckets]
            if span > 1800:
                return None  # buckets are too spread out, not concentrated flow

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
            edge_cents=edge, confidence=min(80, int(vpin * 95)),
            entry_price=round(entry, 3), entry_side=f"{dom_dir} YES",
            target_price=round(target, 3), stop_price=round(stop, 3),
            depth_available=throughput,
            time_horizon="10-60 min", url=mkt.url(),
            category=mkt.category, token_id=token_id,
            details=(
                f"VPIN: {vpin:.2f} | Direction: {dom_dir} ({dom_pct * 100:.0f}% of {self.n_buckets} buckets)\n"
                f"Throughput: {as_money(throughput)} | Cat: {mkt.category}\n"
                f"Informed flow detected — smart money aggressively {dom_dir.lower()}ing"
            ),
        )


# ═══════════════════════════════════════════
# STRATEGY 5 — Liquidity Vacuum Detection
# ═══════════════════════════════════════════

class LiquidityVacuum:
    def __init__(
        self, markets: Dict[str, PolyMarket],
        min_gap_cents: float = 4.0,
        min_depth_before: float = 3_000.0,
        depth_drop_pct: float = 70.0,
    ):
        self.markets = markets
        self.min_gap = min_gap_cents
        self.min_prev = min_depth_before
        self.drop_pct = depth_drop_pct
        self._prev: Dict[str, Tuple[float, float]] = {}
        self._last_alert: Dict[str, float] = {}
        self._cooldown = 900.0

    def update(
        self, token_id: str,
        bids: List[Tuple[float, float]],
        asks: List[Tuple[float, float]],
    ) -> Optional[AlertSignal]:
        if token_id not in self.markets:
            return None
        mkt = self.markets[token_id]

        now = time.time()
        if token_id in self._last_alert and (now - self._last_alert[token_id]) < self._cooldown:
            return None
        if not bids or not asks:
            return None

        best_bid = max(p for p, _ in bids)
        best_ask = min(p for p, _ in asks)
        mid = (best_bid + best_ask) / 2
        if mid < 0.08 or mid > 0.92:
            return None

        bid_d = sum(p * s for p, s in bids if 0.08 <= p <= 0.92)
        ask_d = sum(p * s for p, s in asks if 0.08 <= p <= 0.92)

        ask_ps = sorted(p for p, _ in asks if 0.08 <= p <= 0.92)
        bid_ps = sorted((p for p, _ in bids if 0.08 <= p <= 0.92), reverse=True)
        ask_gap = max((ask_ps[i + 1] - ask_ps[i] for i in range(len(ask_ps) - 1)), default=0)
        bid_gap = max((bid_ps[i] - bid_ps[i + 1] for i in range(len(bid_ps) - 1)), default=0)

        prev = self._prev.get(token_id)
        self._prev[token_id] = (bid_d, ask_d)
        if prev is None:
            return None

        prev_bid, prev_ask = prev
        drop = 1.0 - self.drop_pct / 100.0
        outcome = mkt.outcome_for_token(token_id)

        if prev_ask > self.min_prev and ask_d < prev_ask * drop and ask_gap * 100 >= self.min_gap:
            self._last_alert[token_id] = now
            return AlertSignal(
                strategy="LIQ VACUUM",
                market_name=f"{mkt.question[:60]} ({outcome})",
                edge_cents=ask_gap * 50, confidence=min(75, int(40 + ask_gap * 150)),
                entry_price=best_ask, entry_side="BUY YES",
                target_price=round(best_ask + ask_gap * 0.5, 3),
                stop_price=round(best_bid - 0.01, 3),
                depth_available=ask_d,
                time_horizon="1-15 min", url=mkt.url(),
                category=mkt.category, token_id=token_id,
                details=(
                    f"Ask depth: {as_money(prev_ask)} → {as_money(ask_d)} "
                    f"({(1 - ask_d / prev_ask) * 100:.0f}% drop)\n"
                    f"Gap: {ask_gap * 100:.1f}¢ | Cat: {mkt.category}"
                ),
            )

        if prev_bid > self.min_prev and bid_d < prev_bid * drop and bid_gap * 100 >= self.min_gap:
            self._last_alert[token_id] = now
            return AlertSignal(
                strategy="LIQ VACUUM",
                market_name=f"{mkt.question[:60]} ({outcome})",
                edge_cents=bid_gap * 50, confidence=min(75, int(40 + bid_gap * 150)),
                entry_price=best_bid, entry_side="SELL YES",
                target_price=round(best_bid - bid_gap * 0.5, 3),
                stop_price=round(best_ask + 0.01, 3),
                depth_available=bid_d,
                time_horizon="1-15 min", url=mkt.url(),
                category=mkt.category, token_id=token_id,
                details=(
                    f"Bid depth: {as_money(prev_bid)} → {as_money(bid_d)} "
                    f"({(1 - bid_d / prev_bid) * 100:.0f}% drop)\n"
                    f"Gap: {bid_gap * 100:.1f}¢ | Cat: {mkt.category}"
                ),
            )

        return None


# ═══════════════════════════════════════════
# Alpha Engine v3 — Orchestrator
# ═══════════════════════════════════════════

class AlphaEngine:
    def __init__(
        self,
        poly_markets: Dict[str, PolyMarket],
        kalshi: Optional[KalshiClient] = None,
        logger: Optional[AlertLogger] = None,
        telegram: bool = True,
        min_arb_edge: float = 1.5,
        imbalance_threshold: float = 3.5,
        vwap_deviation: float = 10.0,
        toxicity_threshold: float = 0.70,
        arb_poll_sec: float = 5.0,
    ):
        self.poly_markets = poly_markets
        self.token_ids = list(poly_markets.keys())
        self.logger = logger or AlertLogger()
        self.telegram = telegram
        self.arb_poll = arb_poll_sec

        # Quality gate + composite tracker
        self.gate = MarketQualityGate(poly_markets)
        self.composite = CompositeTracker()

        # Strategies
        self.imbalance = OrderbookImbalance(poly_markets, imbalance_threshold=imbalance_threshold)
        self.vwap = VWAPDivergence(poly_markets, min_deviation_pct=vwap_deviation)
        self.toxicity = FlowToxicity(poly_markets, toxicity_threshold=toxicity_threshold)
        self.vacuum = LiquidityVacuum(poly_markets)

        self.arb: Optional[CrossPlatformArbitrage] = None
        if kalshi and kalshi.enabled:
            self.arb = CrossPlatformArbitrage(kalshi, poly_markets, min_edge_cents=min_arb_edge)

        self._t0 = time.time()
        self._msg_count = 0
        self._alert_count = 0
        self._suppressed_count = 0
        self._trade_count = 0
        self._last_msg_ts = time.time()

    def _emit(self, signal: AlertSignal) -> None:
        # Apply category confidence boost
        signal = self.gate.apply_category_boost(signal)

        # Apply composite agreement/conflict adjustment
        signal = self.composite.adjust_confidence(signal)

        # Quality gate check
        rejection = self.gate.check(signal)
        if rejection:
            self._suppressed_count += 1
            if self._suppressed_count <= 20 or self._suppressed_count % 100 == 0:
                print(f"[gate] SUPPRESSED #{self._suppressed_count}: {signal.strategy} | "
                      f"{signal.market_name[:40]} | reason: {rejection}")
            return

        # Record in composite tracker
        direction = "BUY" if "BUY" in signal.entry_side else "SELL"
        self.composite.record(signal.token_id, signal.strategy, direction)

        # Record in gate
        self.gate.record(signal)

        self._alert_count += 1
        msg = signal.format()
        print(f"\n{'=' * 60}\n{msg}\n{'=' * 60}\n")
        self.logger.log(signal)
        if self.telegram:
            send_telegram(msg)

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
            if self._trade_count % 100 == 0:
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
        while True:
            time.sleep(180)
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
                f"trades={self._trade_count} ({rate:.1f}/m) | "
                f"alerts={self._alert_count} | suppressed={self._suppressed_count} | "
                f"arb_pairs={arb_pairs}"
            )

    def run(self) -> None:
        if not self.token_ids:
            print("[engine] No markets to monitor")
            return

        for fn in (self._arb_loop, self._rediscovery_loop, self._vwap_sweep_loop, self._heartbeat_loop):
            threading.Thread(target=fn, daemon=True).start()

        max_tokens = 1800
        sub_ids = self.token_ids[:max_tokens]
        strategies = ["BOOK IMBALANCE", "VWAP", "TOXIC FLOW", "LIQ VACUUM"]
        if self.arb:
            strategies.insert(0, "CROSS-PLATFORM ARB")

        # Count categories in subscribed tokens
        cat_summary: Dict[str, int] = defaultdict(int)
        for tid in sub_ids:
            if mkt := self.poly_markets.get(tid):
                cat_summary[mkt.category] += 1

        def on_open(ws):
            time.sleep(0.25)
            ws.send(json.dumps({"type": "market", "assets_ids": sub_ids}))
            print(f"[engine] Subscribed to {len(sub_ids)} tokens")
            print(f"[engine] Categories: {dict(cat_summary)}")
            print(f"[engine] Active strategies: {', '.join(strategies)}")
            print(f"[engine] Quality gate: price [{self.gate.min_px}-{self.gate.max_px}], "
                  f"max {self.gate.max_days:.0f}d to resolution, "
                  f"min vol {as_money(self.gate.min_vol)}, "
                  f"max {self.gate.global_max} alerts/hr")
            if self.telegram:
                arb_info = f"{len(self.arb.pairs)} discovered" if self.arb else "disabled"
                send_telegram(
                    f"🎯 Alpha Engine v3 Online\n\n"
                    f"Tokens: {len(sub_ids)}\n"
                    f"Strategies: {', '.join(strategies)}\n"
                    f"Cross-platform pairs: {arb_info}\n"
                    f"Quality gate: active (kills sports longshots, zero-depth, spam)\n"
                    f"Category-aware | Composite signal confirmation\n"
                    f"Target: 2-5 high-conviction alerts/day"
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
                on_open=on_open, on_message=on_message,
                on_error=on_error, on_close=on_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
            print("[ws] Reconnecting in 3s...")
            time.sleep(3)


# ═══════════════════════════════════════════
# CLI + Entry Point
# ═══════════════════════════════════════════

def main() -> None:
    ap = argparse.ArgumentParser(description="Polymarket Alpha Engine v3 — Precision Alpha System")
    ap.add_argument("--pages", type=int, default=30)
    ap.add_argument("--per-page", type=int, default=100)
    ap.add_argument("--telegram", action="store_true")
    # Backwards-compat flags (no-ops)
    ap.add_argument("--gamma-active", action="store_true", help="(deprecated)")
    ap.add_argument("--broad-test", action="store_true", help="(deprecated)")
    ap.add_argument("--whale-threshold", type=float, default=0.0, help="(deprecated)")
    # v3 tuning
    ap.add_argument("--min-arb-edge", type=float, default=1.5)
    ap.add_argument("--imbalance-threshold", type=float, default=3.5)
    ap.add_argument("--vwap-deviation", type=float, default=10.0)
    ap.add_argument("--toxicity-threshold", type=float, default=0.70)
    ap.add_argument("--arb-poll-sec", type=float, default=5.0)
    ap.add_argument("--db-path", type=str, default="data/alpha_engine.db")
    args = ap.parse_args()

    print("=" * 60)
    print("  Polymarket Alpha Engine v3")
    print("  Fewer alerts. Higher conviction. Every alert tradeable.")
    print("=" * 60)

    print("\n[fetch] Loading Polymarket markets from Gamma...")
    poly_markets = fetch_polymarket_markets(args.per_page, args.pages)
    print(f"[fetch] Loaded {len(poly_markets)} tokens")

    kalshi_key = os.environ.get("KALSHI_API_KEY")
    kalshi_pem = os.environ.get("KALSHI_PRIVATE_KEY_PEM")
    if not kalshi_pem:
        pem_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH")
        if pem_path:
            p = Path(pem_path)
            if p.is_file():
                kalshi_pem = p.read_text()

    kalshi = KalshiClient(os.getenv("KALSHI_BASE_URL", KALSHI_BASE_DEFAULT), kalshi_key, kalshi_pem)
    if kalshi.enabled:
        print("[kalshi] CROSS-PLATFORM ARB enabled")
    else:
        print("[kalshi] No credentials — cross-platform arb DISABLED")

    logger = AlertLogger(args.db_path)
    engine = AlphaEngine(
        poly_markets=poly_markets, kalshi=kalshi, logger=logger,
        telegram=args.telegram, min_arb_edge=args.min_arb_edge,
        imbalance_threshold=args.imbalance_threshold,
        vwap_deviation=args.vwap_deviation,
        toxicity_threshold=args.toxicity_threshold,
        arb_poll_sec=args.arb_poll_sec,
    )
    engine.run()


if __name__ == "__main__":
    main()
