#!/usr/bin/env python3
"""
Top Alpha watcher (separate service)

Implements "Informed sweep in a thin book" alert using ONLY the /ws/market stream:
  - trade events for sweep detection + VWAP
  - book events for spread + thinness/depth
  - price_change / last_trade_price events for follow-through confirmation

Designed to run as a SECOND Railway service, leaving your existing polymarket_v1_alerts.py untouched.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple

import requests
from websocket import WebSocketApp

CLOB_WS_BASE = "wss://ws-subscriptions-clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"


# -------------------------
# Helpers
# -------------------------

def env_truthy(v: Optional[str]) -> bool:
    if v is None:
        return False
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def send_telegram_message(body: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    enabled_env = env_truthy(os.getenv("TELEGRAM_ENABLED")) or env_truthy(os.getenv("ALERT_TELEGRAM_ENABLED"))
    enabled = enabled_env or (token and chat_id)
    if not enabled or not token or not chat_id:
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        requests.post(url, json={"chat_id": chat_id, "text": body, "disable_web_page_preview": True}, timeout=10)
    except Exception:
        pass


# -------------------------
# Data models
# -------------------------

@dataclass
class MarketMeta:
    market_question: str
    event_title: str
    event_slug: Optional[str]
    volume: Optional[float]
    liquidity: Optional[float]
    outcomes: List[str]
    clob_token_ids: List[str]
    end_ts: Optional[float]

    def outcome_for_token(self, token_id: str) -> str:
        try:
            i = self.clob_token_ids.index(token_id)
            return self.outcomes[i] if 0 <= i < len(self.outcomes) else "UNKNOWN"
        except Exception:
            return "UNKNOWN"

    def market_url(self) -> Optional[str]:
        if not self.event_slug:
            return None
        # Gamma uses event slug; Polymarket UI often uses /event/<slug>
        return f"https://polymarket.com/event/{self.event_slug}"


class TopAlphaLoggerSQLite:
    def __init__(self, path: str = "top_alpha_alerts.sqlite"):
        self.path = path
        self._init()

    def _init(self) -> None:
        con = sqlite3.connect(self.path)
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS top_alpha_alerts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts REAL NOT NULL,
              ts_utc TEXT NOT NULL,
              token_id TEXT NOT NULL,
              event_slug TEXT,
              question TEXT,
              outcome TEXT,
              direction TEXT,
              net_notional REAL,
              trade_count INTEGER,
              spread REAL,
              depth_cost_5c REAL,
              vwap5 REAL,
              move_from_vwap5 REAL
            )
            """
        )
        con.commit()
        con.close()

    def log(
        self,
        *,
        token_id: str,
        meta: MarketMeta,
        outcome: str,
        direction: str,
        net_notional: float,
        trade_count: int,
        spread: Optional[float],
        depth_cost_5c: Optional[float],
        vwap5: Optional[float],
        move_from_vwap5: Optional[float],
    ) -> None:
        con = sqlite3.connect(self.path)
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO top_alpha_alerts (
              ts, ts_utc, token_id, event_slug, question, outcome, direction,
              net_notional, trade_count, spread, depth_cost_5c, vwap5, move_from_vwap5
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                time.time(),
                now_utc_iso(),
                token_id,
                meta.event_slug,
                meta.market_question,
                outcome,
                direction,
                float(net_notional),
                int(trade_count),
                float(spread) if spread is not None else None,
                float(depth_cost_5c) if depth_cost_5c is not None else None,
                float(vwap5) if vwap5 is not None else None,
                float(move_from_vwap5) if move_from_vwap5 is not None else None,
            ),
        )
        con.commit()
        con.close()


# -------------------------
# Gamma fetch
# -------------------------

def fetch_gamma_markets(per_page: int, pages: int, *, active_only: bool = True) -> Tuple[List[MarketMeta], int]:
    now_ts = time.time()
    metas: List[MarketMeta] = []
    raw_markets_count = 0

    for p in range(pages):
        params = {"limit": per_page, "offset": p * per_page, "closed": "false"}
        if active_only:
            params["active"] = "true"

        url = f"{GAMMA_BASE}/markets"
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        markets = data if isinstance(data, list) else data.get("markets", [])
        raw_markets_count += len(markets)

        for m in markets:
            # Basic filters (mirror your current worker approach)
            end_date = m.get("endDate") or m.get("end_date")
            end_ts = None
            if end_date:
                try:
                    # Gamma endDate is usually ISO8601
                    end_ts = datetime.fromisoformat(end_date.replace("Z", "+00:00")).timestamp()
                    if end_ts < now_ts:
                        continue
                except Exception:
                    end_ts = None

            clob_ids = m.get("clobTokenIds") or m.get("clob_token_ids") or []
            if not clob_ids:
                continue

            outcomes = m.get("outcomes") or []
            if isinstance(outcomes, str):
                try:
                    outcomes = json.loads(outcomes)
                except Exception:
                    outcomes = [outcomes]

            # Ensure lengths match if possible; otherwise keep as-is and map best-effort
            meta = MarketMeta(
                market_question=m.get("question") or m.get("marketQuestion") or "UNKNOWN",
                event_title=m.get("eventTitle") or m.get("title") or "UNKNOWN",
                event_slug=m.get("eventSlug") or m.get("slug"),
                volume=safe_float(m.get("volume") or m.get("volumeNum") or m.get("volumeUSD")),
                liquidity=safe_float(m.get("liquidity") or m.get("liquidityNum") or m.get("liquidityUSD")),
                outcomes=list(outcomes) if isinstance(outcomes, list) else [str(outcomes)],
                clob_token_ids=list(clob_ids),
                end_ts=end_ts,
            )
            metas.append(meta)

        # be nice to Gamma
        time.sleep(0.2)

    return metas, raw_markets_count


# -------------------------
# Top Alpha logic
# -------------------------

@dataclass
class TradeEvent:
    ts: float
    price: float
    size: float
    notional: float
    direction: str  # "BUY" or "SELL"
    taker: bool


class TopAlphaWatcher:
    """
    Detects: "Informed sweep in a thin book" (your #1 TOP ALPHA)
    """

    def __init__(
        self,
        metas: List[MarketMeta],
        *,
        sweep_window_sec: int = 90,
        min_sweep_trades: int = 4,
        min_net_notional: float = 12500.0,
        max_depth_cost_5c: float = 3000.0,
        max_spread: float = 0.03,
        spread_compress_cents: float = 0.01,   # 1¢
        vwap_window_sec: int = 300,            # 5 minutes
        min_move_from_vwap: float = 0.04,      # 4¢
        max_revert: float = 0.015,             # 1.5¢
        follow_through_sec: int = 60,
        resolve_days_min: int = 7,
        resolve_days_max: int = 45,
        max_assets: int = 1800,
        heartbeat_sec: int = 30,
        telegram: bool = True,
        logger: Optional[TopAlphaLoggerSQLite] = None,
    ):
        self.metas = metas
        self.asset_to_meta: Dict[str, MarketMeta] = {tid: m for m in metas for tid in m.clob_token_ids}

        # Relevance filter (7–45 days)
        now_ts = time.time()
        self.eligible_assets: List[str] = []
        for tid, meta in self.asset_to_meta.items():
            if meta.end_ts is None:
                continue
            days = (meta.end_ts - now_ts) / 86400.0
            if resolve_days_min <= days <= resolve_days_max:
                self.eligible_assets.append(tid)

        self.asset_ids = self.eligible_assets[:max_assets]

        # Parameters
        self.sweep_window_sec = sweep_window_sec
        self.min_sweep_trades = min_sweep_trades
        self.min_net_notional = min_net_notional
        self.max_depth_cost_5c = max_depth_cost_5c
        self.max_spread = max_spread
        self.spread_compress_cents = spread_compress_cents
        self.vwap_window_sec = vwap_window_sec
        self.min_move_from_vwap = min_move_from_vwap
        self.max_revert = max_revert
        self.follow_through_sec = follow_through_sec
        self.heartbeat_sec = heartbeat_sec
        self.telegram = telegram
        self.logger = logger or TopAlphaLoggerSQLite()

        # State buffers
        self.trades: Dict[str, Deque[TradeEvent]] = defaultdict(lambda: deque(maxlen=5000))
        self.price_hist: Dict[str, Deque[Tuple[float, float]]] = defaultdict(lambda: deque(maxlen=5000))  # (ts, mid_or_last)
        self.spread_hist: Dict[str, Deque[Tuple[float, float]]] = defaultdict(lambda: deque(maxlen=2000))  # (ts, spread)
        self.last_quote: Dict[str, Tuple[Optional[float], Optional[float], Optional[float]]] = {}  # best_bid, best_ask, mid
        self.last_depth_cost_5c: Dict[str, Tuple[float, float]] = {}  # token -> (ts, cost)

        # Pending candidates for follow-through confirmation
        self.pending: Dict[str, Dict[str, Any]] = {}

        # WS heartbeat/debug
        self._last_msg_ts = time.time()

    # ---- parsing helpers ----

    def _infer_taker_and_direction(self, token_id: str, price: float) -> Tuple[bool, str]:
        """
        Best-effort inference:
          - If we have a quote: price >= best_ask -> BUY taker, price <= best_bid -> SELL taker
          - Else: assume taker and infer BUY if price >= 0.5 else SELL (fallback)
        """
        best_bid, best_ask, _mid = self.last_quote.get(token_id, (None, None, None))
        if best_ask is not None and price >= best_ask - 1e-9:
            return True, "BUY"
        if best_bid is not None and price <= best_bid + 1e-9:
            return True, "SELL"
        # ambiguous
        direction = "BUY" if price >= 0.5 else "SELL"
        return False, direction

    def _vwap(self, token_id: str, window_sec: int) -> Optional[float]:
        now = time.time()
        num = 0.0
        den = 0.0
        for t in reversed(self.trades[token_id]):
            if now - t.ts > window_sec:
                break
            num += t.price * t.size
            den += t.size
        if den <= 0:
            return None
        return num / den

    def _current_spread(self, token_id: str) -> Optional[float]:
        best_bid, best_ask, _mid = self.last_quote.get(token_id, (None, None, None))
        if best_bid is None or best_ask is None:
            return None
        return max(0.0, best_ask - best_bid)

    def _spread_5min_ago(self, token_id: str) -> Optional[float]:
        now = time.time()
        target = now - 300.0
        hist = self.spread_hist[token_id]
        if not hist:
            return None
        # find closest older point
        older = [s for (ts, s) in hist if ts <= target]
        if not older:
            return None
        return older[-1]

    def _depth_cost_to_move_5c(self, token_id: str, book: Dict[str, Any], direction: str) -> Optional[float]:
        """
        Approximates "cost to move price 5¢" by summing notional in the book
        until you reach (mid +/- 0.05). Uses asks for BUY, bids for SELL.

        book schema varies; we handle common shapes:
          - {"bids":[[price, size],...], "asks":[[price, size],...]}
          - {"bids":[{"price":..,"size":..},...], ...}
        """
        _bid, _ask, mid = self.last_quote.get(token_id, (None, None, None))
        if mid is None:
            # try infer mid from book top
            best_bid = self._best_from_side(book.get("bids") or book.get("bid") or [], want="bid")
            best_ask = self._best_from_side(book.get("asks") or book.get("ask") or [], want="ask")
            if best_bid is None or best_ask is None:
                return None
            mid = (best_bid + best_ask) / 2.0

        target = mid + 0.05 if direction == "BUY" else mid - 0.05
        side = book.get("asks") if direction == "BUY" else book.get("bids")
        if side is None:
            side = book.get("ask") if direction == "BUY" else book.get("bid")
        if side is None:
            return None

        levels = self._normalize_levels(side)
        if not levels:
            return None

        # Ensure sorted correctly
        levels.sort(key=lambda x: x[0], reverse=(direction == "SELL"))

        cost = 0.0
        for px, sz in levels:
            if sz <= 0:
                continue
            cost += px * sz
            if (direction == "BUY" and px >= target) or (direction == "SELL" and px <= target):
                break
        return cost

    def _normalize_levels(self, levels: Any) -> List[Tuple[float, float]]:
        out: List[Tuple[float, float]] = []
        if not isinstance(levels, list):
            return out
        for lv in levels:
            if isinstance(lv, (list, tuple)) and len(lv) >= 2:
                px = safe_float(lv[0]); sz = safe_float(lv[1])
            elif isinstance(lv, dict):
                px = safe_float(lv.get("price") or lv.get("p"))
                sz = safe_float(lv.get("size") or lv.get("s") or lv.get("quantity") or lv.get("q"))
            else:
                continue
            if px is None or sz is None:
                continue
            out.append((px, sz))
        return out

    def _best_from_side(self, levels: Any, *, want: str) -> Optional[float]:
        lvls = self._normalize_levels(levels)
        if not lvls:
            return None
        if want == "bid":
            return max(px for px, _ in lvls)
        return min(px for px, _ in lvls)

    # ---- event handlers ----

    def _handle_book(self, it: Dict[str, Any]) -> None:
        token_id = it.get("asset_id") or it.get("token_id") or it.get("market") or it.get("assetId")
        if token_id not in self.asset_to_meta:
            return

        book = it.get("book") if isinstance(it.get("book"), dict) else it
        bids = book.get("bids") or book.get("bid") or []
        asks = book.get("asks") or book.get("ask") or []
        best_bid = self._best_from_side(bids, want="bid")
        best_ask = self._best_from_side(asks, want="ask")
        if best_bid is None or best_ask is None:
            return
        mid = (best_bid + best_ask) / 2.0
        self.last_quote[token_id] = (best_bid, best_ask, mid)

        spread = max(0.0, best_ask - best_bid)
        self.spread_hist[token_id].append((time.time(), spread))

        # Update depth cost estimates for both directions (store as last seen BUY and SELL separately via suffix)
        buy_cost = self._depth_cost_to_move_5c(token_id, book, "BUY")
        sell_cost = self._depth_cost_to_move_5c(token_id, book, "SELL")
        if buy_cost is not None:
            self.last_depth_cost_5c[token_id + ":BUY"] = (time.time(), buy_cost)
        if sell_cost is not None:
            self.last_depth_cost_5c[token_id + ":SELL"] = (time.time(), sell_cost)

        # price history (mid)
        self.price_hist[token_id].append((time.time(), mid))

    def _handle_price(self, token_id: str, price: float) -> None:
        self.price_hist[token_id].append((time.time(), price))

    def _handle_trade(self, it: Dict[str, Any]) -> None:
        token_id = it.get("asset_id") or it.get("token_id") or it.get("market") or it.get("assetId")
        if token_id not in self.asset_to_meta:
            return

        price = safe_float(it.get("price") or it.get("p"))
        size = safe_float(it.get("size") or it.get("s") or it.get("amount") or it.get("qty"))
        if price is None or size is None:
            return
        notional = price * size

        # Determine taker + direction
        taker = bool(it.get("taker")) if "taker" in it else None
        side = it.get("side") or it.get("direction")
        if side:
            side = str(side).upper()
        if taker is None or side not in {"BUY", "SELL"}:
            inferred_taker, inferred_dir = self._infer_taker_and_direction(token_id, price)
            taker = inferred_taker if taker is None else bool(taker)
            direction = inferred_dir if side not in {"BUY", "SELL"} else side
        else:
            direction = side

        te = TradeEvent(ts=time.time(), price=price, size=size, notional=notional, direction=direction, taker=bool(taker))
        self.trades[token_id].append(te)
        self._handle_price(token_id, price)

        # each trade: see if we should start/refresh a pending candidate
        self._maybe_trigger_sweep(token_id)

    # ---- trigger evaluation ----

    def _maybe_trigger_sweep(self, token_id: str) -> None:
        now = time.time()
        meta = self.asset_to_meta[token_id]

        # If already pending, don't re-trigger (avoid spam). You can relax later.
        if token_id in self.pending:
            return

        # Gather last sweep_window_sec trades
        recent: List[TradeEvent] = []
        for t in reversed(self.trades[token_id]):
            if now - t.ts > self.sweep_window_sec:
                break
            recent.append(t)
        if len(recent) < self.min_sweep_trades:
            return
        recent.reverse()

        # Require taker-side (best-effort): at least min_sweep_trades are taker==True
        takers = [t for t in recent if t.taker]
        if len(takers) < self.min_sweep_trades:
            return

        # Require one-directional net shares / notional
        buy_notional = sum(t.notional for t in takers if t.direction == "BUY")
        sell_notional = sum(t.notional for t in takers if t.direction == "SELL")
        direction = "BUY" if buy_notional >= sell_notional else "SELL"
        net_notional = abs(buy_notional - sell_notional)
        if net_notional < self.min_net_notional:
            return

        # Spread filter
        spread_now = self._current_spread(token_id)
        spread_ok = False
        if spread_now is not None and spread_now <= self.max_spread:
            spread_ok = True
        else:
            s_5m = self._spread_5min_ago(token_id)
            if spread_now is not None and s_5m is not None:
                if (s_5m - spread_now) >= self.spread_compress_cents:
                    spread_ok = True
        if not spread_ok:
            return

        # Book thinness: use last depth estimate, within freshness window
        depth_key = token_id + (":BUY" if direction == "BUY" else ":SELL")
        depth_cost = None
        if depth_key in self.last_depth_cost_5c:
            ts, cost = self.last_depth_cost_5c[depth_key]
            if now - ts <= 120.0:  # require a fairly fresh book snapshot
                depth_cost = cost
        if depth_cost is None or depth_cost > self.max_depth_cost_5c:
            return

        # VWAP baseline for 5 minutes
        vwap5 = self._vwap(token_id, self.vwap_window_sec)
        if vwap5 is None:
            return

        # Candidate created — confirm follow-through after follow_through_sec
        self.pending[token_id] = {
            "created_ts": now,
            "direction": direction,
            "net_notional": net_notional,
            "trade_count": len(takers),
            "spread": spread_now,
            "depth_cost_5c": depth_cost,
            "vwap5": vwap5,
            "min_price": None,
            "max_price": None,
        }

    def _check_pending(self) -> None:
        now = time.time()
        to_delete: List[str] = []

        for token_id, p in list(self.pending.items()):
            age = now - p["created_ts"]
            # Track min/max during follow-through window
            hist = self.price_hist[token_id]
            if hist:
                # only consider points after creation
                prices = [px for (ts, px) in hist if ts >= p["created_ts"]]
                if prices:
                    p["min_price"] = min(prices) if p["min_price"] is None else min(p["min_price"], min(prices))
                    p["max_price"] = max(prices) if p["max_price"] is None else max(p["max_price"], max(prices))

            if age < self.follow_through_sec:
                continue

            # Evaluate follow-through
            meta = self.asset_to_meta[token_id]
            outcome = meta.outcome_for_token(token_id)
            vwap5 = float(p["vwap5"])
            last_px = hist[-1][1] if hist else None
            if last_px is None:
                to_delete.append(token_id)
                continue

            move = (last_px - vwap5)
            # direction-adjusted move:
            dir_sign = 1.0 if p["direction"] == "BUY" else -1.0
            move_dir = dir_sign * move

            # mean reversion (against direction) within window
            min_px = p["min_price"] if p["min_price"] is not None else last_px
            max_px = p["max_price"] if p["max_price"] is not None else last_px
            if p["direction"] == "BUY":
                # worst drawdown from peak: peak - last? approximate using max then min
                revert = max_px - last_px
            else:
                revert = last_px - min_px
            revert = abs(revert)

            passes = (move_dir >= self.min_move_from_vwap) and (revert <= self.max_revert)

            if passes:
                url = meta.market_url() or ""
                msg = (
                    f"🚨 TOP ALPHA: Informed sweep in thin book\n"
                    f"{meta.market_question}\n"
                    f"Outcome: {outcome} | Dir: {p['direction']}\n"
                    f"Net notional (90s): ${p['net_notional']:,.0f} | Trades: {p['trade_count']}\n"
                    f"Spread: {p['spread'] if p['spread'] is not None else 'n/a'} | Depth(5¢): ${p['depth_cost_5c']:,.0f}\n"
                    f"VWAP(5m): {vwap5:.3f} | Now: {last_px:.3f} | Move: {move_dir:.3f}\n"
                    f"{url}"
                )
                if self.telegram:
                    send_telegram_message(msg)

                self.logger.log(
                    token_id=token_id,
                    meta=meta,
                    outcome=outcome,
                    direction=p["direction"],
                    net_notional=p["net_notional"],
                    trade_count=p["trade_count"],
                    spread=p["spread"],
                    depth_cost_5c=p["depth_cost_5c"],
                    vwap5=vwap5,
                    move_from_vwap5=move_dir,
                )

            to_delete.append(token_id)

        for tid in to_delete:
            self.pending.pop(tid, None)

    # ---- WS run ----

    def run(self) -> None:
        if not self.asset_ids:
            print("[top_alpha] No eligible assets (7–45d). Consider widening resolve window or increasing Gamma pages.")
            return

        ws_url = f"{CLOB_WS_BASE}/ws/market"

        def on_open(ws):
            time.sleep(0.25)
            ws.send(json.dumps({"type": "market", "assets_ids": self.asset_ids}))
            print(f"[ws] subscribed to {len(self.asset_ids)} tokens (market channel)")

        def on_message(ws, msg):
            self._last_msg_ts = time.time()
            try:
                payload = json.loads(msg)
            except Exception:
                return
            items = payload if isinstance(payload, list) else [payload]

            for it in items:
                if not isinstance(it, dict):
                    continue
                et = it.get("event_type", "unknown")

                if et == "book":
                    self._handle_book(it)
                elif et == "trade":
                    self._handle_trade(it)
                elif et == "price_change":
                    token_id = it.get("asset_id") or it.get("token_id") or it.get("assetId")
                    price = safe_float(it.get("price") or it.get("p") or it.get("value"))
                    if token_id and price is not None and token_id in self.asset_to_meta:
                        self._handle_price(token_id, price)
                elif et == "last_trade_price":
                    token_id = it.get("asset_id") or it.get("token_id") or it.get("assetId")
                    price = safe_float(it.get("price") or it.get("p") or it.get("value"))
                    if token_id and price is not None and token_id in self.asset_to_meta:
                        self._handle_price(token_id, price)

            # periodically confirm pending candidates
            self._check_pending()

            # Heartbeat: if we are alive but slow messages, still evaluate pending
            if (time.time() - self._last_msg_ts) > self.heartbeat_sec:
                self._check_pending()

        def on_error(ws, err):
            print(f"[ws][error] {err}")

        def on_close(ws, code, reason):
            print(f"[ws][close] code={code} reason={reason}")

        while True:
            ws = WebSocketApp(ws_url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
            ws.run_forever(ping_interval=20, ping_timeout=10)
            print("[ws] reconnecting in 3s...")
            time.sleep(3)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Top Alpha watcher: informed sweep in thin book")
    ap.add_argument("--pages", type=int, default=40)
    ap.add_argument("--per-page", type=int, default=100)
    ap.add_argument("--max-assets", type=int, default=1800)
    ap.add_argument("--min-net-notional", type=float, default=12500.0)
    ap.add_argument("--whale-threshold", type=float, default=0.0, help="Unused here; kept for compatibility")
    ap.add_argument("--max-depth-cost-5c", type=float, default=3000.0)
    ap.add_argument("--max-spread", type=float, default=0.03)
    ap.add_argument("--min-sweep-trades", type=int, default=4)
    ap.add_argument("--telegram", action="store_true")
    ap.add_argument("--resolve-days-min", type=int, default=7)
    ap.add_argument("--resolve-days-max", type=int, default=45)
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    metas, raw_count = fetch_gamma_markets(args.per_page, args.pages, active_only=True)
    print(f"[market_fetch] raw={raw_count} metas={len(metas)}")

    watcher = TopAlphaWatcher(
        metas,
        min_sweep_trades=args.min_sweep_trades,
        min_net_notional=args.min_net_notional,
        max_depth_cost_5c=args.max_depth_cost_5c,
        max_spread=args.max_spread,
        max_assets=args.max_assets,
        resolve_days_min=args.resolve_days_min,
        resolve_days_max=args.resolve_days_max,
        telegram=bool(args.telegram),
    )
    print(f"[top_alpha] eligible_assets={len(watcher.asset_ids)}")
    watcher.run()


if __name__ == "__main__":
    main()
