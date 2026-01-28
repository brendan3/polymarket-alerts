#!/usr/bin/env python3
"""
Polymarket alert worker (single WS) with:
- Price move alerts (price_change events)
- Whale trade alerts (trade events)
- Telegram/email notifications
- SQLite logging

Key points:
✅ Uses ONLY /ws/market (price_change + trade events in same socket)
✅ Fetches markets from Gamma with server-side filters: active=true, closed=false
✅ Filters "live" markets client-side by endDate (must NOT be in the past) + clobTokenIds present
✅ DOES NOT use Gamma's `ready` / `funded` as hard filters (they appear unreliable)
✅ broad-test mode subscribes to as many assets as possible (up to max_assets)
"""

from __future__ import annotations

import argparse
import json
import os
import smtplib
import sqlite3
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

import requests
from websocket import WebSocketApp

# =========================
# Constants
# =========================

CLOB_WS_BASE = "wss://ws-subscriptions-clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"


# =========================
# Utilities
# =========================

def now_ms() -> int:
    return int(time.time() * 1000)

def fmt_ts(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat(timespec="seconds")

def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def compute_odds(bid: Any, ask: Any) -> Optional[float]:
    """Compute mid-price odds from bid/ask or return whichever is available."""
    b = safe_float(bid)
    a = safe_float(ask)
    if b is not None and a is not None and 0 < b < 1 and 0 < a < 1:
        return (b + a) / 2
    return b if b is not None else a

def normalize_token_id(s: str) -> str:
    return (s or "").strip().lower()

def env_truthy(val: Optional[str]) -> bool:
    if val is None:
        return False
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}

def to_bool(value: Any) -> bool:
    """
    Robust boolean parser that handles:
    - bool: True/False
    - str: "true"/"false", "1"/"0", "yes"/"no" (case-insensitive)
    - int/float: 1/0
    - None: False
    """
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value != 0)
    if isinstance(value, str):
        s = value.strip().lower()
        if s in {"true", "1", "yes", "y", "on"}:
            return True
        if s in {"false", "0", "no", "n", "off", ""}:
            return False
    return False

def coerce_json_list(raw: Any) -> List[str]:
    """
    Gamma often returns list fields as JSON strings, e.g. outcomes: '["Yes","No"]'
    This normalizes to a python list[str].
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw]
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        # JSON list in a string
        if s.startswith("[") and s.endswith("]"):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return [str(x) for x in parsed]
            except Exception:
                return []
        # fallback single string
        return [s]
    return [str(raw)]

def coerce_clob_token_ids(raw: Any) -> List[str]:
    """
    Gamma sometimes returns clobTokenIds as:
      - list[str]
      - JSON-encoded string like '["id1","id2"]'
      - comma-separated string like 'id1,id2'
    Normalize to List[str].
    """
    if raw is None:
        return []

    if isinstance(raw, (list, tuple)):
        return [normalize_token_id(str(x)) for x in raw if str(x).strip()]

    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []

        if s.startswith("[") and s.endswith("]"):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return [normalize_token_id(str(x)) for x in parsed if str(x).strip()]
            except Exception:
                pass

        if "," in s:
            parts = [p.strip() for p in s.split(",")]
            return [normalize_token_id(p.strip('"').strip("'")) for p in parts if p.strip('"').strip("'")]

        return [normalize_token_id(s.strip('"').strip("'"))]

    return [normalize_token_id(str(raw))] if str(raw).strip() else []

def parse_end_date(value: Any, now_ts: float) -> Optional[float]:
    """
    Parse endDate and return unix timestamp if date is IN THE PAST.
    Returns None if:
      - missing
      - parse fails
      - date is in the future
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        ts = float(value)
        if 946684800 <= ts <= 4102444800:
            return ts if ts < now_ts else None

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            elif "T" in s and "+" not in s and s.count("-") >= 2:
                s = s + "+00:00"
            dt = datetime.fromisoformat(s)
            ts = dt.timestamp()
            return ts if ts < now_ts else None
        except Exception:
            try:
                ts = float(s)
                if 946684800 <= ts <= 4102444800:
                    return ts if ts < now_ts else None
            except Exception:
                return None

    return None

def as_money(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    if x >= 1_000_000:
        return f"${x / 1_000_000:.2f}M"
    if x >= 1_000:
        return f"${x / 1_000:.1f}K"
    return f"${x:.0f}"

def fmt_spread(spread: Optional[float]) -> str:
    return f"{spread:.4f}" if spread is not None else "n/a"


# =========================
# Email & Telegram
# =========================

def _get_email_config() -> dict:
    host = os.getenv("ALERT_SMTP_HOST") or os.getenv("SMTP_HOST")
    port = int(os.getenv("ALERT_SMTP_PORT") or os.getenv("SMTP_PORT") or "587")
    user = os.getenv("ALERT_SMTP_USER") or os.getenv("SMTP_USER")
    pw   = os.getenv("ALERT_SMTP_PASS") or os.getenv("SMTP_PASS")

    email_from = os.getenv("ALERT_EMAIL_FROM") or os.getenv("EMAIL_FROM")
    email_to   = os.getenv("ALERT_EMAIL_TO") or os.getenv("EMAIL_TO")

    enabled = env_truthy(os.getenv("EMAIL_ENABLED")) or env_truthy(os.getenv("ALERT_EMAIL_ENABLED"))
    if os.getenv("EMAIL_ENABLED") is None and os.getenv("ALERT_EMAIL_ENABLED") is None:
        enabled = True

    return {
        "enabled": enabled,
        "host": host,
        "port": port,
        "user": user,
        "pw": pw,
        "from": email_from,
        "to": email_to,
    }

def send_email(subject: str, body: str) -> None:
    cfg = _get_email_config()
    if not cfg["enabled"]:
        return
    if not all([cfg["host"], cfg["user"], cfg["pw"], cfg["from"], cfg["to"]]):
        return

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"]    = cfg["from"]
    msg["To"]      = cfg["to"]

    try:
        with smtplib.SMTP(cfg["host"], cfg["port"]) as s:
            if env_truthy(os.getenv("SMTP_STARTTLS")) or os.getenv("SMTP_STARTTLS") is None:
                s.starttls()
            s.login(cfg["user"], cfg["pw"])
            s.sendmail(cfg["from"], [cfg["to"]], msg.as_string())
    except Exception as e:
        print(f"[email] send_email exception: {e}")

def send_telegram_message(body: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    enabled_env = env_truthy(os.getenv("TELEGRAM_ENABLED")) or env_truthy(os.getenv("ALERT_TELEGRAM_ENABLED"))
    enabled = enabled_env or (token and chat_id)
    if not enabled or not token or not chat_id:
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": body}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"[telegram] send_telegram_message exception: {e}")


# =========================
# Gamma Metadata
# =========================

@dataclass
class MarketMeta:
    market_question: str
    event_title: str
    event_slug: Optional[str]
    volume: Optional[float]
    liquidity: Optional[float]
    outcomes: List[str]
    clob_token_ids: List[str]

    def title(self) -> str:
        if self.event_title and self.event_title != self.market_question:
            return f"{self.event_title} — {self.market_question}"
        return self.market_question


def fetch_gamma_markets(per_page: int, pages: int, *, active_only: bool = True, broad_test: bool = False) -> Tuple[List[MarketMeta], int]:
    """
    Fetch markets from Gamma API using server-side filters:
      - active=true (optional)
      - closed=false (always)

    Client-side filters:
      - endDate must NOT be in the past
      - must have clobTokenIds
      - optionally skip archived/restricted markets

    NOTE: We intentionally do NOT hard-filter on `ready` or `funded` because they appear unreliable.
    """
    now_ts = time.time()

    metas: List[MarketMeta] = []
    raw_markets_count = 0

    filtered_closed = 0
    filtered_archived = 0
    filtered_restricted = 0
    filtered_enddate = 0
    filtered_missing_tokens = 0

    samples_logged = 0

    print(f"[market_fetch] Fetching {pages} pages ({per_page} per page) with closed=false filter...")

    for p in range(pages):
        params = {"limit": per_page, "offset": p * per_page, "closed": "false"}
        if active_only:
            params["active"] = "true"

        url = f"{GAMMA_BASE}/markets"
        if p == 0:
            qs = "&".join([f"{k}={v}" for k, v in params.items()])
            print(f"[market_fetch] Full URL: {url}?{qs}")

        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        markets = r.json()

        raw_markets_count += len(markets)

        # Page stats: how many came back closed anyway (sanity)
        if p == 0 and markets:
            c_false = sum(1 for m in markets if not to_bool(m.get("closed")))
            c_true = sum(1 for m in markets if to_bool(m.get("closed")))
            print(f"[market_fetch] Page 0: closed=false={c_false} closed=true={c_true} total={len(markets)}")

        for m in markets:
            if samples_logged < 3:
                print(
                    f"[market_sample] market[{samples_logged}] id={m.get('id') or m.get('slug')} "
                    f"closed={m.get('closed')!r} archived={m.get('archived')!r} "
                    f"restricted={m.get('restricted')!r} active={m.get('active')!r} "
                    f"ready={m.get('ready')!r} funded={m.get('funded')!r} "
                    f"endDate={m.get('endDate')!r} updatedAt={m.get('updatedAt')!r}"
                )
                samples_logged += 1

            # Safety: if API ever returns closed=true despite closed=false param
            if to_bool(m.get("closed")):
                filtered_closed += 1
                continue

            # Skip archived (archived markets are not tradable)
            if to_bool(m.get("archived")):
                filtered_archived += 1
                continue
            
            # Skip restricted markets UNLESS in broad_test mode
            # In broad_test, we want maximum coverage for testing alerts
            # Restricted markets can still be monitored for price moves/trades
            if not broad_test and to_bool(m.get("restricted")):
                filtered_restricted += 1
                continue

            # Skip markets whose endDate is in the past
            end_date_ts = parse_end_date(m.get("endDate"), now_ts)
            if end_date_ts is not None:
                filtered_enddate += 1
                continue

            raw_clob = m.get("clobTokenIds")
            clob_ids = coerce_clob_token_ids(raw_clob)
            if not clob_ids:
                filtered_missing_tokens += 1
                continue

            outcomes = coerce_json_list(m.get("outcomes"))
            metas.append(
                MarketMeta(
                    market_question=m.get("question") or "Unknown",
                    event_title=(m.get("title") or ""),
                    event_slug=m.get("eventSlug"),
                    volume=safe_float(m.get("volume") or m.get("volumeNum")),
                    liquidity=safe_float(m.get("liquidity") or m.get("liquidityNum")),
                    outcomes=outcomes,
                    clob_token_ids=clob_ids,
                )
            )

    print(
        f"[market_filter] raw_markets={raw_markets_count} "
        f"filtered_closed={filtered_closed} filtered_archived={filtered_archived} "
        f"filtered_restricted={filtered_restricted} filtered_enddate={filtered_enddate} "
        f"filtered_missing_tokens={filtered_missing_tokens} remaining={len(metas)}"
    )

    # Defensive check for suspicious token IDs
    bad = [m for m in metas if any(len(tid) <= 2 for tid in m.clob_token_ids)]
    print(f"[sanity] markets_with_suspicious_token_ids={len(bad)}")

    return metas, raw_markets_count


# =========================
# SQLite Logger
# =========================

class AlertLoggerSQLite:
    def __init__(self, path: str):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(path) as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS alerts (
                  id INTEGER PRIMARY KEY,
                  ts_ms INTEGER,
                  ts_iso TEXT,
                  market_title TEXT,
                  event_title TEXT,
                  outcome TEXT,
                  from_odds REAL,
                  to_odds REAL,
                  move_pct REAL,
                  volume REAL,
                  liquidity REAL,
                  best_bid REAL,
                  best_ask REAL,
                  spread REAL,
                  asset_id TEXT,
                  ws_market_id TEXT,
                  alert_type TEXT,
                  notional REAL,
                  decision TEXT,
                  notes TEXT,
                  result TEXT,
                  pnl REAL
                )
                """
            )
            c.commit()

    def log(self, row: dict) -> None:
        with sqlite3.connect(self.path) as c:
            cols = ",".join(row.keys())
            qs   = ",".join("?" * len(row))
            c.execute(f"INSERT INTO alerts ({cols}) VALUES ({qs})", list(row.values()))
            c.commit()


# =========================
# Core Watcher (single WS)
# =========================

@dataclass
class PricePoint:
    ts: int
    odds: float

class MarketWatcher:
    """
    Single websocket subscriber for /ws/market that handles:
      - price_change -> price move alerts
      - trade        -> whale alerts
    """

    def __init__(
        self,
        metas: List[MarketMeta],
        *,
        window_min: int,
        move_pct: float,
        min_volume: float,
        min_liquidity: float,
        max_spread: float,
        max_assets: int,
        whale_threshold: float,
        whale_cooldown_sec: int,
        email: bool,
        telegram: bool,
        logger: AlertLoggerSQLite,
        heartbeat_sec: int,
        broad_test: bool = False,
    ):
        self.metas = metas
        self.asset_to_meta: Dict[str, MarketMeta] = {tid: m for m in metas for tid in m.clob_token_ids}

        self.window_ms = window_min * 60 * 1000
        self.move_threshold = move_pct / 100.0
        self.min_volume = min_volume
        self.min_liquidity = min_liquidity
        self.max_spread = max_spread

        self.max_assets = max_assets
        self.whale_threshold = whale_threshold
        self.whale_cooldown_sec = whale_cooldown_sec

        self.email = email
        self.telegram = telegram
        self.logger = logger

        self.price_history: Dict[str, Deque[PricePoint]] = defaultdict(deque)
        self.last_price_alert_ms: Dict[str, int] = {}
        self.last_whale_alert_s: Dict[str, float] = {}

        self.heartbeat_sec = heartbeat_sec
        self._last_heartbeat = time.time()
        self._last_msg_ts = time.time()
        self._last_pong_ts = time.time()
        self._idle_timeout_sec = 180
        self._ws_connected = False
        self._ws_instance = None

        self._msg_count = 0
        self._msg_timestamps: Deque[float] = deque(maxlen=60)
        self._last_message_type = "none"

        self.asset_ids = self._select_asset_ids(broad_test=broad_test)

    def _select_asset_ids(self, broad_test: bool = False) -> List[str]:
        """
        In broad_test mode:
          - subscribe to as many assets as possible (up to max_assets), sorted by liquidity/volume desc
          - thresholds can be 0 so we don't exclude low-liquidity markets during testing
        """
        total_markets = len(self.metas)
        candidates: List[Tuple[float, float, str, MarketMeta]] = []

        filtered_volume = 0
        filtered_liquidity = 0
        passed_markets = 0

        for m in self.metas:
            vol = m.volume or 0.0
            liq = m.liquidity or 0.0

            if vol < self.min_volume:
                filtered_volume += 1
                continue
            if liq < self.min_liquidity:
                filtered_liquidity += 1
                continue

            passed_markets += 1
            for aid in m.clob_token_ids:
                candidates.append((liq, vol, aid, m))

        candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)

        print(
            "[asset_select] markets_total=%d passed=%d filtered_volume=%d filtered_liquidity=%d "
            "candidates=%d min_volume=%.2f min_liquidity=%.2f max_assets=%d"
            % (
                total_markets,
                passed_markets,
                filtered_volume,
                filtered_liquidity,
                len(candidates),
                self.min_volume,
                self.min_liquidity,
                self.max_assets,
            )
        )

        preview_n = min(25, len(candidates))
        if preview_n:
            print(f"[asset_select] Top {preview_n} candidates by (liquidity, volume):")
            for i in range(preview_n):
                liq, vol, aid, meta = candidates[i]
                title = meta.title()
                if len(title) > 120:
                    title = title[:117] + "..."
                print(f"  #{i+1:02d} aid={aid} liq={liq:,.0f} vol={vol:,.0f} market='{title}'")

        picked: List[str] = []
        seen = set()
        for liq, vol, aid, meta in candidates:
            if aid in seen:
                continue
            seen.add(aid)
            picked.append(aid)
            if self.max_assets and len(picked) >= self.max_assets:
                break

        if not picked:
            fallback = list(self.asset_to_meta.keys())
            picked = fallback[: min(200, len(fallback))]
            print(f"[asset_select] FALLBACK: subscribing to {len(picked)} assets from asset_to_meta")

        log_n = min(25, len(picked))
        print(f"[asset_select] Selected assets (showing {log_n}/{len(picked)}):")
        for i in range(log_n):
            aid = picked[i]
            meta = self.asset_to_meta.get(aid)
            liq = meta.liquidity or 0.0 if meta else 0.0
            vol = meta.volume or 0.0 if meta else 0.0
            title = meta.title() if meta else "n/a"
            if len(title) > 120:
                title = title[:117] + "..."
            print(f"  #{i+1:02d} aid={aid} liq={liq:,.0f} vol={vol:,.0f} market='{title}'")

        print(f"Subscription set: {len(picked)} assets (max_assets={self.max_assets})")
        return picked

    def _send(self, subject: str, body: str) -> None:
        print(body)
        if self.email:
            send_email(subject, body)
        if self.telegram:
            send_telegram_message(body)

    def _outcome_for(self, meta: MarketMeta, aid: str) -> str:
        if meta.outcomes and aid in meta.clob_token_ids:
            idx = meta.clob_token_ids.index(aid)
            if 0 <= idx < len(meta.outcomes):
                return meta.outcomes[idx]
        return "n/a"

    # ---------- price alerts ----------

    def _handle_price_change(self, p: dict) -> None:
        ts = int(p.get("timestamp") or now_ms())
        ws_market_id = p.get("market", "")

        for pc in p.get("price_changes", []) or []:
            aid = normalize_token_id(pc.get("asset_id", ""))
            meta = self.asset_to_meta.get(aid)
            if not meta:
                continue

            odds = compute_odds(pc.get("best_bid"), pc.get("best_ask"))
            if odds is None or odds <= 0:
                continue

            bid = safe_float(pc.get("best_bid"))
            ask = safe_float(pc.get("best_ask"))
            spread = (ask - bid) if (bid is not None and ask is not None) else None

            self.price_history[aid].append(PricePoint(ts, odds))

            window_start = ts - self.window_ms
            while self.price_history[aid] and self.price_history[aid][0].ts < window_start:
                self.price_history[aid].popleft()

            if len(self.price_history[aid]) < 2:
                continue

            start = self.price_history[aid][0]
            if start.odds <= 0:
                continue

            change = (odds - start.odds) / start.odds
            if abs(change) < self.move_threshold:
                continue

            vol = meta.volume or 0.0
            liq = meta.liquidity or 0.0
            if vol < self.min_volume or liq < self.min_liquidity:
                continue
            if spread is not None and spread > self.max_spread:
                continue

            if ts - self.last_price_alert_ms.get(aid, 0) < 60_000:
                continue
            self.last_price_alert_ms[aid] = ts

            direction = "UP" if change > 0 else "DOWN"
            outcome = self._outcome_for(meta, aid)
            pct = change * 100.0

            body = (
                "=== POLY ALERT ===\n"
                f"when: {fmt_ts(now_ms())}\n"
                f"market: {meta.title()}\n"
                f"outcome: {outcome}\n"
                f"move: {direction} {pct:.2f}%\n"
                f"from: {start.odds:.4f} → {odds:.4f}\n"
                f"volume: {as_money(meta.volume)}\n"
                f"liquidity: {as_money(meta.liquidity)}\n"
                f"spread: {fmt_spread(spread)}\n"
                f"asset_id: {aid}\n"
                "==================\n"
            )

            self.logger.log({
                "ts_ms": now_ms(),
                "ts_iso": fmt_ts(now_ms()),
                "market_title": meta.market_question,
                "event_title": meta.event_title,
                "outcome": outcome,
                "from_odds": start.odds,
                "to_odds": odds,
                "move_pct": pct,
                "volume": meta.volume,
                "liquidity": meta.liquidity,
                "best_bid": bid,
                "best_ask": ask,
                "spread": spread,
                "asset_id": aid,
                "ws_market_id": ws_market_id,
                "alert_type": "price_move",
                "notional": None,
                "decision": None,
                "notes": None,
                "result": "OPEN",
                "pnl": None,
            })

            self._send(f"[Polymarket] {direction} {pct:.1f}%", body)

    # ---------- whale alerts ----------

    def _handle_trade_event(self, p: dict) -> None:
        if not self.whale_threshold or self.whale_threshold <= 0:
            return

        ws_market_id = p.get("market", "")
        ts = int(p.get("timestamp") or now_ms())

        for trade in p.get("trades", []) or []:
            aid = normalize_token_id(trade.get("asset_id", ""))
            meta = self.asset_to_meta.get(aid)
            if not meta:
                continue

            price = safe_float(trade.get("price"))
            qty = safe_float(trade.get("qty") or trade.get("size"))
            if price is None or qty is None:
                continue

            notional = price * qty
            if notional < self.whale_threshold:
                continue

            last = self.last_whale_alert_s.get(aid, 0.0)
            if time.time() - last < float(self.whale_cooldown_sec):
                continue
            self.last_whale_alert_s[aid] = time.time()

            outcome = self._outcome_for(meta, aid)
            body = (
                "=== POLY WHALE ALERT ===\n"
                f"when: {fmt_ts(ts)}\n"
                f"market: {meta.title()}\n"
                f"outcome: {outcome}\n"
                f"trade price: {price:.4f}\n"
                f"trade size: {qty:.2f}\n"
                f"notional: ${notional:,.2f}\n"
                f"threshold: ${self.whale_threshold:,.0f}\n"
                f"volume: {as_money(meta.volume)}\n"
                f"liquidity: {as_money(meta.liquidity)}\n"
                f"asset_id: {aid}\n"
                "========================\n"
            )

            self.logger.log({
                "ts_ms": ts,
                "ts_iso": fmt_ts(ts),
                "market_title": meta.market_question,
                "event_title": meta.event_title,
                "outcome": outcome,
                "from_odds": None,
                "to_odds": None,
                "move_pct": None,
                "volume": meta.volume,
                "liquidity": meta.liquidity,
                "best_bid": None,
                "best_ask": None,
                "spread": None,
                "asset_id": aid,
                "ws_market_id": ws_market_id,
                "alert_type": "whale_trade",
                "notional": notional,
                "decision": None,
                "notes": None,
                "result": "OPEN",
                "pnl": None,
            })

            self._send(f"[Polymarket] Whale ${notional:,.0f}", body)

    # ---------- websocket loop ----------

    def run_forever(self) -> None:
        if not self.asset_ids:
            print("No assets to subscribe to.")
            return

        backoff = 5

        def on_open(ws):
            self._ws_connected = True
            self._last_msg_ts = time.time()
            self._last_pong_ts = time.time()
            self._msg_count = 0
            self._msg_timestamps.clear()
            print("[ws] connected/open")
            time.sleep(0.1)
            ws.send(json.dumps({"type": "market", "assets_ids": self.asset_ids}))
            print(f"[ws] subscribed to {len(self.asset_ids)} tokens (market channel)")

        def on_message(ws, msg):
            now = time.time()
            self._last_msg_ts = now
            self._msg_timestamps.append(now)

            try:
                payload = json.loads(msg)
            except Exception:
                return

            items = payload if isinstance(payload, list) else [payload]

            self._msg_count += 1
            for it in items:
                if isinstance(it, dict):
                    et = it.get("event_type", "unknown")
                    self._last_message_type = et
                    if et == "price_change":
                        self._handle_price_change(it)
                    elif et == "trade":
                        self._handle_trade_event(it)

        def on_pong(ws, msg):
            self._last_pong_ts = time.time()
            self._last_msg_ts = time.time()

        def on_error(ws, err):
            print(f"[ws] error: {err}")
            self._ws_connected = False

        def on_close(ws, code, reason):
            print(f"[ws] closed: code={code} reason={reason}")
            self._ws_connected = False

        def check_health():
            while True:
                time.sleep(30)
                if not self._ws_connected:
                    continue
                now = time.time()
                time_since_msg = now - self._last_msg_ts
                time_since_pong = now - self._last_pong_ts
                cutoff = now - 60
                msgs_last_minute = sum(1 for ts in self._msg_timestamps if ts > cutoff)

                if (time_since_pong > 50 and time_since_msg > self._idle_timeout_sec):
                    print(f"[heartbeat] CONNECTION DEAD: last_msg={int(time_since_msg)}s last_pong={int(time_since_pong)}s; reconnecting")
                    self._ws_connected = False
                    try:
                        if self._ws_instance:
                            self._ws_instance.close()
                    except Exception:
                        pass
                elif now - self._last_heartbeat >= self.heartbeat_sec:
                    print(
                        f"[heartbeat] healthy: msgs_last_min={msgs_last_minute} "
                        f"last_msg={int(time_since_msg)}s last_pong={int(time_since_pong)}s "
                        f"last_type={self._last_message_type} assets={len(self.asset_ids)}"
                    )
                    self._last_heartbeat = now

        threading.Thread(target=check_health, daemon=True).start()

        while True:
            try:
                self._ws_connected = False
                self._ws_instance = WebSocketApp(
                    f"{CLOB_WS_BASE}/ws/market",
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                    on_pong=on_pong,
                )
                self._ws_instance.run_forever(ping_interval=25, ping_timeout=10)
            except Exception as e:
                print(f"[ws] run_forever exception: {e}")
                self._ws_connected = False

            print(f"[ws] reconnecting in {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)


# =========================
# Main
# =========================

def main() -> None:
    ap = argparse.ArgumentParser(description="Polymarket alerts worker (price moves & whale trades)")
    ap.add_argument("--gamma-active", action="store_true", help="Request Gamma markets with active=true.")
    ap.add_argument("--pages", type=int, default=20, help="Number of pages of markets to fetch from Gamma.")
    ap.add_argument("--per-page", type=int, default=100, help="Markets per page.")
    ap.add_argument("--window-min", type=int, default=10, help="Window size (minutes) for price move detection.")
    ap.add_argument("--move-pct", type=float, default=25.0, help="Percent move threshold.")
    ap.add_argument("--min-volume", type=float, default=5_000, help="Minimum market volume.")
    ap.add_argument("--min-liquidity", type=float, default=500, help="Minimum market liquidity.")
    ap.add_argument("--max-spread", type=float, default=0.06, help="Max bid-ask spread allowed (absolute).")
    ap.add_argument("--max-assets", type=int, default=1000, help="Max number of assets to subscribe to.")
    ap.add_argument("--whale-threshold", type=float, default=1000.0, help="Notional USD threshold to trigger whale alerts.")
    ap.add_argument("--whale-cooldown", type=int, default=300, help="Cooldown per asset for whale alerts (seconds).")
    ap.add_argument("--db-path", type=str, default="data/alerts.db", help="SQLite DB path.")
    ap.add_argument("--email", action="store_true", help="Send alerts by email.")
    ap.add_argument("--telegram", action="store_true", help="Send alerts via Telegram.")
    ap.add_argument("--heartbeat-sec", type=int, default=60, help="Heartbeat interval (seconds).")
    ap.add_argument("--broad-test", action="store_true", help="Subscribe broadly (relax thresholds; for testing alerts).")

    args = ap.parse_args()

    # Broad-test overrides (so you see messages quickly)
    if args.broad_test:
        print("[config] BROAD-TEST mode enabled: relaxing thresholds")
        args.min_volume = 0.0
        args.min_liquidity = 0.0
        if args.max_assets < 1000:
            args.max_assets = 1000
        print(f"[config] Override: min_volume={args.min_volume:g} min_liquidity={args.min_liquidity:g} max_assets={args.max_assets}")

    backoff_sec = 60
    max_backoff = 600

    while True:
        metas, raw_count = fetch_gamma_markets(
            args.per_page,
            args.pages,
            active_only=args.gamma_active,
            broad_test=args.broad_test,
        )
        total_assets = sum(len(m.clob_token_ids) for m in metas)
        print(f"Loaded {len(metas)} markets from Gamma (assets total: {total_assets})")

        logger = AlertLoggerSQLite(args.db_path)

        watcher = MarketWatcher(
            metas,
            window_min=args.window_min,
            move_pct=args.move_pct,
            min_volume=args.min_volume,
            min_liquidity=args.min_liquidity,
            max_spread=args.max_spread,
            max_assets=args.max_assets,
            whale_threshold=args.whale_threshold,
            whale_cooldown_sec=args.whale_cooldown,
            email=args.email,
            telegram=args.telegram,
            logger=logger,
            heartbeat_sec=args.heartbeat_sec,
            broad_test=args.broad_test,
        )

        if not watcher.asset_ids:
            print(
                f"[WARNING] No assets selected (raw_markets={raw_count}, metas={len(metas)}). "
                f"Retrying in {backoff_sec}s..."
            )
            time.sleep(backoff_sec)
            backoff_sec = min(backoff_sec * 2, max_backoff)
            continue

        backoff_sec = 60

        if args.telegram:
            send_telegram_message("Polymarket alerts worker started ✅")
        if args.email:
            send_email("[Polymarket] Worker started", "Polymarket alerts worker started ✅")

        watcher.run_forever()
        print("[main] watcher.run_forever() returned; refetching markets...")
        time.sleep(10)


if __name__ == "__main__":
    main()
