#!/usr/bin/env python3
"""
Polymarket alert worker with support for price-move alerts and whale-trade alerts.

Fixes:
- Whale watcher MUST connect to /ws/market (NOT /ws/trades). /ws/trades is not a valid channel.
- Whale detection uses event_type == "last_trade_price" which includes price + size so we can compute notional.

Notes:
- Telegram: set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in Railway env vars.
- If you want to monitor *more* markets, reduce --min-liquidity / --min-volume and/or increase pages.
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
    b = safe_float(bid)
    a = safe_float(ask)
    if b is not None and a is not None and 0 < b < 1 and 0 < a < 1:
        return (b + a) / 2
    return b if b is not None else a

def normalize_token_id(s: str) -> str:
    return (s or "").strip().lower()

def as_money(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    if x >= 1_000_000:
        return f"${x/1_000_000:.2f}M"
    if x >= 1_000:
        return f"${x/1_000:.1f}K"
    return f"${x:.0f}"

def fmt_spread(spread: Optional[float]) -> str:
    return f"{spread:.4f}" if spread is not None else "n/a"

def env_truthy(val: Optional[str]) -> bool:
    if val is None:
        return False
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}

# =========================
# Email + Telegram
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

    return {"enabled": enabled, "host": host, "port": port, "user": user, "pw": pw, "from": email_from, "to": email_to}

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
            s.starttls()
            s.login(cfg["user"], cfg["pw"])
            s.sendmail(cfg["from"], [cfg["to"]], msg.as_string())
    except Exception as e:
        print(f"[email] send_email exception: {e}")

def send_telegram_message(body: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    enabled_env = env_truthy(os.getenv("TELEGRAM_ENABLED")) or env_truthy(os.getenv("ALERT_TELEGRAM_ENABLED"))
    enabled = bool(enabled_env or (token and chat_id))

    if not enabled or not token or not chat_id:
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    # Don't force Markdown parse mode—your alerts contain lots of punctuation that can break formatting.
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

def fetch_gamma_markets(per_page: int, pages: int, *, active_only: bool = True) -> List[MarketMeta]:
    metas: List[MarketMeta] = []
    for p in range(pages):
        params = {"limit": per_page, "offset": p * per_page}
        if active_only:
            params["active"] = "true"

        r = requests.get(f"{GAMMA_BASE}/markets", params=params, timeout=30)
        r.raise_for_status()

        for m in r.json():
            clob_ids = m.get("clobTokenIds") or []
            if not clob_ids:
                continue

            metas.append(
                MarketMeta(
                    market_question=m.get("question") or "Unknown",
                    event_title=m.get("title") or "",
                    event_slug=m.get("eventSlug"),
                    volume=safe_float(m.get("volume")),
                    liquidity=safe_float(m.get("liquidity")),
                    outcomes=m.get("outcomes") or [],
                    clob_token_ids=[normalize_token_id(x) for x in clob_ids],
                )
            )
    return metas

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
            qs = ",".join("?" * len(row))
            c.execute(f"INSERT INTO alerts ({cols}) VALUES ({qs})", list(row.values()))
            c.commit()

# =========================
# Helpers: subscription filtering
# =========================

def build_asset_index(metas: List[MarketMeta]) -> Dict[str, MarketMeta]:
    return {tid: m for m in metas for tid in m.clob_token_ids}

def pick_subscribed_assets(
    metas: List[MarketMeta],
    min_volume: float,
    min_liquidity: float,
    *,
    max_assets: int,
) -> Tuple[List[str], Dict[str, MarketMeta]]:
    """
    Prefilter assets to subscribe to, to avoid sending 80k+ asset ids to the WS in one subscribe frame.

    Strategy:
    - Keep only metas meeting volume/liquidity
    - Sort by (liquidity desc, volume desc)
    - Take up to max_assets worth of token ids
    """
    eligible = []
    for m in metas:
        vol = m.volume or 0.0
        liq = m.liquidity or 0.0
        if vol >= min_volume and liq >= min_liquidity:
            eligible.append(m)

    eligible.sort(key=lambda m: ((m.liquidity or 0.0), (m.volume or 0.0)), reverse=True)

    assets: List[str] = []
    for m in eligible:
        for tid in m.clob_token_ids:
            assets.append(tid)
            if len(assets) >= max_assets:
                break
        if len(assets) >= max_assets:
            break

    asset_to_meta = {tid: build_asset_index(eligible).get(tid) for tid in assets}
    asset_to_meta = {k: v for k, v in asset_to_meta.items() if v is not None}

    # Ensure normalized + unique (keep order)
    seen = set()
    uniq_assets = []
    for a in assets:
        a = normalize_token_id(a)
        if a and a not in seen and a in asset_to_meta:
            uniq_assets.append(a)
            seen.add(a)

    return uniq_assets, asset_to_meta

# =========================
# Odds Watcher
# =========================

@dataclass
class PricePoint:
    ts: int
    odds: float

class OddsWatcher:
    def __init__(
        self,
        metas: List[MarketMeta],
        subscribed_asset_ids: List[str],
        asset_to_meta: Dict[str, MarketMeta],
        window_min: int,
        move_pct: float,
        min_volume: float,
        min_liquidity: float,
        max_spread: float,
        email: bool,
        telegram: bool,
        logger: AlertLoggerSQLite,
        heartbeat_sec: int = 60,
    ):
        self.metas = metas
        self.asset_to_meta = asset_to_meta
        self.asset_ids = subscribed_asset_ids

        self.window_ms = window_min * 60 * 1000
        self.threshold = move_pct / 100.0
        self.min_volume = min_volume
        self.min_liquidity = min_liquidity
        self.max_spread = max_spread
        self.email = email
        self.telegram = telegram
        self.logger = logger

        self.history: Dict[str, Deque[PricePoint]] = defaultdict(deque)
        self.last_alert: Dict[str, int] = {}

        self.heartbeat_sec = heartbeat_sec
        self._last_heartbeat = time.time()
        self._last_msg_ts = time.time()

    def related_markets(self, meta: MarketMeta) -> List[MarketMeta]:
        if not meta.event_slug:
            return []
        return [m for m in self.metas if m.event_slug == meta.event_slug][:6]

    def _send_alert(self, ts: int, direction: str, change_pct: float, start: PricePoint, odds: float,
                    meta: MarketMeta, outcome: str, spread: Optional[float], aid: str, ws_market_id: str,
                    best_bid: Optional[float], best_ask: Optional[float]) -> None:
        related = "\n".join(
            f"- {m.market_question} ({as_money(m.volume)})"
            for m in self.related_markets(meta)
        ) or "n/a"

        body = (
            "=== POLY ALERT V1.2 (PRICE) ===\n"
            f"when: {fmt_ts(ts)}\n"
            f"market: {meta.title()}\n"
            f"outcome: {outcome}\n"
            f"move: {direction} {change_pct:.2f}%\n"
            f"from: {start.odds:.4f} → {odds:.4f}\n"
            f"volume: {as_money(meta.volume)}\n"
            f"liquidity: {as_money(meta.liquidity)}\n"
            f"spread: {fmt_spread(spread)}\n\n"
            f"Related markets:\n{related}\n\n"
            f"asset_id: {aid}\n"
            "==============================\n"
        )

        print(body)
        self.logger.log({
            "ts_ms": ts,
            "ts_iso": fmt_ts(ts),
            "market_title": meta.market_question,
            "event_title": meta.event_title,
            "outcome": outcome,
            "from_odds": start.odds,
            "to_odds": odds,
            "move_pct": change_pct,
            "volume": meta.volume,
            "liquidity": meta.liquidity,
            "best_bid": best_bid,
            "best_ask": best_ask,
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

        subj = f"[Polymarket] {direction} {change_pct:.1f}%"
        if self.email:
            send_email(subj, body)
        if self.telegram:
            send_telegram_message(body)

    def run_forever(self) -> None:
        if not self.asset_ids:
            print("Price watcher: no asset_ids found; nothing to subscribe to.")
            return

        def on_open(ws):
            ws.send(json.dumps({"type": "market", "assets_ids": self.asset_ids}))
            print(f"Subscribed to {len(self.asset_ids)} tokens (price watcher)")

        def on_message(ws, msg):
            self._last_msg_ts = time.time()

            try:
                payload = json.loads(msg)
            except Exception:
                return

            items = payload if isinstance(payload, list) else [payload]

            for p in items:
                if not isinstance(p, dict):
                    continue

                if p.get("event_type") != "price_change":
                    continue

                ts_raw = p.get("timestamp", now_ms())
                ts = int(ts_raw) if str(ts_raw).isdigit() else now_ms()
                ws_market_id = p.get("market", "")

                for pc in p.get("price_changes", []):
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

                    self.history[aid].append(PricePoint(ts, odds))
                    window_start = ts - self.window_ms
                    while self.history[aid] and self.history[aid][0].ts < window_start:
                        self.history[aid].popleft()

                    if len(self.history[aid]) < 2:
                        continue

                    start = self.history[aid][0]
                    if start.odds <= 0:
                        continue

                    change = (odds - start.odds) / start.odds
                    if abs(change) < self.threshold:
                        continue

                    vol = meta.volume or 0.0
                    liq = meta.liquidity or 0.0
                    if vol < self.min_volume or liq < self.min_liquidity:
                        continue

                    if spread is not None and spread > self.max_spread:
                        continue

                    # Cooldown: 60 seconds per asset
                    if ts - self.last_alert.get(aid, 0) < 60_000:
                        continue
                    self.last_alert[aid] = ts

                    outcome = "n/a"
                    if meta.outcomes and aid in meta.clob_token_ids:
                        try:
                            idx = meta.clob_token_ids.index(aid)
                            if 0 <= idx < len(meta.outcomes):
                                outcome = meta.outcomes[idx]
                        except Exception:
                            pass

                    direction = "UP" if change > 0 else "DOWN"
                    self._send_alert(
                        ts=ts,
                        direction=direction,
                        change_pct=change * 100,
                        start=start,
                        odds=odds,
                        meta=meta,
                        outcome=outcome,
                        spread=spread,
                        aid=aid,
                        ws_market_id=ws_market_id,
                        best_bid=bid,
                        best_ask=ask,
                    )

        def on_error(ws, err):
            print(f"[ws] error: {err}")

        def on_close(ws, code, reason):
            print(f"[ws] closed: code={code} reason={reason}")

        while True:
            try:
                ws = WebSocketApp(
                    f"{CLOB_WS_BASE}/ws/market",
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                print(f"[ws] run_forever exception: {e}")

            print("[ws] reconnecting in 5s...")
            time.sleep(5)

            now = time.time()
            if now - self._last_heartbeat >= self.heartbeat_sec:
                idle = int(now - self._last_msg_ts)
                print(f"[heartbeat] running; last message {idle}s ago; subscribed assets={len(self.asset_ids)}")
                self._last_heartbeat = now

# =========================
# Whale Watcher (FIXED)
# =========================

class WhaleWatcher:
    """
    Watches public market channel and triggers alerts when a single trade notional >= threshold.

    IMPORTANT:
    - This MUST use /ws/market.
    - The event to watch is event_type == "last_trade_price" which includes price and size.
    """
    def __init__(
        self,
        asset_to_meta: Dict[str, MarketMeta],
        asset_ids: List[str],
        threshold_usd: float,
        cooldown_sec: int,
        email: bool,
        telegram: bool,
        logger: AlertLoggerSQLite,
        heartbeat_sec: int = 60,
        global_min_gap_sec: int = 2,  # extra spam safety across all assets
    ):
        self.asset_to_meta = asset_to_meta
        self.asset_ids = asset_ids
        self.threshold = threshold_usd
        self.cooldown_sec = cooldown_sec
        self.email = email
        self.telegram = telegram
        self.logger = logger

        self.last_alert_by_asset: Dict[str, float] = {}
        self._last_global_alert: float = 0.0
        self.global_min_gap_sec = global_min_gap_sec

        self.heartbeat_sec = heartbeat_sec
        self._last_heartbeat = time.time()
        self._last_msg_ts = time.time()

    def _send_alert(self, ts: int, aid: str, price: float, size: float, notional: float, ws_market_id: str) -> None:
        meta = self.asset_to_meta.get(aid)
        if not meta:
            return

        outcome = "n/a"
        if meta.outcomes and aid in meta.clob_token_ids:
            try:
                idx = meta.clob_token_ids.index(aid)
                if 0 <= idx < len(meta.outcomes):
                    outcome = meta.outcomes[idx]
            except Exception:
                pass

        body = (
            "=== POLY ALERT V1.2 (WHALE TRADE) ===\n"
            f"when: {fmt_ts(ts)}\n"
            f"market: {meta.title()}\n"
            f"outcome: {outcome}\n"
            f"trade price: {price:.4f}\n"
            f"trade size: {size:.4f}\n"
            f"notional: ${notional:,.2f}\n"
            f"threshold: ${self.threshold:,.0f}\n"
            f"volume: {as_money(meta.volume)}\n"
            f"liquidity: {as_money(meta.liquidity)}\n"
            f"asset_id: {aid}\n"
            "====================================\n"
        )

        print(body)
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

        subj = f"[Polymarket] Whale trade ${notional:,.0f}"
        if self.email:
            send_email(subj, body)
        if self.telegram:
            send_telegram_message(body)

    def run_forever(self) -> None:
        if not self.asset_ids:
            print("Whale watcher: no asset_ids to subscribe to.")
            return

        def on_open(ws):
            # Same subscribe message as price watcher — market channel
            ws.send(json.dumps({"type": "market", "assets_ids": self.asset_ids}))
            print(f"[whale] subscribed to {len(self.asset_ids)} tokens (market channel)")

        def on_message(ws, msg):
            self._last_msg_ts = time.time()

            try:
                payload = json.loads(msg)
            except Exception:
                return

            items = payload if isinstance(payload, list) else [payload]

            for p in items:
                if not isinstance(p, dict):
                    continue

                if p.get("event_type") != "last_trade_price":
                    continue

                # Expected fields (per Polymarket docs): asset_id, market, price, size, timestamp
                aid = normalize_token_id(p.get("asset_id", ""))
                if not aid or aid not in self.asset_to_meta:
                    continue

                price = safe_float(p.get("price"))
                size = safe_float(p.get("size"))
                if price is None or size is None:
                    continue

                notional = price * size
                if notional < self.threshold:
                    continue

                now_s = time.time()

                # global throttle
                if now_s - self._last_global_alert < self.global_min_gap_sec:
                    continue

                # per-asset cooldown
                last_asset = self.last_alert_by_asset.get(aid, 0.0)
                if now_s - last_asset < self.cooldown_sec:
                    continue

                ts_raw = p.get("timestamp", now_ms())
                ts = int(ts_raw) if str(ts_raw).isdigit() else now_ms()
                ws_market_id = p.get("market", "") or ""

                self.last_alert_by_asset[aid] = now_s
                self._last_global_alert = now_s

                self._send_alert(ts=ts, aid=aid, price=price, size=size, notional=notional, ws_market_id=ws_market_id)

        def on_error(ws, err):
            print(f"[whale ws] error: {err}")

        def on_close(ws, code, reason):
            print(f"[whale ws] closed: code={code} reason={reason}")

        while True:
            try:
                ws = WebSocketApp(
                    f"{CLOB_WS_BASE}/ws/market",
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                print(f"[whale ws] run_forever exception: {e}")

            print("[whale ws] reconnecting in 5s...")
            time.sleep(5)

            now = time.time()
            if now - self._last_heartbeat >= self.heartbeat_sec:
                idle = int(now - self._last_msg_ts)
                print(f"[whale heartbeat] running; last message {idle}s ago; subscribed assets={len(self.asset_ids)}")
                self._last_heartbeat = now

# =========================
# Main
# =========================

def main() -> None:
    ap = argparse.ArgumentParser(description="Polymarket alerts worker (price moves + whale trades)")
    ap.add_argument("--gamma-active", action="store_true", help="Filter Gamma markets to active=true.")
    ap.add_argument("--pages", type=int, default=5)
    ap.add_argument("--per-page", type=int, default=100)

    ap.add_argument("--window-min", type=int, default=10)
    ap.add_argument("--move-pct", type=float, default=25.0)
    ap.add_argument("--min-volume", type=float, default=250_000)
    ap.add_argument("--min-liquidity", type=float, default=100_000)
    ap.add_argument("--max-spread", type=float, default=0.06)

    ap.add_argument("--whale-threshold", type=float, default=0.0, help="USD notional threshold (0 disables whale watcher).")
    ap.add_argument("--whale-cooldown", type=int, default=300)

    ap.add_argument("--max-assets", type=int, default=250, help="Max assets to subscribe to (protects against 80k+ subs).")

    ap.add_argument("--db-path", type=str, default="data/alerts.db")
    ap.add_argument("--email", action="store_true")
    ap.add_argument("--telegram", action="store_true")
    ap.add_argument("--heartbeat-sec", type=int, default=60)

    args = ap.parse_args()

    metas = fetch_gamma_markets(args.per_page, args.pages, active_only=args.gamma_active)
    total_assets = sum(len(m.clob_token_ids) for m in metas)
    print(f"Loaded {len(metas)} markets from Gamma (assets total: {total_assets})")

    logger = AlertLoggerSQLite(args.db_path)

    # IMPORTANT: prefilter subscription set (this is why you saw “Subscribed to 15 tokens” earlier)
    subscribed_assets, asset_to_meta = pick_subscribed_assets(
        metas,
        min_volume=args.min_volume,
        min_liquidity=args.min_liquidity,
        max_assets=args.max_assets,
    )
    print(f"Subscription set: {len(subscribed_assets)} assets (max_assets={args.max_assets})")

    price_watcher = OddsWatcher(
        metas=metas,
        subscribed_asset_ids=subscribed_assets,
        asset_to_meta=asset_to_meta,
        window_min=args.window_min,
        move_pct=args.move_pct,
        min_volume=args.min_volume,
        min_liquidity=args.min_liquidity,
        max_spread=args.max_spread,
        email=args.email,
        telegram=args.telegram,
        logger=logger,
        heartbeat_sec=args.heartbeat_sec,
    )

    threads: List[threading.Thread] = []

    t_price = threading.Thread(target=price_watcher.run_forever, daemon=True)
    t_price.start()
    threads.append(t_price)

    if args.whale_threshold and args.whale_threshold > 0.0:
        whale_watcher = WhaleWatcher(
            asset_to_meta=asset_to_meta,
            asset_ids=subscribed_assets,
            threshold_usd=args.whale_threshold,
            cooldown_sec=args.whale_cooldown,
            email=args.email,
            telegram=args.telegram,
            logger=logger,
            heartbeat_sec=args.heartbeat_sec,
        )
        t_whale = threading.Thread(target=whale_watcher.run_forever, daemon=True)
        t_whale.start()
        threads.append(t_whale)

    # Startup notices
    if args.telegram:
        send_telegram_message("Polymarket alerts worker started successfully.")
    if args.email:
        send_email("[Polymarket] Worker started", "Polymarket alerts worker started successfully.")

    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
