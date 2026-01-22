#!/usr/bin/env python3
"""
Polymarket alert worker (single WS) with:
- Price move alerts (price_change events)
- Whale trade alerts (trade events)
- Telegram/email notifications
- SQLite logging

Key change vs your earlier versions:
✅ Uses ONLY /ws/market (there is no /ws/trades on this host)
✅ Processes both price_change + trade events from the same socket
✅ Adds --max-assets (your Railway command already passes it)
"""

from __future__ import annotations

import argparse
import json
import os
import smtplib
import sqlite3
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

def env_truthy(val: Optional[str]) -> bool:
    if val is None:
        return False
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}


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

        self.asset_ids = self._select_asset_ids()

    def _select_asset_ids(self) -> List[str]:
        """
        Pick a subset of assets to subscribe to:
          - market passes min volume/liquidity
          - rank by (liquidity, volume) desc
          - take up to max_assets
        Adds verbose logging so you can see why/how assets were chosen.
        """
        total_markets = len(self.metas)
        passed_markets = 0
        filtered_volume = 0
        filtered_liquidity = 0

        # candidates are (liq, vol, aid, meta)
        candidates: List[Tuple[float, float, str, MarketMeta]] = []

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

        # sort by liquidity desc then volume desc
        candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)

        # Print summary
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

        # Print top candidates (preview)
        preview_n = min(30, len(candidates))
        if preview_n > 0:
            print(f"[asset_select] Top {preview_n} candidates by (liquidity, volume):")
            for i in range(preview_n):
                liq, vol, aid, meta = candidates[i]
                title = meta.title()
                # prevent mega-long log lines
                if len(title) > 120:
                    title = title[:117] + "..."
                print(
                    f"  #{i+1:02d} aid={aid} liq={liq:,.0f} vol={vol:,.0f} market='{title}'"
                )
        else:
            print("[asset_select] No candidates after filtering.")

        # Pick unique assets
        picked: List[str] = []
        seen = set()
        for liq, vol, aid, meta in candidates:
            if aid in seen:
                continue
            seen.add(aid)
            picked.append(aid)
            if self.max_assets and len(picked) >= self.max_assets:
                break

        # Fallback if nothing passed filters
        if not picked:
            fallback = list(self.asset_to_meta.keys())
            picked = fallback[: min(50, len(fallback))]
            print(
                f"[asset_select] FALLBACK: no markets passed filters; subscribing to {len(picked)} assets from asset_to_meta"
            )

        # If you only want to log the first 50 selected, keep it readable
        log_selected_n = min(50, len(picked))
        print(f"[asset_select] Selected assets (showing {log_selected_n}/{len(picked)}):")
        for i in range(log_selected_n):
            aid = picked[i]
            meta = self.asset_to_meta.get(aid)
            if not meta:
                print(f"  #{i+1:02d} aid={aid} meta=n/a")
                continue
            liq = meta.liquidity or 0.0
            vol = meta.volume or 0.0
            title = meta.title()
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

            # throttle: 60s per asset
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
            # Market channel subscription. This is the important part.
            ws.send(json.dumps({"type": "market", "assets_ids": self.asset_ids}))
            print(f"Subscribed to {len(self.asset_ids)} tokens (market channel)")

        def on_message(ws, msg):
            self._last_msg_ts = time.time()
            try:
                payload = json.loads(msg)
            except Exception:
                return

            items = payload if isinstance(payload, list) else [payload]

            # lightweight message-type counter (prints every 200 messages)
            self._msg_count = getattr(self, "_msg_count", 0) + 1
            if self._msg_count % 200 == 0:
                types = {}
                for it in items:
                    if isinstance(it, dict):
                        t = it.get("event_type", "unknown")
                        types[t] = types.get(t, 0) + 1
                print(f"[ws] msg_count={self._msg_count} recent_types={types}")

            for p in items:
                if not isinstance(p, dict):
                    continue
                et = p.get("event_type")
                if et == "price_change":
                    self._handle_price_change(p)
                elif et == "trade":
                    self._handle_trade_event(p)

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
                # Cloudflare WS can silently die; pings help.
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                print(f"[ws] run_forever exception: {e}")

            print(f"[ws] reconnecting in {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

            now = time.time()
            if now - self._last_heartbeat >= self.heartbeat_sec:
                idle = int(now - self._last_msg_ts)
                print(f"[heartbeat] running; last message {idle}s ago; subscribed assets={len(self.asset_ids)}")
                self._last_heartbeat = now


# =========================
# Main
# =========================

def main() -> None:
    ap = argparse.ArgumentParser(description="Polymarket alerts worker (price moves & whale trades)")
    ap.add_argument("--gamma-active", action="store_true", help="Filter Gamma markets to active=true.")
    ap.add_argument("--pages", type=int, default=5, help="Number of pages of markets to fetch from Gamma.")
    ap.add_argument("--per-page", type=int, default=100, help="Markets per page.")
    ap.add_argument("--window-min", type=int, default=10, help="Window size (minutes) for price move detection.")
    ap.add_argument("--move-pct", type=float, default=25.0, help="Percent move threshold.")
    ap.add_argument("--min-volume", type=float, default=250_000, help="Minimum market volume.")
    ap.add_argument("--min-liquidity", type=float, default=100_000, help="Minimum market liquidity.")
    ap.add_argument("--max-spread", type=float, default=0.06, help="Max bid-ask spread allowed (absolute).")
    ap.add_argument("--max-assets", type=int, default=500, help="Max number of assets to subscribe to.")
    ap.add_argument("--whale-threshold", type=float, default=0.0, help="Notional USD threshold to trigger whale alerts.")
    ap.add_argument("--whale-cooldown", type=int, default=300, help="Cooldown per asset for whale alerts (seconds).")
    ap.add_argument("--db-path", type=str, default="data/alerts.db", help="SQLite DB path.")
    ap.add_argument("--email", action="store_true", help="Send alerts by email.")
    ap.add_argument("--telegram", action="store_true", help="Send alerts via Telegram.")
    ap.add_argument("--heartbeat-sec", type=int, default=60, help="Heartbeat interval (seconds).")

    args = ap.parse_args()

    metas = fetch_gamma_markets(args.per_page, args.pages, active_only=args.gamma_active)
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
    )

    # Optional startup message
    if args.telegram:
        send_telegram_message("Polymarket alerts worker started ✅")
    if args.email:
        send_email("[Polymarket] Worker started", "Polymarket alerts worker started ✅")

    watcher.run_forever()


if __name__ == "__main__":
    main()
