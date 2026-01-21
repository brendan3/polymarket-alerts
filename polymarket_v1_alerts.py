#!/usr/bin/env python3
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
from typing import Any, Deque, Dict, List, Optional

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
        return float(x)
    except Exception:
        return None

def compute_odds(bid: Any, ask: Any) -> Optional[float]:
    b = safe_float(bid)
    a = safe_float(ask)
    if b and a and 0 < b < 1 and 0 < a < 1:
        return (b + a) / 2
    return b or a

def normalize_token_id(s: str) -> str:
    return s.strip().lower()

def as_money(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    if x >= 1_000_000:
        return f"${x/1_000_000:.2f}M"
    if x >= 1_000:
        return f"${x/1_000:.1f}K"
    return f"${x:.0f}"


# =========================
# Email
# =========================

def send_email(subject: str, body: str) -> None:
    host = os.getenv("ALERT_SMTP_HOST")
    port = int(os.getenv("ALERT_SMTP_PORT", "587"))
    user = os.getenv("ALERT_SMTP_USER")
    pw = os.getenv("ALERT_SMTP_PASS")
    email_from = os.getenv("ALERT_EMAIL_FROM")
    email_to = os.getenv("ALERT_EMAIL_TO")

    if not all([host, user, pw, email_from, email_to]):
        return

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = email_to

    with smtplib.SMTP(host, port) as s:
        s.starttls()
        s.login(user, pw)
        s.sendmail(email_from, [email_to], msg.as_string())


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


def fetch_gamma_markets(per_page: int, pages: int) -> List[MarketMeta]:
    metas: List[MarketMeta] = []

    for p in range(pages):
        r = requests.get(
            f"{GAMMA_BASE}/markets",
            params={"limit": per_page, "offset": p * per_page, "active": "true"},
            timeout=30,
        )
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
            c.execute("""
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
              decision TEXT,
              notes TEXT,
              result TEXT,
              pnl REAL
            )
            """)
            c.commit()

    def log(self, row: dict):
        with sqlite3.connect(self.path) as c:
            cols = ",".join(row.keys())
            qs = ",".join("?" * len(row))
            c.execute(f"INSERT INTO alerts ({cols}) VALUES ({qs})", list(row.values()))
            c.commit()


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
        window_min: int,
        move_pct: float,
        min_volume: float,
        min_liquidity: float,
        max_spread: float,
        email: bool,
        logger: AlertLoggerSQLite,
    ):
        self.metas = metas
        self.asset_to_meta = {tid: m for m in metas for tid in m.clob_token_ids}
        self.asset_ids = list(self.asset_to_meta.keys())

        self.window_ms = window_min * 60 * 1000
        self.threshold = move_pct / 100
        self.min_volume = min_volume
        self.min_liquidity = min_liquidity
        self.max_spread = max_spread
        self.email = email
        self.logger = logger

        self.history: Dict[str, Deque[PricePoint]] = defaultdict(deque)
        self.last_alert: Dict[str, int] = {}

    def related_markets(self, meta: MarketMeta) -> List[MarketMeta]:
        if not meta.event_slug:
            return []
        return [m for m in self.metas if m.event_slug == meta.event_slug][:6]

    def run(self):
        def on_open(ws):
            ws.send(json.dumps({"type": "market", "assets_ids": self.asset_ids}))
            print(f"Subscribed to {len(self.asset_ids)} tokens")

        def on_message(ws, msg):
            p = json.loads(msg)
            if p.get("event_type") != "price_change":
                return

            ts = p.get("timestamp", now_ms())

            for pc in p.get("price_changes", []):
                aid = normalize_token_id(pc.get("asset_id", ""))
                meta = self.asset_to_meta.get(aid)
                if not meta:
                    continue

                odds = compute_odds(pc.get("best_bid"), pc.get("best_ask"))
                if odds is None:
                    continue

                bid = safe_float(pc.get("best_bid"))
                ask = safe_float(pc.get("best_ask"))
                spread = ask - bid if bid and ask else None

                self.history[aid].append(PricePoint(ts, odds))
                while self.history[aid] and self.history[aid][0].ts < ts - self.window_ms:
                    self.history[aid].popleft()

                if len(self.history[aid]) < 2:
                    continue

                start = self.history[aid][0]
                change = (odds - start.odds) / start.odds
                if abs(change) < self.threshold:
                    continue

                if meta.volume < self.min_volume or meta.liquidity < self.min_liquidity:
                    continue

                if spread and spread > self.max_spread:
                    continue

                if ts - self.last_alert.get(aid, 0) < 60_000:
                    continue
                self.last_alert[aid] = ts

                outcome = meta.outcomes[meta.clob_token_ids.index(aid)] if meta.outcomes else "n/a"
                direction = "UP" if change > 0 else "DOWN"

                related = "\n".join(
                    f"- {m.market_question} ({as_money(m.volume)})"
                    for m in self.related_markets(meta)
                )

                alert = f"""
=== POLY ALERT V1.1 ===
when: {fmt_ts(ts)}
market: {meta.title()}
outcome: {outcome}
move: {direction} {change*100:.2f}%
from: {start.odds:.4f} → {odds:.4f}
volume: {as_money(meta.volume)}
liquidity: {as_money(meta.liquidity)}
spread: {spread:.4f if spread else 'n/a'}

Related markets:
{related}

asset_id: {aid}
======================
"""
                print(alert)

                self.logger.log({
                    "ts_ms": ts,
                    "ts_iso": fmt_ts(ts),
                    "market_title": meta.market_question,
                    "event_title": meta.event_title,
                    "outcome": outcome,
                    "from_odds": start.odds,
                    "to_odds": odds,
                    "move_pct": change * 100,
                    "volume": meta.volume,
                    "liquidity": meta.liquidity,
                    "best_bid": bid,
                    "best_ask": ask,
                    "spread": spread,
                    "asset_id": aid,
                    "ws_market_id": p.get("market"),
                    "decision": None,
                    "notes": None,
                    "result": "OPEN",
                    "pnl": None,
                })

                if self.email:
                    send_email(f"[Polymarket] {direction} {change*100:.1f}%", alert)

        ws = WebSocketApp(
            f"{CLOB_WS_BASE}/ws/market",
            on_open=on_open,
            on_message=on_message,
        )
        ws.run_forever()


# =========================
# Main
# =========================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=5)
    ap.add_argument("--per-page", type=int, default=100)
    ap.add_argument("--window-min", type=int, default=10)
    ap.add_argument("--move-pct", type=float, default=25)
    ap.add_argument("--min-volume", type=float, default=250_000)
    ap.add_argument("--min-liquidity", type=float, default=100_000)
    ap.add_argument("--max-spread", type=float, default=0.06)
    ap.add_argument("--db-path", type=str, default="data/alerts.db")
    ap.add_argument("--email", action="store_true")
    args = ap.parse_args()

    metas = fetch_gamma_markets(args.per_page, args.pages)
    logger = AlertLoggerSQLite(args.db_path)

    watcher = OddsWatcher(
        metas,
        args.window_min,
        args.move_pct,
        args.min_volume,
        args.min_liquidity,
        args.max_spread,
        args.email,
        logger,
    )
    watcher.run()


if __name__ == "__main__":
    main()
