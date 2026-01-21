#!/usr/bin/env python3
"""
polymarket_v1_alerts.py

V1: "Rare & important" Polymarket alerts with:
- Gamma metadata cache (market question/title, volume, liquidity)
- CLOB market websocket subscription for outcome tokens (asset_ids)
- Alert on odds move >= X% in rolling Y-minute window
- Filters: min market volume + min market liquidity
- Optional SMTP email alerts
- Heartbeat logging (so silence isn't scary)
- Exponential reconnect backoff (production friendly)

Install:
  pip install websocket-client requests

Local run:
  python3 polymarket_v1_alerts.py --gamma-active --pages 5 --per-page 100 \
    --window-min 10 --move-pct 25 --min-volume 250000 --min-liquidity 100000

Email (optional):
  export ALERT_SMTP_HOST="smtp.gmail.com"
  export ALERT_SMTP_PORT="587"
  export ALERT_SMTP_USER="you@gmail.com"
  export ALERT_SMTP_PASS="app_password"
  export ALERT_EMAIL_FROM="you@gmail.com"
  export ALERT_EMAIL_TO="you@gmail.com"
  python3 polymarket_v1_alerts.py ... --email
"""

from __future__ import annotations

import argparse
import json
import os
import smtplib
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import Any, Deque, Dict, List, Optional, Tuple

import requests
from websocket import WebSocketApp

CLOB_WS_BASE = "wss://ws-subscriptions-clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"


# -------------------------
# Helpers
# -------------------------

def now_ms() -> int:
    return int(time.time() * 1000)


def fmt_ts(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.isoformat(timespec="seconds")


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def compute_odds(best_bid: Any, best_ask: Any) -> Optional[float]:
    bid = safe_float(best_bid)
    ask = safe_float(best_ask)

    def valid(p: Optional[float]) -> bool:
        return p is not None and 0.0 < p < 1.0

    if valid(bid) and valid(ask) and ask >= bid:
        return (bid + ask) / 2.0
    if valid(bid):
        return bid
    if valid(ask):
        return ask
    return None


def normalize_token_id(s: str) -> str:
    return s.strip().lower()


def parse_clob_token_ids(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [normalize_token_id(str(x)) for x in raw if str(x).strip()]
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    return [normalize_token_id(str(x)) for x in arr if str(x).strip()]
            except Exception:
                pass
        return [normalize_token_id(p) for p in s.split(",") if p.strip()]
    return [normalize_token_id(str(raw))]


def parse_outcomes(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw]
    if isinstance(raw, str):
        s = raw.strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    return [str(x) for x in arr]
            except Exception:
                pass
        return [raw]
    return [str(raw)]


def as_money(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    if x >= 1_000_000_000:
        return f"${x/1_000_000_000:.2f}B"
    if x >= 1_000_000:
        return f"${x/1_000_000:.2f}M"
    if x >= 1_000:
        return f"${x/1_000:.1f}K"
    return f"${x:.0f}"


# -------------------------
# Email (optional)
# -------------------------

def send_email(subject: str, body: str) -> None:
    host = os.getenv("ALERT_SMTP_HOST")
    port = int(os.getenv("ALERT_SMTP_PORT", "587"))
    user = os.getenv("ALERT_SMTP_USER")
    pw = os.getenv("ALERT_SMTP_PASS")
    email_from = os.getenv("ALERT_EMAIL_FROM")
    email_to = os.getenv("ALERT_EMAIL_TO")

    if not (host and user and pw and email_from and email_to):
        return  # email disabled or not configured

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = email_to

    with smtplib.SMTP(host, port, timeout=30) as server:
        server.starttls()
        server.login(user, pw)
        server.sendmail(email_from, [email_to], msg.as_string())


# -------------------------
# Gamma metadata cache
# -------------------------

@dataclass
class MarketMeta:
    market_id: Optional[Any]
    market_question: str
    event_title: str
    market_slug: Optional[str]
    event_slug: Optional[str]
    volume: Optional[float]
    liquidity: Optional[float]
    outcomes: List[str]
    clob_token_ids: List[str]

    def title_line(self) -> str:
        if self.event_title and self.market_question and self.event_title != self.market_question:
            return f"{self.event_title} — {self.market_question}"
        return self.market_question or self.event_title or "Unknown market"


def fetch_gamma_markets_page(limit: int, offset: int, active: bool = True, closed: bool = False) -> List[Dict[str, Any]]:
    params = {
        "limit": str(limit),
        "offset": str(offset),
        "active": "true" if active else "false",
        "closed": "true" if closed else "false",
    }
    r = requests.get(f"{GAMMA_BASE}/markets", params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise ValueError(f"Unexpected Gamma /markets response type: {type(data)}")
    return data


def build_metadata_cache(per_page: int, pages: int) -> Tuple[Dict[str, MarketMeta], List[MarketMeta]]:
    asset_to_meta: Dict[str, MarketMeta] = {}
    markets: List[MarketMeta] = []

    for p in range(pages):
        offset = p * per_page
        batch = fetch_gamma_markets_page(limit=per_page, offset=offset, active=True, closed=False)
        if not batch:
            break

        for m in batch:
            question = str(m.get("question") or "").strip()
            event_title = str(m.get("title") or m.get("eventTitle") or "").strip()

            if not question and event_title:
                question = event_title
            if not event_title:
                event_title = ""

            # Gamma fields can vary; try several common keys
            volume = (
                safe_float(m.get("volume")) or
                safe_float(m.get("volumeNum")) or
                safe_float(m.get("volumeUSD")) or
                safe_float(m.get("volume_usd"))
            )
            liquidity = (
                safe_float(m.get("liquidity")) or
                safe_float(m.get("liquidityNum")) or
                safe_float(m.get("liquidityUSD")) or
                safe_float(m.get("liquidity_usd"))
            )

            clob_ids = parse_clob_token_ids(m.get("clobTokenIds"))
            if not clob_ids:
                continue

            outcomes = parse_outcomes(m.get("outcomes"))

            meta = MarketMeta(
                market_id=m.get("id"),
                market_question=question or "Unknown question",
                event_title=event_title,
                market_slug=m.get("slug"),
                event_slug=m.get("eventSlug") or m.get("event_slug"),
                volume=volume,
                liquidity=liquidity,
                outcomes=outcomes,
                clob_token_ids=clob_ids,
            )

            markets.append(meta)
            for tok in clob_ids:
                asset_to_meta[tok] = meta

    return asset_to_meta, markets


# -------------------------
# Odds watcher with filters + heartbeat + backoff
# -------------------------

@dataclass
class PricePoint:
    ts_ms: int
    odds: float


class OddsWatcherV1:
    def __init__(
        self,
        asset_ids: List[str],
        asset_to_meta: Dict[str, MarketMeta],
        window_min: int,
        move_pct: float,
        min_volume: float,
        min_liquidity: float,
        verbose: bool = False,
        email: bool = False,
        heartbeat_sec: int = 300,
        backoff_base_sec: int = 2,
        max_backoff_sec: int = 60,
    ) -> None:
        self.asset_ids = list(dict.fromkeys(asset_ids))
        self.asset_to_meta = asset_to_meta

        self.window_ms = int(window_min * 60 * 1000)
        self.threshold = move_pct / 100.0
        self.min_volume = min_volume
        self.min_liquidity = min_liquidity

        self.verbose = verbose
        self.email = email

        self.heartbeat_sec = heartbeat_sec
        self.backoff_base_sec = backoff_base_sec
        self.max_backoff_sec = max_backoff_sec

        self.history: Dict[str, Deque[PricePoint]] = defaultdict(deque)
        self.last_alert_ms: Dict[str, int] = {}

        self.alert_count = 0

        self.ws: Optional[WebSocketApp] = None
        self.connected = False
        self.last_ws_msg_ms: Optional[int] = None

        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def _prune(self, asset_id: str, current_ms: int) -> None:
        dq = self.history[asset_id]
        cutoff = current_ms - self.window_ms
        while dq and dq[0].ts_ms < cutoff:
            dq.popleft()

    def _passes_filters(self, asset_id: str) -> bool:
        meta = self.asset_to_meta.get(asset_id)
        if meta is None:
            return False

        vol = meta.volume or 0.0
        liq = meta.liquidity or 0.0

        if vol < self.min_volume:
            return False
        if liq < self.min_liquidity:
            return False
        return True

    def _eligible_token_count(self) -> int:
        c = 0
        for tok, meta in self.asset_to_meta.items():
            vol = meta.volume or 0.0
            liq = meta.liquidity or 0.0
            if vol >= self.min_volume and liq >= self.min_liquidity:
                c += 1
        return c

    def _describe_outcome(self, asset_id: str) -> str:
        meta = self.asset_to_meta.get(asset_id)
        if not meta:
            return ""
        try:
            idx = meta.clob_token_ids.index(asset_id)
        except ValueError:
            idx = -1
        if idx >= 0 and idx < len(meta.outcomes):
            return meta.outcomes[idx]
        if idx == 0:
            return "Yes"
        if idx == 1:
            return "No"
        return ""

    def _maybe_alert(self, asset_id: str, market_ws_id: Optional[str], current: PricePoint) -> None:
        dq = self.history[asset_id]
        if len(dq) < 2:
            return

        oldest = dq[0]
        if oldest.odds <= 0:
            return

        change = (current.odds - oldest.odds) / oldest.odds
        if abs(change) < self.threshold:
            return

        if not self._passes_filters(asset_id):
            return

        cooldown_ms = 60_000
        last = self.last_alert_ms.get(asset_id, 0)
        if current.ts_ms - last < cooldown_ms:
            return
        self.last_alert_ms[asset_id] = current.ts_ms
        self.alert_count += 1

        meta = self.asset_to_meta.get(asset_id)
        direction = "UP" if change > 0 else "DOWN"
        outcome = self._describe_outcome(asset_id)

        title = meta.title_line() if meta else "Unknown market"
        vol_str = as_money(meta.volume if meta else None)
        liq_str = as_money(meta.liquidity if meta else None)

        alert_text = (
            "\n=== POLY ALERT (V1) ===\n"
            f"when:      {fmt_ts(current.ts_ms)}\n"
            f"market:    {title}\n"
            f"outcome:   {outcome or 'n/a'}\n"
            f"window:    {self.window_ms/60000:.0f} min\n"
            f"from:      {oldest.odds:.4f} at {fmt_ts(oldest.ts_ms)}\n"
            f"to:        {current.odds:.4f} at {fmt_ts(current.ts_ms)}\n"
            f"move:      {direction} {change*100:.2f}%\n"
            f"volume:    {vol_str}\n"
            f"liquidity: {liq_str}\n"
            f"asset_id:  {asset_id}\n"
            f"ws_market: {market_ws_id or 'n/a'}\n"
            "========================\n"
        )

        print(alert_text)

        if self.email:
            subject = f"[Polymarket Alert] {direction} {change*100:.1f}% — {title}"
            send_email(subject, alert_text)

    def on_open(self, ws: WebSocketApp) -> None:
        self.connected = True
        sub_msg = {"assets_ids": self.asset_ids, "type": "market"}
        ws.send(json.dumps(sub_msg))

        threading.Thread(target=self._ping_loop, args=(ws,), daemon=True).start()

        print(f"Subscribed to {len(self.asset_ids)} outcome tokens (asset_ids).")
        print(
            f"Filters: move>={self.threshold*100:.1f}% in {self.window_ms/60000:.0f}m, "
            f"min_volume={as_money(self.min_volume)}, min_liquidity={as_money(self.min_liquidity)}"
        )

        if self.email:
            enabled = all(
                os.getenv(k)
                for k in ["ALERT_SMTP_HOST", "ALERT_SMTP_USER", "ALERT_SMTP_PASS", "ALERT_EMAIL_FROM", "ALERT_EMAIL_TO"]
            )
            print(f"Email alerts: {'ENABLED' if enabled else 'requested but missing env vars (DISABLED)'}")

    def on_message(self, ws: WebSocketApp, message: str) -> None:
        self.last_ws_msg_ms = now_ms()

        try:
            payload = json.loads(message)
        except Exception:
            if self.verbose:
                print(f"[ws] non-json: {message!r}")
            return

        if not isinstance(payload, dict):
            return

        event_type = payload.get("event_type")
        if event_type != "price_change":
            if self.verbose:
                print(f"[ws] event_type={event_type}")
            return

        ts_ms = int(payload.get("timestamp") or now_ms())
        market_ws_id = payload.get("market")
        pcs = payload.get("price_changes") or []
        if not isinstance(pcs, list):
            return

        for pc in pcs:
            if not isinstance(pc, dict):
                continue
            asset_id = normalize_token_id(str(pc.get("asset_id") or ""))
            if not asset_id:
                continue

            odds = compute_odds(pc.get("best_bid"), pc.get("best_ask"))
            if odds is None:
                continue

            point = PricePoint(ts_ms=ts_ms, odds=odds)
            self.history[asset_id].append(point)
            self._prune(asset_id, ts_ms)
            self._maybe_alert(asset_id, market_ws_id, point)

            if self.verbose:
                meta = self.asset_to_meta.get(asset_id)
                label = meta.title_line() if meta else asset_id
                print(f"[tick] {fmt_ts(ts_ms)} {label} odds={odds:.4f}")

    def on_error(self, ws: WebSocketApp, error: Exception) -> None:
        # errors can happen during shutdown; keep message short
        if not self._stop.is_set():
            print(f"[ws] error: {error}")

    def on_close(self, ws: WebSocketApp, code: int, msg: str) -> None:
        self.connected = False
        if not self._stop.is_set():
            print(f"[ws] closed: code={code} msg={msg}")

    def _ping_loop(self, ws: WebSocketApp) -> None:
        while not self._stop.is_set():
            try:
                ws.send("PING")
            except Exception:
                return
            time.sleep(10)

    def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            time.sleep(self.heartbeat_sec)

            conn = "CONNECTED" if self.connected else "DISCONNECTED"
            last_msg = "never"
            if self.last_ws_msg_ms is not None:
                last_msg = fmt_ts(self.last_ws_msg_ms)

            print(
                f"[heartbeat] {fmt_ts(now_ms())} | {conn} | "
                f"last_ws_msg={last_msg} | alerts={self.alert_count} | "
                f"eligible_tokens~{self._eligible_token_count()}/{len(self.asset_ids)}"
            )

    def run(self) -> None:
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

        url = f"{CLOB_WS_BASE}/ws/market"
        backoff = self.backoff_base_sec

        while not self._stop.is_set():
            self.connected = False
            self.last_ws_msg_ms = None

            self.ws = WebSocketApp(
                url,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
            )

            try:
                self.ws.run_forever(ping_interval=None)
            except KeyboardInterrupt:
                print("Exiting on Ctrl+C.")
                self.stop()
                return
            except Exception as e:
                if not self._stop.is_set():
                    print(f"[ws] run_forever exception: {e}")

            if self._stop.is_set():
                return

            # Exponential backoff before reconnect
            sleep_s = min(backoff, self.max_backoff_sec)
            print(f"[ws] reconnecting in {sleep_s}s ...")
            time.sleep(sleep_s)
            backoff = min(backoff * 2, self.max_backoff_sec)

            # If we successfully open again, we'll reset backoff in on_open by setting connected=True.
            # But to be safe, reset here when we get a "clean" loop exit due to transient network issues:
            # We'll reset backoff after any successful open:
            if self.connected:
                backoff = self.backoff_base_sec


# -------------------------
# CLI
# -------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Polymarket V1: High-signal odds swing alerts with metadata + filters.")
    p.add_argument("--gamma-active", action="store_true", help="Fetch active markets from Gamma and subscribe to their clobTokenIds.")
    p.add_argument("--per-page", type=int, default=100, help="Gamma /markets page size (default 100).")
    p.add_argument("--pages", type=int, default=3, help="How many pages of markets to fetch (default 3).")

    p.add_argument("--window-min", type=int, default=10, help="Rolling window minutes (default 10).")
    p.add_argument("--move-pct", type=float, default=25.0, help="Alert if odds move >= this percent within window (default 25).")

    p.add_argument("--min-volume", type=float, default=250_000.0, help="Only alert if market volume >= this USD.")
    p.add_argument("--min-liquidity", type=float, default=100_000.0, help="Only alert if market liquidity >= this USD.")

    p.add_argument("--verbose", action="store_true", help="Print ticks (noisy).")
    p.add_argument("--email", action="store_true", help="Send email alerts using ALERT_SMTP_* env vars.")

    p.add_argument("--heartbeat-sec", type=int, default=300, help="Heartbeat log interval seconds (default 300).")
    p.add_argument("--backoff-base-sec", type=int, default=2, help="Reconnect backoff start seconds (default 2).")
    p.add_argument("--max-backoff-sec", type=int, default=60, help="Reconnect backoff cap seconds (default 60).")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if not args.gamma_active:
        raise SystemExit("Run with --gamma-active (this script is built around Gamma metadata + filtering).")

    print(f"Fetching Gamma metadata: pages={args.pages}, per_page={args.per_page} ...")
    asset_to_meta, metas = build_metadata_cache(per_page=args.per_page, pages=args.pages)

    asset_ids = list(asset_to_meta.keys())
    print(f"Gamma markets parsed: {len(metas)} (may include markets without clobTokenIds, which are skipped)")
    print(f"Outcome tokens discovered: {len(asset_ids)}")

    passing = 0
    for tok, meta in asset_to_meta.items():
        vol = meta.volume or 0.0
        liq = meta.liquidity or 0.0
        if vol >= args.min_volume and liq >= args.min_liquidity:
            passing += 1
    print(f"Tokens meeting filters (volume/liquidity): ~{passing} / {len(asset_ids)}")

    watcher = OddsWatcherV1(
        asset_ids=asset_ids,
        asset_to_meta=asset_to_meta,
        window_min=args.window_min,
        move_pct=args.move_pct,
        min_volume=args.min_volume,
        min_liquidity=args.min_liquidity,
        verbose=args.verbose,
        email=args.email,
        heartbeat_sec=args.heartbeat_sec,
        backoff_base_sec=args.backoff_base_sec,
        max_backoff_sec=args.max_backoff_sec,
    )
    watcher.run()


if __name__ == "__main__":
    main()
