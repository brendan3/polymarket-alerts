#!/usr/bin/env python3
"""
pair_gap_alert_bot.py
Cross-venue monitoring + Telegram alerts for Kalshi/Polymarket:
- Strategy A: Locked-payout arbitrage (buy YES on one venue + buy NO on the other)
- Strategy B: Spread-capture opportunities using the other venue as reference price

Uses Polymarket CLOB /book and Kalshi /markets/{ticker}/orderbook.
Uses ONLY public/market-data endpoints; no trading execution.

Env:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID (required)
  PAIRS_JSON (default: pairs.json) - path to JSON list of PairSpecs
  POLL_SECONDS (default: 2.0)
  MIN_EDGE_CENTS, MIN_DEPTH_USD (optional thresholds)
  KALSHI_API_KEY - Kalshi API key ID (required for orderbook)
  KALSHI_PRIVATE_KEY_PEM or KALSHI_PRIVATE_KEY_PATH - RSA private key for request signing
  KALSHI_BASE_URL (optional, default Kalshi trade-api v2)
"""
from __future__ import annotations

import asyncio
import base64
import json
import math
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp


# ----------------------------
# Config / utilities
# ----------------------------

@dataclass(frozen=True)
class PairSpec:
    name: str
    kalshi_ticker: str
    kalshi_close_time_iso: str
    poly_yes_token_id: str
    poly_no_token_id: str
    poly_end_date_iso: str
    equivalence_grade: str  # "A", "B", "C"


@dataclass
class BotConfig:
    telegram_token: str
    telegram_chat_id: str
    poll_seconds: float = 2.0
    min_edge_cents: float = 2.0
    min_depth_usd: float = 200.0
    max_slippage_cents: float = 2.0
    min_days_to_close: float = 0.2
    max_days_to_close: float = 400.0
    kalshi_fee_bps: float = 0.0
    poly_fee_bps: float = 0.0
    kalshi_base: str = "https://api.elections.kalshi.com/trade-api/v2"
    polymarket_clob_base: str = "https://clob.polymarket.com"
    kalshi_api_key: Optional[str] = None
    kalshi_private_key_pem: Optional[str] = None


class AlertDeduper:
    def __init__(self, ttl_seconds: float = 300.0):
        self.ttl = ttl_seconds
        self._seen: Dict[str, float] = {}

    def should_send(self, key: str) -> bool:
        now = time.time()
        for k, ts in list(self._seen.items()):
            if now - ts > self.ttl:
                del self._seen[k]
        if key in self._seen:
            return False
        self._seen[key] = now
        return True


def iso_to_epoch_seconds(iso_str: str) -> float:
    if "T" not in iso_str:
        y, m, d = map(int, iso_str.split("-")[:3])
        return time.mktime((y, m, d, 23, 59, 59, 0, 0, -1))
    s = iso_str.replace("Z", "").replace("+00:00", "")
    date_part, time_part = s.split("T")
    y, m, d = map(int, date_part.split("-"))
    hh, mm, ss = 0, 0, 0
    if ":" in time_part:
        parts = time_part.split(":")
        hh = int(parts[0])
        if len(parts) > 1:
            mm = int(parts[1])
        if len(parts) > 2:
            ss = int(parts[2].split(".")[0])
    return time.mktime((y, m, d, hh, mm, ss, 0, 0, -1))


async def send_telegram(cfg: BotConfig, text: str, session: aiohttp.ClientSession) -> None:
    url = "https://api.telegram.org/bot{}/sendMessage".format(cfg.telegram_token)
    payload = {"chat_id": cfg.telegram_chat_id, "text": text, "disable_web_page_preview": True}
    async with session.post(url, json=payload, timeout=10) as r:
        if r.status != 200:
            body = await r.text()
            raise RuntimeError("Telegram send failed: {} {}".format(r.status, body))


# ----------------------------
# Kalshi request signing
# ----------------------------

def _sign_kalshi_request(private_key_pem: str, timestamp_ms: str, method: str, path: str) -> str:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding

    key = serialization.load_pem_private_key(
        private_key_pem.encode() if isinstance(private_key_pem, str) else private_key_pem,
        password=None,
    )
    path_no_query = path.split("?")[0]
    message = (timestamp_ms + method + path_no_query).encode("utf-8")
    signature = key.sign(message, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())
    return base64.b64encode(signature).decode("ascii")


def _kalshi_headers(cfg: BotConfig, method: str, path: str) -> Optional[Dict[str, str]]:
    if not cfg.kalshi_api_key or not cfg.kalshi_private_key_pem:
        return None
    try:
        ts_ms = str(int(time.time() * 1000))
        sig = _sign_kalshi_request(cfg.kalshi_private_key_pem, ts_ms, method, path)
        return {
            "KALSHI-ACCESS-KEY": cfg.kalshi_api_key,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": sig,
        }
    except Exception:
        return None


# ----------------------------
# Data fetchers
# ----------------------------

async def fetch_json(
    session: aiohttp.ClientSession,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Any:
    async with session.get(url, params=params, headers=headers, timeout=15) as r:
        r.raise_for_status()
        return await r.json()


async def fetch_polymarket_book(cfg: BotConfig, session: aiohttp.ClientSession, token_id: str) -> Dict[str, Any]:
    return await fetch_json(session, "{}/book".format(cfg.polymarket_clob_base), params={"token_id": token_id})


async def fetch_kalshi_orderbook(
    cfg: BotConfig, session: aiohttp.ClientSession, market_ticker: str, depth: int = 50
) -> Dict[str, Any]:
    base = cfg.kalshi_base.rstrip("/")
    # Path for signing: /trade-api/v2/markets/{ticker}/orderbook (no query)
    path = "/trade-api/v2/markets/{}/orderbook".format(market_ticker)
    url = "{}/markets/{}/orderbook".format(base, market_ticker)
    params = {"depth": depth}
    headers = _kalshi_headers(cfg, "GET", path)
    return await fetch_json(session, url, params=params, headers=headers)


# ----------------------------
# Pricing helpers
# ----------------------------

def best_bid_ask_from_polymarket_book(book: Dict[str, Any]) -> Tuple[float, float, float, float]:
    bids = book.get("bids", [])
    asks = book.get("asks", [])
    if not bids or not asks:
        return (math.nan, math.nan, 0.0, 0.0)
    best_bid_level = max(bids, key=lambda x: float(x["price"]))
    best_ask_level = min(asks, key=lambda x: float(x["price"]))
    return (
        float(best_bid_level["price"]),
        float(best_ask_level["price"]),
        float(best_bid_level.get("size", 0)),
        float(best_ask_level.get("size", 0)),
    )


def best_yes_bid_ask_from_kalshi_orderbook(ob: Dict[str, Any]) -> Tuple[float, float, float, float]:
    orderbook = (ob.get("orderbook_fp") or ob.get("orderbook") or {})
    if isinstance(orderbook, list):
        orderbook = {}
    yes_levels = orderbook.get("yes_dollars") or orderbook.get("yes") or []
    no_levels = orderbook.get("no_dollars") or orderbook.get("no") or []

    def parse_levels(levels: List[Any]) -> List[Tuple[float, float]]:
        parsed: List[Tuple[float, float]] = []
        for lvl in levels:
            if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
                continue
            price, qty = lvl[0], lvl[1]
            if isinstance(price, str):
                p = float(price)
            else:
                p = float(price) / 100.0 if float(price) > 1.0 else float(price)
            parsed.append((p, float(qty)))
        return parsed

    yes = parse_levels(yes_levels)
    no = parse_levels(no_levels)
    if not yes or not no:
        return (math.nan, math.nan, 0.0, 0.0)

    best_yes_bid = max(yes, key=lambda t: t[0])
    best_no_bid = max(no, key=lambda t: t[0])
    best_yes_bid_px, best_yes_bid_sz = best_yes_bid
    best_yes_ask_px = 1.0 - best_no_bid[0]
    best_yes_ask_sz = best_no_bid[1]
    return (best_yes_bid_px, best_yes_ask_px, best_yes_bid_sz, best_yes_ask_sz)


def fee_cost(price: float, fee_bps: float) -> float:
    return price * (fee_bps / 10_000.0)


def fmt_pct(p: float) -> str:
    if math.isnan(p):
        return "NA"
    return "{:.1f}%".format(100.0 * p)


def locked_payout_edge(
    buy_yes_ask: float,
    buy_no_ask: float,
    yes_fee_bps: float,
    no_fee_bps: float,
) -> float:
    total_cost = (
        buy_yes_ask + buy_no_ask
        + fee_cost(buy_yes_ask, yes_fee_bps)
        + fee_cost(buy_no_ask, no_fee_bps)
    )
    return 1.0 - total_cost


# ----------------------------
# Main loop
# ----------------------------

async def run(cfg: BotConfig, pairs: List[PairSpec]) -> None:
    dedupe = AlertDeduper(ttl_seconds=300)
    async with aiohttp.ClientSession() as session:
        # Startup notification to Telegram: how many pairs and which ones
        try:
            summary_lines = [
                f"Pair gap alert bot online. Tracking {len(pairs)} pairs:",
                "",
            ] + [
                f"- {p.name} (Kalshi {p.kalshi_ticker})"
                for p in pairs
            ]
            await send_telegram(cfg, "\n".join(summary_lines), session)
        except Exception:
            # Don't kill the bot if the intro message fails
            pass

        while True:
            start = time.time()
            for pair in pairs:
                now = time.time()
                try:
                    k_close = iso_to_epoch_seconds(pair.kalshi_close_time_iso)
                except Exception:
                    continue
                days_to_close = max(0.0, (k_close - now) / 86400.0)
                if not (cfg.min_days_to_close <= days_to_close <= cfg.max_days_to_close):
                    continue
                if pair.equivalence_grade.upper() not in ("A", "B"):
                    continue

                try:
                    poly_yes_book, poly_no_book, kalshi_ob = await asyncio.gather(
                        fetch_polymarket_book(cfg, session, pair.poly_yes_token_id),
                        fetch_polymarket_book(cfg, session, pair.poly_no_token_id),
                        fetch_kalshi_orderbook(cfg, session, pair.kalshi_ticker, depth=50),
                    )
                except Exception as e:
                    continue

                py_bid, py_ask, py_bid_sz, py_ask_sz = best_bid_ask_from_polymarket_book(poly_yes_book)
                pn_bid, pn_ask, pn_bid_sz, pn_ask_sz = best_bid_ask_from_polymarket_book(poly_no_book)
                ky_bid, ky_ask, ky_bid_sz, ky_ask_sz = best_yes_bid_ask_from_kalshi_orderbook(kalshi_ob)

                if any(math.isnan(x) for x in (py_ask, pn_ask, ky_ask)):
                    continue

                if py_ask * py_ask_sz < cfg.min_depth_usd:
                    continue
                if pn_ask * pn_ask_sz < cfg.min_depth_usd:
                    continue
                if ky_ask * ky_ask_sz < cfg.min_depth_usd:
                    continue

                # Strategy A: Locked payout
                edge_kn = locked_payout_edge(
                    buy_yes_ask=ky_ask,
                    buy_no_ask=pn_ask,
                    yes_fee_bps=cfg.kalshi_fee_bps,
                    no_fee_bps=cfg.poly_fee_bps,
                )
                k_no_ask = 1.0 - ky_bid
                edge_nk = locked_payout_edge(
                    buy_yes_ask=py_ask,
                    buy_no_ask=k_no_ask,
                    yes_fee_bps=cfg.poly_fee_bps,
                    no_fee_bps=cfg.kalshi_fee_bps,
                )

                min_edge = cfg.min_edge_cents / 100.0
                if edge_kn >= min_edge:
                    key = "arb_kn:{}:{}".format(pair.kalshi_ticker, round(edge_kn, 4))
                    if dedupe.should_send(key):
                        msg = (
                            "[ARB] {}\n"
                            "Buy YES Kalshi @ {:.3f}, Buy NO Poly @ {:.3f}\n"
                            "Edge ~{:.1f}c per $1 (grade {}, ~{:.1f}d)\n"
                            "Poly YES mid~{}; Kalshi YES mid~{}"
                        ).format(
                            pair.name,
                            ky_ask,
                            pn_ask,
                            edge_kn * 100,
                            pair.equivalence_grade,
                            days_to_close,
                            fmt_pct((py_bid + py_ask) / 2),
                            fmt_pct((ky_bid + ky_ask) / 2),
                        )
                        await send_telegram(cfg, msg, session)

                if edge_nk >= min_edge:
                    key = "arb_nk:{}:{}".format(pair.kalshi_ticker, round(edge_nk, 4))
                    if dedupe.should_send(key):
                        msg = (
                            "[ARB] {}\n"
                            "Buy YES Poly @ {:.3f}, Buy NO Kalshi @ {:.3f}\n"
                            "Edge ~{:.1f}c per $1 (grade {}, ~{:.1f}d)"
                        ).format(
                            pair.name,
                            py_ask,
                            k_no_ask,
                            edge_nk * 100,
                            pair.equivalence_grade,
                            days_to_close,
                        )
                        await send_telegram(cfg, msg, session)

                # Strategy B: Spread capture
                poly_mid_yes = (py_bid + py_ask) / 2.0
                kalshi_spread = max(0.0, ky_ask - ky_bid)
                if kalshi_spread >= 0.05:
                    quote_bid = min(ky_bid + 0.01, poly_mid_yes - 0.01)
                    quote_bid = max(0.01, round(quote_bid, 3))
                    quote_no_bid = min((1.0 - ky_ask) + 0.01, (1.0 - poly_mid_yes) - 0.01)
                    quote_no_bid = max(0.01, round(quote_no_bid, 3))
                    key = "spread:{}:{}".format(pair.kalshi_ticker, round(kalshi_spread, 3))
                    if dedupe.should_send(key):
                        msg = (
                            "[SPREAD] {}\n"
                            "Kalshi YES bid/ask {:.3f}/{:.3f} (spread {:.1f}c)\n"
                            "Poly YES mid ~{:.3f}\n"
                            "Idea: post Kalshi YES bid ~{:.3f} and/or Kalshi NO bid ~{:.3f}\n"
                            "(Only if comfortable warehousing; check depth + rules.)"
                        ).format(
                            pair.name,
                            ky_bid,
                            ky_ask,
                            kalshi_spread * 100,
                            poly_mid_yes,
                            quote_bid,
                            quote_no_bid,
                        )
                        await send_telegram(cfg, msg, session)

            elapsed = time.time() - start
            await asyncio.sleep(max(0.0, cfg.poll_seconds - elapsed))


def load_pairs(path: str) -> List[PairSpec]:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return [PairSpec(**x) for x in raw]


def main() -> None:
    telegram_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not telegram_token or not telegram_chat_id:
        raise SystemExit("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID")

    pairs_path = os.getenv("PAIRS_JSON", "pairs.json")
    if not os.path.isfile(pairs_path):
        raise SystemExit("PAIRS_JSON file not found: {}. Create it or set PAIRS_JSON.".format(pairs_path))

    kalshi_key = os.environ.get("KALSHI_API_KEY")
    kalshi_pem = os.environ.get("KALSHI_PRIVATE_KEY_PEM")
    if not kalshi_pem and os.environ.get("KALSHI_PRIVATE_KEY_PATH"):
        key_path = Path(os.environ["KALSHI_PRIVATE_KEY_PATH"])
        if key_path.is_file():
            kalshi_pem = key_path.read_text()
    if not kalshi_key or not kalshi_pem:
        print("Warning: KALSHI_API_KEY and KALSHI_PRIVATE_KEY_PEM (or KALSHI_PRIVATE_KEY_PATH) not set. Kalshi orderbook may 401.")

    cfg = BotConfig(
        telegram_token=telegram_token,
        telegram_chat_id=telegram_chat_id,
        poll_seconds=float(os.getenv("POLL_SECONDS", "2.0")),
        min_edge_cents=float(os.getenv("MIN_EDGE_CENTS", "2.0")),
        min_depth_usd=float(os.getenv("MIN_DEPTH_USD", "200.0")),
        kalshi_base=os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2"),
        kalshi_api_key=kalshi_key,
        kalshi_private_key_pem=kalshi_pem,
    )
    pairs = load_pairs(pairs_path)
    if not pairs:
        raise SystemExit("No pairs in {}.".format(pairs_path))
    print("Pair gap alert bot started with {} pairs.".format(len(pairs)))
    asyncio.run(run(cfg, pairs))


if __name__ == "__main__":
    main()
