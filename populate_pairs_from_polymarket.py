#!/usr/bin/env python3
"""
populate_pairs_from_polymarket.py

One-off helper script to fill in Polymarket CLOB token IDs in pairs.json
using Gamma's /markets/slug/{slug} endpoint.

Usage (from repo root):

  python populate_pairs_from_polymarket.py

It expects a pairs file (default: pairs.json) with entries like:

  {
    "name": "...",
    "kalshi_ticker": "CONTROLH-2026",
    "kalshi_close_time_iso": "...",
    "poly_yes_token_id": "REPLACE_WITH_...",
    "poly_no_token_id": "REPLACE_WITH_...",
    ...
  }

The script looks up known slugs for a small starter set of high-signal pairs,
fetches clobTokenIds from Gamma, and overwrites poly_yes_token_id /
poly_no_token_id where they still have REPLACE_WITH... placeholders.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List

import requests

GAMMA_BASE = "https://gamma-api.polymarket.com"

# Mapping from Kalshi market ticker -> Polymarket *market* slug
# IMPORTANT: for multi-outcome events (like House/Senate control),
# we want the specific outcome market that matches our Kalshi leg,
# not the parent event slug.
#
# These are the two high-signal 2026 control markets:
# - House control, Democratic Party outcome
# - Senate control, Republican Party outcome
#
# The Warsh confirmation pair is left for manual fill until we know
# the exact Polymarket slug for that contract.
PAIR_SLUGS: Dict[str, str] = {
    "CONTROLH-2026": "will-the-democratic-party-control-the-house-after-the-2026-midterm-elections",
    "CONTROLS-2026": "will-the-republican-party-control-the-senate-after-the-2026-midterm-elections",
    "KXRECSSNBER-26": "us-recession-by-end-of-2026",
    "KXSHUTDOWNBY-26DEC31": "will-there-be-another-us-government-shutdown-by-december-31",
    "KXBTC2026200-27JAN01": "will-bitcoin-reach-200000-by-december-31-2026-752",
    "KXHIGHINFLATION-26DEC": "will-inflation-reach-more-than-3-in-2026",
}


def fetch_market_by_slug(slug: str) -> Dict[str, Any]:
    url = f"{GAMMA_BASE}/markets/slug/{slug}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    # Gamma may return either a list or a single object depending on version.
    if isinstance(data, list):
        if not data:
            raise ValueError(f"Empty response list for slug {slug}")
        return data[0]
    if not isinstance(data, dict):
        raise ValueError(f"Unexpected response type for slug {slug}: {type(data)}")
    return data


def parse_clob_ids(raw: Any) -> List[str]:
    """
    Normalize clobTokenIds field to a list of string token IDs.
    Handles:
      - actual list
      - comma-separated string
      - JSON-encoded string (e.g. "[\"id1\",\"id2\"]")
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw]
    if isinstance(raw, str):
        s = raw.strip()
        # Try JSON first
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                return [str(x) for x in arr]
            except Exception:
                pass
        # Fallback: comma-separated
        return [part.strip() for part in s.split(",") if part.strip()]
    # Unexpected type
    return []


def main() -> None:
    pairs_path = os.getenv("PAIRS_JSON", "pairs.json")
    if not os.path.isfile(pairs_path):
        print(f"[error] pairs file not found: {pairs_path}", file=sys.stderr)
        sys.exit(1)

    with open(pairs_path, "r", encoding="utf-8") as f:
        try:
            pairs = json.load(f)
        except Exception as e:
            print(f"[error] could not parse {pairs_path} as JSON: {e}", file=sys.stderr)
            sys.exit(1)

    if not isinstance(pairs, list):
        print(f"[error] {pairs_path} must contain a JSON list", file=sys.stderr)
        sys.exit(1)

    updated_any = False

    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        ticker = str(pair.get("kalshi_ticker") or "").strip()
        if not ticker:
            continue
        slug = PAIR_SLUGS.get(ticker)
        if not slug:
            # Not one of the starter pairs we know about
            continue

        try:
            market = fetch_market_by_slug(slug)
        except Exception as e:
            print(f"[warn] failed to fetch slug '{slug}' for ticker '{ticker}': {e}", file=sys.stderr)
            continue

        clob_ids = parse_clob_ids(market.get("clobTokenIds") or market.get("clob_token_ids"))
        if len(clob_ids) < 2:
            print(
                f"[warn] unexpected clobTokenIds for slug '{slug}': {market.get('clobTokenIds')}",
                file=sys.stderr,
            )
            continue

        yes_id, no_id = clob_ids[0], clob_ids[1]

        # Only overwrite if still placeholders / empty
        yes_before = pair.get("poly_yes_token_id", "")
        no_before = pair.get("poly_no_token_id", "")
        if yes_before.startswith("REPLACE_WITH") or not yes_before:
            pair["poly_yes_token_id"] = yes_id
        if no_before.startswith("REPLACE_WITH") or not no_before:
            pair["poly_no_token_id"] = no_id

        updated_any = True
        print(f"{ticker}: slug={slug}")
        print(f"  YES token: {yes_id}")
        print(f"  NO  token: {no_id}")
        print()

    if not updated_any:
        print("[info] No pairs updated (token IDs may already be filled).", file=sys.stderr)
        return

    with open(pairs_path, "w", encoding="utf-8") as f:
        json.dump(pairs, f, indent=2)
        f.write("\n")

    print(f"[ok] Updated pairs written to {pairs_path}")


if __name__ == "__main__":
    main()

