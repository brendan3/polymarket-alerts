# Pair Gap Alert Bot (Kalshi / Polymarket)

Cross-venue arbitrage monitor. Alerts via Telegram when:
- **Strategy A:** Locked-payout arb (buy YES on one venue + NO on the other, cost < $1).
- **Strategy B:** Kalshi spread is wide and Polymarket gives a stable reference mid; suggests passive quotes.

## Railway (3rd service)

- **Start command:** `python pair_gap_alert_bot.py`
- **Environment variables:**

| Variable | Required | Description |
|---------|----------|-------------|
| `TELEGRAM_BOT_TOKEN` | Yes | Telegram bot token (you have this) |
| `TELEGRAM_CHAT_ID` | Yes | Telegram chat ID (you have this) |
| `PYTHONUNBUFFERED` | Optional | Set to `1` for live logs (you have this) |
| `KALSHI_API_KEY` | Yes | Kalshi API key ID (e.g. `482a59a5-81b4-45bc-81cd-1ddf1a91fd94`) |
| `KALSHI_PRIVATE_KEY_PEM` | Yes* | RSA private key in PEM format (full string, including `-----BEGIN ...-----`) |
| `KALSHI_PRIVATE_KEY_PATH` | Alt | Path to a file containing the PEM (e.g. in a secret mount). If set, used when `KALSHI_PRIVATE_KEY_PEM` is not. |
| `PAIRS_JSON` | No | Path to pairs JSON (default: `pairs.json`) |
| `POLL_SECONDS` | No | Poll interval in seconds (default: `2.0`) |
| `MIN_EDGE_CENTS` | No | Min edge per $1 to alert (default: `2.0`) |
| `MIN_DEPTH_USD` | No | Min top-of-book depth per leg in USD (default: `200`) |
| `KALSHI_BASE_URL` | No | Kalshi API base (default: `https://api.elections.kalshi.com/trade-api/v2`) |

\* Kalshi’s orderbook endpoint requires signed requests (API key + RSA-PSS signature). You need either `KALSHI_PRIVATE_KEY_PEM` or `KALSHI_PRIVATE_KEY_PATH`.

## Pairs file

The bot reads a JSON list of pair specs. Copy and edit the example:

```bash
cp pairs_example.json pairs.json
```

Edit `pairs.json` and replace with real values:

- **kalshi_ticker** – from Kalshi (e.g. market ticker).
- **kalshi_close_time_iso** – Kalshi close/expiry (ISO).
- **poly_yes_token_id** / **poly_no_token_id** – Polymarket CLOB token IDs for YES and NO (from Gamma market’s `clobTokenIds`).
- **poly_end_date_iso** – Polymarket end date (for your own sanity checks).
- **equivalence_grade** – `"A"` or `"B"` (only A/B pairs are used by default).

Commit `pairs.json` to the repo (or mount it in Railway and set `PAIRS_JSON` to that path).

## RSA private key on Railway

- **Option A:** Put the full PEM string in `KALSHI_PRIVATE_KEY_PEM` (newlines as `\n` in the value, or paste the whole block if your platform allows multiline).
- **Option B:** Store the key in a file (e.g. secret volume), set `KALSHI_PRIVATE_KEY_PATH` to that path.

The bot uses the key only to sign **read-only** orderbook requests; it does not place orders.

## Run locally

```bash
pip install -r requirements.txt
export TELEGRAM_BOT_TOKEN=...
export TELEGRAM_CHAT_ID=...
export KALSHI_API_KEY=482a59a5-81b4-45bc-81cd-1ddf1a91fd94
export KALSHI_PRIVATE_KEY_PEM="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
# Optional: export PAIRS_JSON=pairs.json
python pair_gap_alert_bot.py
```
