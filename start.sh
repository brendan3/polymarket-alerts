#!/bin/bash
# Railway start script for polymarket_v1_alerts.py
# Single-line command to avoid parsing issues

exec python3 polymarket_v1_alerts.py \
  --gamma-active \
  --broad-test \
  --pages 40 \
  --per-page 100 \
  --whale-threshold 20000 \
  --telegram
