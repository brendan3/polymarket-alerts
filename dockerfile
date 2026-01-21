FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

CMD ["python", "polymarket_v1_alerts.py", "--gamma-active", "--pages", "5", "--per-page", "100", "--window-min", "10", "--move-pct", "25", "--min-volume", "250000", "--min-liquidity", "100000", "--email"]
