#!/bin/bash

# 确保在运行前启动了服务器:
# python -m uvicorn quantide.web.apis.broker:app --reload

BASE_URL="http://localhost:8000/broker"

echo "=== 1. Listing Strategies ==="
curl -s "$BASE_URL/strategies"

echo -e "\n=== 2. Running Backtest ==="
curl -s -X POST "$BASE_URL/backtest/run" \
  -H "Content-Type: application/json" \
  -d '{
    "strategy_name": "DualMAStrategy",
    "config": {
        "symbol": "000001.SZ",
        "fast": 5,
        "slow": 10
    },
    "start_date": "2023-01-01",
    "end_date": "2023-06-01",
    "interval": "1d",
    "initial_cash": 1000000
  }'
