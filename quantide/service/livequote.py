import asyncio
import datetime
import json
import threading
import time
from collections import defaultdict, deque
from typing import Any

import polars as pl
import websockets
from loguru import logger

from quantide.config.settings import get_settings
from quantide.core.enums import Topics
from quantide.core.message import msg_hub
from quantide.core.scheduler import scheduler
from quantide.data.fetchers.registry import get_data_fetcher


class LiveQuote:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._mode: str | None = None
        self._is_running = False
        self._quotes: dict[str, dict[str, Any]] = {}
        self._limits: dict[str, dict[str, float]] = {}
        self._minute_bars: dict[str, deque[dict[str, Any]]] = defaultdict(lambda: deque(maxlen=480))
        self._daily_bars: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._ws_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self):
        if self._is_running:
            return
        self._mode = "gateway"
        self._is_running = True
        self._stop_event.clear()
        self._start_limit_schedule()
        self._ws_thread = threading.Thread(target=self._run_ws, daemon=True, name="livequote-ws")
        self._ws_thread.start()

    def stop(self):
        self._is_running = False
        self._stop_event.set()

    def _run_ws(self):
        asyncio.run(self._ws_loop())

    async def _ws_loop(self):
        while not self._stop_event.is_set():
            ws_url = self._build_ws_url()
            try:
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as ws:
                    while not self._stop_event.is_set():
                        raw = await ws.recv()
                        payload = self._parse_ws_payload(raw)
                        if not payload:
                            continue
                        self._cache_and_broadcast(payload)
            except Exception as e:
                logger.warning(f"gateway ws disconnected: {e}")
                await asyncio.sleep(2)

    def _build_ws_url(self) -> str:
        base_url = get_settings().gateway_base_url.rstrip("/")
        if base_url.startswith("https://"):
            return "wss://" + base_url[len("https://") :] + "/ws/quotes"
        if base_url.startswith("http://"):
            return "ws://" + base_url[len("http://") :] + "/ws/quotes"
        return f"ws://{base_url}/ws/quotes"

    def _parse_ws_payload(self, raw: str | bytes) -> dict[str, dict[str, Any]]:
        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            data = json.loads(raw)
        except Exception:
            return {}
        if not isinstance(data, dict):
            return {}
        symbol = str(data.get("symbol") or "")
        if not symbol:
            return {}
        ts = data.get("timestamp")
        ts_ms = int(float(ts or time.time()) * 1000)
        m1_raw = data.get("1m")
        d1_raw = data.get("1d")
        m1: dict[str, Any] = m1_raw if isinstance(m1_raw, dict) else {}
        d1: dict[str, Any] = d1_raw if isinstance(d1_raw, dict) else {}
        close_value = self._to_float(m1.get("close"), self._to_float(d1.get("close"), 0.0))
        quote = {
            "price": close_value,
            "lastPrice": close_value,
            "open": self._to_float(m1.get("open"), self._to_float(d1.get("open"), close_value)),
            "high": self._to_float(m1.get("high"), self._to_float(d1.get("high"), close_value)),
            "low": self._to_float(m1.get("low"), self._to_float(d1.get("low"), close_value)),
            "volume": self._to_float(m1.get("vol"), self._to_float(d1.get("vol"), 0.0)),
            "amount": self._to_float(m1.get("amount"), self._to_float(d1.get("amount"), 0.0)),
            "time": ts_ms,
        }
        minute_bar = {
            "asset": symbol,
            "frame": "1m",
            "dt": datetime.datetime.fromtimestamp(ts_ms / 1000),
            "open": quote["open"],
            "high": quote["high"],
            "low": quote["low"],
            "close": quote["price"],
            "volume": quote["volume"],
            "amount": quote["amount"],
        }
        daily_bar = {
            "asset": symbol,
            "frame": "1d",
            "dt": datetime.datetime.fromtimestamp(ts_ms / 1000).date(),
            "open": self._to_float(d1.get("open"), quote["open"]),
            "high": self._to_float(d1.get("high"), quote["high"]),
            "low": self._to_float(d1.get("low"), quote["low"]),
            "close": self._to_float(d1.get("close"), quote["price"]),
            "volume": self._to_float(d1.get("vol"), quote["volume"]),
            "amount": self._to_float(d1.get("amount"), quote["amount"]),
        }
        with self._lock:
            self._minute_bars[symbol].append(minute_bar)
            self._daily_bars[symbol] = daily_bar
        return {symbol: quote}

    def _start_limit_schedule(self):
        scheduler.add_job(
            self._refresh_limits,
            "cron",
            hour=9,
            minute=0,
            name="livequote.limit.refresh",
        )
        self._refresh_limits()

    def _refresh_limits(self, dt: datetime.date | None = None):
        dt = dt or datetime.date.today()
        try:
            df, _ = get_data_fetcher().fetch_limit_price(dt)
        except Exception as e:
            logger.warning(f"refresh limits failed: {e}")
            return
        if df is None or df.empty:
            return
        symbol_col = "asset" if "asset" in df.columns else "ts_code" if "ts_code" in df.columns else None
        if symbol_col is None:
            return
        up_col = "up_limit" if "up_limit" in df.columns else None
        down_col = "down_limit" if "down_limit" in df.columns else None
        if up_col is None:
            df["up_limit"] = 0.0
            up_col = "up_limit"
        if down_col is None:
            df["down_limit"] = 0.0
            down_col = "down_limit"
        data = {
            str(row[symbol_col]): {
                "up_limit": float(row[up_col] or 0),
                "down_limit": float(row[down_col] or 0),
            }
            for _, row in df.iterrows()
        }
        self._cache_limits_and_broadcast(data)

    def _cache_and_broadcast(self, data: dict[str, Any]):
        with self._lock:
            self._quotes.update(data)
        msg_hub.publish(Topics.QUOTES_ALL.value, data)

    def _cache_limits(self, data: dict[str, Any] | None):
        if not data:
            return
        with self._lock:
            self._limits.update(data)

    def _cache_limits_and_broadcast(self, data: dict[str, Any]):
        self._cache_limits(data)
        msg_hub.publish(Topics.STOCK_LIMIT.value, data)

    def get_quote(self, asset: str) -> dict[str, Any] | None:
        with self._lock:
            return self._quotes.get(asset)

    def get_price_limits(self, asset: str) -> tuple[float, float]:
        with self._lock:
            data = self._limits.get(asset)
        if data is None:
            return 0.0, 0.0
        return float(data.get("down_limit", 0)), float(data.get("up_limit", 0))

    def get_limit(self, asset: str) -> dict[str, float] | None:
        with self._lock:
            return self._limits.get(asset)

    def get_minute_bars(self, symbol: str) -> pl.DataFrame:
        with self._lock:
            data = list(self._minute_bars.get(symbol, []))
        if not data:
            return pl.DataFrame(schema={"asset": pl.Utf8, "frame": pl.Utf8, "dt": pl.Datetime, "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64, "volume": pl.Float64, "amount": pl.Float64})
        return pl.DataFrame(data)

    def get_daily_bar(self, symbol: str) -> dict[str, Any] | None:
        with self._lock:
            return self._daily_bars.get(symbol)

    @property
    def all_limits(self) -> dict[str, dict[str, float]]:
        with self._lock:
            return dict(self._limits)

    @property
    def all_quotes(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return dict(self._quotes)

    @property
    def all_minute_bars(self) -> pl.DataFrame:
        with self._lock:
            data: list[dict[str, Any]] = []
            for rows in self._minute_bars.values():
                data.extend(rows)
        if not data:
            return pl.DataFrame(schema={"asset": pl.Utf8, "frame": pl.Utf8, "dt": pl.Datetime, "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64, "volume": pl.Float64, "amount": pl.Float64})
        return pl.DataFrame(data)

    @property
    def all_daily_bars(self) -> pl.DataFrame:
        with self._lock:
            values = list(self._daily_bars.values())
        if not values:
            return pl.DataFrame(schema={"asset": pl.Utf8, "frame": pl.Utf8, "dt": pl.Date, "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64, "volume": pl.Float64, "amount": pl.Float64})
        return pl.DataFrame(values)

    @property
    def mode(self) -> str | None:
        return self._mode

    @property
    def is_running(self) -> bool:
        return self._is_running

    def _to_float(self, value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return default


live_quote = LiveQuote()
