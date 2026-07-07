#!/usr/bin/env python3
"""
Bot Momentum/ATR para Hyperliquid Testnet – EJECUCIÓN CRON CADA 30 MIN.
Versión institucional completa (10 mejoras integradas).
"""

import asyncio
import csv
import json
import os
import time
import logging
import random
import traceback
import fcntl
import glob
import subprocess
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple

import requests
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants
from eth_account import Account
from eth_account.signers.local import LocalAccount

# -------------------------------------------------------------------
# CONFIGURACIÓN DESDE VARIABLES DE ENTORNO
# -------------------------------------------------------------------
HL_PRIVATE_KEY = os.environ["HL_PRIVATE_KEY"]
HL_WALLET_ADDRESS = os.environ["HL_WALLET_ADDRESS"]
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

GIT_USER_NAME = os.environ.get("GIT_USER_NAME", "github-actions[bot]")
GIT_USER_EMAIL = os.environ.get("GIT_USER_EMAIL", "github-actions[bot]@users.noreply.github.com")
GIT_TOKEN = os.environ.get("GIT_TOKEN", "")

# -------------------------------------------------------------------
# CONFIGURACIÓN DEL BOT
# -------------------------------------------------------------------
SYMBOLS = ['BTC', 'ETH', 'SOL', 'ADA', 'ATOM', 'AVAX', 'DOGE']
LEVERAGE = 5
COMMISSION = 0.00045
SLIPPAGE = 0.0005
MAX_SLIPPAGE_ENTRY = 0.01       # 1% máximo de desviación en entrada
MAX_MARGIN_TOTAL = 0.30
MAX_POSITIONS = 6
MAX_POSITION_PERCENT = 0.15
MAX_SAME_DIRECTION = 4           # máximo de posiciones en la misma dirección
EQUITY_DD_STOP = 0.20

MOMENTUM_PERIOD = 90
SMA_TREND = 200
ATR_PERIOD = 14
ADX_PERIOD = 14

BEST_PARAMS = {
    'mult_atr_trailing': 5.0,
    'momentum_min': 0.04,
    'cooldown_velas': 3,
    'risk_per_trade': 0.05,
    'adx_min': 20,
    'take_profit_r': 2.0,
    'vol_filter_perc': 20,
    'breakeven_r': 1.5
}

BASE_URL = constants.TESTNET_API_URL
STATUS_UPDATE_INTERVAL = 14400
STOP_UPDATE_THRESHOLD_ATR = 0.05
ORDER_DISTANCE_THRESHOLD_ATR = 2.0
MAX_CONSECUTIVE_ERRORS = 10
LATENCY_WARNING_SECONDS = 3.0
API_HEALTH_LATENCY_LIMIT = 5.0
API_HEALTH_ERRORS_LIMIT = 3
ATR_MIN_PCT = 0.005
ATR_MAX_PCT = 0.25
SAFE_RESTART_DELAY = 5           # segundos de espera tras reinicio

BOT_STATE_FILE = "bot_state.json"
STATS_FILE = "stats.json"
CANDLES_CACHE_FILE = "candles_cache.json"
TRADES_CSV = "trades.csv"
ORDERS_CSV = "orders.csv"
SENT_ORDERS_FILE = "sent_orders.json"
LOCK_FILE = "/tmp/bot_cron.lock"
CLIENT_ORDER_PREFIX = "bot_inst_"
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# -------------------------------------------------------------------
# TELEGRAM
# -------------------------------------------------------------------
def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        logging.error(f"Error Telegram: {e}")

# -------------------------------------------------------------------
# HTTP CON REINTENTOS Y MEDICIÓN DE LATENCIA
# -------------------------------------------------------------------
class ApiLatencyTracker:
    def __init__(self):
        self.total_time = 0.0
        self.calls = 0

    def record(self, seconds: float):
        self.total_time += seconds
        self.calls += 1

    def avg(self):
        return self.total_time / max(self.calls, 1)

    def to_dict(self):
        return {'total_time': self.total_time, 'calls': self.calls, 'avg': self.avg()}

latency_tracker = ApiLatencyTracker()

def http_post_with_retry(url, json_payload, max_retries=5, timeout=30):
    for attempt in range(max_retries):
        start = time.monotonic()
        try:
            resp = requests.post(url, json=json_payload, timeout=timeout)
            elapsed = time.monotonic() - start
            latency_tracker.record(elapsed)
            if elapsed > LATENCY_WARNING_SECONDS:
                logging.warning(f"⏱️ Latencia alta: {elapsed:.1f}s en {url}")
            if resp.status_code == 429:
                retry_after = int(resp.headers.get('Retry-After', '1'))
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            elapsed = time.monotonic() - start
            latency_tracker.record(elapsed)
            if attempt == max_retries - 1:
                raise
            wait = (2 ** attempt) + random.uniform(0, 1)
            logging.warning(f"HTTP fallo (intento {attempt+1}): {e}. Reintentando en {wait:.1f}s...")
            time.sleep(wait)
    return None

# -------------------------------------------------------------------
# VELAS DIARIAS CERRADAS (DESCARGA PARALELA INICIAL)
# -------------------------------------------------------------------
def fetch_candles(coin: str, start_ms: int, end_ms: int, interval: str = "1d") -> list:
    all_candles = []
    cursor = start_ms
    interval_ms = 24 * 60 * 60 * 1000 if interval == "1d" else 60 * 60 * 1000
    while cursor < end_ms:
        window_end = min(cursor + 4000 * interval_ms, end_ms)
        payload = {
            "type": "candleSnapshot",
            "req": {"coin": coin, "interval": interval, "startTime": cursor, "endTime": window_end}
        }
        try:
            resp = http_post_with_retry(BASE_URL + "/info", payload)
            candles = resp.json()
            if candles:
                all_candles.extend(candles)
                cursor = candles[-1]['t'] + interval_ms
            else:
                cursor = window_end
        except Exception as e:
            logging.warning(f"Error descargando {coin}: {e}")
            cursor = window_end
        time.sleep(0.2)
    unique = {c['t']: c for c in all_candles}
    return sorted(unique.values(), key=lambda x: x['t'])

async def historical_candles_async(coin: str, days: int = 300) -> list:
    end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days)
    return fetch_candles(coin, int(start.timestamp() * 1000), int(end.timestamp() * 1000))

async def fetch_all_initial_candles(store: 'OHLCVStore'):
    tasks = [historical_candles_async(sym) for sym in SYMBOLS]
    results = await asyncio.gather(*tasks)
    for sym, candles in zip(SYMBOLS, results):
        for c in candles:
            store.add_candle(sym, c)
        print(f"  {sym}: {len(candles)} velas")

def last_closed_daily_candle(coin: str) -> Optional[dict]:
    now = datetime.now(timezone.utc)
    today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_ms = int(today_midnight.timestamp() * 1000)
    start_ms = end_ms - 24 * 60 * 60 * 1000
    candles = fetch_candles(coin, start_ms, end_ms)
    return candles[-1] if candles else None

# -------------------------------------------------------------------
# INDICADORES (WILDER SMOOTHING)
# -------------------------------------------------------------------
class OHLCVStore:
    def __init__(self, symbols):
        self.data = {s: {'time': [], 'open': [], 'high': [], 'low': [], 'close': [], 'volume': []} for s in symbols}
        self.indicators = {s: {} for s in symbols}
        self.last_candle_time = {s: 0 for s in symbols}

    def add_candle(self, symbol: str, candle: dict):
        d = self.data[symbol]
        if d['time'] and d['time'][-1] == candle['t']:
            return
        d['time'].append(candle['t'])
        d['open'].append(float(candle['o']))
        d['high'].append(float(candle['h']))
        d['low'].append(float(candle['l']))
        d['close'].append(float(candle['c']))
        d['volume'].append(float(candle['v']))
        self.last_candle_time[symbol] = candle['t']
        self._update_indicators(symbol)

    def _update_indicators(self, symbol: str):
        d = self.data[symbol]
        closes = d['close']
        highs = d['high']
        lows = d['low']
        vols = d['volume']
        n = len(closes)
        if n < SMA_TREND:
            self.indicators[symbol] = {}
            return

        sma200 = sum(closes[-SMA_TREND:]) / SMA_TREND
        if sma200 <= 0: sma200 = 1e-8

        tr = [0.0] * n
        for i in range(1, n):
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i - 1])
            lc = abs(lows[i] - closes[i - 1])
            tr[i] = max(hl, hc, lc)

        if n <= ATR_PERIOD:
            atr = sum(tr) / max(1, len(tr))
        else:
            atr = sum(tr[1:ATR_PERIOD + 1]) / ATR_PERIOD
            for i in range(ATR_PERIOD + 1, n):
                atr = (atr * (ATR_PERIOD - 1) + tr[i]) / ATR_PERIOD
        if atr <= 0: atr = 1e-8

        mom = 0.0
        if n > MOMENTUM_PERIOD:
            prev_close = closes[-1 - MOMENTUM_PERIOD]
            if prev_close > 0:
                mom = (closes[-1] / prev_close) - 1

        plus_dm = [0.0] * n
        minus_dm = [0.0] * n
        for i in range(1, n):
            up = highs[i] - highs[i - 1]
            down = lows[i - 1] - lows[i]
            if up > down and up > 0: plus_dm[i] = up
            if down > up and down > 0: minus_dm[i] = down

        dx_series = []
        _tr14 = 0.0
        _plus_dm14 = 0.0
        _minus_dm14 = 0.0
        for i in range(1, n):
            if i == ADX_PERIOD:
                _tr14 = sum(tr[1:ADX_PERIOD + 1]) / ADX_PERIOD
                _plus_dm14 = sum(plus_dm[1:ADX_PERIOD + 1]) / ADX_PERIOD
                _minus_dm14 = sum(minus_dm[1:ADX_PERIOD + 1]) / ADX_PERIOD
            elif i > ADX_PERIOD:
                _tr14 = (_tr14 * (ADX_PERIOD - 1) + tr[i]) / ADX_PERIOD
                _plus_dm14 = (_plus_dm14 * (ADX_PERIOD - 1) + plus_dm[i]) / ADX_PERIOD
                _minus_dm14 = (_minus_dm14 * (ADX_PERIOD - 1) + minus_dm[i]) / ADX_PERIOD
            if _tr14 > 0 and i >= ADX_PERIOD:
                sum_di = _plus_dm14 + _minus_dm14
                if sum_di == 0: continue
                _plus_di = 100 * _plus_dm14 / _tr14
                _minus_di = 100 * _minus_dm14 / _tr14
                _dx = abs(_plus_di - _minus_di) / sum_di * 100
                dx_series.append(_dx)

        if len(dx_series) >= ADX_PERIOD:
            adx = sum(dx_series[:ADX_PERIOD]) / ADX_PERIOD
            for i in range(ADX_PERIOD, len(dx_series)):
                adx = (adx * (ADX_PERIOD - 1) + dx_series[i]) / ADX_PERIOD
        elif len(dx_series) > 0:
            adx = sum(dx_series) / len(dx_series)
        else:
            adx = 0.0

        if n >= 90:
            recent_vols = vols[-90:]
            vol_now = vols[-1]
            rank = sum(1 for v in recent_vols if v <= vol_now)
            perc = rank / 90.0
        else:
            perc = 0.0

        self.indicators[symbol] = {
            'sma200': sma200,
            'atr': atr,
            'momentum': mom,
            'adx': adx,
            'vol_percentile': perc,
            'close': closes[-1],
            'open': d['open'][-1] if d['open'] else 0.0
        }

    def to_dict(self):
        return self.data

    @classmethod
    def from_dict(cls, data):
        store = cls([])
        store.data = data
        for sym in data:
            if data[sym]['close']:
                store._update_indicators(sym)
        return store

    def save_cache(self, filepath):
        try:
            with open(filepath, 'w') as f:
                json.dump(self.to_dict(), f)
        except Exception as e:
            logging.error(f"Error guardando caché de velas: {e}")

    @classmethod
    def load_cache(cls, filepath):
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)
                return cls.from_dict(data)
            except:
                pass
        return None

# -------------------------------------------------------------------
# ESTADÍSTICAS AVANZADAS (CON PERSISTENCIA DE LATENCIA Y ERRORES)
# -------------------------------------------------------------------
class StatsTracker:
    def __init__(self):
        self.trades = []
        self.total_pnl = 0.0
        self.gross_profit = 0.0
        self.gross_loss = 0.0
        self.wins = 0
        self.losses = 0
        self.max_drawdown = 0.0
        self.peak_balance = 0.0
        self.current_balance = 0.0
        self.last_trade_time = None
        self.durations = []
        self.latency_records = []
        self.consecutive_errors = 0
        self._load()

    def _load(self):
        if os.path.exists(STATS_FILE):
            try:
                with open(STATS_FILE, 'r') as f:
                    data = json.load(f)
                self.trades = data.get('trades', [])
                self.total_pnl = data.get('total_pnl', 0.0)
                self.gross_profit = data.get('gross_profit', 0.0)
                self.gross_loss = data.get('gross_loss', 0.0)
                self.wins = data.get('wins', 0)
                self.losses = data.get('losses', 0)
                self.max_drawdown = data.get('max_drawdown', 0.0)
                self.peak_balance = data.get('peak_balance', 0.0)
                self.current_balance = data.get('current_balance', 0.0)
                if data.get('last_trade_time'):
                    self.last_trade_time = datetime.fromisoformat(data['last_trade_time'])
                self.durations = data.get('durations', [])
                self.latency_records = data.get('latency_records', [])
                self.consecutive_errors = data.get('consecutive_errors', 0)
            except:
                pass

    def add_trade(self, symbol: str, side: int, sz: float, entry_px: float, exit_px: float,
                  pnl: float, reason: str, duration_sec: float = 0, indicators: dict = None):
        now = datetime.now(timezone.utc)
        trade = {
            'pnl': pnl, 'time': now.isoformat(), 'symbol': symbol,
            'side': 'LONG' if side == 1 else 'SHORT', 'size': sz,
            'entry': entry_px, 'exit': exit_px, 'reason': reason,
            'duration_sec': duration_sec
        }
        if indicators:
            trade.update(indicators)
        self.trades.append(trade)
        self.total_pnl += pnl
        if pnl > 0:
            self.gross_profit += pnl
            self.wins += 1
        elif pnl < 0:
            self.gross_loss += abs(pnl)
            self.losses += 1
        self.current_balance += pnl
        if self.current_balance > self.peak_balance:
            self.peak_balance = self.current_balance
        drawdown = self.peak_balance - self.current_balance
        if drawdown > self.max_drawdown:
            self.max_drawdown = drawdown
        self.last_trade_time = now
        if duration_sec > 0:
            self.durations.append(duration_sec)
        self._save()
        self._save_csv(trade)

    def record_latency(self, avg_latency: float):
        self.latency_records.append({'time': datetime.now(timezone.utc).isoformat(), 'latency': avg_latency})
        if len(self.latency_records) > 200:
            self.latency_records = self.latency_records[-200:]

    def update_errors(self, consecutive_errors: int):
        self.consecutive_errors = consecutive_errors

    def _save(self):
        try:
            with open(STATS_FILE, 'w') as f:
                json.dump({
                    'trades': self.trades[-500:],
                    'total_pnl': self.total_pnl,
                    'gross_profit': self.gross_profit,
                    'gross_loss': self.gross_loss,
                    'wins': self.wins,
                    'losses': self.losses,
                    'max_drawdown': self.max_drawdown,
                    'peak_balance': self.peak_balance,
                    'current_balance': self.current_balance,
                    'last_trade_time': self.last_trade_time.isoformat() if self.last_trade_time else None,
                    'durations': self.durations[-500:],
                    'latency_records': self.latency_records,
                    'consecutive_errors': self.consecutive_errors
                }, f, default=str)
        except Exception as e:
            logging.error(f"Error guardando stats: {e}")

    def _save_csv(self, trade: dict):
        file_exists = os.path.isfile(TRADES_CSV)
        try:
            with open(TRADES_CSV, 'a', newline='') as csvfile:
                fieldnames = ['time', 'symbol', 'side', 'size', 'entry', 'exit', 'pnl', 'reason',
                              'duration_sec', 'momentum', 'adx', 'atr', 'sma200', 'vol_percentile']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
                if not file_exists:
                    writer.writeheader()
                writer.writerow(trade)
        except Exception as e:
            logging.error(f"Error escribiendo CSV: {e}")

    def win_rate(self):
        total = self.wins + self.losses
        return self.wins / total if total > 0 else 0.0

    def profit_factor(self):
        return self.gross_profit / self.gross_loss if self.gross_loss > 0 else float('inf')

    def avg_win(self):
        return self.gross_profit / self.wins if self.wins > 0 else 0.0

    def avg_loss(self):
        return self.gross_loss / self.losses if self.losses > 0 else 0.0

    def payoff_ratio(self):
        return self.avg_win() / self.avg_loss() if self.avg_loss() > 0 else 0.0

    def expectancy(self):
        return (self.win_rate() * self.avg_win()) - ((1 - self.win_rate()) * self.avg_loss())

    def avg_trade_duration(self) -> float:
        if not self.durations:
            return 0.0
        return sum(self.durations) / len(self.durations)

    def sharpe(self):
        if len(self.trades) < 2:
            return 0.0
        pnls = [t['pnl'] for t in self.trades]
        mean_pnl = sum(pnls) / len(pnls)
        std_pnl = (sum((p - mean_pnl) ** 2 for p in pnls) / (len(pnls) - 1)) ** 0.5
        if std_pnl == 0:
            return 0.0
        return (mean_pnl / std_pnl) * ((48 * 365) ** 0.5)

    def sortino(self):
        if len(self.trades) < 2:
            return 0.0
        pnls = [t['pnl'] for t in self.trades]
        mean_pnl = sum(pnls) / len(pnls)
        downside = [min(0, p - 0) ** 2 for p in pnls]
        downside_std = (sum(downside) / len(downside)) ** 0.5
        if downside_std == 0:
            return 0.0
        return (mean_pnl / downside_std) * ((48 * 365) ** 0.5)

    def summary(self) -> str:
        return (f"PnL: ${self.total_pnl:,.2f} | Trades: {len(self.trades)} | "
                f"Win: {self.win_rate():.1%} | PF: {self.profit_factor():.2f} | "
                f"AvgWin: ${self.avg_win():.2f} | AvgLoss: ${self.avg_loss():.2f} | "
                f"Expect: ${self.expectancy():.2f} | Sharpe: {self.sharpe():.2f}")

# -------------------------------------------------------------------
# REGISTRO DE ÓRDENES (AUDITORÍA)
# -------------------------------------------------------------------
class OrderLogger:
    def __init__(self):
        self.file = ORDERS_CSV
        self._ensure_header()

    def _ensure_header(self):
        if not os.path.isfile(self.file):
            with open(self.file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['timestamp', 'type', 'symbol', 'side', 'orderId', 'clientOrderId',
                                 'status', 'price', 'size', 'filledSize'])

    def log(self, order_type: str, symbol: str, side: str, order_id: str, client_id: str,
            status: str, price: float, size: float, filled: float = 0.0):
        with open(self.file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([datetime.now(timezone.utc).isoformat(), order_type, symbol, side,
                             order_id, client_id, status, price, size, filled])

# -------------------------------------------------------------------
# GESTIÓN DE CLIENT ORDER ID
# -------------------------------------------------------------------
class OrderIdTracker:
    def __init__(self):
        self.ids = set()
        self._load()

    def _load(self):
        if os.path.exists(SENT_ORDERS_FILE):
            try:
                with open(SENT_ORDERS_FILE, 'r') as f:
                    self.ids = set(json.load(f))
            except:
                pass

    def add(self, client_order_id: str):
        self.ids.add(client_order_id)
        self._save()

    def exists(self, client_order_id: str) -> bool:
        return client_order_id in self.ids

    def _save(self):
        try:
            with open(SENT_ORDERS_FILE, 'w') as f:
                json.dump(list(self.ids), f)
        except Exception as e:
            logging.error(f"Error guardando sent_orders: {e}")

# -------------------------------------------------------------------
# FUNCIONES AUXILIARES PARA PRECIO REAL DEL FILL
# -------------------------------------------------------------------
def extract_fill_price_and_size(order_response: dict) -> Tuple[float, float]:
    try:
        if order_response.get('status') != 'ok':
            return 0.0, 0.0
        st = order_response['response']['data']['statuses'][0]
        filled = st.get('filled') or st.get('partialFill')
        if filled:
            avg_px = float(filled.get('avgPx', 0))
            total_sz = float(filled.get('totalSz', 0))
            return avg_px, total_sz
        if 'filled' in st:
            avg_px = float(st['filled'].get('avgPx', 0))
            total_sz = float(st['filled'].get('totalSz', 0))
            return avg_px, total_sz
    except:
        pass
    return 0.0, 0.0

# -------------------------------------------------------------------
# BOT INSTITUCIONAL COMPLETO (con las 10 mejoras)
# -------------------------------------------------------------------
class LiveBotCron:
    def __init__(self, account, info, exchange, store, stats, order_tracker, order_logger):
        self.account = account
        self.info = info
        self.exchange = exchange
        self.store = store
        self.stats = stats
        self.order_tracker = order_tracker
        self.order_logger = order_logger
        self.address = HL_WALLET_ADDRESS

        meta = self.info.meta()
        self.sz_decimals = {a['name']: a.get('szDecimals', 4) for a in meta['universe']}
        self.max_sizes   = {a['name']: float(a.get('maxSz', '1000000000')) for a in meta['universe']}
        self.min_sizes   = {a['name']: float(a.get('minSz', '0.0001')) for a in meta['universe']}

        self.positions = {s: {
            'side': 0, 'entrada': 0.0, 'nocional': 0.0, 'margin': 0.0,
            'trail': 0.0, 'highest_price': 0.0, 'lowest_price': 0.0,
            'cooldown_until_vela': None, 'last_entry_vela': None,
            'riesgo_inicial': 0.0, 'breakeven_activated': False,
            'tp_taken': False, 'orig_sz': 0.0, 'current_sz': 0.0,
            'stop_oid': None, 'last_stop_price': 0.0,
            'entry_time': None, 'entry_indicators': {}
        } for s in SYMBOLS}

        self.pending_orders = {}
        self.start_time = datetime.now(timezone.utc)
        self.peak_equity = 0.0
        self.api_healthy = True
        self.last_status_update = datetime.min.replace(tzinfo=timezone.utc)
        self.last_heartbeat = datetime.min.replace(tzinfo=timezone.utc)
        self.consecutive_errors = 0
        self.bot_state = {}
        self._load_bot_state()
        self.first_run = not os.path.exists(BOT_STATE_FILE)

    # ---------- Persistencia ----------
    def _load_bot_state(self):
        if os.path.exists(BOT_STATE_FILE):
            try:
                with open(BOT_STATE_FILE, 'r') as f:
                    self.bot_state = json.load(f)
                if 'last_status_update' in self.bot_state:
                    self.last_status_update = datetime.fromisoformat(self.bot_state['last_status_update'])
                    if self.last_status_update.tzinfo is None:
                        self.last_status_update = self.last_status_update.replace(tzinfo=timezone.utc)
                if 'last_heartbeat' in self.bot_state:
                    self.last_heartbeat = datetime.fromisoformat(self.bot_state['last_heartbeat'])
                self.consecutive_errors = self.bot_state.get('consecutive_errors', 0)
                self.pending_orders = self.bot_state.get('pending_orders', {})
                self.peak_equity = self.bot_state.get('peak_equity', 0.0)
                logging.info("Estado cargado.")
            except Exception as e:
                logging.error(f"Error cargando estado: {e}")
                self.bot_state = {}

    def _save_bot_state(self):
        state = {
            'last_status_update': self.last_status_update.isoformat(),
            'last_heartbeat': self.last_heartbeat.isoformat(),
            'consecutive_errors': self.consecutive_errors,
            'pending_orders': self.pending_orders,
            'peak_equity': self.peak_equity,
            'positions': {}
        }
        for sym, pos in self.positions.items():
            if pos['side'] != 0 or pos['cooldown_until_vela'] is not None:
                state['positions'][sym] = {
                    'trail': pos['trail'],
                    'highest_price': pos['highest_price'],
                    'lowest_price': pos['lowest_price'],
                    'breakeven_activated': pos['breakeven_activated'],
                    'tp_taken': pos['tp_taken'],
                    'orig_sz': pos['orig_sz'],
                    'current_sz': pos['current_sz'],
                    'riesgo_inicial': pos['riesgo_inicial'],
                    'cooldown_until_vela': pos['cooldown_until_vela'],
                    'last_entry_vela': pos['last_entry_vela'],
                    'side': pos['side'],
                    'entrada': pos['entrada'],
                    'nocional': pos['nocional'],
                    'stop_oid': pos['stop_oid'],
                    'last_stop_price': pos['last_stop_price'],
                    'entry_time': pos['entry_time'].isoformat() if pos['entry_time'] else None
                }
        self.bot_state = state
        try:
            with open(BOT_STATE_FILE, 'w') as f:
                json.dump(state, f, default=str)
        except Exception as e:
            logging.error(f"Error guardando estado: {e}")

    # ---------- Circuit Breaker ----------
    def check_circuit_breaker(self):
        if self.consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            msg = "🚨 Circuit Breaker activado: demasiados errores consecutivos. Bot detenido."
            logging.critical(msg)
            send_telegram(msg)
            raise RuntimeError("Circuit breaker activado.")

    def record_error(self):
        self.consecutive_errors += 1
        self.check_circuit_breaker()

    def reset_errors(self):
        self.consecutive_errors = 0

    # ---------- Comprobación de conexión ----------
    def check_connection(self) -> bool:
        try:
            self.info.user_state(self.address)
            return True
        except Exception as e:
            logging.error(f"Fallo conexión: {e}")
            return False

    def check_server_time(self):
        try:
            resp = requests.post(BASE_URL + "/info", json={"type": "meta"}, timeout=10)
            server_time = resp.json().get('serverTime', 0) / 1000
            local_time = datetime.now(timezone.utc).timestamp()
            diff = abs(server_time - local_time)
            if diff > 30:
                msg = f"⏰ Diferencia horaria con el servidor: {diff:.1f}s"
                logging.warning(msg)
                send_telegram(msg)
        except:
            pass

    # ---------- Utilidades ----------
    def round_sz(self, symbol: str, sz: float) -> float:
        decimals = self.sz_decimals.get(symbol, 4)
        return max(round(sz, decimals), 0.0)

    def clamp_sz(self, symbol: str, desired_sz: float) -> float:
        rounded = self.round_sz(symbol, desired_sz)
        min_sz = self.min_sizes.get(symbol, 0.0)
        max_sz = self.max_sizes.get(symbol, float('inf'))
        capped = min(max(rounded, min_sz), max_sz)
        return self.round_sz(symbol, capped) if capped >= min_sz else 0.0

    def round_px(self, symbol: str, px: float) -> float:
        decimals = max(6 - self.sz_decimals.get(symbol, 4), 0)
        px_5sig = float(f"{px:.5g}")
        return round(px_5sig, decimals)

    def set_leverage_if_needed(self):
        try:
            user_state = self.info.user_state(self.address)
            current_lev = {}
            for ap in user_state.get('assetPositions', []):
                pos = ap.get('position', {})
                sym = pos.get('coin')
                if sym:
                    current_lev[sym] = int(pos.get('leverage', '1'))
            for sym in SYMBOLS:
                if current_lev.get(sym, 0) != LEVERAGE:
                    self.exchange.update_leverage(LEVERAGE, sym, is_cross=True)
                    logging.info(f"Leverage {LEVERAGE}x en {sym}")
        except Exception as e:
            logging.error(f"Error leverage: {e}")
            self.record_error()

    # ---------- Sincronización completa (con mejoras 1,2,3,8) ----------
    async def full_sync(self):
        logging.info("--- Sincronización completa ---")
        if not self.check_connection():
            return

        try:
            user_state = self.info.user_state(self.address)
            open_orders = self.info.open_orders(self.address)
        except Exception as e:
            logging.error(f"Error en full_sync: {e}")
            self.record_error()
            return

        try:
            equity = float(user_state['crossMarginSummary']['accountValue'])
            if equity > self.peak_equity:
                self.peak_equity = equity
        except:
            pass

        exchange_positions = {}
        for ap in user_state.get('assetPositions', []):
            p = ap['position']
            sym = p['coin']
            if sym not in self.positions:
                continue
            szi = float(p['szi'])
            if szi == 0: continue
            entry = float(p['entryPx'])
            side = 1 if szi > 0 else -1
            exchange_positions[sym] = {
                'side': side, 'entry': entry,
                'szi': abs(szi), 'nocional': abs(szi) * entry
            }

        for sym in SYMBOLS:
            saved = self.bot_state.get('positions', {}).get(sym, {})
            pos = self.positions[sym]

            if sym in exchange_positions:
                rp = exchange_positions[sym]
                pos['side'] = rp['side']
                pos['entrada'] = rp['entry']
                pos['nocional'] = rp['nocional']
                pos['margin'] = rp['nocional'] / LEVERAGE
                pos['current_sz'] = rp['szi']

                atr_now = self.store.indicators.get(sym, {}).get('atr', 0.01)
                if atr_now <= 0: atr_now = 0.01
                default_trail = rp['entry'] - rp['side'] * (BEST_PARAMS['mult_atr_trailing'] * atr_now)

                stop_order = None
                for o in open_orders:
                    if (o.get('coin') == sym and 'triggerPx' in o and
                        o.get('clientOrderId', '').startswith(CLIENT_ORDER_PREFIX + "stop")):
                        stop_order = o
                        break
                if stop_order:
                    pos['stop_oid'] = stop_order['oid']
                    old_trigger = float(stop_order['triggerPx'])
                    pos['last_stop_price'] = old_trigger
                    # Mejora 3: verificar y corregir stop si difiere
                    if abs(old_trigger - pos.get('trail', default_trail)) > atr_now * STOP_UPDATE_THRESHOLD_ATR:
                        logging.info(f"Stop {sym} desactualizado, corrigiendo...")
                        await self.update_stop_order(sym, pos.get('trail', default_trail), force=True)
                    else:
                        pos['trail'] = old_trigger
                else:
                    pos['stop_oid'] = None
                    pos['last_stop_price'] = 0.0
                    pos['trail'] = default_trail

                if saved:
                    pos['highest_price'] = saved.get('highest_price', rp['entry'])
                    pos['lowest_price'] = saved.get('lowest_price', rp['entry'])
                    pos['breakeven_activated'] = saved.get('breakeven_activated', False)
                    pos['tp_taken'] = saved.get('tp_taken', False)
                    pos['orig_sz'] = saved.get('orig_sz', rp['szi'])
                    pos['riesgo_inicial'] = saved.get('riesgo_inicial', BEST_PARAMS['mult_atr_trailing'] * atr_now)
                    pos['cooldown_until_vela'] = saved.get('cooldown_until_vela')
                    pos['last_entry_vela'] = saved.get('last_entry_vela')
                    pos['entry_time'] = datetime.fromisoformat(saved['entry_time']) if saved.get('entry_time') else None
                    # Mejora 8: reconciliar TP parciales/cambios manuales no registrados
                    if pos['orig_sz'] > 0 and pos['current_sz'] < pos['orig_sz'] * 0.9:
                        if not pos['tp_taken']:
                            pos['tp_taken'] = True
                            pos['breakeven_activated'] = True
                            pos['trail'] = pos['entrada']
                            logging.info(f"{sym}: TP parcial no registrado, reconciliando.")
                else:
                    pos['highest_price'] = rp['entry']
                    pos['lowest_price'] = rp['entry']
                    pos['breakeven_activated'] = False
                    pos['tp_taken'] = False
                    pos['orig_sz'] = rp['szi']
                    pos['riesgo_inicial'] = BEST_PARAMS['mult_atr_trailing'] * atr_now
                    pos['cooldown_until_vela'] = None
                    pos['last_entry_vela'] = None
                    pos['entry_time'] = None
            else:
                if saved.get('cooldown_until_vela'):
                    pos['cooldown_until_vela'] = saved['cooldown_until_vela']
                else:
                    pos['side'] = 0
                    pos.update({k: 0.0 for k in ['entrada','nocional','margin','trail','highest_price','lowest_price','riesgo_inicial','current_sz','last_stop_price']})
                    pos['breakeven_activated'] = False
                    pos['tp_taken'] = False
                    pos['orig_sz'] = 0.0
                    pos['stop_oid'] = None
                    pos['entry_time'] = None

        # Mejora 2: reconciliación de órdenes huérfanas
        self.pending_orders = {}
        for o in open_orders:
            if not o.get('clientOrderId', '').startswith(CLIENT_ORDER_PREFIX):
                continue
            if 'triggerPx' in o:
                continue
            sym = o.get('coin')
            if sym not in self.positions:
                continue
            side = 1 if o.get('side') == 'B' else -1
            limit_px = float(o.get('limitPx', 0))
            remaining = float(o.get('remainingSz', o.get('origSz', 0)))
            oid = o.get('oid')
            self.pending_orders[sym] = {
                'oid': oid, 'side': side, 'limit_price': limit_px,
                'nocional': limit_px * remaining, 'margin': (limit_px * remaining) / LEVERAGE,
                'sz': remaining, 'dist': 0, 'riesgo_inicial': 0
            }
            logging.info(f"Orden huérfana {sym} recuperada.")

        # Añadir última vela
        for sym in SYMBOLS:
            c = last_closed_daily_candle(sym)
            if c and c['t'] > self.store.last_candle_time.get(sym, 0):
                self.store.add_candle(sym, c)

        send_telegram("🔄 Sincronización completada.")
        await self.repair_orphan_stops()

    async def repair_orphan_stops(self):
        for sym in SYMBOLS:
            pos = self.positions[sym]
            if pos['side'] != 0 and pos['stop_oid'] is None:
                await self.update_stop_order(sym, pos['trail'], force=True)
                send_telegram(f"🛑 Stop {sym} recreado.")
            elif pos['side'] == 0 and pos['stop_oid'] is not None:
                try: self.exchange.cancel(sym, pos['stop_oid'])
                except: pass
                pos['stop_oid'] = None

    # ---------- Actualización de stops ----------
    async def update_stop_order(self, sym: str, trail_px: float, force: bool = False):
        pos = self.positions[sym]
        if pos['side'] == 0 or pos['current_sz'] <= 0:
            return
        side = pos['side']
        stop_px = self.round_px(sym, trail_px)
        atr = self.store.indicators.get(sym, {}).get('atr', 0.01)
        if atr <= 0: atr = 1e-8

        if not force and pos['last_stop_price']:
            if side == 1 and stop_px <= pos['last_stop_price']: return
            if side == -1 and stop_px >= pos['last_stop_price']: return
            if abs(stop_px - pos['last_stop_price']) < atr * STOP_UPDATE_THRESHOLD_ATR: return

        try:
            user_state = self.info.user_state(self.address)
            account_val = float(user_state['crossMarginSummary']['accountValue'])
            total_margin = float(user_state['crossMarginSummary']['totalMarginUsed'])
            if total_margin >= account_val * 0.99:
                logging.warning("Sin margen suficiente para colocar stop.")
                return
        except:
            pass

        client_id = f"{CLIENT_ORDER_PREFIX}stop_{sym}_{int(time.time()*1000)}"
        if self.order_tracker.exists(client_id):
            return
        self.order_tracker.add(client_id)

        try:
            resp = self.exchange.order(
                name=sym, is_buy=(side == -1), sz=pos['current_sz'],
                limit_px=stop_px, order_type={"trigger": {"triggerPx": stop_px, "isMarket": True}},
                reduce_only=True, clientOrderId=client_id
            )
            if resp['status'] == 'ok':
                st = resp['response']['data']['statuses'][0]
                if 'resting' in st:
                    new_oid = st['resting']['oid']
                    old_oid = pos['stop_oid']
                    if old_oid:
                        try: self.exchange.cancel(sym, old_oid)
                        except: pass
                    pos['stop_oid'] = new_oid
                    pos['last_stop_price'] = stop_px
                    self.order_logger.log('STOP', sym, 'SELL' if side==1 else 'BUY', new_oid, client_id,
                                          'NEW', stop_px, pos['current_sz'])
            else:
                send_telegram(f"⚠️ Error colocando stop {sym}: {resp}")
        except Exception as e:
            logging.error(f"Error colocando stop {sym}: {e}")
            self.record_error()

    # ---------- Mercado ----------
    async def all_mids(self) -> Dict[str, float]:
        return self.info.all_mids()

    # ---------- Órdenes límite (mejora 1: detección de fills parciales) ----------
    async def check_and_clean_orders(self):
        prices = await self.all_mids()
        for sym, o in list(self.pending_orders.items()):
            px = float(prices.get(sym, 0))
            atr = self.store.indicators.get(sym, {}).get('atr', 0.01)
            if px > 0 and atr > 0 and abs(o['limit_price'] - px) > ORDER_DISTANCE_THRESHOLD_ATR * atr:
                try: self.exchange.cancel(sym, o['oid'])
                except: pass
                del self.pending_orders[sym]
                send_telegram(f"🗑️ Orden {sym} cancelada (precio lejos).")
                continue
            try:
                status = self.info.query_order_by_oid(self.address, o['oid'])
                order_status = status.get('order', {}).get('status')
                order_info = status.get('order', {}).get('order', {})
                if order_status == 'filled':
                    # Mejora 4: comprobar slippage excesivo
                    fill_px = float(order_info.get('limitPx', o['limit_price']))
                    if abs(fill_px / o['limit_price'] - 1) > MAX_SLIPPAGE_ENTRY:
                        logging.warning(f"Entrada {sym} cancelada por slippage excesivo: {fill_px} vs {o['limit_price']}")
                        # Si ya se ejecutó, cerrar inmediatamente
                        if self.positions[sym]['side'] != 0:
                            await self.close_position(sym, "Slippage excesivo", fill_px)
                        del self.pending_orders[sym]
                        continue
                    await self._refresh_position_from_exchange(sym)
                    del self.pending_orders[sym]
                elif order_status == 'open':
                    remaining = float(order_info.get('remainingSz', o['sz']))
                    if remaining < o['sz']:
                        o['sz'] = remaining
                        o['nocional'] = o['limit_price'] * remaining
                        o['margin'] = o['nocional'] / LEVERAGE
                        # Mejora 1: verificar si ya existe posición real (fill parcial)
                        user_state = self.info.user_state(self.address)
                        for ap in user_state.get('assetPositions', []):
                            if ap['position']['coin'] == sym and float(ap['position']['szi']) != 0:
                                await self._refresh_position_from_exchange(sym)
                                del self.pending_orders[sym]
                                break
                elif order_status in ('canceled', 'rejected'):
                    del self.pending_orders[sym]
            except Exception as e:
                logging.error(f"Error check_pending {sym}: {e}")

    async def _refresh_position_from_exchange(self, sym: str):
        try:
            state = self.info.user_state(self.address)
            for ap in state.get('assetPositions', []):
                p = ap['position']
                if p['coin'] != sym: continue
                szi = float(p['szi'])
                if szi == 0:
                    self.positions[sym].update(side=0, entrada=0.0, nocional=0.0, margin=0.0,
                                               trail=0.0, highest_price=0.0, lowest_price=0.0,
                                               riesgo_inicial=0.0, breakeven_activated=False,
                                               tp_taken=False, orig_sz=0.0, current_sz=0.0,
                                               stop_oid=None, last_stop_price=0.0, entry_time=None)
                else:
                    self.positions[sym]['current_sz'] = abs(szi)
                    self.positions[sym]['nocional'] = abs(szi) * float(p['entryPx'])
                    self.positions[sym]['side'] = 1 if szi > 0 else -1
                return
        except Exception as e:
            logging.error(f"Error refresh {sym}: {e}")
            self.record_error()

    # ---------- Evaluar señales (mejora 5: control de exposición direccional) ----------
    async def evaluate_signals(self):
        if not self.api_healthy:
            logging.info("API no saludable, omitiendo nuevas entradas.")
            return

        try:
            user = self.info.user_state(self.address)
            account_val = float(user['crossMarginSummary']['accountValue'])
            margin_used = float(user['crossMarginSummary']['totalMarginUsed'])
        except:
            return

        for sym in SYMBOLS:
            if self.positions[sym]['side'] != 0 or sym in self.pending_orders:
                continue
            if self.positions[sym]['cooldown_until_vela'] and self.positions[sym]['last_entry_vela']:
                last_vela = self.store.last_candle_time.get(sym, 0)
                if last_vela <= self.positions[sym]['last_entry_vela'] + BEST_PARAMS['cooldown_velas'] * 86400000:
                    continue

            ind = self.store.indicators.get(sym, {})
            if not ind:
                continue

            close_ = ind['close']; open_ = ind['open']
            s200 = ind['sma200']; atr_val = ind['atr']
            mom = ind['momentum']; adx_val = ind['adx']
            vol_perc = ind['vol_percentile']

            if any(v <= 0 for v in [close_, open_, s200, atr_val]):
                continue
            if atr_val < close_ * ATR_MIN_PCT or atr_val > close_ * ATR_MAX_PCT:
                logging.info(f"{sym} ATR fuera de rango ({atr_val/close_:.2%}). Ignorando.")
                continue

            trend_up = close_ > s200 and adx_val > BEST_PARAMS['adx_min']
            trend_down = close_ < s200 and adx_val > BEST_PARAMS['adx_min']
            mom_up = mom > BEST_PARAMS['momentum_min']
            mom_down = mom < -BEST_PARAMS['momentum_min']
            vol_ok = vol_perc > (BEST_PARAMS['vol_filter_perc']/100)
            signal_valid = ((trend_up and mom_up) or (trend_down and mom_down)) and vol_ok

            dist = BEST_PARAMS['mult_atr_trailing'] * atr_val
            if not signal_valid or dist <= 0:
                continue

            side = 1 if (trend_up and mom_up) else -1

            # Mejora 5: control de exposición por dirección
            long_count = sum(1 for p in self.positions.values() if p['side'] == 1)
            short_count = sum(1 for p in self.positions.values() if p['side'] == -1)
            if side == 1 and long_count >= MAX_SAME_DIRECTION:
                continue
            if side == -1 and short_count >= MAX_SAME_DIRECTION:
                continue

            limit_px = self.round_px(sym, open_ * (1+SLIPPAGE) if side==1 else open_ * (1-SLIPPAGE))
            if limit_px <= 0: continue

            if sum(1 for p in self.positions.values() if p['side']!=0) >= MAX_POSITIONS:
                continue

            free_margin = max(0, account_val*MAX_MARGIN_TOTAL - margin_used)
            risk_usd = account_val * BEST_PARAMS['risk_per_trade']
            max_risk_noc = risk_usd / (dist/limit_px) if limit_px>0 else 0
            max_margin_noc = free_margin * LEVERAGE
            max_pos_noc = account_val * MAX_POSITION_PERCENT * LEVERAGE
            nocional = min(max_risk_noc, max_margin_noc, max_pos_noc)
            sz = self.clamp_sz(sym, nocional/limit_px)
            if sz <= 0: continue
            nocional = sz * limit_px
            margin = nocional/LEVERAGE
            if margin < 10: continue

            duplicate = False
            for o in self.info.open_orders(self.address):
                if o.get('coin') == sym and o.get('side') == ('B' if side==1 else 'A') and 'triggerPx' not in o:
                    duplicate = True
                    break
            if duplicate: continue

            client_id = f"{CLIENT_ORDER_PREFIX}entry_{sym}_{int(time.time()*1000)}"
            if self.order_tracker.exists(client_id): continue
            self.order_tracker.add(client_id)

            try:
                order = self.exchange.order(
                    name=sym, is_buy=(side==1), sz=sz, limit_px=limit_px,
                    order_type={"limit":{"tif":"Gtc"}}, reduce_only=False,
                    clientOrderId=client_id
                )
                if order['status'] == 'ok':
                    st = order['response']['data']['statuses'][0]
                    if 'resting' in st:
                        oid = st['resting']['oid']
                        self.pending_orders[sym] = {
                            'oid': oid, 'side': side, 'limit_price': limit_px,
                            'nocional': nocional, 'margin': margin, 'sz': sz,
                            'dist': dist, 'riesgo_inicial': dist
                        }
                        self.order_logger.log('LIMIT', sym, 'BUY' if side==1 else 'SELL', oid, client_id,
                                              'NEW', limit_px, sz)
                        send_telegram(f"📊 ORDEN LÍMITE {sym} {'LONG' if side==1 else 'SHORT'} | Sz:{sz:.4f} @ {limit_px:.4f}")
                    elif 'filled' in st:
                        fill_px = float(st['filled']['avgPx'])
                        # Mejora 4: control de slippage en entrada inmediata
                        if abs(fill_px / limit_px - 1) > MAX_SLIPPAGE_ENTRY:
                            logging.warning(f"Entrada inmediata {sym} cancelada por slippage: {fill_px}")
                            continue
                        fill_sz = float(st['filled']['totalSz'])
                        self.positions[sym].update(
                            side=side, entrada=fill_px, nocional=fill_px*fill_sz,
                            margin=fill_px*fill_sz/LEVERAGE, highest_price=fill_px, lowest_price=fill_px,
                            trail=fill_px - side*dist, riesgo_inicial=dist,
                            breakeven_activated=False, tp_taken=False, orig_sz=fill_sz,
                            current_sz=fill_sz, last_entry_vela=self.store.last_candle_time.get(sym),
                            cooldown_until_vela=None, entry_time=datetime.now(timezone.utc),
                            entry_indicators=ind  # Mejora 9: guardar indicadores de entrada
                        )
                        await self.update_stop_order(sym, self.positions[sym]['trail'])
                        send_telegram(f"✅ ENTRADA INMEDIATA {sym}")
                else:
                    send_telegram(f"⚠️ Orden {sym} rechazada: {order}")
            except Exception as e:
                logging.error(f"Error señal {sym}: {e}")
                self.record_error()

    # ---------- Gestión de salidas (con equity stop ya integrado) ----------
    async def manage_exits(self):
        try:
            user = self.info.user_state(self.address)
            equity = float(user['crossMarginSummary']['accountValue'])
            if equity > self.peak_equity:
                self.peak_equity = equity
            if self.peak_equity > 0 and equity < self.peak_equity * (1 - EQUITY_DD_STOP):
                msg = f"🛑 Equity Stop: capital ${equity:,.2f} < {1-EQUITY_DD_STOP:.0%} del máximo ${self.peak_equity:,.2f}. Cerrando todo."
                logging.warning(msg)
                send_telegram(msg)
                prices = await self.all_mids()
                for sym in SYMBOLS:
                    if self.positions[sym]['side'] != 0:
                        px = float(prices.get(sym, 0))
                        if px > 0:
                            await self.close_position(sym, "Equity Stop", px)
                raise RuntimeError("Equity Stop ejecutado.")
        except RuntimeError:
            raise
        except Exception as e:
            logging.error(f"Error en equity stop: {e}")

        prices = await self.all_mids()
        for sym, pos in self.positions.items():
            if pos['side'] == 0 or pos['current_sz'] <= 0:
                continue
            px = float(prices.get(sym, 0))
            if px <= 0: continue

            side = pos['side']; entry = pos['entrada']
            atr = self.store.indicators.get(sym, {}).get('atr', 0.01)
            if atr <= 0: continue
            riesgo = pos['riesgo_inicial']

            if side == 1:
                if px > pos['highest_price']: pos['highest_price'] = px
                new_trail = pos['highest_price'] - BEST_PARAMS['mult_atr_trailing']*atr
                if new_trail > pos['trail']: pos['trail'] = new_trail
            else:
                if px < pos['lowest_price']: pos['lowest_price'] = px
                new_trail = pos['lowest_price'] + BEST_PARAMS['mult_atr_trailing']*atr
                if new_trail < pos['trail'] or pos['trail'] == 0: pos['trail'] = new_trail

            if not pos['breakeven_activated'] and riesgo > 0:
                if (side==1 and px >= entry + BEST_PARAMS['breakeven_r']*riesgo) or \
                   (side==-1 and px <= entry - BEST_PARAMS['breakeven_r']*riesgo):
                    pos['trail'] = entry
                    pos['breakeven_activated'] = True
                    send_telegram(f"🔒 Breakeven activado en {sym}")

            await self.update_stop_order(sym, pos['trail'])

            if not pos['tp_taken']:
                tp_px = entry + side * BEST_PARAMS['take_profit_r'] * riesgo
                if (side==1 and px >= tp_px) or (side==-1 and px <= tp_px):
                    half_sz = self.clamp_sz(sym, min(pos['current_sz']*0.5, pos['current_sz']*0.999))
                    if half_sz > 0:
                        exit_px = self.round_px(sym, px*(1-SLIPPAGE) if side==1 else px*(1+SLIPPAGE))
                        if exit_px <= 0: continue
                        client_id = f"{CLIENT_ORDER_PREFIX}tp_{sym}_{int(time.time()*1000)}"
                        if self.order_tracker.exists(client_id): continue
                        self.order_tracker.add(client_id)
                        try:
                            resp = self.exchange.order(
                                name=sym, is_buy=(side==-1), sz=half_sz,
                                limit_px=exit_px, order_type={"limit":{"tif":"Ioc"}}, reduce_only=True,
                                clientOrderId=client_id
                            )
                            real_px, filled_sz = extract_fill_price_and_size(resp)
                            if filled_sz > 0:
                                if real_px == 0.0: real_px = exit_px
                                if side == 1:
                                    pnl = (real_px - entry) * filled_sz
                                else:
                                    pnl = (entry - real_px) * filled_sz
                                comision = (entry * filled_sz + real_px * filled_sz) * COMMISSION
                                pnl -= comision
                                duration = (datetime.now(timezone.utc) - pos['entry_time']).total_seconds() if pos['entry_time'] else 0
                                self.stats.add_trade(sym, side, filled_sz, entry, real_px, pnl, "TP parcial",
                                                     duration, pos.get('entry_indicators'))
                                pos['current_sz'] -= filled_sz
                                pos['tp_taken'] = True
                                pos['breakeven_activated'] = True
                                pos['trail'] = entry
                                await self._refresh_position_from_exchange(sym)
                                send_telegram(f"🏷️ TP parcial {sym}: +{pnl:.2f} USDC")
                        except Exception as e:
                            logging.error(f"Error TP parcial {sym}: {e}")
                            self.record_error()

            if (side==1 and px <= pos['trail']) or (side==-1 and px >= pos['trail']):
                await self.close_position(sym, "Trailing stop", px)
                continue

            ind = self.store.indicators.get(sym, {})
            if ind:
                close_ = ind['close']; s200 = ind['sma200']; adx = ind['adx']
                if close_ <= 0 or s200 <= 0 or adx < 0: continue
                trend_up = close_ > s200 and adx > BEST_PARAMS['adx_min']
                trend_down = close_ < s200 and adx > BEST_PARAMS['adx_min']
                if (side==1 and not trend_up) or (side==-1 and not trend_down):
                    await self.close_position(sym, "Cambio tendencia", px)

    async def close_position(self, sym: str, reason: str, px: float):
        pos = self.positions[sym]
        if pos['side'] == 0 or pos['current_sz'] <= 0:
            return
        try:
            state = self.info.user_state(self.address)
            real_sz = 0.0
            for ap in state['assetPositions']:
                if ap['position']['coin'] == sym:
                    real_sz = abs(float(ap['position']['szi']))
            sz = self.clamp_sz(sym, real_sz)
        except:
            sz = self.clamp_sz(sym, pos['current_sz'])
        if sz <= 0: return

        side = pos['side']
        exit_px = self.round_px(sym, px*(1-SLIPPAGE) if side==1 else px*(1+SLIPPAGE))
        if exit_px <= 0: return
        client_id = f"{CLIENT_ORDER_PREFIX}exit_{sym}_{int(time.time()*1000)}"
        if self.order_tracker.exists(client_id): return
        self.order_tracker.add(client_id)
        try:
            resp = self.exchange.order(
                name=sym, is_buy=(side==-1), sz=sz,
                limit_px=exit_px, order_type={"limit":{"tif":"Ioc"}}, reduce_only=True,
                clientOrderId=client_id
            )
            real_px, filled_sz = extract_fill_price_and_size(resp)
            if filled_sz > 0:
                if real_px == 0.0: real_px = exit_px
                if side == 1:
                    pnl = (real_px - pos['entrada']) * filled_sz
                else:
                    pnl = (pos['entrada'] - real_px) * filled_sz
                comision = (pos['entrada'] * filled_sz + real_px * filled_sz) * COMMISSION
                pnl -= comision
                duration = (datetime.now(timezone.utc) - pos['entry_time']).total_seconds() if pos['entry_time'] else 0
                self.stats.add_trade(sym, side, filled_sz, pos['entrada'], real_px, pnl, reason,
                                     duration, pos.get('entry_indicators'))
                if pos['stop_oid']:
                    try: self.exchange.cancel(sym, pos['stop_oid'])
                    except: pass
                pos.update(side=0, entrada=0.0, nocional=0.0, margin=0.0, trail=0.0,
                           highest_price=0.0, lowest_price=0.0, riesgo_inicial=0.0,
                           breakeven_activated=False, tp_taken=False, orig_sz=0.0,
                           current_sz=0.0, stop_oid=None, last_stop_price=0.0,
                           cooldown_until_vela=self.store.last_candle_time.get(sym),
                           entry_time=None, entry_indicators={})
                send_telegram(f"💰 Cierre {sym}: {reason} | PnL: {pnl:+.2f} USDC")
        except Exception as e:
            logging.error(f"Error cierre {sym}: {e}")
            self.record_error()

    # ---------- Heartbeat enriquecido ----------
    async def heartbeat(self):
        now = datetime.now(timezone.utc)
        if (now - self.last_heartbeat).total_seconds() >= 86400:
            try:
                user = self.info.user_state(self.address)
                account_val = float(user['crossMarginSummary']['accountValue'])
                uptime = now - self.start_time
                last_trade_str = "Nunca"
                if self.stats.last_trade_time:
                    since_last = now - self.stats.last_trade_time
                    last_trade_str = f"{since_last.days}d {since_last.seconds//3600}h"
                open_pos = [f"{sym} {'LONG' if p['side']==1 else 'SHORT'} {p['current_sz']:.4f}"
                            for sym, p in self.positions.items() if p['side']!=0]
                pending = [f"{sym} {'LONG' if o['side']==1 else 'SHORT'} {o['sz']:.4f} @{o['limit_price']:.4f}"
                           for sym, o in self.pending_orders.items()]
                msg = (f"❤️ <b>Heartbeat diario</b>\n"
                       f"Saldo: ${account_val:,.2f}\n"
                       f"PnL acumulado: ${self.stats.total_pnl:,.2f}\n"
                       f"Posiciones: {len(open_pos)}\n"
                       f"{', '.join(open_pos) if open_pos else 'Ninguna'}\n"
                       f"Órdenes: {len(pending)}\n"
                       f"{', '.join(pending) if pending else 'Ninguna'}\n"
                       f"Último trade: {last_trade_str}\n"
                       f"Uptime: {uptime.days}d {uptime.seconds//3600}h\n"
                       f"Métricas: {self.stats.summary()}")
                send_telegram(msg)
                self.last_heartbeat = now
            except:
                pass

    async def send_status_update(self):
        try:
            user = self.info.user_state(self.address)
            account_val = float(user['crossMarginSummary']['accountValue'])
            margin_used = float(user['crossMarginSummary']['totalMarginUsed'])
        except:
            return

        prices = await self.all_mids()
        lines = ["<b>📊 ESTADO PERIÓDICO</b>",
                 f"Saldo: <b>${account_val:,.2f}</b> | Margen: <b>${margin_used:,.2f}</b>",
                 f"<b>📈 Estadísticas:</b> {self.stats.summary()}"]
        for sym, pos in self.positions.items():
            if pos['side'] == 0: continue
            px = float(prices.get(sym, 0))
            side = 'LONG' if pos['side'] == 1 else 'SHORT'
            pnl_pct = (px/pos['entrada']-1)*100 if side=='LONG' else (pos['entrada']/px-1)*100
            lines.append(f"  {sym} {side} | E:{pos['entrada']:.4f} Px:{px:.4f} | PnL%:{pnl_pct:+.2f}%")
        send_telegram("\n".join(lines))
        self.last_status_update = datetime.now(timezone.utc)

    async def print_status(self):
        now = datetime.now(timezone.utc)
        elapsed = (now - self.start_time).total_seconds()
        print(f"\n{'='*60}\n🕒 {now.strftime('%Y-%m-%d %H:%M:%S UTC')} (t={elapsed:.1f}s)\n{'='*60}")
        prices = await self.all_mids()
        print("\nIndicadores:")
        for sym in SYMBOLS:
            ind = self.store.indicators.get(sym, {})
            if ind:
                print(f"{sym:6s} | Close:{ind['close']:10.2f} SMA200:{ind['sma200']:10.2f} ATR:{ind['atr']:.4f} ADX:{ind['adx']:.1f}")
        print("\nPosiciones:")
        for sym, pos in self.positions.items():
            if pos['side'] != 0:
                side = 'LONG' if pos['side']==1 else 'SHORT'
                print(f"{sym} {side} | Entrada:{pos['entrada']:.4f} Trail:{pos['trail']:.4f} Sz:{pos['current_sz']:.4f} TP:{pos['tp_taken']}")
        print(f"\nEstadísticas: {self.stats.summary()}")
        print(f"Latencia API: {latency_tracker.avg():.2f}s | Errores: {self.consecutive_errors}")
        if elapsed > LATENCY_WARNING_SECONDS:
            logging.warning(f"⏱️ Ejecución lenta: {elapsed:.1f}s")
            send_telegram(f"⏱️ Alerta de latencia: ejecución duró {elapsed:.1f}s")
        print(f"{'='*60}\n")

    # ---------- Flujo principal (con health check y safe restart) ----------
    async def run_once(self):
        logging.info("===== NUEVA EJECUCIÓN =====")
        send_telegram("▶️ Inicio de ejecución programada.")
        self.reset_errors()
        self.check_server_time()
        await self.full_sync()

        # Mejora 7: comprobar salud de la API antes de enviar órdenes
        self.stats.record_latency(latency_tracker.avg())
        self.stats.update_errors(self.consecutive_errors)
        if latency_tracker.avg() > API_HEALTH_LATENCY_LIMIT or self.consecutive_errors >= API_HEALTH_ERRORS_LIMIT:
            self.api_healthy = False
            send_telegram("⚠️ API degradada. Solo se gestionarán salidas.")
        else:
            self.api_healthy = True

        await self.check_and_clean_orders()
        await self.manage_exits()
        await self.evaluate_signals()
        await self.print_status()

        now = datetime.now(timezone.utc)
        if (now - self.last_status_update).total_seconds() >= STATUS_UPDATE_INTERVAL:
            await self.send_status_update()
        await self.heartbeat()
        self._save_bot_state()
        self.store.save_cache(CANDLES_CACHE_FILE)
        self._git_commit_state()
        logging.info("===== FIN =====")
        send_telegram("✅ Ejecución completada correctamente.")

    # ---------- Auto‑commit ----------
    def _git_commit_state(self):
        if not GIT_TOKEN:
            return
        try:
            subprocess.run(["git", "config", "user.name", GIT_USER_NAME], check=False)
            subprocess.run(["git", "config", "user.email", GIT_USER_EMAIL], check=False)
            subprocess.run(["git", "checkout", "main"], check=False, capture_output=True)
            subprocess.run(["git", "checkout", "master"], check=False, capture_output=True)
            files = [BOT_STATE_FILE, STATS_FILE, SENT_ORDERS_FILE, TRADES_CSV, ORDERS_CSV, CANDLES_CACHE_FILE]
            for f in files:
                if os.path.exists(f):
                    subprocess.run(["git", "add", f], check=False)
            result = subprocess.run(["git", "diff", "--cached", "--quiet"], capture_output=True)
            if result.returncode != 0:
                subprocess.run(["git", "commit", "-m", f"auto-save {datetime.now().isoformat()}"], check=False)
                remote_url = f"https://x-access-token:{GIT_TOKEN}@github.com/{os.environ.get('GITHUB_REPOSITORY', '')}.git"
                subprocess.run(["git", "push", remote_url, "HEAD"], check=False)
                logging.info("Estado auto‑guardado en el repositorio.")
        except Exception as e:
            logging.error(f"Error en auto‑commit: {e}")

# -------------------------------------------------------------------
# ARRANQUE (con safe restart)
# -------------------------------------------------------------------
def rotate_logs():
    cutoff = time.time() - 30 * 24 * 3600
    for f in glob.glob(os.path.join(LOG_DIR, "*.log")):
        if os.path.getmtime(f) < cutoff:
            os.remove(f)

async def main():
    lock_fd = open(LOCK_FILE, 'w')
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logging.warning("Ya hay una instancia corriendo, saliendo.")
        return

    rotate_logs()
    log_filename = os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.log")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )

    account = Account.from_key(HL_PRIVATE_KEY)
    info = Info(constants.TESTNET_API_URL, skip_ws=True)
    exchange = Exchange(account, constants.TESTNET_API_URL, account_address=HL_WALLET_ADDRESS)

    store = OHLCVStore.load_cache(CANDLES_CACHE_FILE)
    if store is None:
        print("Descargando histórico completo (300 días) en paralelo...")
        store = OHLCVStore(SYMBOLS)
        await fetch_all_initial_candles(store)
        store.save_cache(CANDLES_CACHE_FILE)
    else:
        print("Caché de velas cargado. Añadiendo última vela diaria...")
        for sym in SYMBOLS:
            c = last_closed_daily_candle(sym)
            if c and c['t'] > store.last_candle_time.get(sym, 0):
                store.add_candle(sym, c)

    stats = StatsTracker()
    order_tracker = OrderIdTracker()
    order_logger = OrderLogger()
    bot = LiveBotCron(account, info, exchange, store, stats, order_tracker, order_logger)
    bot.set_leverage_if_needed()
    await bot.full_sync()

    # Mejora 10: Safe Restart – esperar antes de operar tras un reinicio
    if not bot.first_run:
        logging.info(f"Safe restart: esperando {SAFE_RESTART_DELAY}s...")
        await asyncio.sleep(SAFE_RESTART_DELAY)
        send_telegram("🔄 Safe restart completado, reanudando operaciones.")

    if bot.first_run:
        send_telegram("🤖 Bot institucional completo iniciado por primera vez.")
    else:
        send_telegram("🔄 Bot reiniciado, recuperando estado...")

    max_retries = 3
    for attempt in range(max_retries):
        try:
            await bot.run_once()
            break
        except RuntimeError:
            break
        except Exception as e:
            logging.error(f"Intento {attempt+1} fallido: {traceback.format_exc()}")
            if attempt < max_retries - 1:
                time.sleep(30)
            else:
                send_telegram(f"❌ Fallo tras {max_retries} intentos: {str(e)[:200]}")
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()

if __name__ == "__main__":
    asyncio.run(main())
