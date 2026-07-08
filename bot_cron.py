#!/usr/bin/env python3
"""
Bot Momentum/ATR para Hyperliquid Testnet – ADAPTACIÓN A GITHUB ACTIONS (cada 30 min).
Basado fielmente en el código original, con correcciones:
- Usa vela diaria actual (no cerrada) para señales y órdenes límite.
- Persistencia de cooldown_until.
- Cache de velas con clave fija en GitHub Actions.
- Envía estado por Telegram en cada ejecución.
"""

import asyncio
import json
import os
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List

import requests
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants
from eth_account import Account
from eth_account.signers.local import LocalAccount

# -------------------------------------------------------------------
# CARGAR CREDENCIALES DESDE VARIABLES DE ENTORNO
# -------------------------------------------------------------------
HL_PRIVATE_KEY = os.environ["HL_PRIVATE_KEY"]
HL_WALLET_ADDRESS = os.environ["HL_WALLET_ADDRESS"]
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# -------------------------------------------------------------------
# CONFIGURACIÓN DEL BOT (idéntica al original)
# -------------------------------------------------------------------
SYMBOLS = ['BTC', 'ETH', 'SOL', 'ADA', 'ATOM', 'AVAX', 'DOGE']
LEVERAGE = 5
COMMISSION = 0.00045
SLIPPAGE = 0.0005
MAX_MARGIN_TOTAL = 0.30
MAX_POSITIONS = 6

MOMENTUM_PERIOD = 90
SMA_TREND = 200
ATR_PERIOD = 14
ADX_PERIOD = 14

BEST_PARAMS = {
    'mult_atr_trailing': 5.0,
    'momentum_min': 0.04,
    'cooldown_days': 3,
    'risk_per_trade': 0.05,
    'adx_min': 20,
    'take_profit_r': 2.0,
    'vol_filter_perc': 20,
    'breakeven_r': 1.5
}

BASE_URL = constants.TESTNET_API_URL
STATUS_UPDATE_INTERVAL = 14400   # 4 horas (ya no se usa, pero se mantiene por si acaso)

POS_STATE_FILE = "position_state.json"
BOT_LIGHT_STATE = "bot_light_state.json"   # para last_day_checked y last_status_update
CANDLES_CACHE = "candles_cache.json"

# -------------------------------------------------------------------
# FUNCIONES DE TELEGRAM
# -------------------------------------------------------------------
def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        logging.error(f"Error enviando Telegram: {e}")

# -------------------------------------------------------------------
# FUNCIONES AUXILIARES DE DATOS
# -------------------------------------------------------------------
def fetch_candles(coin: str, start_ms: int, end_ms: int, interval: str = "1d") -> List[Dict]:
    all_candles = []
    cursor = start_ms
    interval_ms = 24 * 60 * 60 * 1000 if interval == "1d" else 60 * 60 * 1000
    while cursor < end_ms:
        window_end = min(cursor + 4000 * interval_ms, end_ms)
        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval,
                "startTime": cursor,
                "endTime": window_end
            }
        }
        try:
            resp = requests.post(BASE_URL + "/info", json=payload, timeout=30)
            resp.raise_for_status()
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

def historical_candles(coin: str, days: int = 300) -> List[Dict]:
    end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days)
    return fetch_candles(coin, int(start.timestamp() * 1000), int(end.timestamp() * 1000))

def last_daily_candle(coin: str) -> Optional[Dict]:
    """Vela diaria actual (la que se está formando hoy)."""
    now = datetime.now(timezone.utc)
    end_ms = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    start_ms = end_ms - 24 * 60 * 60 * 1000
    candles = fetch_candles(coin, start_ms, end_ms)
    return candles[-1] if candles else None

# -------------------------------------------------------------------
# ALMACÉN DE VELAS E INDICADORES (idéntico al original)
# -------------------------------------------------------------------
class OHLCVStore:
    def __init__(self, symbols):
        self.data = {s: {'open': [], 'high': [], 'low': [], 'close': [], 'volume': []} for s in symbols}
        self.indicators = {s: {} for s in symbols}

    def add_candle(self, symbol: str, candle: dict):
        d = self.data[symbol]
        # Evitar duplicados por timestamp
        if d['close'] and d['close'][-1] == float(candle['c']):
            return
        d['open'].append(float(candle['o']))
        d['high'].append(float(candle['h']))
        d['low'].append(float(candle['l']))
        d['close'].append(float(candle['c']))
        d['volume'].append(float(candle['v']))
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

        mom = 0.0
        if n > MOMENTUM_PERIOD:
            mom = (closes[-1] / closes[-1 - MOMENTUM_PERIOD]) - 1

        plus_dm = [0.0] * n
        minus_dm = [0.0] * n
        for i in range(1, n):
            up = highs[i] - highs[i - 1]
            down = lows[i - 1] - lows[i]
            if up > down and up > 0:
                plus_dm[i] = up
            if down > up and down > 0:
                minus_dm[i] = down

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
            if _tr14 != 0 and i >= ADX_PERIOD:
                _plus_di = 100 * _plus_dm14 / _tr14
                _minus_di = 100 * _minus_dm14 / _tr14
                _dx = abs(_plus_di - _minus_di) / (_plus_di + _minus_di) * 100
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

# -------------------------------------------------------------------
# BOT PRINCIPAL (adaptado a una sola ejecución)
# -------------------------------------------------------------------
class LiveBotCron:
    def __init__(self, account: LocalAccount, info: Info, exchange: Exchange, store: OHLCVStore):
        self.account = account
        self.info = info
        self.exchange = exchange
        self.store = store
        self.address = HL_WALLET_ADDRESS

        meta = self.info.meta()
        self.sz_decimals = {a['name']: a.get('szDecimals', 4) for a in meta['universe']}
        self.max_sizes   = {a['name']: float(a.get('maxSz', '1000000000')) for a in meta['universe']}
        self.min_sizes   = {a['name']: float(a.get('minSz', '0.0001')) for a in meta['universe']}

        self.positions = {s: {
            'side': 0, 'entrada': 0.0, 'nocional': 0.0, 'margin': 0.0,
            'trail': 0.0, 'max_price': 0.0, 'min_price': 0.0,
            'cooldown_until': datetime.min.replace(tzinfo=timezone.utc),
            'riesgo_inicial': 0.0, 'breakeven_activated': False,
            'tp_taken': False,
            'orig_sz': 0.0
        } for s in SYMBOLS}

        self.pending_orders = {}
        self.last_day_checked = None
        self.last_status_update = datetime.min.replace(tzinfo=timezone.utc)
        self.position_state = self._load_position_state()
        self._load_light_state()

    # ------------------------------------------------------------------
    # Persistencia del estado
    # ------------------------------------------------------------------
    def _load_position_state(self) -> dict:
        if os.path.exists(POS_STATE_FILE):
            try:
                with open(POS_STATE_FILE, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logging.error(f"Error cargando position_state: {e}")
        return {}

    def _save_position_state(self):
        try:
            with open(POS_STATE_FILE, 'w') as f:
                json.dump(self.position_state, f)
        except Exception as e:
            logging.error(f"Error guardando position_state: {e}")

    def _remove_from_position_state(self, symbol: str):
        if symbol in self.position_state:
            del self.position_state[symbol]
            self._save_position_state()

    def _load_light_state(self):
        if os.path.exists(BOT_LIGHT_STATE):
            try:
                with open(BOT_LIGHT_STATE, 'r') as f:
                    data = json.load(f)
                if 'last_day_checked' in data:
                    self.last_day_checked = datetime.fromisoformat(data['last_day_checked']).date()
                if 'last_status_update' in data:
                    self.last_status_update = datetime.fromisoformat(data['last_status_update'])
            except:
                pass

    def _save_light_state(self):
        try:
            with open(BOT_LIGHT_STATE, 'w') as f:
                json.dump({
                    'last_day_checked': self.last_day_checked.isoformat() if self.last_day_checked else None,
                    'last_status_update': self.last_status_update.isoformat()
                }, f)
        except Exception as e:
            logging.error(f"Error guardando light_state: {e}")

    # ------------------------------------------------------------------
    # Utilidades de tamaño y precio
    # ------------------------------------------------------------------
    def round_sz(self, symbol: str, sz: float) -> float:
        decimals = self.sz_decimals.get(symbol, 4)
        return max(round(sz, decimals), 0.0)

    def clamp_sz(self, symbol: str, desired_sz: float) -> float:
        rounded = self.round_sz(symbol, desired_sz)
        min_sz = self.min_sizes.get(symbol, 0.0)
        max_sz = self.max_sizes.get(symbol, float('inf'))
        capped = min(max(rounded, min_sz), max_sz)
        capped = self.round_sz(symbol, capped)
        return capped if capped >= min_sz else 0.0

    def round_px(self, symbol: str, px: float) -> float:
        decimals = max(6 - self.sz_decimals.get(symbol, 4), 0)
        px_5sig = float(f"{px:.5g}")
        return round(px_5sig, decimals)

    def set_leverage_for_all(self):
        for sym in SYMBOLS:
            try:
                self.exchange.update_leverage(LEVERAGE, sym, is_cross=True)
                logging.info(f"Leverage {LEVERAGE}x configurado para {sym}")
            except Exception as e:
                logging.error(f"No se pudo fijar leverage para {sym}: {e}")

    # ------------------------------------------------------------------
    # Sincronización con el exchange (recupera también cooldown_until)
    # ------------------------------------------------------------------
    def sync_state_from_exchange(self):
        try:
            state = self.info.user_state(self.address)
            for ap in state.get('assetPositions', []):
                p = ap['position']
                sym = p['coin']
                if sym not in self.positions:
                    continue
                szi = float(p['szi'])
                if szi == 0:
                    continue
                side = 1 if szi > 0 else -1
                entry = float(p['entryPx'])
                atr = self.store.indicators.get(sym, {}).get('atr', 0.0)
                dist = BEST_PARAMS['mult_atr_trailing'] * atr if atr > 0 else abs(entry) * 0.02
                pos = self.positions[sym]
                pos['side'] = side
                pos['entrada'] = entry
                pos['nocional'] = abs(szi) * entry
                pos['margin'] = pos['nocional'] / LEVERAGE
                pos['max_price'] = entry
                pos['min_price'] = entry
                pos['trail'] = entry - side * dist
                pos['riesgo_inicial'] = dist
                # Recuperar estado de position_state
                if sym in self.position_state:
                    ps = self.position_state[sym]
                    orig_sz = ps.get('orig_sz', 0.0)
                    pos['orig_sz'] = orig_sz
                    if orig_sz > 0 and abs(szi) < orig_sz * 0.75:
                        pos['tp_taken'] = True
                        pos['breakeven_activated'] = True
                    else:
                        pos['tp_taken'] = False
                    # Recuperar cooldown
                    if 'cooldown_until' in ps:
                        try:
                            pos['cooldown_until'] = datetime.fromisoformat(ps['cooldown_until'])
                        except:
                            pos['cooldown_until'] = datetime.now(timezone.utc)
                else:
                    pos['orig_sz'] = abs(szi)
                    pos['tp_taken'] = False
                    # No hay cooldown previo, se establecerá al abrir
                logging.info(f"Posición sincronizada: {sym} {'LONG' if side == 1 else 'SHORT'} @ {entry}")
        except Exception as e:
            logging.error(f"No se pudieron sincronizar posiciones: {e}")

        # Órdenes abiertas
        try:
            open_orders = self.info.open_orders(self.address)
            orders_by_symbol: Dict[str, list] = {}
            for o in open_orders:
                sym = o.get('coin')
                if sym not in self.positions:
                    continue
                orders_by_symbol.setdefault(sym, []).append(o)

            for sym, orders in orders_by_symbol.items():
                if len(orders) > 1:
                    orders_sorted = sorted(orders, key=lambda x: x.get('oid', 0), reverse=True)
                    keep, to_cancel = orders_sorted[0], orders_sorted[1:]
                    oids_cancel = [o.get('oid') for o in to_cancel]
                    logging.warning(f"{sym}: {len(orders)} órdenes abiertas. Cancelando duplicados {oids_cancel}.")
                    for o in to_cancel:
                        try:
                            self.exchange.cancel(sym, o.get('oid'))
                        except Exception as e:
                            logging.error(f"FALLÓ cancelar duplicado {sym} oid={o.get('oid')}: {e}")
                        time.sleep(0.3)
                    orders = [keep]

                o = orders[0]
                side = 1 if o.get('side') == 'B' else -1
                limit_px = float(o.get('limitPx', 0))
                sz = float(o.get('sz', 0))
                oid = o.get('oid')
                atr = self.store.indicators.get(sym, {}).get('atr', 0.0)
                dist = BEST_PARAMS['mult_atr_trailing'] * atr if atr > 0 else abs(limit_px) * 0.02
                self.pending_orders[sym] = {
                    'oid': oid, 'side': side, 'limit_price': limit_px,
                    'nocional': limit_px * sz, 'margin': (limit_px * sz) / LEVERAGE,
                    'sz': sz, 'dist': dist, 'riesgo_inicial': dist
                }
                logging.info(f"Orden pendiente sincronizada: {sym} oid={oid} @ {limit_px}")
        except Exception as e:
            logging.error(f"No se pudieron sincronizar órdenes abiertas: {e}")

    # ------------------------------------------------------------------
    # Mercado
    # ------------------------------------------------------------------
    async def all_mids(self) -> Dict[str, float]:
        return self.info.all_mids()

    # ------------------------------------------------------------------
    # Cancelación segura
    # ------------------------------------------------------------------
    async def cancel_single(self, sym: str) -> bool:
        o = self.pending_orders.get(sym)
        if not o:
            return True
        try:
            resp = self.exchange.cancel(sym, o['oid'])
            if isinstance(resp, dict) and resp.get('status') != 'ok':
                logging.warning(f"Cancelación no confirmada para {sym}: {resp}")
                return False
        except Exception as e:
            logging.warning(f"No se pudo cancelar {sym} oid={o['oid']}: {e}")
            return False
        del self.pending_orders[sym]
        return True

    async def check_pending(self):
        for sym, o in list(self.pending_orders.items()):
            try:
                status = self.info.query_order_by_oid(self.address, o['oid'])
                order_status = status.get('order', {}).get('status')
                if order_status == 'filled':
                    order_info = status['order']['order']
                    fill_px = float(order_info.get('limitPx', o['limit_price']))
                    fill_sz = float(order_info.get('origSz', o['sz']))
                    pos = self.positions[sym]
                    pos['side'] = o['side']
                    pos['entrada'] = fill_px
                    pos['nocional'] = fill_px * fill_sz
                    pos['margin'] = o['margin']
                    pos['max_price'] = fill_px
                    pos['min_price'] = fill_px
                    pos['trail'] = fill_px - o['side'] * o['dist']
                    pos['riesgo_inicial'] = o['dist']
                    pos['breakeven_activated'] = False
                    pos['tp_taken'] = False
                    pos['orig_sz'] = fill_sz
                    pos['cooldown_until'] = datetime.now(timezone.utc) + timedelta(days=BEST_PARAMS['cooldown_days'])
                    self.position_state[sym] = {
                        'orig_sz': fill_sz,
                        'cooldown_until': pos['cooldown_until'].isoformat()
                    }
                    self._save_position_state()
                    msg = f"✅ ENTRADA {sym} {'LONG' if o['side'] == 1 else 'SHORT'} @ {fill_px:.4f} | Sz: {fill_sz:.4f} | Nocional: ${pos['nocional']:,.2f}"
                    logging.info(msg)
                    send_telegram(msg)
                    del self.pending_orders[sym]
                elif order_status in ('canceled', 'rejected'):
                    del self.pending_orders[sym]
            except Exception as e:
                logging.error(f"Error check_pending {sym}: {e}")

    # ------------------------------------------------------------------
    # Señales diarias (usa vela actual)
    # ------------------------------------------------------------------
    async def daily_signals(self):
        today = datetime.now(timezone.utc).date()
        if self.last_day_checked == today:
            return
        self.last_day_checked = today

        await self.check_pending()

        stuck_symbols = set()
        for sym in list(self.pending_orders.keys()):
            if not await self.cancel_single(sym):
                stuck_symbols.add(sym)

        for sym in SYMBOLS:
            c = last_daily_candle(sym)   # <--- vela actual
            if c:
                self.store.add_candle(sym, c)

        try:
            user = self.info.user_state(self.address)
            account_val = float(user['crossMarginSummary']['accountValue'])
            margin_used = float(user['crossMarginSummary']['totalMarginUsed'])
        except Exception as e:
            logging.error(f"Error obteniendo cuenta: {e}")
            account_val = 0.0
            margin_used = 0.0

        print(f"\n--- Evaluando señales ({today}) ---")
        print(f"Capital: ${account_val:,.2f} | Margen usado: ${margin_used:,.2f}")

        for sym in SYMBOLS:
            # Verificar cooldown
            if self.positions[sym]['cooldown_until'] > datetime.now(timezone.utc):
                continue
            if self.positions[sym]['side'] != 0:
                continue
            if sym in stuck_symbols:
                continue

            ind = self.store.indicators.get(sym, {})
            if not ind:
                continue

            close_ = ind['close']
            open_ = ind['open']
            s200 = ind['sma200']
            atr_val = ind['atr']
            mom = ind['momentum']
            adx_val = ind['adx']
            vol_perc = ind['vol_percentile']

            trend_up = close_ > s200 and adx_val > BEST_PARAMS['adx_min']
            trend_down = close_ < s200 and adx_val > BEST_PARAMS['adx_min']
            mom_up = mom > BEST_PARAMS['momentum_min']
            mom_down = mom < -BEST_PARAMS['momentum_min']
            vol_ok = vol_perc > (BEST_PARAMS['vol_filter_perc'] / 100.0)
            signal_valid = ((trend_up and mom_up) or (trend_down and mom_down)) and vol_ok

            dist = BEST_PARAMS['mult_atr_trailing'] * atr_val
            if not signal_valid or dist <= 0:
                continue

            side = 1 if (trend_up and mom_up) else -1
            limit_px = self.round_px(sym, open_ * (1 + SLIPPAGE)) if side == 1 else self.round_px(sym, open_ * (1 - SLIPPAGE))

            if sum(1 for p in self.positions.values() if p['side'] != 0) >= MAX_POSITIONS:
                continue

            free_margin = max(0, account_val * MAX_MARGIN_TOTAL - margin_used)
            risk_usd = account_val * BEST_PARAMS['risk_per_trade']
            max_risk_noc = risk_usd / (dist / limit_px) if limit_px > 0 else 0
            max_margin_noc = free_margin * LEVERAGE
            nocional = min(max_risk_noc, max_margin_noc)
            margin = nocional / LEVERAGE
            if nocional <= 0 or margin < 10:
                continue

            sz = self.clamp_sz(sym, nocional / limit_px)
            if sz <= 0:
                continue
            nocional = sz * limit_px
            margin = nocional / LEVERAGE
            if margin < 10:
                continue

            try:
                order = self.exchange.order(
                    name=sym, is_buy=(side == 1), sz=sz, limit_px=limit_px,
                    order_type={"limit": {"tif": "Gtc"}}, reduce_only=False
                )
                if order['status'] == 'ok':
                    status = order['response']['data']['statuses'][0]
                    if 'resting' in status:
                        oid = status['resting']['oid']
                        self.pending_orders[sym] = {
                            'oid': oid, 'side': side, 'limit_price': limit_px,
                            'nocional': nocional, 'margin': margin, 'sz': sz,
                            'dist': dist, 'riesgo_inicial': dist
                        }
                        msg = (f"📊 ORDEN LÍMITE {sym} {'LONG' if side == 1 else 'SHORT'} | "
                               f"Sz: {sz:.4f} | Límite: {limit_px:.4f} | Stop inicial: {dist:.4f} | "
                               f"Nocional: ${nocional:,.2f} | Margen: ${margin:,.2f} | Apalancamiento: {LEVERAGE}x")
                        print(msg)
                        send_telegram(msg)
                    elif 'filled' in status:
                        fill_px = float(status['filled']['avgPx'])
                        fill_sz = float(status['filled']['totalSz'])
                        fill_nocional = fill_px * fill_sz
                        pos = self.positions[sym]
                        pos.update(side=side, entrada=fill_px, nocional=fill_nocional,
                                   margin=fill_nocional / LEVERAGE, max_price=fill_px, min_price=fill_px,
                                   trail=fill_px - side * dist, riesgo_inicial=dist,
                                   breakeven_activated=False, tp_taken=False, orig_sz=fill_sz,
                                   cooldown_until=datetime.now(timezone.utc) + timedelta(days=BEST_PARAMS['cooldown_days']))
                        self.position_state[sym] = {
                            'orig_sz': fill_sz,
                            'cooldown_until': pos['cooldown_until'].isoformat()
                        }
                        self._save_position_state()
                        msg = (f"✅ ENTRADA INMEDIATA {sym} {'LONG' if side == 1 else 'SHORT'} @ {fill_px:.4f} | "
                               f"Sz: {fill_sz:.4f} | Nocional: ${fill_nocional:,.2f} | Apalancamiento: {LEVERAGE}x")
                        print(msg)
                        send_telegram(msg)
                else:
                    logging.error(f"Error orden {sym}: {order}")
            except Exception as e:
                logging.error(f"Excepción {sym}: {e}")

    # ------------------------------------------------------------------
    # Monitor de salidas (con verificación post‑orden)
    # ------------------------------------------------------------------
    async def _refresh_position_from_exchange(self, sym: str):
        try:
            state = self.info.user_state(self.address)
            for ap in state.get('assetPositions', []):
                p = ap['position']
                if p['coin'] != sym:
                    continue
                szi = float(p['szi'])
                if szi == 0:
                    self.positions[sym].update(side=0, entrada=0.0, nocional=0.0, margin=0.0,
                                               trail=0.0, max_price=0.0, min_price=0.0,
                                               cooldown_until=datetime.now(timezone.utc) + timedelta(days=BEST_PARAMS['cooldown_days']),
                                               riesgo_inicial=0.0, breakeven_activated=False,
                                               tp_taken=False, orig_sz=0.0)
                    self._remove_from_position_state(sym)
                    return
                entry = float(p['entryPx'])
                side = 1 if szi > 0 else -1
                pos = self.positions[sym]
                pos['side'] = side
                pos['entrada'] = entry
                pos['nocional'] = abs(szi) * entry
                pos['margin'] = pos['nocional'] / LEVERAGE
                return
        except Exception as e:
            logging.error(f"Error refrescando posición {sym}: {e}")

    async def monitor_exits(self):
        prices = await self.all_mids()
        for sym, pos in self.positions.items():
            if pos['side'] == 0:
                continue
            px = float(prices.get(sym, 0))
            if px == 0:
                continue

            side = pos['side']
            entry = pos['entrada']
            atr = self.store.indicators.get(sym, {}).get('atr', 0.0)

            if side == 1:
                if px > pos['max_price']:
                    pos['max_price'] = px
                pos['trail'] = max(pos['trail'], pos['max_price'] - BEST_PARAMS['mult_atr_trailing'] * atr)
            else:
                if px < pos['min_price']:
                    pos['min_price'] = px
                trail = pos['min_price'] + BEST_PARAMS['mult_atr_trailing'] * atr
                if pos['trail'] == 0 or trail < pos['trail']:
                    pos['trail'] = trail

            if not pos['breakeven_activated'] and pos['riesgo_inicial'] > 0:
                if side == 1 and px >= entry + BEST_PARAMS['breakeven_r'] * pos['riesgo_inicial']:
                    pos['trail'] = max(pos['trail'], entry)
                    pos['breakeven_activated'] = True
                elif side == -1 and px <= entry - BEST_PARAMS['breakeven_r'] * pos['riesgo_inicial']:
                    pos['trail'] = min(pos['trail'], entry)
                    pos['breakeven_activated'] = True

            # TP parcial
            tp_px = entry + side * BEST_PARAMS['take_profit_r'] * pos['riesgo_inicial']
            if not pos['tp_taken'] and ((side == 1 and px >= tp_px) or (side == -1 and px <= tp_px)):
                desired_half_sz = (pos['nocional'] / 2) / px
                half_sz = self.clamp_sz(sym, desired_half_sz)
                if half_sz > 0:
                    exit_px = self.round_px(sym, px * (1 - SLIPPAGE) if side == 1 else px * (1 + SLIPPAGE))
                    try:
                        order_resp = self.exchange.order(
                            name=sym, is_buy=(side == -1), sz=half_sz,
                            limit_px=exit_px, order_type={"limit": {"tif": "Ioc"}}, reduce_only=True
                        )
                        if order_resp['status'] == 'ok':
                            status = order_resp['response']['data']['statuses'][0]
                            if 'filled' in status:
                                real_px = float(status['filled'].get('avgPx', exit_px))
                                profit = (pos['nocional'] / 2) * (real_px / entry - 1) if side == 1 else (pos['nocional'] / 2) * (entry / real_px - 1)
                                profit -= (pos['nocional'] / 2) * COMMISSION * 2
                                self._log_trade(sym, side, half_sz, profit, "TP parcial")
                                pos['tp_taken'] = True
                                pos['breakeven_activated'] = True
                                pos['trail'] = entry
                                await self._refresh_position_from_exchange(sym)
                            else:
                                logging.warning(f"TP parcial {sym}: IOC no llenada, se reintentará.")
                    except Exception as e:
                        logging.error(f"Error TP parcial {sym}: {e}")

            if (side == 1 and px <= pos['trail']) or (side == -1 and px >= pos['trail']):
                await self.close_position(sym, "Trailing stop", px)
                continue

            # Cambio de tendencia
            ind = self.store.indicators.get(sym, {})
            if ind:
                close_ = ind['close']
                s200 = ind['sma200']
                adx = ind['adx']
                trend_up = close_ > s200 and adx > BEST_PARAMS['adx_min']
                trend_down = close_ < s200 and adx > BEST_PARAMS['adx_min']
                if (side == 1 and not trend_up) or (side == -1 and not trend_down):
                    await self.close_position(sym, "Cambio de tendencia", px)

    async def close_position(self, sym: str, reason: str, px: float):
        pos = self.positions[sym]
        if pos['side'] == 0:
            return
        side = pos['side']
        sz = self.clamp_sz(sym, pos['nocional'] / px)
        if sz <= 0:
            return
        exit_px = self.round_px(sym, px * (1 - SLIPPAGE) if side == 1 else px * (1 + SLIPPAGE))
        try:
            order_resp = self.exchange.order(
                name=sym, is_buy=(side == -1), sz=sz,
                limit_px=exit_px, order_type={"limit": {"tif": "Ioc"}}, reduce_only=True
            )
            if order_resp['status'] == 'ok':
                status = order_resp['response']['data']['statuses'][0]
                if 'filled' in status:
                    real_px = float(status['filled'].get('avgPx', exit_px))
                    profit = pos['nocional'] * (real_px / pos['entrada'] - 1) if side == 1 else pos['nocional'] * (pos['entrada'] / real_px - 1)
                    profit -= pos['nocional'] * COMMISSION * 2
                    self._log_trade(sym, side, sz, profit, reason)
                    await self._refresh_position_from_exchange(sym)
                    self._remove_from_position_state(sym)
                    pos.update(
                        side=0, entrada=0.0, nocional=0.0, margin=0.0, trail=0.0,
                        max_price=0.0, min_price=0.0,
                        cooldown_until=datetime.now(timezone.utc) + timedelta(days=BEST_PARAMS['cooldown_days']),
                        riesgo_inicial=0.0, breakeven_activated=False, tp_taken=False, orig_sz=0.0
                    )
                else:
                    logging.warning(f"Cierre {sym}: IOC no llenada, se reintentará.")
        except Exception as e:
            logging.error(f"Error cerrando {sym}: {e}")

    def _log_trade(self, sym, side, sz, pnl, reason):
        side_str = 'LONG' if side == 1 else 'SHORT'
        pnl_str = f"+${pnl:,.2f}" if pnl >= 0 else f"-${abs(pnl):,.2f}"
        msg = f"💰 CIERRE {sym} {side_str} | {reason} | PnL: {pnl_str} | Sz: {sz:.4f}"
        logging.info(msg)
        send_telegram(msg)
        with open('trades.log', 'a') as f:
            f.write(f"{datetime.now()}: {msg}\n")

    # ---------- Notificación de estado ----------
    async def send_status_update(self):
        try:
            user = self.info.user_state(self.address)
            account_val = float(user['crossMarginSummary']['accountValue'])
            margin_used = float(user['crossMarginSummary']['totalMarginUsed'])
        except Exception as e:
            logging.error(f"No se pudo obtener estado para notificación: {e}")
            return

        prices = await self.all_mids()

        lines = ["<b>📊 ESTADO PERIÓDICO</b>",
                 f"Saldo: <b>${account_val:,.2f}</b> | Margen usado: <b>${margin_used:,.2f}</b>"]

        pos_lines = []
        for sym, pos in self.positions.items():
            if pos['side'] == 0:
                continue
            px = float(prices.get(sym, 0))
            side = 'LONG' if pos['side'] == 1 else 'SHORT'
            entry = pos['entrada']
            if side == 'LONG':
                pnl_pct = (px / entry - 1) * 100 if entry > 0 else 0
                unreal_pnl = pos['nocional'] * (px / entry - 1) if entry > 0 else 0
            else:
                pnl_pct = (entry / px - 1) * 100 if px > 0 else 0
                unreal_pnl = pos['nocional'] * (entry / px - 1) if px > 0 else 0
            pnl_str = f"+${unreal_pnl:.2f}" if unreal_pnl >= 0 else f"-${abs(unreal_pnl):.2f}"
            pos_lines.append(f"  {sym} {side} | Entrada: {entry:.4f} | Ahora: {px:.4f} | PnL no realizado: {pnl_str} ({pnl_pct:+.2f}%)")
        if pos_lines:
            lines.append("<b>📈 Posiciones abiertas:</b>")
            lines.extend(pos_lines)
        else:
            lines.append("📈 Sin posiciones abiertas")

        if self.pending_orders:
            lines.append("<b>📝 Órdenes límite pendientes:</b>")
            for sym, o in self.pending_orders.items():
                side = 'LONG' if o['side'] == 1 else 'SHORT'
                lines.append(f"  {sym} {side} | Límite: {o['limit_price']:.4f} | Sz: {o['sz']:.4f} | Nocional: ${o['nocional']:,.2f}")
        else:
            lines.append("📝 Sin órdenes límite pendientes")

        msg = "\n".join(lines)
        send_telegram(msg)

    # ---------- Estado en consola ----------
    async def print_status(self):
        now = datetime.now(timezone.utc)
        print(f"\n{'=' * 60}")
        print(f"🕒 STATUS {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"{'=' * 60}")

        prices = await self.all_mids()
        print("\n📊 PRECIOS E INDICADORES DE HOY:")
        for sym in SYMBOLS:
            px = float(prices.get(sym, 0))
            ind = self.store.indicators.get(sym, {})
            if ind:
                print(f"  {sym:6s} | Precio: {px:10.2f} | SMA200: {ind['sma200']:10.2f} | ATR: {ind['atr']:8.4f} | "
                      f"Mom: {ind['momentum']:7.4f} | ADX: {ind['adx']:6.2f} | Vol%: {ind['vol_percentile']:.2f}")
            else:
                print(f"  {sym:6s} | Precio: {px:10.2f} | (sin indicadores)")

        print("\n📈 POSICIONES ABIERTAS:")
        alguna = False
        for sym, pos in self.positions.items():
            if pos['side'] != 0:
                alguna = True
                side_str = 'LONG' if pos['side'] == 1 else 'SHORT'
                print(f"  {sym} {side_str} | Entrada: {pos['entrada']:.4f} | Nocional: ${pos['nocional']:,.2f} | "
                      f"Margen: ${pos['margin']:,.2f} | Apalancamiento: {LEVERAGE}x | "
                      f"Trail: {pos['trail']:.4f} | Breakeven: {pos['breakeven_activated']} | "
                      f"TP tomado: {pos['tp_taken']}")
        if not alguna:
            print("  (ninguna)")

        print("\n📝 ÓRDENES LÍMITE PENDIENTES:")
        if self.pending_orders:
            for sym, o in self.pending_orders.items():
                print(f"  {sym} {'LONG' if o['side'] == 1 else 'SHORT'} | Límite: {o['limit_price']:.4f} | "
                      f"Sz: {o['sz']:.4f} | Nocional: ${o['nocional']:,.2f} | Margen: ${o['margin']:,.2f} | "
                      f"Apalancamiento: {LEVERAGE}x | Dist stop: {o['dist']:.4f}")
        else:
            print("  (ninguna)")

        if self.last_day_checked == now.date():
            print(f"\n⏳ Señales YA evaluadas hoy.")
        else:
            print(f"\n⏳ Señales PENDIENTES para hoy.")
        print(f"{'=' * 60}\n")

    # ---------- EJECUCIÓN PRINCIPAL (una pasada) ----------
    async def run_once(self):
        logging.info("===== NUEVA EJECUCIÓN =====")
        send_telegram("▶️ Inicio de ejecución programada.")

        # Sincronizar estado real del exchange
        self.sync_state_from_exchange()

        # Evaluar señales diarias
        await self.daily_signals()

        # Verificar órdenes pendientes
        await self.check_pending()

        # Monitorizar salidas
        await self.monitor_exits()

        # Mostrar estado
        await self.print_status()

        # Enviar resumen periódico siempre
        await self.send_status_update()

        # Guardar estados
        self._save_position_state()
        self._save_light_state()
        logging.info("===== FIN =====")
        send_telegram("✅ Ejecución completada correctamente.")

# -------------------------------------------------------------------
# ARRANQUE ONE-SHOT
# -------------------------------------------------------------------
async def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )

    account = Account.from_key(HL_PRIVATE_KEY)
    info = Info(constants.TESTNET_API_URL, skip_ws=True)
    exchange = Exchange(account, constants.TESTNET_API_URL, account_address=HL_WALLET_ADDRESS)

    # Cargar velas desde caché o descargar histórico
    store = OHLCVStore(SYMBOLS)
    if os.path.exists(CANDLES_CACHE):
        with open(CANDLES_CACHE, 'r') as f:
            raw = json.load(f)
        store.data = raw
        # Reconstruir indicadores
        for sym in SYMBOLS:
            if raw[sym]['close']:
                store._update_indicators(sym)
        print("Caché de velas cargado. Añadiendo vela de hoy...")
        for sym in SYMBOLS:
            c = last_daily_candle(sym)   # vela actual
            if c:
                store.add_candle(sym, c)
    else:
        print("Descargando histórico de velas (300 días)...")
        for sym in SYMBOLS:
            candles = historical_candles(sym, days=300)
            for c in candles:
                store.add_candle(sym, c)
            print(f"  {sym}: {len(candles)} velas")
        # Guardar caché
        with open(CANDLES_CACHE, 'w') as f:
            json.dump(store.data, f)

    bot = LiveBotCron(account, info, exchange, store)
    bot.set_leverage_for_all()
    await bot.run_once()

    # Guardar caché de velas al final
    with open(CANDLES_CACHE, 'w') as f:
        json.dump(store.data, f)

if __name__ == "__main__":
    asyncio.run(main())
