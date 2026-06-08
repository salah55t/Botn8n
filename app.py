# -*- coding: utf-8 -*-
# ملف app.py - نسخة فريم الـ 4 ساعات (أقل صرامة - زيادة وتيرة الإشارات)
import time
import os
import logging
import requests
import pandas as pd
from datetime import datetime, timezone
from threading import Thread, Lock
from flask import Flask, jsonify, render_template_string, request
from flask_cors import CORS
from binance.client import Client

# --- إعداد التسجيل ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('CryptoBot_4H_Flexible')

app = Flask(__name__)
CORS(app)

BOT_API_KEY = os.getenv("BOT_API_KEY", "2009Hamza@")
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "")
PORT = int(os.getenv("PORT", 5000))

open_signals_cache = {}
IS_TRADING_ENABLED = True
signal_cache_lock = Lock()
trading_status_lock = Lock()
SYMBOLS_FILE_NAME = "crypto_list.txt"

def load_symbols_from_file(filename=SYMBOLS_FILE_NAME) -> list:
    default_symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "LINKUSDT", "AVAXUSDT"]
    if not os.path.exists(filename):
        return default_symbols
    try:
        with open(filename, "r", encoding="utf-8") as f:
            symbols = [line.strip().upper() + ("USDT" if not line.strip().upper().endswith("USDT") else "") 
                       for line in f if line.strip() and not line.startswith("#")]
        return list(dict.fromkeys(symbols))
    except:
        return default_symbols

def send_alert_to_n8n(event_type, data):
    if not N8N_WEBHOOK_URL: return False
    payload = {"event": event_type, "data": data}
    try:
        response = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=5)
        return response.status_code == 200
    except: return False

def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df['ema_21'] = df['close'].ewm(span=21, adjust=False).mean()
    df['ema_50'] = df['close'].ewm(span=50, adjust=False).mean()
    df['ema_200'] = df['close'].ewm(span=200, adjust=False).mean()
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    df['rsi_14'] = 100 - (100 / (1 + (gain / (loss + 1e-10))))
    df['macd'] = df['close'].ewm(span=12, adjust=False).mean() - df['close'].ewm(span=26, adjust=False).mean()
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    df['bb_lower'] = df['close'].rolling(window=20).mean() - (df['close'].rolling(window=20).std() * 2.5)
    df['atr_14'] = pd.concat([(df['high'] - df['low']), (df['high'] - df['close'].shift()).abs(), (df['low'] - df['close'].shift()).abs()], axis=1).max(axis=1).ewm(alpha=1/14, adjust=False).mean()
    df['vol_ma_20'] = df['volume'].rolling(window=20).mean()
    return df

def check_strategies_4h(symbol: str, df: pd.DataFrame):
    if len(df) < 200: return
    last, prev = df.iloc[-2], df.iloc[-3]
    close_price, atr = float(last['close']), float(last['atr_14'])
    
    with signal_cache_lock:
        if symbol in open_signals_cache: return

    # استراتيجية مرنة (تم تخفيف القيود)
    uptrend = last['ema_50'] > last['ema_200']
    pullback = last['low'] <= last['ema_21']
    macd_bull = last['macd'] > last['macd_signal']
    
    # زيادة وتيرة الإشارات
    if uptrend and pullback and macd_bull:
        execute_4h_trade(symbol, close_price, "BUY", atr, "Flexible Confluence")
    elif last['close'] < last['bb_lower'] and last['rsi_14'] < 35:
        execute_4h_trade(symbol, close_price, "BUY", atr, "Mean Reversion")
    elif prev['close'] < prev['ema_50'] and close_price > last['ema_50'] and last['volume'] > (last['vol_ma_20'] * 1.3):
        execute_4h_trade(symbol, close_price, "BUY", atr, "Flexible Breakout")

def execute_4h_trade(symbol, entry, direction, atr, strategy):
    stop_loss = entry - (atr * 2.0) if direction == "BUY" else entry + (atr * 2.0)
    signal = {"symbol": symbol, "direction": direction, "strategy": strategy, "entry_price": round(entry, 4), "stop_loss": round(stop_loss, 4)}
    with signal_cache_lock: open_signals_cache[symbol] = signal
    send_alert_to_n8n("NEW_4H_SIGNAL", signal)
    logger.info(f"🎯 Signal: {symbol} | {strategy}")

def main_bot_loop():
    client = Client(os.getenv("BINANCE_API_KEY", ""), os.getenv("BINANCE_API_SECRET", ""))
    while True:
        if IS_TRADING_ENABLED:
            for symbol in load_symbols_from_file():
                try:
                    klines = client.get_historical_klines(symbol, "4h", "90 day ago UTC")
                    df = pd.DataFrame(klines, columns=['time', 'open', 'high', 'low', 'close', 'volume', 'ct', 'qav', 'nt', 'tb', 'tq', 'i'])
                    for col in ['open', 'high', 'low', 'close', 'volume']: df[col] = df[col].astype(float)
                    check_strategies_4h(symbol, calculate_indicators(df))
                except Exception as e: logger.error(f"Error {symbol}: {e}")
        time.sleep(900)

@app.route('/')
def index(): return "Bot Running"

if __name__ == '__main__':
    Thread(target=main_bot_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=PORT)
