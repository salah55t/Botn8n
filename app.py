# -*- coding: utf-8 -*-
import time, os, logging, requests, pandas as pd
from threading import Thread
from flask import Flask
from binance.client import Client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('TradingBot_Pro')

app = Flask(__name__)
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "")
SYMBOLS_FILE = "crypto_list.txt"

def load_symbols_from_file():
    """تعديل: يقرأ الرموز من الملف ويضيف USDT تلقائياً"""
    symbols = []
    if os.path.exists(SYMBOLS_FILE):
        with open(SYMBOLS_FILE, "r") as f:
            for line in f:
                symbol = line.strip().upper()
                if symbol and "USDT" not in symbol:
                    symbols.append(symbol + "USDT")
                elif symbol:
                    symbols.append(symbol)
    else:
        symbols = ["BTCUSDT", "ETHUSDT"] # قائمة افتراضية
    return symbols

def calculate_indicators(df):
    # الحسابات الفنية تعتمد على بيانات Binance الخام
    df['ema_50'] = df['close'].ewm(span=50).mean()
    df['rsi'] = 100 - (100 / (1 + (df['close'].diff().clip(lower=0).ewm(span=14).mean() / 
                                  (-df['close'].diff().clip(upper=0).ewm(span=14).mean() + 1e-10))))
    df['vol_ma'] = df['volume'].rolling(20).mean()
    return df

def analyze_symbol(symbol, client):
    # هنا المصدر: جلب البيانات من Binance مباشرة
    try:
        klines = client.get_historical_klines(symbol, "4h", "20 day ago UTC")
        df = pd.DataFrame(klines, columns=['t','open','high','low','close','volume','ct','qav','nt','tb','tq','i'])
        df[['close','volume']] = df[['close','volume']].astype(float)
        df = calculate_indicators(df)
        last = df.iloc[-1]
        
        # بيانات التحليل
        signal_data = {
            "symbol": symbol,
            "price": last['close'],
            "indicators": {"RSI": round(last['rsi'], 2), "VolRatio": round(last['volume'] / last['vol_ma'], 2)}
        }

        # منطق اتخاذ القرار
        if last['close'] > last['ema_50'] or last['rsi'] < 45:
            logger.info(f"✅ تم إرسال تحليل {symbol} لـ AI")
            requests.post(N8N_WEBHOOK_URL, json=signal_data, timeout=5)
        else:
            logger.info(f"🚫 {symbol} لا يستوفي الشروط.")
    except Exception as e:
        logger.error(f"خطأ في جلب بيانات {symbol} من Binance: {e}")

def main_loop():
    client = Client("", "") # أضف مفاتيحك هنا
    while True:
        symbols = load_symbols_from_file()
        for s in symbols:
            analyze_symbol(s, client)
        time.sleep(3600)

if __name__ == '__main__':
    Thread(target=main_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=5000)
