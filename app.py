# -*- coding: utf-8 -*-
# ملف app.py - نسخة فريم الـ 4 ساعات (استراتيجيات عالية الاحتمالية) مجهزة لـ Render و n8n مع قراءة ديناميكية للعملات

import time
import os
import json
import logging
import requests
import secrets
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from threading import Thread, Lock
from flask import Flask, jsonify, render_template_string, request
from flask_cors import CORS
from binance.client import Client

# --- إعداد نظام التسجيل (Logging) ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('CryptoBot_4H_Pro')

# --- تهيئة تطبيق Flask وعقد الأمان مع n8n ---
app = Flask(__name__)
CORS(app)

BOT_API_KEY = os.getenv("BOT_API_KEY", "2009Hamza@")
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "")
PORT = int(os.getenv("PORT", 5000))

# إعدادات فريم الـ 4 ساعات
TIMEFRAME = "4h"  
LOOKBACK_DAYS = "90 day ago UTC"  # نحتاج 90 يوماً لجمع شموع كافية لفريم 4 ساعات (حوالي 540 شمعة)

print(f"🔑 Your BOT_API_KEY for n8n configuration is: {BOT_API_KEY}")

# --- المتغيرات المشتركة والـ Locks لحماية البيانات ---
open_signals_cache = {}
IS_TRADING_ENABLED = True
signal_cache_lock = Lock()
trading_status_lock = Lock()

# اسم الملف النصي الذي يحتوي على قائمة العملات المراقبة
SYMBOLS_FILE_NAME = "crypto_list.txt"

# --- دالة قراءة العملات من الملف النصي وتنسيقها تلقائياً ---
def load_symbols_from_file(filename=SYMBOLS_FILE_NAME) -> list:
    """قراءة رموز العملات من ملف نصي وتنسيقها لتنتهي بـ USDT تلقائياً"""
    default_symbols = ["BTC", "ETH", "SOL", "BNB", "LINK", "AVAX"]
    
    # إذا لم يكن الملف موجوداً، نقوم بإنشائه تلقائياً بالعملات الافتراضية كحماية للسيرفر
    if not os.path.exists(filename):
        try:
            with open(filename, "w", encoding="utf-8") as f:
                for sym in default_symbols:
                    f.write(f"{sym}\n")
            logger.info(f"📝 Created default '{filename}' file with baseline symbols.")
        except Exception as e:
            logger.error(f"❌ Failed to create default symbols file: {e}")
            return [f"{s}USDT" for s in default_symbols]

    symbols = []
    try:
        with open(filename, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip().upper()
                # تجاهل السطور الفارغة أو السطور المخصصة للتعليقات
                if not line or line.startswith("#") or line.startswith("//"):
                    continue
                
                # تنسيق الرمز تلقائياً لإضافة زوج USDT إذا لم يكن مكتوباً
                if not line.endswith("USDT"):
                    formatted_symbol = f"{line}USDT"
                else:
                    formatted_symbol = line
                
                symbols.append(formatted_symbol)
                
        # إزالة التكرار إن وجد مع الحفاظ على الترتيب
        symbols = list(dict.fromkeys(symbols))
        logger.info(f"✅ Active monitored symbols dynamically reloaded: {symbols}")
        return symbols
    except Exception as e:
        logger.error(f"❌ Error reading '{filename}': {e}. Using baseline fallback.")
        return [f"{s}USDT" for s in default_symbols]

# --- دالة إرسال التنبيهات الفورية إلى n8n ---
def send_alert_to_n8n(event_type, data):
    """ إرسال بيانات الصفقات والإشارات إلى n8n لمعالجتها بالذكاء الاصطناعي """
    if not N8N_WEBHOOK_URL:
        logger.warning("[n8n] Webhook URL is not configured. Alert skipped.")
        return False
    
    payload = {
        "event": event_type,
        "bot_version": "4H_Pro_V1",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": data
    }
    try:
        response = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=5)
        if response.status_code == 200:
            logger.info(f"🔔 [n8n] Alert sent successfully: {event_type}")
            return True
        else:
            logger.error(f"❌ [n8n] Failed to send alert. Status code: {response.status_code}")
    except Exception as e:
        logger.error(f"❌ [n8n] Connection error to n8n Webhook: {e}")
    return False

# --- جدار الحماية (Middleware) لطلبات n8n ---
def require_api_key(f):
    def decorated(*args, **kwargs):
        api_key = request.headers.get("X-Bot-API-Key") or request.args.get("api_key")
        if not api_key or api_key != BOT_API_KEY:
            return jsonify({"status": "error", "message": "Unauthorized access."}), 401
        return f(*args, **kwargs)
    decorated.__name__ = f.__name__
    return decorated

# =====================================================================
# محرك التحليل الفني والاستراتيجيات لفريم 4 ساعات (High Probability)
# =====================================================================

def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """ حساب المؤشرات الفنية الثقيلة لفريم 4 ساعات """
    if len(df) < 200:
        return df

    # المتوسطات المتحركة لتحديد الاتجاه العام
    df['ema_21'] = df['close'].ewm(span=21, adjust=False).mean()
    df['ema_50'] = df['close'].ewm(span=50, adjust=False).mean()
    df['ema_200'] = df['close'].ewm(span=200, adjust=False).mean()

    # مؤشر RSI (الزخم)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    rs = gain / (loss + 1e-10)
    df['rsi_14'] = 100 - (100 / (1 + rs))

    # مؤشر MACD
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = exp1 - exp2
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = df['macd'] - df['macd_signal']

    # بولينجر باندز (للانعكاسات)
    df['bb_middle'] = df['close'].rolling(window=20).mean()
    df['bb_std'] = df['close'].rolling(window=20).std()
    df['bb_upper'] = df['bb_middle'] + (df['bb_std'] * 2.5)  # 2.5 لالتقاط الانحرافات الشديدة فقط
    df['bb_lower'] = df['bb_middle'] - (df['bb_std'] * 2.5)

    # مؤشر ATR (متوسط المدى الحقيقي) لحساب المخاطرة والأهداف
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['atr_14'] = true_range.ewm(alpha=1/14, adjust=False).mean()
    
    # متوسط حجم التداول لـ 20 شمعة (لتأكيد الاختراقات)
    df['vol_ma_20'] = df['volume'].rolling(window=20).mean()

    return df

def check_strategies_4h(symbol: str, df: pd.DataFrame):
    """ فحص 3 استراتيجيات عالية الاحتمالية وتوليد التوصيات """
    if df.empty or 'atr_14' not in df.columns:
        return

    # نأخذ آخر شمعة مكتملة تماماً (قبل الأخيرة) لضمان ثبات المؤشرات
    last = df.iloc[-2]
    prev = df.iloc[-3]
    
    close_price = float(last['close'])
    atr = float(last['atr_14'])
    strategy_triggered = None
    direction = None

    # التحقق من عدم وجود صفقة مفتوحة لنفس العملة
    with signal_cache_lock:
        if symbol in open_signals_cache:
            return

    # --- استراتيجية 1: التوافق الذهبي (Golden Confluence) ---
    # الشروط: اتجاه صاعد قوي (EMA50 > EMA200)، السعر يصحح ويلمس EMA21، وتقاطع الماكد إيجابي
    uptrend_strong = last['ema_50'] > last['ema_200']
    pullback_to_ema21 = last['low'] <= last['ema_21'] and close_price > last['ema_21']
    macd_bullish_cross = last['macd'] > last['macd_signal'] and prev['macd'] <= prev['macd_signal']
    rsi_healthy = 45 < last['rsi_14'] < 65

    if uptrend_strong and pullback_to_ema21 and macd_bullish_cross and rsi_healthy:
        strategy_triggered = "Golden Confluence (Trend Continuation)"
        direction = "BUY"

    # --- استراتيجية 2: الانعكاس العنيف (Extreme Mean Reversion) ---
    # الشروط: السعر يكسر الحد السفلي لبولينجر بقوة، مع RSI تحت 25 (تشبع بيعي حاد جداً)
    pierced_lower_bb = last['close'] < last['bb_lower']
    extreme_oversold = last['rsi_14'] < 25
    reversal_candle = last['close'] > last['open']  # شمعة خضراء أغلقت إيجابية

    if pierced_lower_bb and extreme_oversold and reversal_candle:
        strategy_triggered = "Extreme Mean Reversion (Oversold Bounce)"
        direction = "BUY"

    # --- استراتيجية 3: اختراق السيولة المدعوم بالحجم (Volume Breakout) ---
    # الشروط: السعر يخترق مقاومة EMA50 بحجم تداول أعلى من المتوسط بـ 200% وزخم قوي
    price_breakout = prev['close'] < prev['ema_50'] and close_price > last['ema_50']
    massive_volume = last['volume'] > (last['vol_ma_20'] * 2.0)
    strong_momentum = last['rsi_14'] > 60

    if price_breakout and massive_volume and strong_momentum:
        strategy_triggered = "Liquidity Volume Breakout"
        direction = "BUY"

    # في حال تحقق إحدى الاستراتيجيات، نقوم بفتح التوصية
    if strategy_triggered and direction:
        execute_4h_trade(symbol, close_price, direction, atr, strategy_triggered)

def execute_4h_trade(symbol: str, entry_price: float, direction: str, atr: float, strategy_name: str):
    """ إدارة المخاطر لفريم 4 ساعات (أهداف بعيدة ووقف خسارة منطقي) """
    
    # في فريم 4 ساعات، نستخدم ATR * 2 لوقف الخسارة لتجنب ضرب الوقف بسبب الذبذبات
    if direction == "BUY":
        stop_loss = entry_price - (atr * 2.0)
        target_1 = entry_price + (atr * 3.0)
        target_2 = entry_price + (atr * 6.0)
    else:
        stop_loss = entry_price + (atr * 2.0)
        target_1 = entry_price - (atr * 3.0)
        target_2 = entry_price - (atr * 6.0)

    trade_signal = {
        "symbol": symbol,
        "direction": direction,
        "strategy": strategy_name,
        "entry_price": round(entry_price, 4),
        "stop_loss": round(stop_loss, 4),
        "target_1": round(target_1, 4),
        "target_2": round(target_2, 4),
        "risk_reward_ratio": "1:3",
        "time": datetime.now(timezone.utc).isoformat()
    }

    with signal_cache_lock:
        open_signals_cache[symbol] = trade_signal

    logger.info(f"🎯 [4H SIGNAL] {direction} | {symbol} | Strategy: {strategy_name}")
    
    # إرسال التوصية إلى n8n
    send_alert_to_n8n("NEW_4H_SIGNAL", trade_signal)


# =====================================================================
# المحركات الخلفية (Background Loops)
# =====================================================================

def main_bot_loop():
    """ محرك الفحص الرئيسي، يشتغل كل فترة للبحث عن صفقات جديدة """
    try:
        client = Client(os.getenv("BINANCE_API_KEY", ""), os.getenv("BINANCE_API_SECRET", ""))
    except Exception as e:
        logger.error(f"❌ Failed to initialize Binance client: {e}")
        return

    while True:
        with trading_status_lock:
            enabled = IS_TRADING_ENABLED
        
        if enabled:
            # إعادة تحميل قائمة العملات ديناميكياً من الملف في بداية كل دورة فحص
            current_symbols = load_symbols_from_file()
            
            logger.info(f"⏳ [Engine] Scanning 4H Charts for {len(current_symbols)} active symbols...")
            for symbol in current_symbols:
                try:
                    # جلب البيانات لآخر 90 يوم على فريم 4 ساعات
                    klines = client.get_historical_klines(symbol, TIMEFRAME, LOOKBACK_DAYS)
                    if not klines or len(klines) < 200:
                        continue
                    
                    df = pd.DataFrame(klines, columns=[
                        'time', 'open', 'high', 'low', 'close', 'volume', 
                        'close_time', 'qav', 'num_trades', 'taker_base', 'taker_quote', 'ignore'
                    ])
                    # تنظيف البيانات وتحويل الأنواع
                    for col in ['open', 'high', 'low', 'close', 'volume']:
                        df[col] = df[col].astype(float)
                    
                    # معالجة المؤشرات وتدقيق الاستراتيجيات
                    df = calculate_indicators(df)
                    check_strategies_4h(symbol, df)
                    
                    time.sleep(1)  # حماية من حظر الـ API Rate Limit
                    
                except Exception as e:
                    logger.error(f"❌ [Engine Error] Failed to scan {symbol}: {e}")
                    
        # فحص دوري كل 15 دقيقة
        time.sleep(60 * 15)

# =====================================================================
# واجهات الـ API ولوحة التحكم (Dashboard)
# =====================================================================

@app.route('/api/v1/status', methods=['GET'])
@require_api_key
def get_bot_status():
    """ واجهة فحص حالة البوت لـ n8n """
    current_symbols = load_symbols_from_file()
    with trading_status_lock:
        status = {
            "is_running": IS_TRADING_ENABLED,
            "timeframe": "4 Hours",
            "active_signals": len(open_signals_cache),
            "monitored_symbols": current_symbols
        }
    return jsonify({"status": "success", "data": status}), 200

@app.route('/api/v1/control', methods=['POST'])
@require_api_key
def control_bot():
    """ واجهة تشغيل وإيقاف البوت من n8n """
    global IS_TRADING_ENABLED
    data = request.get_json() or {}
    action = data.get("action")
    
    with trading_status_lock:
        if action == "start": 
            IS_TRADING_ENABLED = True
        elif action == "stop": 
            IS_TRADING_ENABLED = False
        else: 
            return jsonify({"error": "Invalid action"}), 400
            
    return jsonify({"status": "success", "is_running": IS_TRADING_ENABLED}), 200

# تمكين إرسال إشارات يدوية اختيارياً لاختبار التوصيل مع n8n فوراً
@app.route('/api/v1/external-trigger', methods=['GET', 'POST'])
@require_api_key
def external_trigger():
    symbol = request.args.get("symbol", "BTCUSDT")
    direction = request.args.get("direction", "BUY")
    
    # حساب قيم افتراضية مبنية على آخر سعر
    entry_price = 65000.0 if "BTC" in symbol else 3500.0
    atr = entry_price * 0.02
    
    execute_4h_trade(symbol, entry_price, direction, atr, "Manual External Trigger (Test)")
    return jsonify({
        "status": "success", 
        "message": f"Test signal triggered for {symbol}",
        "entry_price": entry_price
    }), 200

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <title>4H Pro Trading Bot</title>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #0b1120; color: #f8fafc; padding: 40px; text-align: center; }
        .container { max-width: 800px; margin: 0 auto; }
        .card { background: #1e293b; border-radius: 12px; padding: 30px; box-shadow: 0 10px 15px -3px rgba(0,0,0,0.3); border: 1px solid #334155; }
        h1 { color: #38bdf8; margin-top: 0; }
        .badge { background: #059669; color: white; padding: 5px 15px; border-radius: 20px; font-weight: bold; font-size: 14px;}
        .badge.stopped { background: #e11d48; }
        .stats { display: flex; justify-content: space-around; margin: 30px 0; padding: 20px; background: #0f172a; border-radius: 8px; }
        .stat-item { font-size: 18px; }
        .stat-value { font-size: 28px; font-weight: bold; color: #fbbf24; display: block; margin-top: 10px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="card">
            <h1>روبوت التداول الاحترافي (فريم 4 ساعات) 📈</h1>
            <p>يستخدم استراتيجيات عالية الاحتمالية مبنية على السيولة والزخم المؤسسي.</p>
            
            <div style="margin: 20px 0;">
                <span class="badge {% if not status.is_running %}stopped{% endif %}">
                    الحالة: {{ 'يعمل ويبحث عن صفقات' if status.is_running else 'متوقف مؤقتاً' }}
                </span>
            </div>

            <div class="stats">
                <div class="stat-item">
                    الإطار الزمني
                    <span class="stat-value">4 ساعات</span>
                </div>
                <div class="stat-item">
                    التوصيات النشطة
                    <span class="stat-value">{{ status.active_signals }}</span>
                </div>
                <div class="stat-item">
                    العملات المراقبة
                    <span class="stat-value">{{ status.monitored_symbols|length }}</span>
                </div>
            </div>
            
            <p style="color: #94a3b8; font-size: 14px;">تم دمج البوت بالكامل مع n8n ويعمل بنظام Webhooks.</p>
        </div>
    </div>
</body>
</html>
"""

@app.route('/', methods=['GET'])
def index():
    """ لوحة القيادة البسيطة للمتصفح """
    current_symbols = load_symbols_from_file()
    with trading_status_lock:
        status = {
            "is_running": IS_TRADING_ENABLED,
            "active_signals": len(open_signals_cache),
            "monitored_symbols": current_symbols
        }
    return render_template_string(DASHBOARD_HTML, status=status)

if __name__ == '__main__':
    # تشغيل محرك الفحص في الخلفية
    Thread(target=main_bot_loop, daemon=True).start()
    
    logger.info(f"🚀 Starting 4H Pro Bot on Port {PORT}...")
    app.run(host='0.0.0.0', port=PORT)
