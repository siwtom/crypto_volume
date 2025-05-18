# Ryuryu's Bybit Trades and OHLC Saver
# Redis Edition (Production Mode #6973)
# -------------------------------------
# (c) 2023 Ryan Hayabusa 
# GitGub: https://github.com/ryu878
# Web: https://aadresearch.xyz
# Discord: https://discord.gg/zSw58e9Uvf
# Telegram: https://t.me/aadresearch
# -------------------------------------
# Instructions:
# docker run -d --name redis-stack -p 6379:6379 -p 8001:8001 redis/redis-stack:latest
# Redis instance: http://localhost:8001
# Release the port if used:
# sudo fuser -k 6379/tcp

import time
import redis
import datetime
from pybit.unified_trading import HTTP, WebSocket
from config import interval, redis_host, redis_port, redis_db, redis_trade_ttl, redis_kline_ttl, endpoint, domain
# Your API credentials
API_KEY = "VQpBubtPJY1kG9Ic2Y"
API_SECRET = "cSBDXfMkv63CsGHobxz33PEVpt4QzfQ3PJlS"
r = redis.Redis(host=redis_host,port=redis_port,db=redis_db)
ws = WebSocket(
    testnet=False, 
    api_key=API_KEY,
    api_secret=API_SECRET,
    channel_type="linear"
)

session_unauth = HTTP(testnet=False)
max_leverage = session_unauth.get_instruments_info(category="linear")

assets = []

for leverage in max_leverage['result']['list']:
    symbol = leverage['symbol']
    leverage_value = float(leverage['leverageFilter']['maxLeverage'])
    if leverage_value >= 50 and 'USDT' in symbol:
        assets.append(symbol)
    print('----------- List Assets ---------------\n')
    print(assets)

def get_trades(trades):
    """Process and store trade data in Redis"""
    try:
        data = trades.get('data')
        if not data:
            return
            
        # Handle both single trade and trade arrays
        trade_list = data if isinstance(data, list) else [data]
        
        for trade in trade_list:
            symbol = trade['s']  # symbol
            price = float(trade['p'])  # price
            side = trade['S']  # side (Buy/Sell)
            size = float(trade['v'])  # volume/quantity
            timestamp = int(trade['T'])  # timestamp
            
            # Create a more comprehensive trade key with timestamp
            trade_key = f"{symbol}:trade:{timestamp}"
            
            # Store latest trade data
            r.hset(str(symbol), mapping={
                'price': price,
                'side': side,
                'size': size,
                'timestamp': timestamp
            })
            r.expire(str(symbol), redis_trade_ttl)
            
            # Store detailed trade
            r.hset(trade_key, mapping={
                'symbol': symbol,
                'price': price,
                'side': side,
                'size': size,
                'timestamp': timestamp,
                'liquidity': trade['L']  # liquidity type
            })
            r.expire(trade_key, redis_trade_ttl)
            
            # Add to time-series list (most recent 1000 trades)
            r.lpush(f"{symbol}:trades", trade_key)
            r.ltrim(f"{symbol}:trades", 0, 999)
            
            print(f"Trade: {symbol} {price} {side} {size}")
    except Exception as e:
        print(f"Error processing trade: {e}")
        print(f"Trade data: {trades}")

for asset in assets:
    ws.trade_stream(
        callback=get_trades,
        symbol=asset
    )

def get_ohcl(ohcl):
    symbol = ohcl['topic'].replace('kline.1.','')
    kline_data = ohcl['data'][0]
    timestamp_ms = kline_data['start']
    timestamp_ms = datetime.datetime.fromtimestamp(timestamp_ms/1000)
    timestamp_ms = timestamp_ms.strftime('%Y-%m-%d %H:%M:00') 
    
    r.hset(symbol+'-'+timestamp_ms, 'symbol', symbol)
    r.hset(symbol+'-'+timestamp_ms, 'open_time', int(kline_data['start']))
    r.hset(symbol+'-'+timestamp_ms, 'open', kline_data['open'])
    r.hset(symbol+'-'+timestamp_ms, 'high', kline_data['high'])
    r.hset(symbol+'-'+timestamp_ms, 'low', kline_data['low'])
    r.hset(symbol+'-'+timestamp_ms, 'close', kline_data['close'])
    r.hset(symbol+'-'+timestamp_ms, 'volume', kline_data['volume'])
    r.execute_command('expire', str(symbol+'-'+timestamp_ms), redis_kline_ttl)
    
    print(' Kline:',symbol,timestamp_ms)

ws.kline_stream(
    callback=get_ohcl,
    symbol=assets,
    interval=interval
)

while True:
    time.sleep(1)
