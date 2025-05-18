# Improved and optimized version of Ryan Hayabusa's Bybit data collector

import time
import redis
import datetime
import logging
from pybit.unified_trading import HTTP, WebSocket
from threading import Thread
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bybit_collector.log")
    ]
)
logger = logging.getLogger("BybitCollector")

# Configuration - would typically be imported from config.py
API_KEY = "VQpBubtPJY1kG9Ic2Y"
API_SECRET = "cSBDXfMkv63CsGHobxz33PEVpt4QzfQ3PJlS"
redis_host = "localhost"
redis_port = 6379
redis_db = 0
redis_trade_ttl = 3600 * 6  # 6h
redis_kline_ttl = 86400 * 1  # 1 days
interval = "1"  # 1 minute candles

# Initialize Redis connection with connection pooling
try:
    redis_pool = redis.ConnectionPool(host=redis_host, port=redis_port, db=redis_db)
    r = redis.Redis(connection_pool=redis_pool)
    r.ping()  # Test connection
    logger.info("Redis connection established")
except Exception as e:
    logger.error(f"Failed to connect to Redis: {e}")
    sys.exit(1)

# Initialize HTTP and WebSocket connections
session_unauth = HTTP(testnet=False)
ws = WebSocket(
    testnet=False, 
    api_key=API_KEY,
    api_secret=API_SECRET,
    channel_type="linear"
)

# Track connection status and enable auto-reconnect
connected = True
last_message_time = time.time()
TIMEOUT_SECONDS = 30

def get_eligible_assets():
    """Get USDT-paired assets with leverage >= 50"""
    try:
        max_leverage = session_unauth.get_instruments_info(category="linear")
        assets = []
        
        for leverage in max_leverage['result']['list']:
            symbol = leverage['symbol']
            leverage_value = float(leverage['leverageFilter']['maxLeverage'])
            if leverage_value >= 50 and 'USDT' in symbol:
                assets.append(symbol)
        
        logger.info(f"Found {len(assets)} eligible assets")
        return assets
    except Exception as e:
        logger.error(f"Error getting eligible assets: {e}")
        return []

# Callback for trade data
def get_trades(trades, limit = 1000000):
    """Process and store trade data in Redis"""
    try:
        global last_message_time
        last_message_time = time.time()
        
        data = trades.get('data')
        if not data:
            return
            
        # Handle both single trade and trade arrays
        trade_list = data if isinstance(data, list) else [data]
        
        for trade in trade_list:
            #print(trade)
            symbol = trade['s']
            price = trade['p']
            side = trade['S']
            size = trade['v']
            timestamp = trade['T']
            
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
                'timestamp': timestamp
            })
            r.expire(trade_key, redis_trade_ttl)
            
            # Add to time-series list (most recent 1000 trades)
            r.lpush(f"{symbol}:trades", trade_key)
            r.ltrim(f"{symbol}:trades", 0, limit)
            
            logger.debug(f"Trade: {symbol} {price} {side} {size}")
    except Exception as e:
        logger.error(f"Error processing trade: {e}")

# Callback for OHLC data
def get_ohlc(ohlc):
    """Process and store OHLC/kline data in Redis"""
    try:
        global last_message_time
        last_message_time = time.time()
        
        topic = ohlc.get('topic', '')
        if not topic.startswith('kline.'):
            return
            
        symbol = topic.replace(f'kline.{interval}.', '')
        kline_data = ohlc.get('data', [{}])[0]
        
        if not kline_data:
            return
            
        timestamp_ms = kline_data.get('start')
        if not timestamp_ms:
            return
            
        dt = datetime.datetime.fromtimestamp(timestamp_ms/1000)
        timestamp_str = dt.strftime('%Y-%m-%d %H:%M:00')
        candle_key = f"{symbol}:{interval}:{timestamp_str}"
        
        # Store kline data with mapping for better performance
        r.hset(candle_key, mapping={
            'symbol': symbol,
            'interval': interval,
            'open_time': int(kline_data['start']),
            'open': kline_data['open'],
            'high': kline_data['high'],
            'low': kline_data['low'],
            'close': kline_data['close'],
            'volume': kline_data['volume'],
            'turnover': kline_data.get('turnover', '0')
        })
        r.expire(candle_key, redis_kline_ttl)
        
        # Store time-ordered list of candles (keeps latest 1000 candles)
        r.lpush(f"{symbol}:klines:{interval}", candle_key)
        r.ltrim(f"{symbol}:klines:{interval}", 0, 999)
        
        # Update latest candle reference
        r.set(f"{symbol}:latest:{interval}", candle_key)
        
        logger.debug(f"Kline: {symbol} {timestamp_str}")
    except Exception as e:
        logger.error(f"Error processing kline: {e}")

def monitor_connection():
    """Monitor WebSocket connection and reconnect if necessary"""
    global connected, last_message_time
    
    while True:
        time.sleep(5)
        current_time = time.time()
        
        if current_time - last_message_time > TIMEOUT_SECONDS:
            logger.warning("No messages received recently, reconnecting WebSockets...")
            try:
                # Reset WebSocket connection
                ws.exit()
                time.sleep(1)
                
                # Reinitialize WebSocket
                #global ws
                ws = WebSocket(
                    testnet=False, 
                    api_key=API_KEY,
                    api_secret=API_SECRET,
                    channel_type="linear"
                )
                
                # Resubscribe to streams
                setup_data_streams()
                
                last_message_time = time.time()
                logger.info("Successfully reconnected to WebSockets")
            except Exception as e:
                logger.error(f"Failed to reconnect: {e}")

def setup_data_streams():
    """Set up all data stream subscriptions"""
    try:
        # Get eligible assets
        assets = get_eligible_assets()
        if not assets:
            logger.error("No eligible assets found, retrying in 10 seconds...")
            time.sleep(10)
            return setup_data_streams()
            
        logger.info(f"Setting up data streams for {len(assets)} assets")
        
        # Subscribe to trade streams for all assets
        for asset in assets:
            try:
                ws.trade_stream(
                    callback=get_trades,
                    symbol=asset
                )
                logger.info(f"Subscribed to trade stream for {asset}")
            except Exception as e:
                logger.error(f"Failed to subscribe to trade stream for {asset}: {e}")
        
        # Subscribe to kline streams
        try:
            ws.kline_stream(
                callback=get_ohlc,
                symbol=assets,
                interval=interval
            )
            logger.info(f"Subscribed to {interval}m kline streams for all assets")
        except Exception as e:
            logger.error(f"Failed to subscribe to kline streams: {e}")
            
        # Store asset list in Redis
        r.delete("bybit:assets")
        r.sadd("bybit:assets", *assets)
        r.set("bybit:last_update", int(time.time()))
        
        return True
    except Exception as e:
        logger.error(f"Error setting up data streams: {e}")
        return False

if __name__ == "__main__":
    try:
        logger.info("Starting Bybit data collector")
        
        # Setup data streams
        setup_success = setup_data_streams()
        if not setup_success:
            logger.error("Failed initial setup, exiting")
            sys.exit(1)
            
        # Start connection monitor in a separate thread
        monitor_thread = Thread(target=monitor_connection, daemon=True)
        monitor_thread.start()
        
        logger.info("Data collector running, press Ctrl+C to exit")
        
        # Main loop - keep the program running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        ws.exit()
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}")
        ws.exit()
        sys.exit(1)