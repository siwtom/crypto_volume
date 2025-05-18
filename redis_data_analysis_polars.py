#!/usr/bin/env python3
# Redis Data Analysis for Bybit Market Data using Polars instead of Pandas
# This script analyzes trade and candle data stored in Redis by the Bybit data collector

import redis
import polars as pl
import numpy as np
import time
import datetime
import logging
import sys
import json
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("market_analysis.log")
    ]
)
logger = logging.getLogger("MarketAnalysis")

# Redis connection settings (match the collector's settings)
redis_host = "localhost"
redis_port = 6379
redis_db = 0

# Connect to Redis
try:
    redis_pool = redis.ConnectionPool(host=redis_host, port=redis_port, db=redis_db)
    r = redis.Redis(connection_pool=redis_pool)
    r.ping()  # Test connection
    logger.info("Redis connection established")
except Exception as e:
    logger.error(f"Failed to connect to Redis: {e}")
    sys.exit(1)

# Time intervals for analysis
INTERVALS = {
    "1min": 60,
    "5min": 300,
    "15min": 900,
    "30min": 1800,
    "1hour": 3600,
    "4hour": 14400,
    "1day": 86400
}

def get_all_assets():
    """Get all assets that are tracked in Redis"""
    try:
        # Check if we have the assets list stored
        if r.exists("bybit:assets"):
            assets = list(r.smembers("bybit:assets"))
            assets = [asset.decode('utf-8') for asset in assets]
            logger.info(f"Retrieved {len(assets)} assets from Redis")
            return assets
        
        # Otherwise, scan Redis for keys to identify assets
        logger.info("Asset list not found, scanning Redis for assets...")
        all_keys = []
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match="*:trades", count=1000)
            all_keys.extend(keys)
            if cursor == 0:
                break
        
        # Extract asset names from keys
        assets = set()
        for key in all_keys:
            key_str = key.decode('utf-8')
            asset = key_str.split(':')[0]
            assets.add(asset)
        
        logger.info(f"Found {len(assets)} assets by scanning Redis")
        return list(assets)
    except Exception as e:
        logger.error(f"Error getting assets: {e}")
        return []

def get_recent_trades(symbol, limit=1000000, time_limit = 30):
    """Get the most recent trades for a symbol from the last 30 minutes"""
    try:
        # Get trade keys
        trade_keys = r.lrange(f"{symbol}:trades", 0, limit-1)
        if not trade_keys:
            logger.warning(f"No trades found for {symbol}")
            return []
        
        # Calculate timestamp for 30 minutes ago
        thirty_mins_ago = int(time.time() * 1000) - (time_limit * 60 * 1000)
        
        trades = []
        for key in trade_keys:
            key_str = key.decode('utf-8')
            trade_data = r.hgetall(key_str)
            if trade_data:
                # Convert bytes to string for all values and ensure proper numeric types
                trade_dict = {
                    k.decode('utf-8'): v.decode('utf-8') for k, v in trade_data.items()
                }
                # Convert numeric fields to float
                trade_dict['price'] = float(trade_dict['price'])
                trade_dict['size'] = float(trade_dict['size'])
                trade_dict['timestamp'] = float(trade_dict['timestamp'])
                
                # Only include trades from the last 30 minutes
                if trade_dict['timestamp'] >= thirty_mins_ago:
                    trades.append(trade_dict)
        
        logger.info(f"Retrieved {len(trades)} trades for {symbol} from the last 30 minutes")
        return trades
    except Exception as e:
        logger.error(f"Error getting trades for {symbol}: {e}")
        return []

def get_klines(symbol, interval="1", limit=1000):
    """Get candlestick data for a symbol"""
    try:
        # Get candle keys
        candle_keys = r.lrange(f"{symbol}:klines:{interval}", 0, limit-1)
        if not candle_keys:
            logger.warning(f"No klines found for {symbol} with interval {interval}")
            return []
        
        candles = []
        for key in candle_keys:
            key_str = key.decode('utf-8')
            candle_data = r.hgetall(key_str)
            if candle_data:
                # Convert bytes to string for all values
                candle_dict = {k.decode('utf-8'): v.decode('utf-8') for k, v in candle_data.items()}
                candles.append(candle_dict)
        
        logger.info(f"Retrieved {len(candles)} klines for {symbol} with interval {interval}")
        return candles
    except Exception as e:
        logger.error(f"Error getting klines for {symbol}: {e}")
        return []

def calculate_volume_metrics(trades_data):
    """Calculate volume metrics from trade data using Polars"""
    if not trades_data:
        return None
    
    # Convert to Polars DataFrame for easier analysis
    df = pl.from_dicts(trades_data)
    
    # Ensure proper data types and handle potential string values
    df = df.with_columns([
        pl.col("timestamp").cast(pl.Float64),
        pl.col("price").cast(pl.Float64),
        pl.col("size").cast(pl.Float64)
    ])
    
    # Calculate dollar volume after ensuring numeric types
    df = df.with_columns([
        (pl.col("price") * pl.col("size")).alias("dollar_volume")
    ])
    
    # Calculate buy/sell metrics
    buy_trades = df.filter(pl.col("side") == "Buy")
    sell_trades = df.filter(pl.col("side") == "Sell")
    
    # Calculate metrics
    metrics = {
        'total_trades': len(df),
        'buy_trades': len(buy_trades),
        'sell_trades': len(sell_trades),
        'total_volume': df.select(pl.sum("size")).item(),
        'buy_volume': buy_trades.select(pl.sum("size")).item() if not buy_trades.is_empty() else 0,
        'sell_volume': sell_trades.select(pl.sum("size")).item() if not sell_trades.is_empty() else 0,
        'total_dollar_volume': df.select(pl.sum("dollar_volume")).item(),
        'buy_dollar_volume': buy_trades.select(pl.sum("dollar_volume")).item() if not buy_trades.is_empty() else 0,
        'sell_dollar_volume': sell_trades.select(pl.sum("dollar_volume")).item() if not sell_trades.is_empty() else 0,
        'avg_price': df.select(pl.mean("price")).item(),
        'latest_price': df.select(pl.col("price"))[0, 0] if not df.is_empty() else None,
        'max_price': df.select(pl.max("price")).item(),
        'min_price': df.select(pl.min("price")).item(),
    }
    
    # Calculate volume delta
    metrics['volume_delta'] = metrics['buy_volume'] - metrics['sell_volume']
    metrics['dollar_delta'] = metrics['buy_dollar_volume'] - metrics['sell_dollar_volume']
    
    # Calculate delta percentages
    total_volume = metrics['total_volume']
    total_dollar = metrics['total_dollar_volume']
    sell_volume = metrics['sell_volume']
    total_dollar_sell = metrics['sell_dollar_volume']
    
    if sell_volume > 0:
        metrics['volume_delta_pct'] = (metrics['volume_delta'] / sell_volume) * 100
    else:
        metrics['volume_delta_pct'] = 1000
        
    if total_dollar_sell > 0:
        metrics['dollar_delta_pct'] = (metrics['dollar_delta'] / total_dollar_sell) * 100
    else:
        metrics['dollar_delta_pct'] = 1000
    
    return metrics

def calculate_time_based_metrics(trades_data, interval_seconds):
    """Calculate metrics based on time intervals using Polars"""
    if not trades_data:
        return []
    
    # Convert to Polars DataFrame
    df = pl.from_dicts(trades_data)
    
    # Ensure proper data types and calculate dollar volume
    df = df.with_columns([
        pl.col("timestamp").cast(pl.Float64),
        pl.col("price").cast(pl.Float64),
        pl.col("size").cast(pl.Float64)
    ])
    
    # Calculate dollar volume after ensuring numeric types
    df = df.with_columns([
        (pl.col("price") * pl.col("size")).alias("dollar_volume")
    ])
    
    # Sort by timestamp (newest first)
    df = df.sort("timestamp", descending=True)
    
    # Get current time in milliseconds
    current_time = int(time.time() * 1000)
    
    # Calculate number of intervals to analyze (e.g., last 24 hours in hourly intervals)
    num_intervals = min(24, len(df) // 10)  # At least 10 trades per interval or maximum 24 intervals
    
    results = []
    
    for i in range(num_intervals):
        start_time = current_time - ((i + 1) * interval_seconds * 1000)
        end_time = current_time - (i * interval_seconds * 1000)
        
        # Filter trades for this time bucket
        bucket_df = df.filter((pl.col("timestamp") >= start_time) & (pl.col("timestamp") < end_time))
        
        if bucket_df.is_empty():
            continue
            
        buy_df = bucket_df.filter(pl.col("side") == "Buy")
        sell_df = bucket_df.filter(pl.col("side") == "Sell")
        
        start_datetime = datetime.datetime.fromtimestamp(start_time/1000)
        end_datetime = datetime.datetime.fromtimestamp(end_time/1000)
        
        # Get open and close prices
        open_price = bucket_df.select(pl.col("price")).tail(1)[0, 0] if not bucket_df.is_empty() else 0
        close_price = bucket_df.select(pl.col("price")).head(1)[0, 0] if not bucket_df.is_empty() else 0
        
        interval_metrics = {
            'interval_start': start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            'interval_end': end_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            'total_trades': len(bucket_df),
            'buy_trades': len(buy_df),
            'sell_trades': len(sell_df),
            'total_volume': bucket_df.select(pl.sum("size")).item(),
            'buy_volume': buy_df.select(pl.sum("size")).item() if not buy_df.is_empty() else 0,
            'sell_volume': sell_df.select(pl.sum("size")).item() if not sell_df.is_empty() else 0,
            'total_dollar_volume': bucket_df.select(pl.sum("dollar_volume")).item(),
            'buy_dollar_volume': buy_df.select(pl.sum("dollar_volume")).item() if not buy_df.is_empty() else 0,
            'sell_dollar_volume': sell_df.select(pl.sum("dollar_volume")).item() if not sell_df.is_empty() else 0,
            'avg_price': bucket_df.select(pl.mean("price")).item(),
            'open_price': open_price,
            'close_price': close_price,
            'high_price': bucket_df.select(pl.max("price")).item(),
            'low_price': bucket_df.select(pl.min("price")).item(),
        }
        
        # Calculate volume delta
        interval_metrics['volume_delta'] = interval_metrics['buy_volume'] - interval_metrics['sell_volume']
        interval_metrics['dollar_delta'] = interval_metrics['buy_dollar_volume'] - interval_metrics['sell_dollar_volume']
        
        # Calculate delta percentages
        total_volume = interval_metrics['total_volume']
        total_dollar = interval_metrics['total_dollar_volume']
        sell_volume = interval_metrics['sell_volume']
        total_dollar_sell = interval_metrics['sell_dollar_volume']
        
        if sell_volume > 0:
            interval_metrics['volume_delta_pct'] = (interval_metrics['volume_delta'] / sell_volume) * 100
        else:
            interval_metrics['volume_delta_pct'] = 1000
            
        if total_dollar_sell > 0:
            interval_metrics['dollar_delta_pct'] = (interval_metrics['dollar_delta'] / total_dollar_sell) * 100
        else:
            interval_metrics['dollar_delta_pct'] = 1000
            
        # Add price change percentage
        if open_price > 0:
            interval_metrics['price_change_pct'] = ((close_price - open_price) / open_price) * 100
        else:
            interval_metrics['price_change_pct'] = 0
            
        results.append(interval_metrics)
    
    return results

def analyze_market_direction(trades_by_asset, min_volume=100):
    """Analyze market direction based on cumulative deltas"""
    if not trades_by_asset:
        return {}
    
    # Calculate buy/sell metrics and delta for each asset
    results = {}
    
    for symbol, trades in trades_by_asset.items():
        metrics = calculate_volume_metrics(trades)
        if not metrics:
            continue
            
        # Only include assets with sufficient volume
        if metrics['total_dollar_volume'] < min_volume:
            continue
        
        # Determine market sentiment based on delta
        if metrics['dollar_delta_pct'] > 10:
            sentiment = "Strong Buy"
        elif metrics['dollar_delta_pct'] > 5:
            sentiment = "Buy"
        elif metrics['dollar_delta_pct'] > 0:
            sentiment = "Weak Buy"
        elif metrics['dollar_delta_pct'] > -5:
            sentiment = "Weak Sell"
        elif metrics['dollar_delta_pct'] > -10:
            sentiment = "Sell"
        else:
            sentiment = "Strong Sell"
            
        results[symbol] = {
            'price': metrics['latest_price'],
            'total_volume': metrics['total_volume'],
            'dollar_volume': metrics['total_dollar_volume'],
            'buy_volume': metrics['buy_volume'],
            'sell_volume': metrics['sell_volume'],
            'delta': metrics['volume_delta'],
            'delta_pct': metrics['volume_delta_pct'],
            'dollar_delta': metrics['dollar_delta'],
            'dollar_delta_pct': metrics['dollar_delta_pct'],
            'sentiment': sentiment
        }
    
    return results

def analyze_volume_distribution(trades_by_asset):
    """Analyze volume distribution and identify market leaders"""
    if not trades_by_asset:
        return {}
    
    # Calculate dollar volume for each asset
    volume_data = {}
    total_market_volume = 0
    
    for symbol, trades in trades_by_asset.items():
        metrics = calculate_volume_metrics(trades)
        if not metrics:
            continue
            
        volume_data[symbol] = {
            'dollar_volume': metrics['total_dollar_volume'],
            'buy_volume': metrics['buy_dollar_volume'],
            'sell_volume': metrics['sell_dollar_volume'],
            'delta': metrics['dollar_delta'],
            'delta_pct': metrics['dollar_delta_pct'],
            'avg_price': metrics['avg_price']
        }
        
        total_market_volume += metrics['total_dollar_volume']
    
    # Calculate market share for each asset
    for symbol in volume_data:
        if total_market_volume > 0:
            volume_data[symbol]['market_share'] = (volume_data[symbol]['dollar_volume'] / total_market_volume) * 100
        else:
            volume_data[symbol]['market_share'] = 0
    
    # Sort by market share
    sorted_data = dict(sorted(
        volume_data.items(), 
        key=lambda x: x[1]['dollar_volume'], 
        reverse=True
    ))
    
    return sorted_data

def get_market_snapshot(interval_seconds=1800):
    """Get a comprehensive market snapshot"""
    try:
        # Get all assets
        assets = get_all_assets()
        if not assets:
            logger.error("No assets found in Redis")
            return None
        
        # Get trades for all assets
        trades_by_asset = {}
        for symbol in assets:
            trades = get_recent_trades(symbol)
            if trades:
                trades_by_asset[symbol] = trades
        
        # Analyze market direction
        market_direction = analyze_market_direction(trades_by_asset)
        
        # Analyze volume distribution
        volume_distribution = analyze_volume_distribution(trades_by_asset)
        
        # Calculate overall market stats
        total_market_volume = sum(data['dollar_volume'] for data in volume_distribution.values())
        total_buy_volume = sum(data['buy_volume'] for data in volume_distribution.values())
        total_sell_volume = sum(data['sell_volume'] for data in volume_distribution.values())
        total_delta = total_buy_volume - total_sell_volume
        
        if total_sell_volume > 0:
            delta_pct = (total_delta / total_sell_volume) * 100
        else:
            delta_pct = 10000
        
        # Determine overall market sentiment
        if delta_pct > 10:
            market_sentiment = "Strong Buy"
        elif delta_pct > 5:
            market_sentiment = "Buy"
        elif delta_pct > 0:
            market_sentiment = "Weak Buy"
        elif delta_pct > -5:
            market_sentiment = "Weak Sell"
        elif delta_pct > -10:
            market_sentiment = "Sell"
        else:
            market_sentiment = "Strong Sell"
        
        # Calculate time-based metrics for top assets
        top_assets = list(volume_distribution.keys())[:10]  # Top 10 by volume
        time_metrics = {}
        
        for symbol in top_assets:
            metrics = calculate_time_based_metrics(trades_by_asset[symbol], interval_seconds)
            if metrics:
                time_metrics[symbol] = metrics
        
        # Compile the snapshot
        snapshot = {
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_assets': len(assets),
            'total_market_volume': total_market_volume,
            'total_buy_volume': total_buy_volume,
            'total_sell_volume': total_sell_volume,
            'market_delta': total_delta,
            'market_delta_pct': delta_pct,
            'market_sentiment': market_sentiment,
            'volume_distribution': volume_distribution,
            'market_direction': market_direction,
            'time_metrics': time_metrics
        }
        
        return snapshot
    except Exception as e:
        logger.error(f"Error generating market snapshot: {e}")
        return None

def format_market_report(snapshot):
    """Format market snapshot data into a readable report"""
    if not snapshot:
        return "No market data available"
    
    report = []
    report.append(f"Market Report - {snapshot['timestamp']}")
    report.append("=" * 50)
    
    # Overall market stats
    report.append("\nOVERALL MARKET")
    report.append("-" * 20)
    report.append(f"Total Assets: {snapshot['total_assets']}")
    report.append(f"Total Market Volume: ${snapshot['total_market_volume']:.2f}")
    report.append(f"Market Delta: ${snapshot['market_delta']:.2f} ({snapshot['market_delta_pct']:.2f}%)")
    report.append(f"Market Sentiment: {snapshot['market_sentiment']}")
    
    # Top assets by volume
    report.append("\nTOP ASSETS BY VOLUME")
    report.append("-" * 20)
    
    for i, (symbol, data) in enumerate(list(snapshot['volume_distribution'].items())[:10], 1):
        report.append(f"{i}. {symbol}")
        report.append(f"   Price: ${data['avg_price']:.6f}")
        report.append(f"   Volume: ${data['dollar_volume']:.2f}")
        report.append(f"   Market Share: {data['market_share']:.2f}%")
        report.append(f"   Delta: {data['delta_pct']:.2f}%")
    
    # Assets with strongest buy pressure
    buy_pressure = sorted(
        snapshot['market_direction'].items(),
        key=lambda x: x[1]['delta_pct'],
        reverse=True
    )[:5]
    
    report.append("\nSTRONGEST BUY PRESSURE")
    report.append("-" * 20)
    
    for symbol, data in buy_pressure:
        report.append(f"{symbol}")
        report.append(f"   Price: ${data['price']:.6f}")
        report.append(f"   Volume: ${data['dollar_volume']:.2f}")
        report.append(f"   Delta: {data['delta_pct']:.2f}%")
        report.append(f"   Sentiment: {data['sentiment']}")
    
    # Assets with strongest sell pressure
    sell_pressure = sorted(
        snapshot['market_direction'].items(),
        key=lambda x: x[1]['delta_pct']
    )[:5]
    
    report.append("\nSTRONGEST SELL PRESSURE")
    report.append("-" * 20)
    
    for symbol, data in sell_pressure:
        report.append(f"{symbol}")
        report.append(f"   Price: ${data['price']:.6f}")
        report.append(f"   Volume: ${data['dollar_volume']:.2f}")
        report.append(f"   Delta: {data['delta_pct']:.2f}%")
        report.append(f"   Sentiment: {data['sentiment']}")
    
    return "\n".join(report)

def calculate_large_trades(trades_by_asset, threshold_usd=25000):
    """Identify large trades above a threshold using Polars"""
    if not trades_by_asset:
        return {}
        
    large_trades = {}
    
    for symbol, trades in trades_by_asset.items():
        if not trades:
            continue
            
        # Convert to Polars DataFrame
        df = pl.from_dicts(trades)
        if df.is_empty():
            continue
            
        # Add dollar value column and ensure proper types
        df = df.with_columns([
            pl.col("timestamp").cast(pl.Float64),
            pl.col("price").cast(pl.Float64),
            pl.col("size").cast(pl.Float64),
            (pl.col("price") * pl.col("size")).alias("dollar_value")
        ])
        
        # Filter for large trades
        large_df = df.filter(pl.col("dollar_value") >= threshold_usd)
        
        if not large_df.is_empty():
            # Add datetime column
            large_df = large_df.with_columns([
                pl.col("timestamp").map_elements(
                    lambda x: datetime.datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S')
                ).alias("datetime")
            ])
            
            # Convert to list of dictionaries
            large_trades_list = large_df.select([
                "datetime", "side", "price", "size", "dollar_value"
            ]).to_dicts()
            
            large_trades[symbol] = large_trades_list
    
    return large_trades

def generate_volume_heatmap(trades_by_asset):
    """Generate volume heatmap data using Polars"""
    if not trades_by_asset:
        return {}
        
    # Time buckets - 24 hours in hourly intervals
    buckets = 24
    bucket_size = 3600 * 1000  # 1 hour in milliseconds
    current_time = int(time.time() * 1000)
    
    heatmap_data = {}
    
    for symbol, trades in trades_by_asset.items():
        if not trades:
            continue
            
        # Convert to Polars DataFrame
        df = pl.from_dicts(trades)
        if df.is_empty():
            continue
            
        # Ensure proper data types
        df = df.with_columns([
            pl.col("timestamp").cast(pl.Float64),
            pl.col("price").cast(pl.Float64),
            pl.col("size").cast(pl.Float64)
        ])
        
        # Calculate dollar volume after ensuring numeric types
        df = df.with_columns([
            (pl.col("price") * pl.col("size")).alias("dollar_volume")
        ])
        
        # Create time buckets
        time_buckets = {}
        
        for i in range(buckets):
            start_time = current_time - ((i + 1) * bucket_size)
            end_time = current_time - (i * bucket_size)
            
            # Filter trades for this bucket
            bucket_df = df.filter((pl.col("timestamp") >= start_time) & (pl.col("timestamp") < end_time))
            
            if not bucket_df.is_empty():
                # Group by side and calculate volumes
                buy_volume = bucket_df.filter(pl.col("side") == "Buy").select(pl.sum("dollar_volume")).item()
                sell_volume = bucket_df.filter(pl.col("side") == "Sell").select(pl.sum("dollar_volume")).item()
                
                bucket_time = datetime.datetime.fromtimestamp(start_time/1000).strftime('%H:00')
                
                time_buckets[bucket_time] = {
                    'buy_volume': buy_volume,
                    'sell_volume': sell_volume,
                    'total_volume': buy_volume + sell_volume,
                    'net_volume': buy_volume - sell_volume
                }
        
        if time_buckets:
            heatmap_data[symbol] = time_buckets
    
    return heatmap_data

def analyze_historical_volume_patterns(trades_by_asset):
    """Analyze historical volume patterns to identify trends using Polars"""
    if not trades_by_asset:
        return {}
        
    results = {}
    
    for symbol, trades in trades_by_asset.items():
        if not trades:
            continue
            
        # Convert to Polars DataFrame
        df = pl.from_dicts(trades)
        if df.is_empty():
            continue
            
        # Ensure proper data types
        df = df.with_columns([
            pl.col("timestamp").cast(pl.Float64),
            pl.col("price").cast(pl.Float64),
            pl.col("size").cast(pl.Float64)
        ])
        
        # Calculate dollar volume after ensuring numeric types
        df = df.with_columns([
            (pl.col("price") * pl.col("size")).alias("dollar_volume"),
            pl.col("timestamp").map_elements(
                lambda x: datetime.datetime.fromtimestamp(x/1000)
            ).alias("datetime")
        ])
        
        # Sort by datetime
        df = df.sort("datetime")
        
        # Define timeframes with their durations in seconds
        timeframes = {
            '5min': 5*60,
            '15min': 15*60,
            '1H': 60*60,
            '4H': 4*60*60,
            '1D': 24*60*60
        }
        
        # Calculate volume trends
        volume_trends = {}
        
        for timeframe_name, timeframe_seconds in timeframes.items():
            try:
                # Add a timeframe_group column
                df = df.with_columns([
                    (pl.col("timestamp") / (timeframe_seconds * 1000)).floor().alias(f"group_{timeframe_name}")
                ])
                
                # Group by the timeframe
                grouped = df.group_by(f"group_{timeframe_name}").agg([
                    pl.sum("dollar_volume").alias("total_volume"),
                    pl.min("datetime").alias("start_time")
                ]).sort("start_time")
                
                if len(grouped) >= 5:  # Need at least 5 periods for SMA
                    # Calculate volume SMA using window functions
                    volume_sma = grouped.select([
                        "start_time",
                        "total_volume",
                        pl.col("total_volume").rolling_mean(window_size=5).alias("volume_sma")
                    ])
                    
                    # Get the last entry
                    last_row = volume_sma.tail(1)
                    
                    if not last_row.is_empty():
                        last_volume = last_row[0, "total_volume"]
                        last_sma = last_row[0, "volume_sma"] 
                        
                        if last_sma is not None and last_sma != 0:
                            if last_volume > last_sma * 1.5:
                                trend = "Strong Above Average"
                            elif last_volume > last_sma:
                                trend = "Above Average"
                            elif last_volume > last_sma * 0.75:
                                trend = "Below Average"
                            else:
                                trend = "Low Volume"
                        else:
                            trend = "Insufficient Data"
                        
                        volume_trends[timeframe_name] = {
                            'current_volume': float(last_volume),
                            'average_volume': float(last_sma) if last_sma is not None else 0,
                            'trend': trend
                        }
                else:
                    volume_trends[timeframe_name] = {
                        'current_volume': 0,
                        'average_volume': 0,
                        'trend': "Insufficient Data"
                    }
                    
            except Exception as e:
                logger.error(f"Error calculating volume trends for {symbol} on {timeframe_name}: {e}")
                volume_trends[timeframe_name] = {
                    'current_volume': 0,
                    'average_volume': 0,
                    'trend': "Error in Calculation"
                }
            
        if volume_trends:
            results[symbol] = volume_trends
    
    return results

def main():
    """Main function to run the analysis"""
    try:
        logger.info("Starting Redis data analysis with Polars")
        
        # Generate market snapshot
        snapshot = get_market_snapshot(interval_seconds=1800)  # 30-minute intervals
        
        if snapshot:
            # Print market report
            report = format_market_report(snapshot)
            print(report)
            
            # Get all assets and their trades
            assets = get_all_assets()
            trades_by_asset = {}
            
            for symbol in assets:
                trades = get_recent_trades(symbol)
                if trades:
                    trades_by_asset[symbol] = trades
            
            # Identify large trades
            large_trades = calculate_large_trades(trades_by_asset)
            if large_trades:
                print("\nLARGE TRADES DETECTED")
                print("-" * 20)
                
                for symbol, trades in large_trades.items():
                    print(f"\n{symbol} - {len(trades)} large trades")
                    for trade in trades[:5]:  # Show the first 5 large trades
                        print(f"  {trade['datetime']} - {trade['side']} ${trade['dollar_value']:.2f}")
            
            # Generate volume heatmap
            heatmap_data = generate_volume_heatmap(trades_by_asset)
            
            # Analyze volume patterns
            volume_patterns = analyze_historical_volume_patterns(trades_by_asset)
            
            # Example of using the calculated data
            logger.info("Analysis complete")
            
            # Save results to JSON files
            with open('market_snapshot.json', 'w') as f:
                json.dump(snapshot, f, indent=2)
                
            with open('large_trades.json', 'w') as f:
                json.dump(large_trades, f, indent=2)
                
            with open('volume_heatmap.json', 'w') as f:
                json.dump(heatmap_data, f, indent=2)
                
            with open('volume_patterns.json', 'w') as f:
                json.dump(volume_patterns, f, indent=2)
            
            logger.info("Results saved to JSON files")
            
        else:
            logger.error("Failed to generate market snapshot")
            
    except KeyboardInterrupt:
        logger.info("Analysis interrupted by user")
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()