# Script to add Volume data to the MySQL database using Binance Websocket.
import json
import time
from matplotlib.pyplot import axes
import pymysql
import asyncio
import bybit_config
import pandas as pd
from dbutils.pooled_db import PooledDB
from time import sleep
from colorama import Fore, Style
# Set up logging (optional)
import logging
logging.basicConfig(filename="pybit.log", level=logging.DEBUG,
                    format="%(asctime)s %(levelname)s %(message)s")

cfg = bybit_config
host = cfg.host
port = cfg.port
user = cfg.user
password = cfg.password
database = cfg.database

list_symbol = ['BTCUSDT',
'ETHUSDT',
'EOSUSDT',
'XRPUSDT',
'BCHUSDT',
'LTCUSDT',
'LINKUSDT',
'ADAUSDT',
'DOTUSDT',
'DOGEUSDT',
'MATICUSDT',
'ETCUSDT',
'BNBUSDT',
'SOLUSDT',
'AXSUSDT',
'SANDUSDT',
'ATOMUSDT',
'AVAXUSDT',
'FTMUSDT',
'NEARUSDT',
'BITUSDT',
'GALAUSDT',
'APEUSDT',
'GMTUSDT',
'GSTUSDT',
'GALUSDT',
'FITFIUSDT']
#My SQL configuration
MYSQL_CONFIG = {
    "host": host,
    "port": port,
    "db": database,
    "password": password,
    "user": user,
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": True,
}

POOL_CONFIG = {
    # Modules using linked databases
    "creator": pymysql,
    # Maximum connections allowed for connection pool,
    # 0 and None Indicates unlimited connections
    "maxconnections": 6,
    # At least idle links created in the link pool during initialization,
    # 0 means not to create
    "mincached": 2,
    # The most idle links in the link pool,
    # 0 and None No restriction
    "maxcached": 5,
    # The maximum number of links shared in the link pool,
    # 0 and None Represents all shares.
    # PS: It's useless because pymysql and MySQLdb Equal module threadsafety
    # All are 1, no matter how many values are set,_maxcached Always 0,
    # so always all links are shared.
    "maxshared": 3,
    # If there is no connection available in the connection pool,
    # whether to block waiting. True，Waiting;
    # False，Don't wait and report an error
    "blocking": True,
    # The maximum number of times a link is reused,
    # None Indicates unlimited
    "maxusage": None,
    # List of commands executed before starting a session.
    # Such as:["set datestyle to ...", "set time zone ..."]
    "setsession": [],
    # ping MySQL Server, check whether the service is available.
    # # For example: 0 = None = never,
    # 1 = default = whenever it is requested,
    # 2 = when a cursor is created,
    # 4 = when a query is executed,
    # 7 = always
    "ping": 0,
}

POOL = PooledDB(**MYSQL_CONFIG, **POOL_CONFIG)

class SqlPooled:
    def __init__(self):
        self._connection = POOL.connection()
        self._cursor = self._connection.cursor()

    def fetch_one(self, sql, args):
        self._cursor.execute(sql, args)
        result = self._cursor.fetchone()
        return result

    def fetch_all(self, sql, args):
        self._cursor.execute(sql, args)
        result = self._cursor.fetchall()
        return result

    def __del__(self):
        self._connection.close()
    
    def insert_one(self, sql, args):
        result = self._cursor.execute(sql, args)
        return result
    
    def delete(self, sql, args):
        result = self._cursor.execute(sql, args)
        return result


# Using the class in a function that creates a table in the localdb
obj = SqlPooled()

print('XRP Bybit Volume data getting from the database...')

def calculate_volume_total(interval = 1):
    total_df = pd.DataFrame(columns=['symbol', 'price', 'total_delta', 'average_volume', 'total_volume', 'average_notation', 'total_notation', 'delta_current_min', 'delta_pre_1min'])
    for _symbol in list_symbol:
        symbol = "'" + _symbol + "'"
        try:
            #From interval mins
            sql = f'SELECT `symbol`, `buy_vol`, `sel_vol`, `tot_vol`, `price`  FROM `bybit_volume` WHERE time > NOW() - INTERVAL {interval} MINUTE AND symbol like {symbol}'
            result = obj.fetch_all(sql, None)
            result_df = pd.DataFrame(result, columns=['symbol', 'buy_vol', 'sel_vol', 'tot_vol', 'price'])
            result_df['total_buy'] = round(result_df['buy_vol'].sum(),2)
            result_df['total_sell'] = round(result_df['sel_vol'].sum(),2)
            result_df['total_delta'] = round(result_df['total_buy'] - result_df['total_sell'],2)
            result_df['total_volume'] = round(result_df['tot_vol'].sum(),2)
            result_df['average_volume'] = round(result_df['total_volume']/interval, 2)
            result_df['total_notation'] = round(result_df['total_volume'] * result_df['price'], 2)
            result_df['average_notation'] = round(result_df['total_notation'] / interval, 2)
            result_df.drop(columns=['tot_vol'], axis=1, inplace=True)
            #Get last min
            #----------Get last 3 bar volume
            sql_last_3_min = f'SELECT bv.`symbol`, bv.`buy_vol`, bv.`sel_vol`, bv.`tot_vol`, bv.`price`, bv.`trade_timestamp`,ohcl.`start`, ohcl.`end`   FROM `bybit_volume` bv\
                JOIN (select distinct(start), end, symbol from `bybit_ochl` bo \
                where bo.symbol like {symbol} ORDER BY start desc LIMIT 3) ohcl\
                ON (bv.symbol = ohcl.symbol AND bv.trade_timestamp <= ohcl.end AND bv.trade_timestamp >= ohcl.start) \
                WHERE bv.symbol like {symbol}'
            result_last_3min = obj.fetch_all(sql_last_3_min, None)
            result_df_last_3_min = pd.DataFrame(result_last_3min, columns=['symbol', 'buy_vol', 'sel_vol', 'tot_vol', 'price', 'trade_timestamp', 'start', 'end'])
            #print(result_df_last_3_min)
            result_df_last_3_min.fillna(0, inplace=True)
            result_df_last_3_min['total_notation'] = round(result_df_last_3_min['tot_vol'] * result_df_last_3_min['price'],2)
            result_df_last_3_min = result_df_last_3_min.groupby(['start', 'end'])[["buy_vol", "sel_vol", "tot_vol", "total_notation"]].apply(sum).reset_index()
            result_df_last_3_min['total_delta'] = round((result_df_last_3_min['buy_vol'] - result_df_last_3_min['sel_vol']),2)
            #print(result_df_last_3_min.iloc[[0]])
            #---------
            result_df['total_vol_3min'] = round(result_df_last_3_min['tot_vol'].sum(),0)
            result_df['total_notation_current'] = round(result_df_last_3_min['total_notation'].sum(),0)
            result_df['delta_last_min'] = round(result_df_last_3_min.iloc[0]['total_delta'],0)
            result_df['delta_last_2min'] = round(result_df_last_3_min.iloc[1]['total_delta'],0)
            result_df['delta_last_3min'] = round(result_df_last_3_min.iloc[2]['total_delta'],0)
            result_df = result_df.iloc[[-1]]
            #print(result_df)
            total_df = pd.concat([total_df,result_df], ignore_index=True, sort=False)
            #del result_df
        except Exception as ex:
            print('Error', ex)
            pass
    #print(bcolors.WARNING + "Warning: No active frommets remain. Continue?" + bcolors.ENDC)
    #print(total_df['total_notation'])
    #return 0
    print(Style.RESET_ALL)
    print("-"*145)
    print(("{: ^12}|{: ^10}|{: ^10}|{: ^13}|{: ^15}|{: >10}|{: >11}|{: >18}|{: >10}|{: >10}|")
          .format('symbol', 'price', f'delta ({interval})', f'ave vol ({interval})',  f'total vol({interval})',  'total vol(3)', 'total $ cur',
                  'cur delta(3-in-row)', f'ave $ ({interval})', f'total $ ({interval})'))
    print("-"*145)
    for index, row in total_df.iterrows():
        print("{: >16}|{: >10}|{: >10}|{: >13}|{: >15}|{: >12}|{: >10} $|{: >10} {: >1} {: >1} {: >1} |{: >10}|{: >9}|".format(Style.RESET_ALL + row['symbol'], row['price'], row['total_delta'], row['average_volume'],  row['total_volume'],
                                                                                           row['total_vol_3min'], row['total_notation_current'], row['delta_last_min'],
            Fore.GREEN + '🞕' + Style.RESET_ALL if row['delta_last_3min'] > 0 else ( Fore.RED + '🞕' + Style.RESET_ALL  if row['delta_last_3min'] < 0 else '🞕'), 
            Fore.GREEN + '🞕' + Style.RESET_ALL if row['delta_last_2min'] > 0 else ( Fore.RED + '🞕' + Style.RESET_ALL if row['delta_last_2min'] < 0 else '🞕'), 
            Fore.GREEN + '🞕' + Style.RESET_ALL if row['delta_last_min'] > 0 else ( Fore.RED + '🞕' + Style.RESET_ALL if row['delta_last_min'] < 0 else '🞕'),
            Fore.GREEN + "{: >10}".format(int(row['average_notation'])) + Style.RESET_ALL if  row['average_notation'] > 50000 else "{: >10}".format(int(row['average_notation'])),
            Fore.GREEN + "{: >8} $".format(int(row['total_notation'])) + Style.RESET_ALL if  row['total_notation'] > 500000 else "{: >8} $".format(int(row['total_notation']))
              ))
    #del total_df
    
async def main():
    while True:
        calculate_volume_total(20)
        sleep(1)
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    
