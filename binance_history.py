import os
import json
import csv
import time
import requests
from tqdm import tqdm
from datetime import datetime
from binance.client import Client

# 获取 Binance 现货的所有交易对信息
spot_url = "https://api.binance.com/api/v3/exchangeInfo"
r = requests.get(spot_url)
spot_info = r.json()
spot_coins = {item['baseAsset'] for item in spot_info.get('symbols', [])}

future_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
r = requests.get(future_url)
future_info = r.json()
future_coins = set()
for item in future_info.get('symbols', []):
    if item.get('quoteAsset') == 'USDT':
        future_coins.add(item['baseAsset'])

# 求交集，得到同时支持现货和期货的币种
common_coins = list(spot_coins.intersection(future_coins))

# 设置路径和 API
acct = "tt16"
config_path = '/data/market_maker/Binance/tt16.json'
data_dir = '/data/market_maker/Binance/history_data/'  # 合并数据保存路径
os.makedirs(data_dir, exist_ok=True)

# 获取 API key
with open(config_path, "r") as f:
    key_sec = json.load(f)

client = Client(key_sec[acct][0], key_sec[acct][1])

# CSV 文件列
header = ["time", "symbol", "max_borrowable", "spot_best_bid", "spot_best_ask", "future_best_bid", "future_best_ask"]

# 确保每个币种有独立的文件并写入表头
def ensure_csv_header(csv_file):
    if not os.path.exists(csv_file):
        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)

# 获取订单簿函数
def get_orderbook(endpoint, symbol):
    url = f"{endpoint}?symbol={symbol}USDT&limit=5"
    try:
        res = requests.get(url).json()
        best_bid = float(res['bids'][0][0]) if res.get('bids') else None
        best_ask = float(res['asks'][0][0]) if res.get('asks') else None
        return best_bid, best_ask
    except Exception as e:
        print(f"{symbol} orderbook error: {e}")
        return None, None

# 主循环
while True:
    timestamp = time.time()  # float timestamp
    for coin in tqdm(common_coins):
        # 获取最大可借量
        try:
            borrow_data = client.papi_get_margin_max_borrowable(asset=coin)
            max_borrowable = float(borrow_data.get("amount", 0))

        except Exception as e:
            pass


        # 获取现货/期货 orderbook
        spot_best_bid, spot_best_ask = get_orderbook("https://api.binance.com/api/v3/depth", coin)
        future_best_bid, future_best_ask = get_orderbook("https://fapi.binance.com/fapi/v1/depth", coin)

        # 保存数据
        csv_path = os.path.join(data_dir, f"{coin}.csv")
        ensure_csv_header(csv_path)
        with open(csv_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([timestamp, coin, max_borrowable, spot_best_bid, spot_best_ask, future_best_bid, future_best_ask])
    
    time.sleep(60)
