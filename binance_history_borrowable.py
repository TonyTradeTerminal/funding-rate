import requests

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


import os
import json
import csv
import time
from datetime import datetime
from binance.client import Client
from tqdm import tqdm

# 配置相关参数
acct = "tt16"
config_path = '/data/market_maker/Binance/tt16.json'
data_dir = '/data/market_maker/Binance/borrow_data/'

# 确保数据目录存在
os.makedirs(data_dir, exist_ok=True)

# 读取配置文件
with open(config_path, "r") as config_file:
    key_sec = json.load(config_file)

api_key = key_sec[acct][0]
api_secret = key_sec[acct][1]
client = Client(api_key, api_secret)


while True:
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for coin in tqdm(common_coins):
        try:
            response = client.papi_get_margin_max_borrowable(asset=coin)
            max_borrowable = response.get('amount')
        except Exception as e:
            max_borrowable = f"Error: {e}"
        
        csv_file = os.path.join(data_dir, f"{coin}.csv")
        
        file_exists = os.path.exists(csv_file)
        
        with open(csv_file, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["Time", "Symbol", "Max_Borrowable"])
            writer.writerow([current_time, coin, max_borrowable])
            
    time.sleep(60)