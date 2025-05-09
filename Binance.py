import os
import json
import time
import hmac
import hashlib
import requests
import pandas as pd
from tqdm import tqdm
from binance.client import Client
from datetime import datetime
import argparse

# ---- 工具函数 ----
# Function to get 24h spot volume data
def get_spot_24h_volume(symbol):
    ticker = client.get_ticker(symbol=symbol)
    return ticker['quoteVolume']

# Function to get 24h futures volume data
def get_futures_24h_volume(symbol):
    ticker = client.futures_ticker(symbol=symbol)
    return ticker['quoteVolume']

def get_orderbook(endpoint, symbol):
    url = f"{endpoint}?symbol={symbol}USDT&limit=5"
    try:
        res = requests.get(url).json()
        best_bid = float(res['bids'][0][0]) if res.get('bids') else None
        best_ask = float(res['asks'][0][0]) if res.get('asks') else None
        return best_bid, best_ask
    except:
        return None, None

def get_funding_rate_and_interval(symbol):
    # 获取资金费用数据
    funding_rates = client.futures_funding_rate(symbol=symbol, limit = 2)
    
    if len(funding_rates) >= 2:
        # 获取前两次 funding rate 数据
        funding_time_1 = int(funding_rates[0]['fundingTime']) / 1000  # 毫秒转秒
        funding_time_2 = int(funding_rates[1]['fundingTime']) / 1000  # 毫秒转秒
        funding_rate = float(funding_rates[0]['fundingRate'])  # 当前 funding rate
        
        # 计算时间间隔，单位为小时
        funding_interval_hours = (funding_time_2 - funding_time_1) / 3600
        
        # 返回结果：当前时间的 funding rate 和 funding interval
        return funding_rate, funding_interval_hours
    else:
        return None

def get_margin_interest_rate(asset, vipLevel=0):
    path = '/sapi/v1/margin/interestRateHistory'
    timestamp = int(time.time() * 1000)
    params = {
        'asset': asset,
        'vipLevel': vipLevel,
        'timestamp': timestamp,
        'limit': 1
    }
    query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
    signature = hmac.new(API_SECRET.encode(), query_string.encode(), hashlib.sha256).hexdigest()
    headers = {'X-MBX-APIKEY': API_KEY}
    url = f"{BASE_URL}{path}?{query_string}&signature={signature}"
    res = requests.get(url, headers=headers)
    if res.status_code == 200:
        try:
            return float(res.json()[0]['dailyInterestRate'])
        except:
            return None
    return None


# ---- 主程序 ----
if __name__ == "__main__":
    # 直接在这里解析命令行参数
    parser = argparse.ArgumentParser(description="Binance Market Data Script")
    parser.add_argument('--acct', type=str, required=True, help="Account name (e.g., 'tt16')")
    parser.add_argument('--config_path', type=str, required=True, help="Path to the configuration JSON file")
    args = parser.parse_args()

    # ---- 获取 API Key ----
    with open(args.config_path, "r") as f:
        key_sec = json.load(f)

    API_KEY = key_sec[args.acct][0]
    API_SECRET = key_sec[args.acct][1]
    client = Client(API_KEY, API_SECRET)

    BASE_URL = 'https://api.binance.com'

    # ---- 获取现货和期货支持的币种 ----
    spot_info = requests.get("https://api.binance.com/api/v3/exchangeInfo").json()
    spot_coins = {item["baseAsset"] for item in spot_info.get("symbols", []) if item.get("status") == 'TRADING'}

    future_info = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo").json()
    future_coins = {item['baseAsset'] for item in future_info.get('symbols', []) if item.get('quoteAsset') == 'USDT' and item.get("status") == 'TRADING'}

    common_coins = list(spot_coins.intersection(future_coins))

    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    data = []

    for coin in tqdm(common_coins):
        try:
            # 获取最大可借量
            borrow_data = client.papi_get_margin_max_borrowable(asset=coin)
            max_borrowable = float(borrow_data.get("amount", 0))

            # 获取现货和期货订单簿
            spot_best_bid, spot_best_ask = get_orderbook("https://api.binance.com/api/v3/depth", coin)
            future_best_bid, future_best_ask = get_orderbook("https://fapi.binance.com/fapi/v1/depth", coin)

            spot_24h_volume = get_spot_24h_volume(symbol=f'{coin}USDT')
            futures_24h_volume = get_futures_24h_volume(symbol=f'{coin}USDT')

            # 获取 funding interval
            funding_rate, funding_interval_hours = get_funding_rate_and_interval(symbol=f'{coin}USDT')

            # 获取 daily interest rate
            daily_interest_rate = get_margin_interest_rate(coin)

            # 添加一行数据
            data.append({
                'Time': timestamp,
                'coin': coin,
                'funding_rate': float(funding_rate),
                'Borrowable': float(max_borrowable),
                'spot_24h_volume': float(spot_24h_volume),
                'futures_24h_volume': float(futures_24h_volume),
                'spot_best_bid': float(spot_best_bid),
                'spot_best_ask': float(spot_best_ask),
                'future_best_bid': float(future_best_bid),
                'future_best_ask': float(future_best_ask),
                'funding_interval': float(funding_interval_hours),
                'daily_interest_rate': float(daily_interest_rate)
            })

        except Exception as e:
            pass

        time.sleep(0.1)

    # ---- 创建 DataFrame ----
    df = pd.DataFrame(data)

    df['funding_rate_8h'] = (df['funding_rate'] / df['funding_interval']) * 8.0
    df['interest_rate_8h'] = df['daily_interest_rate'] / 3

    df['forward_side_spread'] = (df['future_best_bid'] / df['spot_best_ask']) - 1
    df['reverse_side_spread'] = (df['spot_best_bid'] / df['future_best_ask']) - 1
    df['forward_side_profit'] = df['forward_side_spread'] + df['funding_rate_8h']
    df['reverse_side_profit'] = df['reverse_side_spread'] - df['funding_rate_8h'] - df['interest_rate_8h']

    df['borrowable_value'] = (df['Borrowable'] * ((df['spot_best_bid'] + df['spot_best_ask']) / 2).fillna(0)).round()

    df = df.sort_values("funding_rate", ascending=True)
    df = df[(df['spot_24h_volume'] >= 100000) & (df['futures_24h_volume'] >= 100000)]


    df = df[['Time', 'coin', 'funding_rate', 'funding_interval', 'funding_rate_8h','interest_rate_8h', 'spot_24h_volume', 'futures_24h_volume', 'spot_best_bid','spot_best_ask','future_best_bid','future_best_ask', 'forward_side_spread', 'reverse_side_spread', 'forward_side_profit', 'reverse_side_profit', 'borrowable_value']]


    df = df.rename(columns={
    'funding_rate': 'fr',
    'funding_interval': 'fi',    
    'funding_rate_8h': 'fr_8h',
    'interest_rate_8h': 'int_8h',
    'spot_24h_volume': 'spt_vol',
    'futures_24h_volume': 'ftr_vol',
    'spot_best_bid': 'spt_bid',
    'spot_best_ask': 'spt_ask',
    'future_best_bid': 'ftr_bid',
    'future_best_ask': 'ftr_ask',
    'forward_side_spread': 'spr_fwd',
    'reverse_side_spread': 'spr_rvs',
    'forward_side_profit': 'profit_fwd',
    'reverse_side_profit': 'profit_rvs',
    })

    print(df.head(60))
    print('================================')  

    print(df[df["fr"] > 0].sort_values("profit_fwd", ascending=False).head(60))

    print('================================')  

    print(df[df["fr"] < 0].sort_values("profit_rvs", ascending=False).head(60))

    print('================================')  

    # ---- 展示与保存 ----
    df.to_csv("./data/all_coin_data_binance.csv", index=False)

