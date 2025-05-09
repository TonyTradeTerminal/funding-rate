import requests
import time
import hashlib
import hmac
import requests
import pandas as pd
import os
from tqdm import tqdm
import datetime
import argparse
import json

host = "https://api.gateio.ws"
prefix = "/api/v4"
headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

# 获取现货币种列表
spot_url = '/spot/currencies'
spot_data = requests.get(host + prefix + spot_url, headers=headers).json()
spot_set = {item['currency'] for item in spot_data if 'currency' in item and item['trade_disabled'] == False}

# 获取合约币种列表
future_url = '/futures/usdt/contracts'
future_data = requests.get(host + prefix + future_url, headers=headers).json()
future_processed = {item['name'].replace('_USDT', '') for item in future_data if item['name'].endswith("USDT")}

# 找出现货 & 合约共同币种
common_coins = list(spot_set & future_processed)

# 一次性获取行情数据
spot_tickers = requests.get(host + prefix + '/spot/tickers', headers=headers).json()
spot_volumes = {item['currency_pair'].replace('_USDT', ''): float(item['quote_volume']) 
                for item in spot_tickers if item['currency_pair'].endswith('_USDT')}

future_tickers = requests.get(host + prefix + '/futures/usdt/tickers', headers=headers).json()
future_volumes = {item['contract'].replace('_USDT', ''): float(item['volume_24h_quote']) 
                  for item in future_tickers if item['contract'].endswith('_USDT')}

# 筛选成交量大于 100000 的币种
filtered_common = [
    coin for coin in common_coins
    if spot_volumes.get(coin, 0) >= 100000 and future_volumes.get(coin, 0) >= 100000
]

print("Filtered common coins count:", len(filtered_common))
print("Filtered coins:", filtered_common)


class gate_io:
    def __init__(self, account, config_path, symbol):
        self.symbol = symbol.upper()            
        self.host = "https://api.gateio.ws"
        self.prefix = "/api/v4"
        self.base_headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

        with open(config_path, "r") as f:
            all_accounts = json.load(f)

        if account not in all_accounts:
            raise ValueError(f"Account '{account}' not found in config file.")

        self.key = all_accounts[account][0]
        self.secret = all_accounts[account][1]

        if not self.key or not self.secret:
            raise ValueError(f"Missing api_key or api_secret for account {account}")

    def get_futures_contract_info(self):
        """
        获取期货合约数据，返回 funding_rate 和 funding_interval
        """
        symbol = f"{self.symbol}_USDT"
        url = f"{self.host}{self.prefix}/futures/usdt/contracts/{symbol}"
        try:
            r = requests.request('GET', url, headers=self.base_headers)
            data = r.json()

            # 手动过滤出对应的合约
            funding_rate = float(data.get("funding_rate", 0))
            funding_interval = float(data.get("funding_interval", 0))
            return funding_rate, funding_interval
        except requests.RequestException as e:
            print("Error fetching futures contract info:", e)
            return 0, 0 

    def get_spot_quote_volume(self):
        """
        获取现货交易对的 quote volume
        """
        currency_pair = f"{self.symbol}_USDT"
        url = f"{self.host}{self.prefix}/spot/tickers"
        try:
            params = {'currency_pair': currency_pair}
            r = requests.request('GET', url, headers=self.base_headers, params=params)
            data = r.json()
            quote_volume = float(data[0].get("quote_volume", 0))
            return quote_volume
            
        except requests.RequestException as e:
            print(f"Error requesting data for {currency_pair}: {e}")
        return 0

    def get_future_quote_volume(self):
        """
        获取期货交易对的 24 小时 quote volume
        """
        contract = f"{self.symbol}_USDT"
        url = f"{self.host}{self.prefix}/futures/usdt/tickers"
        try:
            params = {'contract': contract}
            r = requests.request('GET', url, headers=self.base_headers, params=params)
            data = r.json()
            quote_volume = float(data[0].get("volume_24h_quote", 0))
            return quote_volume
            
        except requests.RequestException as e:
            print(f"Error requesting data for {contract}: {e}")
        return 0

    def gen_sign(self, method, url, query_string=None, payload_string=None):
        """
        生成请求签名，适用于需要身份验证的接口
        """
        t = time.time()
        m = hashlib.sha512()
        m.update((payload_string or "").encode('utf-8'))
        hashed_payload = m.hexdigest()
        s = f"{method}\n{url}\n{query_string or ''}\n{hashed_payload}\n{t}"
        sign = hmac.new(self.secret.encode('utf-8'), s.encode('utf-8'), hashlib.sha512).hexdigest()
        return {'KEY': self.key, 'Timestamp': str(t), 'SIGN': sign}

    def get_borrowable(self):
        """
        获取可借资产数据（borrowable）。
        返回：borrowable 数值或错误信息。
        """
        borrowable = None
        borrowable_url = '/unified/borrowable'
        query_param_borrow = f'currency={self.symbol}'
        sign_headers = self.gen_sign('GET', self.prefix + borrowable_url, query_param_borrow)
        headers = self.base_headers.copy()
        headers.update(sign_headers)
        try:
            url_borrow = f"{self.host}{self.prefix}{borrowable_url}?{query_param_borrow}"
            r_borrow = requests.get(url_borrow, headers=headers)
            data_borrow = r_borrow.json()
            if 'currency' in data_borrow:
                borrowable = data_borrow.get("amount")
        except Exception as e:
            borrowable = f"Error: {e}"
        return borrowable

    def get_interest_rate(self):
        """
        获取可借资产数据（borrowable）。
        返回：borrowable 数值或错误信息。
        """
        url = '/unified/estimate_rate'
        query_param = f'currencies={self.symbol}'
        sign_headers = self.gen_sign('GET', prefix + url, query_param)
        headers.update(sign_headers)
        r = requests.request('GET', host + prefix + url + "?" + query_param, headers=headers)
        data = r.json()
        try:
            interest_rate = float(data.get(f'{self.symbol}', 0))
        except Exception as e:
            borrowable = f"Error: {e}"
        return interest_rate

    def get_spot_orderbook(self):
        """
        获取现货订单簿数据，包括最佳买价与卖价。
        返回：包含 'best_bid_spot' 和 'best_ask_spot' 的字典。
        """
        best_bid_spot = None
        best_ask_spot = None
        order_book_url = '/spot/order_book'
        query_param_order = f'currency_pair={self.symbol}_USDT'
        try:
            url_order = f"{self.host}{self.prefix}{order_book_url}?{query_param_order}"
            r_order = requests.get(url_order, headers=self.base_headers)
            data_order = r_order.json()
            if 'bids' in data_order and len(data_order['bids']) > 0:
                best_bid_spot = float(data_order['bids'][0][0])
            if 'asks' in data_order and len(data_order['asks']) > 0:
                best_ask_spot = float(data_order['asks'][0][0])
        except Exception as e:
            best_bid_spot = None
            best_ask_spot = None
        return best_bid_spot, best_ask_spot
        

    def get_futures_orderbook(self):
        """
        获取期货订单簿数据，包括最佳买价与卖价。
        返回：包含 'best_bid_future' 和 'best_ask_future' 的字典。
        """
        best_bid_future = None
        best_ask_future = None
        order_book_url = '/futures/usdt/order_book'
        query_param_order = f'contract={self.symbol}_USDT'
        try:
            url_order = f"{self.host}{self.prefix}{order_book_url}?{query_param_order}"
            r_order = requests.get(url_order, headers=self.base_headers)
            data_order = r_order.json()
            if 'bids' in data_order and len(data_order['bids']) > 0:
                best_bid_future = float(data_order['bids'][0]['p'])
            if 'asks' in data_order and len(data_order['asks']) > 0:
                best_ask_future = float(data_order['asks'][0]['p'])
        except Exception as e:
            best_bid_future = None
            best_ask_future = None  
        return best_bid_future, best_ask_future

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_path", required=True, help="Path to config.json file")
    parser.add_argument("--account", required=True, help="Account name as in config.json, e.g., gt03")
    args = parser.parse_args()

    all_data = []  # 用于存储所有币种数据的列表

    for coin in tqdm(filtered_common):
        try:
            gate = gate_io(args.account, args.config_path, coin)

            funding_rate, funding_interval = gate.get_futures_contract_info()  # 期货合约的 funding rate 与 interest rate
            interest_rate = gate.get_interest_rate()
            spot_quote_volume = gate.get_spot_quote_volume()  # 现货交易对的 quote volume
            future_quote_volume = gate.get_future_quote_volume()  # 期货交易对的 quote volume
            borrowable = gate.get_borrowable()  # 获取可借资产数据
            best_bid_spot, best_ask_spot = gate.get_spot_orderbook()  # 现货订单簿的最佳买卖价
            best_bid_future, best_ask_future = gate.get_futures_orderbook()  # 期货订单簿的最佳买卖价

            current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


            data_dict = {
                    "Time": current_time,
                    "coin": coin,  # 币种标识
                    "funding_rate": float(funding_rate),
                    "interest_rate": float(interest_rate),
                    "funding_interval": float(funding_interval),
                    "spot_24h_volume": float(spot_quote_volume),
                    "futures_24h_volume": float(future_quote_volume),
                    "Borrowable": float(borrowable),
                    "spot_best_bid": float(best_bid_spot),
                    "spot_best_ask": float(best_ask_spot),
                    "future_best_bid": float(best_bid_future),
                    "future_best_ask": float(best_ask_future)
            }

            all_data.append(data_dict)

        except Exception as e:
            continue

    df = pd.DataFrame(all_data)

    df['Time'] = pd.to_datetime(df['Time'])

    df['funding_interval'] /= 3600

    df['funding_rate_8h'] = (df['funding_rate'] / df['funding_interval']) * 8.0
    df['interest_rate_8h'] =  df['interest_rate'] * 8


    df['forward_side_spread'] = (df['future_best_bid'] / df['spot_best_ask']) - 1
    df['reverse_side_spread'] = (df['spot_best_bid'] / df['future_best_ask']) - 1
    df['forward_side_profit'] = df['forward_side_spread'] + df['funding_rate_8h']
    df['reverse_side_profit'] = df['reverse_side_spread'] - df['funding_rate_8h'] - df['interest_rate_8h']

    df['borrowable_value'] = (df['Borrowable'] * ((df['spot_best_bid'] + df['spot_best_ask']) / 2).fillna(0)).round()

    
    # 按照正向套利收益从大到小排序
    df = df.sort_values("funding_rate", ascending=True) 

    # df = df[(df['spot_24h_volume'] >= 100000) & (df['futures_24h_volume'] >= 100000)]

    df = df[['Time', 'coin', 'funding_rate','funding_interval', 'funding_rate_8h','interest_rate_8h', 'spot_24h_volume', 'futures_24h_volume', 'spot_best_bid','spot_best_ask','future_best_bid','future_best_ask', 'forward_side_spread', 'reverse_side_spread', 'forward_side_profit', 'reverse_side_profit', 'borrowable_value']]

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

    # output_dir = '/data/market_maker/Gate_io/'
    output_dir = './data/'
    os.makedirs(output_dir, exist_ok=True)

    csv_path = os.path.join(output_dir, f"all_coins_data_gate.csv")

    df.to_csv(csv_path, index=False) 

    print(df.head(60))
    print('================================================================')

    print(df[df["fr"] > 0].sort_values("profit_fwd", ascending=False).head(60))

    print('================================================================')

    print(df[df["fr"] < 0].sort_values("profit_rvs", ascending=False).head(60))

    print('================================================================')