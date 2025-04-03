import time
import hashlib
import hmac
import requests
import csv
import os
from tqdm import tqdm

# API 设置与签名
host = "https://api.gateio.ws"
prefix = "/api/v4"
headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

def gen_sign(method, url, query_string=None, payload_string=None):
    key = "9bf41c70ab82e45335abaebd4ed61b48"
    secret = "1a2c57da3a55ca89e06455c8233cbb0b16b8f164c9ad99c84f9cc451b9516539"
    t = time.time()
    m = hashlib.sha512()
    m.update((payload_string or "").encode('utf-8'))
    hashed_payload = m.hexdigest()
    s = f"{method}\n{url}\n{query_string or ''}\n{hashed_payload}\n{t}"
    sign = hmac.new(secret.encode('utf-8'), s.encode('utf-8'), hashlib.sha512).hexdigest()
    return {'KEY': key, 'Timestamp': str(t), 'SIGN': sign}

# 获取支持的币种（spot 和 futures 同时有）
spot_data = requests.get(host + prefix + '/spot/currencies', headers=headers).json()
future_data = requests.get(host + prefix + '/futures/usdt/contracts', headers=headers).json()
spot_currencies = {item['currency'] for item in spot_data if 'currency' in item}
future_currencies = {item['name'][:-5] for item in future_data if item.get('name', '').endswith('USDT')}
common_coins = list(spot_currencies & future_currencies)

# 保存路径与字段
merged_dir = "/data/market_maker/Gate_io/history_data/"
os.makedirs(merged_dir, exist_ok=True)
columns = ["Time", "Currency", "Borrowable", "Best_Bid_Spot", "Best_Ask_Spot", "Best_Bid_Future", "Best_Ask_Future"]

# 获取订单簿类
class GateioData:
    def __init__(self, symbol):
        self.symbol = symbol.upper()

    def get_spot_orderbook(self):
        url = f"{host}{prefix}/spot/order_book?currency_pair={self.symbol}_USDT"
        try:
            res = requests.get(url, headers=headers).json()
            bid = float(res['bids'][0][0]) if 'bids' in res and res['bids'] else None
            ask = float(res['asks'][0][0]) if 'asks' in res and res['asks'] else None
            return bid, ask
        except:
            return None, None

    def get_futures_orderbook(self):
        url = f"{host}{prefix}/futures/usdt/order_book?contract={self.symbol}_USDT"
        try:
            res = requests.get(url, headers=headers).json()
            bid = float(res['bids'][0]['p']) if 'bids' in res and res['bids'] else None
            ask = float(res['asks'][0]['p']) if 'asks' in res and res['asks'] else None
            return bid, ask
        except:
            return None, None

# 主循环，每个币写到自己文件里
while True:
    now = time.time()
    for coin in tqdm(common_coins):
        try:
            # 初始化数据字段
            borrowable = None
            best_bid_spot, best_ask_spot = None, None
            best_bid_fut, best_ask_fut = None, None

            # 获取 Borrowable
            try:
                query = f'currency={coin}'
                signed_headers = gen_sign('GET', prefix + '/unified/borrowable', query)
                full_headers = headers.copy()
                full_headers.update(signed_headers)
                res = requests.get(f"{host}{prefix}/unified/borrowable?{query}", headers=full_headers).json()
                if 'amount' in res:
                    borrowable = res['amount']
            except Exception as e:
                print(f"[Borrowable Error] {coin}: {e}")

            # 获取订单簿
            try:
                gate = GateioData(coin)
                best_bid_spot, best_ask_spot = gate.get_spot_orderbook()
                best_bid_fut, best_ask_fut = gate.get_futures_orderbook()
            except Exception as e:
                print(f"[Orderbook Error] {coin}: {e}")

            # 构造文件路径
            file_path = os.path.join(merged_dir, f"{coin}_USDT.csv")
            write_header = not os.path.exists(file_path)

            # 写入到各自 CSV 文件
            with open(file_path, "a", newline="") as f:
                writer = csv.writer(f)
                if write_header:
                    writer.writerow(columns)
                writer.writerow([
                    now, coin, borrowable,
                    best_bid_spot, best_ask_spot,
                    best_bid_fut, best_ask_fut
                ])

        except Exception as e:
            print(f"[Global Error] {coin}: {e}")

