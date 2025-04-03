import time
import hashlib
import hmac
import requests
import json
import csv
import os
from tqdm import tqdm

# -------------------------------
# 公共参数及签名函数
# -------------------------------
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
    s = '%s\n%s\n%s\n%s\n%s' % (method, url, query_string or "", hashed_payload, t)
    sign = hmac.new(secret.encode('utf-8'), s.encode('utf-8'), hashlib.sha512).hexdigest()
    return {'KEY': key, 'Timestamp': str(t), 'SIGN': sign}

# -------------------------------
# 获取币种列表
# -------------------------------
# 获取现货币种
spot_url = '/spot/currencies'
r = requests.get(host + prefix + spot_url, headers=headers)
spot_data = r.json()
spot_currencies = [item['currency'] for item in spot_data if 'currency' in item]

# 获取期货合约名称
future_url = '/futures/usdt/contracts'
r = requests.get(host + prefix + future_url, headers=headers)
future_data = r.json()
future_names = [item['name'] for item in future_data if 'name' in item]
# 处理期货名称，去掉后缀 "USDT"
future_processed = {name[:-5] for name in future_names if name.endswith("USDT")}

spot_set = set(spot_currencies)
common_coins = list(spot_set.intersection(future_processed))
ticker_pool = common_coins

# -------------------------------
# 定义获取订单簿的类
# -------------------------------
class GateioData:
    def __init__(self, symbol):
        self.symbol = symbol.upper()  # 如 'HIVE'
        self.host = "https://api.gateio.ws"
        self.prefix = "/api/v4"
        self.base_headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        # 注意此处的 key/secret为另外一组，如果需要签名可根据具体接口调用
        self.key = "b918fb5bdbead68be24cd499422dcb28"
        self.secret = "c42c3052b63679eb0dd0439d5af9c35f5f64c8491ac4142798450fa227367f71"

    def get_spot_orderbook(self):
        """
        获取现货订单簿数据，包括最佳买价与卖价。
        返回：元组 (best_bid_spot, best_ask_spot)
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
        返回：元组 (best_bid_future, best_ask_future)
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

# -------------------------------
# 数据写入 CSV 相关设置
# -------------------------------
base_dir = "/data/market_maker/Gate_io/price_data/"
if not os.path.exists(base_dir):
    os.makedirs(base_dir)

# CSV 文件列只保留4个订单簿价格
columns = ["Time", "Currency", "Best_Bid_Spot", "Best_Ask_Spot", "Best_Bid_Future", "Best_Ask_Future"]

# -------------------------------
# 主循环：获取订单簿数据，并写入 CSV
# -------------------------------
while True:
    try:
        for ticker in tqdm(ticker_pool):
            # 构造对应 CSV 文件路径（以 USDT 结尾区分）
            csv_file = os.path.join(base_dir, f"{ticker}_USDT.csv")
            file_exists = os.path.exists(csv_file)
            
            with open(csv_file, mode="a", newline="") as file:
                writer = csv.writer(file)
                if not file_exists:
                    writer.writerow(columns)
                
                # -------------------------------
                # 获取现货与期货订单簿数据
                # -------------------------------
                gateio_obj = GateioData(ticker)
                best_bid_spot, best_ask_spot = gateio_obj.get_spot_orderbook()
                best_bid_future, best_ask_future = gateio_obj.get_futures_orderbook()
                
                # 记录当前时间戳
                cur_time = time.time()
                writer.writerow([cur_time, ticker, best_bid_spot, best_ask_spot, best_bid_future, best_ask_future])
        # 每轮完成后等待5秒
        time.sleep(40)

    except Exception as e:
        # 记录错误日志到 error_log.csv
        error_log = os.path.join(base_dir, "error_log.csv")
        with open(error_log, mode="a", newline="") as error_file:
            error_writer = csv.writer(error_file)
            error_writer.writerow([time.time(), str(e)])
        time.sleep(40)
