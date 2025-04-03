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


import requests
import time
import csv
import os
from tqdm import tqdm

class BinanceData:
    def __init__(self, symbol):
        self.symbol = symbol.upper()  # 如 "BTC"、"ETH" 等
        # 注意：现货和期货的交易对均以 "USDT" 结尾，如 "BTCUSDT"
        self.spot_endpoint = "https://api.binance.com/api/v3/depth"
        self.futures_endpoint = "https://fapi.binance.com/fapi/v1/depth"

    def get_spot_orderbook(self):
        """
        获取现货订单簿数据，并返回最佳买一和卖一价格。
        """
        params = {"symbol": f"{self.symbol}USDT", "limit": 5}  # limit 可根据需要调整
        try:
            response = requests.get(self.spot_endpoint, params=params)
            data = response.json()
            best_bid = float(data["bids"][0][0]) if data.get("bids") and len(data["bids"]) > 0 else None
            best_ask = float(data["asks"][0][0]) if data.get("asks") and len(data["asks"]) > 0 else None
            return best_bid, best_ask
        except Exception as e:
            print(f"{self.symbol}现货订单簿数据获取失败：", e)
            return None, None

    def get_futures_orderbook(self):
        """
        获取期货订单簿数据，并返回最佳买一和卖一价格。
        """
        params = {"symbol": f"{self.symbol}USDT", "limit": 5}
        try:
            response = requests.get(self.futures_endpoint, params=params)
            data = response.json()
            best_bid = float(data["bids"][0][0]) if data.get("bids") and len(data["bids"]) > 0 else None
            best_ask = float(data["asks"][0][0]) if data.get("asks") and len(data["asks"]) > 0 else None
            return best_bid, best_ask
        except Exception as e:
            print(f"{self.symbol}期货订单簿数据获取失败：", e)
            return None, None

def ensure_csv_header(csv_file, header):
    """如果 CSV 文件不存在则写入表头"""
    if not os.path.exists(csv_file):
        with open(csv_file, mode="w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)

if __name__ == "__main__":
    # 定义需要遍历的币种列表
    tickers =  common_coins # 可根据需要添加其它币种
    # CSV 保存路径
    base_dir = "/data/market_maker/Binance/price_data/"
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
    
    # CSV 文件列定义
    header = ["Time", "Ticker", "Best_Bid_Spot", "Best_Ask_Spot", "Best_Bid_Future", "Best_Ask_Future"]

    while True:
        for ticker in tqdm(tickers):
            # 初始化 BinanceData 对象
            binance_obj = BinanceData(ticker)
            # 获取现货和期货订单簿数据
            best_bid_spot, best_ask_spot = binance_obj.get_spot_orderbook()
            best_bid_future, best_ask_future = binance_obj.get_futures_orderbook()
            current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            
            # 构造对应的 CSV 文件路径（每个币种单独一个文件）
            csv_file = os.path.join(base_dir, f"{ticker}_USDT.csv")
            ensure_csv_header(csv_file, header)
            
            # 追加写入数据
            with open(csv_file, mode="a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([current_time, ticker, best_bid_spot, best_ask_spot, best_bid_future, best_ask_future])
            
        # 每轮完成后等待10秒（可根据需要调整时间）
        time.sleep(50)
