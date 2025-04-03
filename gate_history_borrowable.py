import time
import hashlib
import hmac
import requests
import json
import csv
import os
from tqdm import tqdm
import requests

host = "https://api.gateio.ws"
prefix = "/api/v4"
headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

# 获取 spot 数据，并提取所有 'currency'
spot_url = '/spot/currencies'
r = requests.get(host + prefix + spot_url, headers=headers)
spot_data = r.json()
spot_currencies = [item['currency'] for item in spot_data if 'currency' in item]

# 获取 future 数据，并提取所有 'name'
future_url = '/futures/usdt/contracts'
r = requests.get(host + prefix + future_url, headers=headers)
future_data = r.json()
future_names = [item['name'] for item in future_data if 'name' in item]

# 对 future 的币种名称做处理，去除尾部 "USDT" 后缀
# 假设币种名称格式为 "BTC_USDT"，去掉后就变成 "BTC"
future_processed = {name[:-5] for name in future_names if name.endswith("USDT")}

spot_set = set(spot_currencies)
common_coins = list(spot_set.intersection(future_processed))

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

host = "https://api.gateio.ws"
prefix = "/api/v4"
headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

ticker_pool = common_coins  
url = '/unified/borrowable'

base_dir = "/data/market_maker/Gate_io/borrow_data/"
if not os.path.exists(base_dir):
    os.makedirs(base_dir)

columns = ["Time", "Currency", "Borrowable"]

while True:
    try:
        for ticker in tqdm(ticker_pool):
            query_param = f'currency={ticker}'
            csv_file = os.path.join(base_dir, f"{ticker}_USDT.csv")
            file_exists = os.path.exists(csv_file)
            
            # Open the CSV file in append mode and write header if needed
            with open(csv_file, mode="a", newline="") as file:
                writer = csv.writer(file)
                if not file_exists:
                    writer.writerow(columns)
                
                # Sign and send the request
                sign_headers = gen_sign('GET', prefix + url, query_param)
                headers.update(sign_headers)
                r = requests.get(host + prefix + url + "?" + query_param, headers=headers)
                result = r.json()
                if 'currency' in result:
                    currency = result['currency']
                    amount = result['amount']
                    writer.writerow([time.time(), currency, amount])

    except Exception as e:
        error_log = os.path.join(base_dir, "error_log.csv")
        with open(error_log, mode="a", newline="") as error_file:
            error_writer = csv.writer(error_file)
            error_writer.writerow([time.time(), str(e)])

    time.sleep(60)