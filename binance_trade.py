from __future__ import print_function
import datetime
import numpy as np
import pandas as pd
import json
from termcolor import colored
from binance.client import Client
import argparse

# --------------------------
# 配置与初始化部分
# --------------------------

# 设置 Pandas 显示选项（防止输出换行）
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.expand_frame_repr", False)

# 加载 API 配置（配置文件结构与 Gate.io 类似：{"gt03": [API_KEY, API_SECRET, total_amount] }）
parser = argparse.ArgumentParser(description="Binance Market Data Script")
parser.add_argument('--acct', type=str, required=True, help="Account name (e.g., 'tt16')")
parser.add_argument('--config_path', type=str, required=True, help="Path to the configuration JSON file")
args = parser.parse_args()

with open(args.config_path, "r") as f:
    data = json.load(f)
    api_key = data[args.acct][0]
    api_secret = data[args.acct][1]
    total_amount = data[args.acct][2]

client = Client(api_key, api_secret)

# --------------------------
# 1. 获取现货市场数据及价格映射
# --------------------------
try:
    tickers = client.get_all_tickers()
    currency_price_map = {
        ticker['symbol'][:-4]: float(ticker['price'])
        for ticker in tickers if ticker['symbol'].endswith("USDT")
        }
    print(currency_price_map)
    
except Exception as e:
    print("调用 Binance Spot API 获取 ticker 时发生错误: %s" % e)

import time, hmac, pandas as pd, numpy as np
from datetime import datetime

def last_income(client, symbol, itype):
    """返回最近一次 income 数值和时间戳"""
    res = client.papi_get_um_income_history(symbol=symbol,
                                    incomeType=itype,
                                    limit=1,
                                    timestamp=int(time.time()*1000))
                                    
    return (float(res[0]['income']), pd.to_datetime(res[0]['time'], unit='ms')) if res else (0.0, None)

futures_data = []
account = client.papi_get_um_account(timestamp=int(time.time()*1000))

for pos in account["positions"]:
    size = float(pos["positionAmt"])

    symbol = pos["symbol"]
    asset  = symbol.replace("USDT", "")
    
    # 公共行情 & 资金费率
    premium  = client.futures_mark_price(symbol=symbol)          # /fapi/v1/premiumIndex
    funding  = client.futures_funding_rate(symbol=symbol, limit=1)[0]  # 最近一次资金费率
    
    mark_price = float(premium["markPrice"])
    fr_last_raw = float(funding["fundingRate"])
    fr_last_ts  = pd.to_datetime(int(funding["fundingTime"]), unit='ms')
    fr_last_8h  = fr_last_raw * 100          # 转百分比
    
    fr_nxt_raw = float(premium["lastFundingRate"])
    fr_nxt_ts  = pd.to_datetime(int(premium["nextFundingTime"]), unit='ms')
    fr_nxt_8h  = fr_nxt_raw * 100
    int_nxt_8h = float(premium["interestRate"]) * 100
    
    # 各类 PnL / 手续费
    r_pnl,   _ = last_income(client, symbol, "REALIZED_PNL")
    pnl_fund, _ = last_income(client, symbol, "FUNDING_FEE")
    pnl_fee,  _ = last_income(client, symbol, "COMMISSION")
    
    # ADL 排名
    adl = client.papi_get_um_adl_quantile(symbol=symbol, timestamp=int(time.time()*1000))
    adl_ranking = adl[0]["adlQuantile"].get("LONG", adl[0]["adlQuantile"].get("BOTH"))
    
    # 核心仓位字段
    value      = np.sign(size) * abs(size) * mark_price
    init_mgn   = float(pos["initialMargin"])
    mait_mgn   = float(pos["maintMargin"])
    ur_pnl     = float(pos["unrealizedProfit"])
    
    futures_data.append({
        "asset": asset,
        "delist": False,
        "max_loan": np.nan,
        "size": size,
        "value": round(value, 4),
        "init_mgn": init_mgn,
        "mait_mgn": mait_mgn,
        "ur_pnl": ur_pnl,
        "r_pnl": r_pnl,
        "pnl_fund": pnl_fund,
        "pnl_fee": pnl_fee,
        "adl_ranking": adl_ranking,
        "entry_price": round(float(pos["entryPrice"]), 6),
        "freq": "8h",
        "fr_last_ts": fr_last_ts,
        "fr_last_raw%": fr_last_raw * 100,
        "fr_last_8h%": fr_last_8h,
        "fr_nxt_ts": fr_nxt_ts,
        "fr_nxt_raw%": fr_nxt_raw * 100,
        "fr_nxt_8h%": fr_nxt_8h,
        "int_nxt_8h%": int_nxt_8h
    })

futures_df = pd.DataFrame(futures_data)
print(futures_df)


# --------------------------
# 3. 获取统一账户（现货账户）数据
# --------------------------
try:
    account_info = client.get_account()
    balances = account_info['balances']
    unified_accounts = pd.DataFrame(balances)
    unified_accounts["equity"] = unified_accounts["free"].astype(float) + unified_accounts["locked"].astype(float)
    unified_accounts["price"] = unified_accounts["asset"].map(lambda x: currency_price_map.get(x, 0))
    unified_accounts["total_value"] = unified_accounts["equity"] * unified_accounts["price"]
    unified_accounts["type"] = "mgn"
    unified_accounts = unified_accounts.sort_values(by="total_value", ascending=False)
    unified_accounts = unified_accounts.reset_index(drop=True)
    unified_accounts = unified_accounts.rename(columns={"asset": "asset", "total_value": "value"})
except Exception as ex:
    print("查询 Binance 现货账户数据时发生错误: %s" % ex)

# --------------------------
# 4. 合并现货与期货数据，并计算相关指标
# --------------------------
# 给期货数据添加类型标记
if not futures_df.empty:
    futures_df["type"] = "fts"
else:
    futures_df = pd.DataFrame()

# 对现货数据与期货数据按照币种（asset）做全连接合并
df = pd.merge(unified_accounts, futures_df, on="asset", how="outer", suffixes=("_mgn", "_futures"))

# 将现货账户的 value 转换为 float 并排序
df["value_mgn"] = df["value_mgn"].astype(float)
df = df.sort_values(by="value_mgn", ascending=False)

# 计算现货与期货投资总和、占总金额比例等指标
df["dismatch"] = df["value_mgn"] + df["value_futures"]
df["inv%"] = df["value_mgn"] / total_amount

# 删除不再需要的冗余字段（此处删除部分 Binance 返回的字段，与 Gate.io 版本保持一致）
cols_to_drop = [
    "free", "locked", "type", "equity"
]
df.drop(columns=[col for col in cols_to_drop if col in df.columns], inplace=True)

# 根据期货数据计算多项衍生指标
# 现货借款数据 Binance 现货账户不提供，设为 0
df["borrowed"] = 0
df["inv%"] = df["inv%"].astype(float).round(3)
df["value_futures"] = df["value_futures"].replace(np.nan, 0)
df["cum_fr_pnl%"] = (df["pnl_fund"] / df["value_futures"].abs()).round(5)
df["nxt_fr_pnl_8h(u)"] = -df["value_futures"] * df["fr_nxt_8h%"]
df["nxt_int_8h(u)"] = df["int_nxt_8h%"] * df["value_mgn"].abs()
df["nxt_pnl_8h_net(u)"] = df["nxt_fr_pnl_8h(u)"] - df["nxt_int_8h(u)"]
df["nxt_fr_pnl_8h%"] = -np.sign(df["value_futures"]) * df["fr_nxt_8h%"]
df["nxt_pnl_8h_net%"] = (df["nxt_pnl_8h_net(u)"] / df["value_futures"].abs()).round(5)

# 仅保留有实际持仓的资产（现货：绝对值大于5，期货：value不为0）
df = df[(df["value_mgn"].abs() > 5) & (df["value_futures"] != 0)]

# 根据现货账户的投资方向标记（正值显示白色，负值显示红色）
df["side"] = df["value_mgn"].apply(lambda x: colored(np.sign(x), "white") if x >= 0 else colored(np.sign(x), "red"))

# --------------------------
# 5. 输出结果与统计信息
# --------------------------
print("合并后的账户数据:")
print(df)

print(f"现货负投资总和: {df[df['inv%'] < 0.0]['inv%'].sum():.1%}")
print(f"现货正投资总和: {df[df['inv%'] > 0.0]['inv%'].sum():.1%}")
print(f"现货净投资: {df['inv%'].sum():.1%}")
print(f"现货绝对投资: {df['inv%'].abs().sum():.1%}")
print(f"借款总值: {df['borrowed'].sum():.1f}")

print(f"累计融资盈亏 (USDT): {df['pnl_fund'].sum():.1f}")
print(f"累计融资盈亏 (%): {(df['pnl_fund'].sum() / total_amount):.5f}")

print(f"预估下8小时融资盈亏 (USDT): {df['nxt_fr_pnl_8h(u)'].sum():.1f}")
print(f"预估下8小时融资盈亏 (%): {(df['nxt_fr_pnl_8h(u)'].sum() / df['value_futures'].abs().sum()):.5f}")
print(f"预估下8小时融资盈亏/市值 (%): {(df['nxt_fr_pnl_8h(u)'].sum() / total_amount):.5f}")

print(f"预估下8小时利息支出 (USDT): {df['nxt_int_8h(u)'].sum():.1f}")
print(f"预估下8小时利息支出 (%): {(df['nxt_int_8h(u)'].sum() / df['value_futures'].abs().sum()):.5f}")

print(f"预估下8小时净融资盈亏 (USDT): {df['nxt_pnl_8h_net(u)'].sum():.1f}")
print(f"预估下8小时净融资盈亏 (%): {(df['nxt_pnl_8h_net(u)'].sum() / df['value_futures'].abs().sum()):.5f}")
print(f"预估下8小时净融资盈亏/市值 (%): {(df['nxt_pnl_8h_net(u)'].sum() / total_amount):.5f}")

# 找出预估下8小时融资盈亏最好的和最差的资产
best_fr_row = df.loc[[df["nxt_fr_pnl_8h(u)"].idxmax()]]
worst_fr_row = df.loc[[df["nxt_fr_pnl_8h(u)"].idxmin()]]
print("预估最佳融资资产:")
print(best_fr_row)
print("预估最差融资资产:")
print(worst_fr_row)

print("当前时间:", datetime.datetime.now())