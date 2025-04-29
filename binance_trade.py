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

def last_income(symbol: str, income_type: str):
    """返回最近一次 income 数值和时间戳"""
    rows = client.papi_get_um_income_history(
        symbol=symbol,
        incomeType=income_type,
        limit=1,
        timestamp=int(time.time()*1000),
    )
    if rows:
        row = rows[0]
        return float(row["income"]), pd.to_datetime(row["time"], unit="ms")
    return 0.0, pd.NaT

now_ms = int(time.time() * 1000)

# ① premiumIndex（含 lastFundingRate / nextFundingTime / interestRate）
premium_map = {p["symbol"]: p for p in client.futures_mark_price()}
time.sleep(0.05)

# ② fundingRate（最近一次实际结算资金费率 + fundingTime）
funding_map = {f["symbol"]: f for f in client.futures_funding_rate(limit=1)}
time.sleep(0.05)

# ③ adlQuantile
adl_map = {q["symbol"]: q["adlQuantile"] for q in client.papi_get_um_adl_quantile(timestamp=now_ms)}
time.sleep(0.05)

# ④ 持仓
account = client.papi_get_um_account(timestamp=now_ms)

records = []
for pos in account["positions"]:
    symbol = pos["symbol"]
    size   = float(pos["positionAmt"])
    if size == 0:
        continue

    asset        = symbol.replace("USDT", "")
    prem         = premium_map[symbol]
    fund_row     = funding_map.get(symbol, {})
    mark_price   = float(prem["markPrice"])

    # 上一次 / 下一次资金费率与时间戳
    fr_last_rate = float(fund_row.get("fundingRate", prem["lastFundingRate"]))
    fr_last_ts   = pd.to_datetime(int(fund_row.get("fundingTime", 0)), unit="ms")
    fr_next_rate = float(prem["lastFundingRate"])
    fr_next_ts   = pd.to_datetime(int(prem["nextFundingTime"]), unit="ms")

    # 实际资金费率周期（小时），大多数是 8，但也有 4、12 等
    funding_interval_h = (fr_next_ts - fr_last_ts).total_seconds() / 3600 or 8

    # 日利率→ 单个 funding_interval 的利率
    int_rate_interval = float(prem["interestRate"]) * (funding_interval_h / 24)

    # 收益类
    ur_pnl       = float(pos["unrealizedProfit"])
    r_pnl, _     = last_income(symbol, "REALIZED_PNL")
    pnl_fee, _   = last_income(symbol, "COMMISSION")
    pnl_fund, _  = last_income(symbol, "FUNDING_FEE")   # 关键：拿到 funding PnL

    # ADL 分位
    adl_q        = adl_map.get(symbol, {})
    adl_rank     = adl_q.get("LONG") or adl_q.get("BOTH") or adl_q.get("SHORT")

    # 记录
    records.append({
        "asset"       : asset,
        "size"        : size,
        "value"       : round(size * mark_price, 4),
        "init_mgn"    : float(pos["initialMargin"]),
        "mait_mgn"    : float(pos["maintMargin"]),

        "ur_pnl"      : ur_pnl,
        "r_pnl"       : r_pnl,
        "pnl_fund"    : pnl_fund,
        "pnl_fee"     : pnl_fee,

        "adl_rank"    : adl_rank,
        "entry_price" : round(float(pos["entryPrice"]), 6),

        "freq_h"      : funding_interval_h,        
        "fr_last_rate": fr_last_rate,              
        "fr_next_rate": fr_next_rate,              
        "fr_next_ts"  : fr_next_ts,

        "int_rate"    : int_rate_interval,
    })

futures_df = pd.DataFrame(records).sort_values("asset")
pd.set_option("display.max_columns", None)
print(futures_df)

# --------------------------
# 3. 获取统一账户（现货账户）数据
# --------------------------
# --------------------------
# 1. 现货账户 (unified_accounts)
# --------------------------
try:
    spot_info  = client.get_account()
    balances   = spot_info["balances"]
    unified_accounts = pd.DataFrame(balances)
    unified_accounts["equity"] = unified_accounts["free"].astype(float) + unified_accounts["locked"].astype(float)
    unified_accounts["price"]  = unified_accounts["asset"].map(lambda x: currency_price_map.get(x, 0))
    unified_accounts["value"]  = unified_accounts["equity"] * unified_accounts["price"]
    unified_accounts["type"]   = "mgn"

    unified_accounts = (
        unified_accounts[unified_accounts["value"] > 0]        # 过滤 0 资产
        .sort_values("value", ascending=False)
        .reset_index(drop=True)
        .rename(columns={"asset": "asset"})                     # 与 futures_df 对齐
    )
except Exception as ex:
    print(f"查询 Binance 现货账户数据时发生错误: {ex}")
    unified_accounts = pd.DataFrame()

# --------------------
# 2. 标记期货数据并合并
# --------------------
if not futures_df.empty:
    futures_df["type"] = "fts"
else:
    futures_df = pd.DataFrame()

df = pd.merge(
    unified_accounts,
    futures_df,
    on="asset",
    how="outer",
    suffixes=("_mgn", "_futures"))

# 用 0 填补缺失值，便于计算
for col in ["value_mgn", "value_futures"]:
    df[col] = df[col].fillna(0).astype(float)

df["borrowed"] = 0.0 

# 总资产（绝对值：现货 + |期货|）
total_amount = df["value_mgn"].sum() + df["value_futures"].abs().sum()

# --------------------------
# 3. 新衍生指标
# --------------------------
# ① 现货占比 / 头寸错配
df["inv%"]     = (df["value_mgn"] / total_amount).round(5)
df["dismatch"] = df["value_mgn"] + df["value_futures"]

# ② 累计资金费率收益率 (%)
df["cum_fr_pnl%"] = (
    df["pnl_fund"].fillna(0) / df["value_futures"].abs().replace(0, np.nan)
).round(5)

# ③ 预测下一资金周期的 Funding-PnL & 利息-PnL
df["nxt_fr_pnl_u"] = -df["value_futures"] * df["fr_next_rate"].fillna(0)
df["nxt_int_u"]    =  df["int_rate"].fillna(0) * df["value_mgn"].abs()
df["nxt_net_pnl_u"] = df["nxt_fr_pnl_u"] - df["nxt_int_u"]

# 对应百分比 (以所持期货市值为分母)
denominator = df["value_futures"].abs().replace(0, np.nan)
df["nxt_fr_pnl%"]  = (df["nxt_fr_pnl_u"]  / denominator).round(5)
df["nxt_net_pnl%"] = (df["nxt_net_pnl_u"] / denominator).round(5)

# ④ 仅保留实际持仓：现货绝对市值 > 5 USDT 或 期货市值 ≠ 0
df = df[(df["value_mgn"].abs() > 5) | (df["value_futures"] != 0)]

# --------------------------
# 4. 输出结果与统计
# --------------------------
print("合并后的账户数据:\n", df)

print(f"现货负投资合计: {(df[df['inv%'] < 0]['inv%'].sum()):.1%}")
print(f"现货正投资合计: {(df[df['inv%'] > 0]['inv%'].sum()):.1%}")
print(f"现货净投资    : {(df['inv%'].sum()):.1%}")
print(f"现货绝对投资  : {(df['inv%'].abs().sum()):.1%}")

print(f"\n借款总值                 : {df['borrowed'].sum():.1f} USDT")
print(f"累计资金费 PnL           : {df['pnl_fund'].sum():.1f} USDT "
      f"({df['pnl_fund'].sum()/total_amount:.5%})")

print(f"\n预测下一周期 Funding-PnL : {df['nxt_fr_pnl_u'].sum():.1f} USDT "
      f"({df['nxt_fr_pnl_u'].sum()/denominator.sum():.5%}) "
      f"(占总市值 {df['nxt_fr_pnl_u'].sum()/total_amount:.5%})")

print(f"预测下一周期 利息-PnL    : {df['nxt_int_u'].sum():.1f} USDT "
      f"({df['nxt_int_u'].sum()/denominator.sum():.5%})")

print(f"预测下一周期 净 PnL      : {df['nxt_net_pnl_u'].sum():.1f} USDT "
      f"({df['nxt_net_pnl_u'].sum()/denominator.sum():.5%}) "
      f"(占总市值 {df['nxt_net_pnl_u'].sum()/total_amount:.5%})")

best_row  = df.loc[[df["nxt_fr_pnl_u"].idxmax()]]
worst_row = df.loc[[df["nxt_fr_pnl_u"].idxmin()]]

print("\n预测 Funding-PnL 最佳资产:\n", best_row[["asset", "nxt_fr_pnl_u", "nxt_fr_pnl%"]])
print("\n预测 Funding-PnL 最差资产:\n", worst_row[["asset", "nxt_fr_pnl_u", "nxt_fr_pnl%"]])

print("当前时间:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))