
#pip install -r requirements.txt # only when running in local mode

import os
from dotenv import load_dotenv, find_dotenv
import requests
import pandas as pd
import matplotlib.pyplot as plt

# PART 1: Fetch UpdateContext events with Alchemy
# ----------------------------------------------------------

# import API key
path = find_dotenv(filename='.env', raise_error_if_not_found=True, usecwd=True)
load_dotenv(path)

# Fetch events from singleton
API_KEY = os.environ.get('ALCHEMY_KEY')
url = f'https://starknet-mainnet.g.alchemy.com/starknet/version/rpc/v0_7/{API_KEY}'
headers = {
    "accept": "application/json",
    "content-type": "application/json"
}
continuation_token = '656900-0'

events_list = []
while True:
     print('Fetch Alchemy page: ' + continuation_token)
     payload = {
         "id": 1,
         "jsonrpc": "2.0",
         "method": "starknet_getEvents",
         "params": [
             {"from_block": {"block_number": 656900},
              "to_block": "latest",
              "address": "0x02545b2e5d519fc230e9cd781046d3a64e092114f07e44771e0d719d148725ef",
              "keys": [["0xe623beb06d0cfbe7f7877cf06290a77c803ca8fde4b54a68b241607c7cc8cc"]],
              "chunk_size": 1000,
              'continuation_token': continuation_token
              }
         ]}
     response = requests.post(url, json=payload, headers=headers)
     events_list = events_list + [ e['keys'] + e['data'] for e in response.json()['result']['events']]
     if 'continuation_token' in response.json()['result']:
          continuation_token = response.json()['result']['continuation_token']
     else:
       print("1. Successfully fetched raw events")
       break

# Decode events from raw data
events_raw = pd.DataFrame(events_list)
columns = [2,4,6,8,10,12,14,16,17,18,20,22,44] # collateral_asset_config (we're not interested in debt asset)
column_names = [
    "collateral_asset",
    "total_collateral_shares",
    "total_nominal_debt",
    "reserve",
    "max_utilization",
    "floor",
    "scale",
    "is_legacy",
    "last_updated",
    "last_rate_accumulator",
    "last_full_utilization_rate",
    "fee_rate",
    "collateral_asset_price"
]
events = events_raw.loc[:,columns]
events.columns = column_names

print("2. Successfully decoded events")

# PART 2: Transform event data so it can be processed
# -------------------------------------------------------

# constants
SCALE = 10**18
YEAR = 365*86400
MARKETS = pd.DataFrame({
	"asset": [ 
        "0x53c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
	    "0x49d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
	    "0x68f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
	    "0x4718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
		"0x3fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
		"0x42b8f0484674ca266ac5d08e4ac6a3fe65bd3129795def2dca5c34ecc5f96d2"
    ],
	"market": ["USDC", "ETH", "USDT", "STRK", "WBTC", "wstETH"]
})

# transform data
data = pd.merge(events, MARKETS, left_on='collateral_asset', right_on='asset')
data['timestamp'] = data["last_updated"].apply(int, base=16)
data["date"] = pd.to_datetime(data['timestamp'], unit="s")
data["debt_dec"] = data["total_nominal_debt"].apply(int, base=16) / SCALE
data["reserve_dec"] = data["reserve"].apply(int, base=16) / data["scale"].apply(int, base=16)
data["accumulator_dec"] = data["last_rate_accumulator"].apply(int, base=16) / SCALE
data["full_rate_dec"] = (1+data["last_full_utilization_rate"].apply(int, base=16) / SCALE)**YEAR - 1
data["price_dec"] = data["collateral_asset_price"].apply(int, base=16) / SCALE

print("3. Successfully transformed data")

# PART 3: Compute variables of interest
# -------------------------------------------------------

# Debt and Total Supplied (native)
data["debt"] = data["debt_dec"] * data["accumulator_dec"]
data["supplied"] = data["debt"] + data["reserve_dec"]

# Debt and Total Supplied (USD)
data["debt_usd"] = data["debt"] * data["price_dec"]
data["supplied_usd"] = data["supplied"] * data["price_dec"]

# Utilization
data["utilization"] = (data["debt_usd"] / data["supplied_usd"])*100

# Interest rate (p.a.)
data = data.sort_values(["timestamp"])
data["time_diff"] = data.groupby(["market"])["timestamp"].diff()
data["last_accumulator"] = data.groupby(["market"])["accumulator_dec"].shift()
data["rate_grow"] = data["accumulator_dec"] / data["last_accumulator"]
data["borrow_rate"] = (data["rate_grow"].pow(YEAR/data["time_diff"])-1)*100
data['full_rate'] = data['full_rate_dec'] * 100

# Resample data to hourly in order to align time scale across markets
data_hourly=data.groupby("market").resample("1h",on="date")[["debt_usd","supplied_usd"]].mean().ffill().reset_index(drop=False)

# Total debt and supplied (USD), and avg utilization across markets
total_hourly = data_hourly.groupby("date")[["debt_usd","supplied_usd"]].sum()
total_hourly["utilization"] = (total_hourly["debt_usd"] / total_hourly["supplied_usd"])*100

print("3. Successfully computed variables")

# PART 4: Plot total
# -------------------------------------------------------

# use subset to remove spikes at start and end of sample
start = total_hourly.reset_index(drop=False).date.min() + pd.DateOffset(5)
end = total_hourly.reset_index(drop=False).date.max() - pd.DateOffset(1)
total_sub = total_hourly.query("date>@start and date<@end")

fig, ax = plt.subplots(figsize=(8,6))
total_sub.plot.area(
    ax=ax,
    y=["supplied_usd","debt_usd"],
    label=["Supplied","Debt"],
    stacked=False)
plt.ylabel("Market Size (USD)")
total_sub.plot.line(
    ax=ax,
    y="utilization",
    secondary_y=True,
    label="Utilization")
plt.xlabel("")
plt.ylabel("Utilization (%)")
plt.savefig(start.strftime('%Y-%m-%d') + '_' + end.strftime('%Y-%m-%d') + '_total.png', transparent=False)

print("4. Successfully plotted total")

# PART 5: Plot market share
# -------------------------------------------------------

# use subset to remove spikes at start and end of sample
hourly_sub = data_hourly.query("date>@start and date<@end")

# compute market shares
hourly_sub["total_supplied"] = hourly_sub.groupby("date")["supplied_usd"].transform(sum)
hourly_sub["market_share"] = hourly_sub["supplied_usd"] / hourly_sub["total_supplied"]                                  

# plot stacked areas
hourly_sub.pivot(index="date", columns="market", values="market_share").plot(
    kind="area", stacked=True, figsize=(8,6))
plt.xlabel("")
plt.ylabel("Market Share (%)")
plt.savefig(start.strftime('%Y-%m-%d') + '_' + end.strftime('%Y-%m-%d') + '_market-share.png', transparent=False)

print("5. Successfully plotted market share")

# PART 6: Plot markets
# -------------------------------------------------------

# use subset and resample to remove outliers
data_sub = data.query("date>@start and date<@end")
data_sub=data_sub.groupby("market").resample("1h",on="date")[["borrow_rate","utilization"]].max().ffill().reset_index(drop=False)

# plot utilization and rate for each market
data_sub.set_index("date", inplace=True, drop=False)
for label, df in data_sub.groupby('market'):
    ax = df.plot(y="borrow_rate", label="Borrow APR", figsize=(8,6))
    plt.ylabel("Borrow Rate (%)")
    df.plot(ax=ax, y="utilization", label="Utilization", secondary_y=True)
    plt.ylabel("Utilization (%)")
    plt.title(label)
    plt.xlabel("")
    plt.savefig(start.strftime('%Y-%m-%d') + '_' + end.strftime('%Y-%m-%d') + '_' + label + '_.png', transparent=False)

print("6. Successfully plotted markets")