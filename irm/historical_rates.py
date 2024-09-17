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
columns = [2,4,6,8,10,12,14,16,17,18,20,22] # collateral_asset_config (we're not interested in debt asset)
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
    "fee_rate"
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

print("3. Successfully transformed data")

# PART 3: Compute variables of interest
# -------------------------------------------------------

# Utilization
data["debt"] = data["debt_dec"] * data["accumulator_dec"]
data["totalSupplied"] = data["debt"] + data["reserve_dec"]
data["utilization"] = (data["debt"] / data["totalSupplied"])*100

# Interest rate (p.a.)
data = data.sort_values(["timestamp"])
data["time_diff"] = data.groupby(["market"])["timestamp"].diff()
data["last_accumulator"] = data.groupby(["market"])["accumulator_dec"].shift()
data["rate_grow"] = data["accumulator_dec"] / data["last_accumulator"]
data["borrow_rate"] = (data["rate_grow"].pow(YEAR/data["time_diff"])-1)*100
data['full_rate'] = data['full_rate_dec'] * 100

print("4. Successfully computed variables")

# PART 4: Plot variables
# -------------------------------------------------------

# set date as index for plotting
data.set_index("date", inplace=True, drop=False)

# utilization
fig, ax = plt.subplots(figsize=(8,6))
for label, df in data.groupby('market'):
    df.utilization.plot(ax=ax, label=label)

plt.xlabel("")
plt.ylabel("Utilization (%)")
plt.legend()
plt.savefig(data.date.min().strftime('%Y-%m-%d') + '_' + data.date.max().strftime('%Y-%m-%d') + '_utilization.png', transparent=False)

# annualized borrow rate
fig, ax = plt.subplots(figsize=(8,6))
for label, df in data.groupby('market'):
    df.borrow_rate.plot(ax=ax, label=label)

plt.xlabel("")
plt.ylabel("Borrow Rate (%)")
plt.legend()
plt.savefig(data.date.min().strftime('%Y-%m-%d') + '_' + data.date.max().strftime('%Y-%m-%d') + '_rates.png', transparent=False)

print("5. Successfully plotted variables")