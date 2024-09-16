import os
from dotenv import load_dotenv, find_dotenv
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

# PART 1: percentage scores per day and asset
# ----------------------------------------------------------

# import db credentials
path = find_dotenv(filename='.env', raise_error_if_not_found=True, usecwd=True)
load_dotenv(path)

# constants
SCALE = 10**18
YEAR = 365*86400

# eligible assets
markets = pd.DataFrame({
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

# PYTHON FUNCTION TO CONNECT TO THE POSTGRESQL DATABASE AND
# RETURN THE SQLACHEMY ENGINE OBJECT
def get_connection():
	try:
		# GET THE CONNECTION OBJECT (ENGINE) FOR THE DATABASE
		conn = psycopg2.connect(
			database = os.environ.get('PG_DB'), 
            user = os.environ.get('PG_USER'), 
            host= os.environ.get('PG_HOST'),
            password = os.environ.get('PG_PWD'),
            port = os.environ.get('PG_PORT'))
		print(
			f"1. Successfully connected to database")
		return conn
	except Exception as ex:
		print("Connection could not be made due to the following error: \n", ex)

# PART 1: Fetch indexed events
# -------------------------------------------------------

# Connect to db and open cursor to perform database operations
conn = get_connection()
cur = conn.cursor()

# Fetch update_context events
cur.execute("SELECT \"timestamp\",\"collateralAsset\",\"collateralAssetPrice\",\"collateral_reserve\",\"collateral_total_nominal_debt\",\"collateral_last_rate_accumulator\",\"collateralAssetScale\" FROM update_contexts")
records = cur.fetchall()
column_names = ["timestamp","asset","price","reserve","nominalDebt","rateAccumulator","scale"]
events = pd.DataFrame(records, columns=column_names)

print("2. Successfully fetched events")

cur.close()
conn.close()

# PART 2: Transform event data so it can be processed
# -------------------------------------------------------

# transform data
data = pd.merge(events, markets, on="asset")
data["date"] = pd.to_datetime(data["timestamp"], unit="s")
data["price"] = data["price"] / SCALE
data["nominalDebt"] = data["nominalDebt"] / SCALE
data["rateAccumulator"] = data["rateAccumulator"] / SCALE
data["reserve"] = data["reserve"] / data["scale"]

# Filter markets and time window of interest
start = pd.to_datetime("2024-07-10")
end = pd.to_datetime("2024-09-30")
data = data.query("date >= @start and date <= @end")

print("3. Successfully transformed data")

# PART 3: Compute variables of interest
# -------------------------------------------------------

# Utilization
data["debt"] = data["nominalDebt"] * data["rateAccumulator"]
data["totalSupplied"] = data["debt"] + data["reserve"]
data["utilization"] = (data["debt"] / data["totalSupplied"])*100

# Interest rate (p.a.)
data = data.sort_values(["timestamp"])
data["timeDiff"] = data.groupby(["market"])["timestamp"].diff()
data["lastRateAccumulator"] = data.groupby(["market"])["rateAccumulator"].shift()
data["rateGrow"] = data["rateAccumulator"] / data["lastRateAccumulator"]
data["borrowRate"] = (data["rateGrow"].pow(YEAR/data["timeDiff"])-1)*100

print("4. Successfully computed variables")

# PART 4: Plot variables
# -------------------------------------------------------

# resample to hourly data for plotting
data.set_index("date", inplace=True, drop=False)
sample = data.groupby(["market"]).resample("1h")["utilization","borrowRate"].agg("max").interpolate().reset_index()
sample.set_index("date", inplace=True, drop=False)

# utilization
fig, ax = plt.subplots(figsize=(8,6))
for label, df in sample.groupby('market'):
    df.utilization.plot(ax=ax, label=label)

plt.xlabel("")
plt.ylabel("Utilization (%)")
plt.legend()
plt.savefig(sample.date.min().strftime('%Y-%m-%d') + '_' + sample.date.max().strftime('%Y-%m-%d') + '_utilization.png', transparent=False)

# annualized borrow rate
fig, ax = plt.subplots(figsize=(8,6))
for label, df in sample.groupby('market'):
    df.borrowRate.plot(ax=ax, label=label)

plt.xlabel("")
plt.ylabel("Borrow Rate (%)")
plt.legend()
plt.savefig(start.strftime('%Y-%m-%d') + '_' + end.strftime('%Y-%m-%d') + '_rates.png', transparent=False)

print("5. Successfully plotted variables")