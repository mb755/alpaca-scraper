import configparser as cfg
import os

# import pandas as pd

from utils.config_parser import default_parser
from utils.alpaca_data import get_active_assets

from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockQuotesRequest

from datetime import datetime, date, time
from pytz import timezone
from tqdm import tqdm

root_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

###########################################################
# parse command line arguments
###########################################################

parser = default_parser(description="Save some marketdata to disk.")

args = vars(parser.parse_args())

output_suffix = args["output_suffix"]
config_file = args["config_file"]

###########################################################
# grab initial values from config file
###########################################################

config = cfg.ConfigParser()
config.read(config_file)

api_key = config.get("alpaca", "api_key")
secret_key = config.get("alpaca", "secret_key")

###########################################################
# get quote data
###########################################################

trading_client = TradingClient(api_key=api_key, secret_key=secret_key)

assets_df = get_active_assets(trading_client)
all_stocks = assets_df["symbol"].tolist()

# CR TODO: eventually switch to all assets
tickers = ["EGRX", "HD", "WMT", "TSLA"]

nyc = timezone("US/Eastern")

marketdata_client = StockHistoricalDataClient(api_key=api_key, secret_key=secret_key)

# CR TODO: get a date or daterange from command line
date = date(2024, 2, 5)
date_str = date.strftime("%Y-%m-%d")

output_filename = f"{root_dir}/output/quote_data_{date_str}{output_suffix}.h5"

# if output already exists, print a warning and exit
if os.path.exists(output_filename):
    print(f"Output file {output_filename} already exists. Exiting.")
    exit(1)

for ticker in (pbar := tqdm(tickers)):
    pbar.set_description(f"Processing {ticker}")

    request_params = StockQuotesRequest(
        symbol_or_symbols=tickers,
        # these times are in UTC, this loads in a full day of ticks
        start=nyc.localize(datetime.combine(date, time(3, 50))),
        end=nyc.localize(datetime.combine(date, time(21, 10))),
    )

    result = marketdata_client.get_stock_quotes(request_params)

    result_df = result.df

    # flatten conditions, so the result can be serialized
    result_df["conditions"] = result_df["conditions"].apply(lambda x: ",".join(x))

    result_df.to_hdf(
        output_filename,
        key=ticker,
        mode="a",
        format="table",
        complevel=9,
        complib="blosc",
    )

# maybe save different assets in different tables, with key=ticker
