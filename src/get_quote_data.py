import configparser as cfg
import os
import pandas as pd

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
all_assets = args["all_assets"]
ticker_file = args["ticker_file"]
overwrite = args["overwrite"]

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

if all_assets:
    assets_df = get_active_assets(trading_client)
    tickers = assets_df["symbol"].tolist()
else:
    tickers = pd.read_csv(ticker_file, header=None).iloc[:, 0].tolist()

nyc = timezone("US/Eastern")

marketdata_client = StockHistoricalDataClient(api_key=api_key, secret_key=secret_key)

# CR TODO: get a date or daterange from command line
date = date(2024, 2, 5)
date_str = date.strftime("%Y-%m-%d")

output_filename = f"{root_dir}/output/quote_data_{date_str}{output_suffix}.h5"

# if output already exists, print a warning
initial_mode = "a"
if os.path.exists(output_filename):
    if overwrite:
        print(f"Output file {output_filename} already exists. Overwriting.", flush=True)
        initial_mode = "w"
    else:
        print(f"Output file {output_filename} already exists. Exiting.", flush=True)
        exit(1)

mode = initial_mode

for ticker in (pbar := tqdm(tickers)):
    pbar.set_description(f"Processing {ticker}")

    request_params = StockQuotesRequest(
        symbol_or_symbols=[ticker],
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
        mode=mode,
        format="table",
        complevel=9,
        complib="blosc",
    )

    # everything after the first table has to be appended
    mode = "a"

print(f"Data saved to {output_filename}", flush=True)
