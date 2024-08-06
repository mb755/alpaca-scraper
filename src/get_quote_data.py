import configparser as cfg

from utils.config_parser import default_parser
from utils.alpaca_data import get_active_assets

from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockQuotesRequest

from datetime import datetime
from pytz import timezone


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

# switch to all assets
tickers = ["HD", "WMT"]

tz = timezone("US/Eastern")

print(datetime.now(tz))

marketdata_client = StockHistoricalDataClient(api_key=api_key, secret_key=secret_key)

request_params = StockQuotesRequest(
    symbol_or_symbols=tickers,
    # these times are in UTC, this loads in a full day of ticks
    start=datetime(2024, 2, 5, 9, 00, 00),
    end=datetime(2024, 2, 6, 1, 00, 00),
)

result = marketdata_client.get_stock_quotes(request_params)

result_df = result.df

print(result_df.head())

print(result_df.shape)

# CR TODO: maybe get this in pages? Maybe the results come sorted by symbol?
