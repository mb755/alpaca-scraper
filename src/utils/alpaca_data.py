import pandas as pd

from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus
from alpaca.data.requests import StockQuotesRequest

from datetime import datetime, time
from pytz import timezone


def get_active_assets(trading_client):
    search_params = GetAssetsRequest(
        asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE
    )

    active_assets = trading_client.get_all_assets(search_params)
    assets_df = pd.DataFrame(active_assets)

    # extract column names
    assets_df.columns = list(zip(*assets_df.loc[0]))[0]
    assets_df = assets_df.map(lambda x: x[1])

    return assets_df


def stockday_request(ticker, date):

    nyc = timezone("US/Eastern")

    request_params = StockQuotesRequest(
        symbol_or_symbols=[ticker],
        # these times are in UTC, this loads in a full day of ticks
        start=nyc.localize(datetime.combine(date, time(3, 50))),
        end=nyc.localize(datetime.combine(date, time(21, 10))),
    )

    return request_params
