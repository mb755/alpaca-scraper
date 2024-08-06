import pandas as pd

from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus


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
