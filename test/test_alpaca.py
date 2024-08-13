import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import os
import sys

root_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, f"{root_dir}/src")


class ParsedArgs:
    def __init__(self, output_suffix, config_file, all_assets, ticker_file, overwrite):
        self.output_suffix = output_suffix
        self.config_file = config_file
        self.all_assets = all_assets
        self.ticker_file = ticker_file
        self.overwrite = overwrite


class MarketdataClient:
    def get_stock_quotes(self, _):
        pass


@pytest.fixture
def mock_dependencies():
    with patch("configparser.ConfigParser") as mock_ConfigParser, patch(
        "alpaca.trading.client.TradingClient"
    ) as mock_TradingClient, patch(
        "alpaca.data.historical.StockHistoricalDataClient"
    ) as mock_StockHistoricalDataClient, patch(
        "alpaca.data.requests.StockQuotesRequest"
    ) as mock_StockQuotesRequest, patch(
        "utils.config_parser.default_parser"
    ) as mock_argument_parser, patch(
        "pandas.read_csv"
    ) as mock_read_csv, patch(
        "os.path.exists"
    ) as mock_exists:

        yield {
            "mock_ConfigParser": mock_ConfigParser,
            "mock_TradingClient": mock_TradingClient,
            "mock_StockHistoricalDataClient": mock_StockHistoricalDataClient,
            "mock_StockQuotesRequest": mock_StockQuotesRequest,
            "mock_argument_parser": mock_argument_parser,
            "mock_read_csv": mock_read_csv,
            "mock_exists": mock_exists,
        }


def test_get_quote_data(mock_dependencies):
    # Mock command line arguments
    mock_arg_parser = MagicMock()
    mock_arg_parser.parse_args.return_value = ParsedArgs(
        output_suffix="_test",
        config_file="config.ini",
        all_assets=False,
        ticker_file="tickers.csv",
        overwrite=False,
    )
    mock_dependencies["mock_argument_parser"].return_value = mock_arg_parser

    # Mock config parser
    mock_config = MagicMock()
    mock_config.get.side_effect = lambda section, key: "test_key"
    mock_dependencies["mock_ConfigParser"].return_value = mock_config

    # Mock TradingClient and StockHistoricalDataClient
    mock_trading_client = MagicMock()
    mock_dependencies["mock_TradingClient"].return_value = mock_trading_client

    mock_marketdata_client = MagicMock()
    mock_dependencies["mock_StockHistoricalDataClient"].return_value = (
        mock_marketdata_client
    )

    # Mock read_csv
    mock_dependencies["mock_read_csv"].return_value = pd.DataFrame(
        {0: ["AAPL", "GOOG"]}
    )

    # Mock os.path.exists
    mock_dependencies["mock_exists"].return_value = False

    # Execute the script
    import get_quote_data  # noqa F401

    # Assertions
    mock_dependencies["mock_ConfigParser"].assert_called_once_with()
    mock_dependencies["mock_TradingClient"].assert_called_with(
        api_key="test_key", secret_key="test_key"
    )
    mock_dependencies["mock_StockHistoricalDataClient"].assert_called_once_with(
        api_key="test_key", secret_key="test_key"
    )
    mock_dependencies["mock_read_csv"].assert_called_once_with(
        "tickers.csv", header=None
    )
    mock_dependencies["mock_exists"].assert_called_with(
        f"{root_dir}/output/quote_data_2024-02-05_test.h5"
    )

    # Check if data is processed and saved correctly
    mock_marketdata_client.get_stock_quotes.assert_called()
