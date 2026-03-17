from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from pykrx import stock

from ticker_list import TICKERS

DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "raw" / "price_data.csv"


def get_latest_business_day():
    # Use the most recent completed trading day.
    d = datetime.today()
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def fetch_price_data():
    end = get_latest_business_day()
    start = (datetime.strptime(end, "%Y%m%d") - timedelta(days=60)).strftime("%Y%m%d")
    # Collect each ticker's OHLCV data into one combined frame.
    frames = [
        stock.get_market_ohlcv_by_date(start, end, ticker).assign(ticker=ticker)
        for ticker in TICKERS
    ]
    return pd.concat(frames).reset_index().rename(columns={"index": "date"})


def main():
    price_df = fetch_price_data()
    price_df.to_csv(DATA_PATH, index=False)
    print(price_df.head())


if __name__ == "__main__":
    main()
