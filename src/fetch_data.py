from datetime import datetime, timedelta

import pandas as pd
from pykrx import stock

from paths import PRICE_DATA, TICKER_NAMES, TOP100


def get_latest_business_day():
    d = datetime.today() - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def get_tickers():
    df = pd.read_csv(TOP100, dtype={"ticker": str})
    df["ticker"] = df["ticker"].str.zfill(6)
    return df["ticker"].tolist()


def fetch_price_data():
    end = get_latest_business_day()
    start = (datetime.strptime(end, "%Y%m%d") - timedelta(days=220)).strftime("%Y%m%d")
    tickers = get_tickers()
    frames = [
        stock.get_market_ohlcv_by_date(start, end, ticker).assign(ticker=ticker)
        for ticker in tickers
    ]
    return pd.concat(frames).reset_index()


def fetch_ticker_names(tickers: list[str]) -> pd.DataFrame:
    """pykrx에서 ticker → 종목명 매핑을 직접 가져온다."""
    return pd.DataFrame({
        "ticker": tickers,
        "name": [stock.get_market_ticker_name(t) for t in tickers],
    })


def main():
    tickers = get_tickers()

    price_df = fetch_price_data()
    price_df.to_csv(PRICE_DATA, index=False)

    names_df = fetch_ticker_names(tickers)
    names_df.to_csv(TICKER_NAMES, index=False)

    print(price_df.head())
    print(names_df)


if __name__ == "__main__":
    main()
