from pathlib import Path

import pandas as pd
from pykrx import stock

SCORED_DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "raw" / "scored_data.csv"
TOP_STOCKS_PATH = Path(__file__).resolve().parents[1] / "data" / "raw" / "top_stocks.csv"


def select_top_stocks():
    df = pd.read_csv(SCORED_DATA_PATH, dtype={"ticker": str})
    df["ticker"] = df["ticker"].str.zfill(6)
    latest_date = df["date"].max()
    # Rank only the most recent trading day's candidates.
    top_stocks = (
        df.loc[df["date"] == latest_date]
        .sort_values(["score", "return_20d"], ascending=[False, False])
        .head(5)
        .copy()
    )
    top_stocks["name"] = top_stocks["ticker"].map(stock.get_market_ticker_name)
    return latest_date, top_stocks


def main():
    latest_date, top_stocks = select_top_stocks()
    top_stocks.to_csv(TOP_STOCKS_PATH, index=False)
    print("latest date:", latest_date)
    print()
    print(top_stocks[["ticker", "name", "close", "score"]])


if __name__ == "__main__":
    main()
