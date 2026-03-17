from pathlib import Path

import pandas as pd

PRICE_DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "raw" / "price_data.csv"
FEATURE_DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "raw" / "feature_data.csv"
RENAME_COLUMNS = {
    "날짜": "date",
    "시가": "open",
    "고가": "high",
    "저가": "low",
    "종가": "close",
    "거래량": "volume",
    "등락률": "return",
}


def build_features():
    df = pd.read_csv(PRICE_DATA_PATH).rename(columns=RENAME_COLUMNS)
    df = df.sort_values(["ticker", "date"])
    # Reuse grouped series so rolling features stay compact.
    close_by_ticker = df.groupby("ticker")["close"]
    volume_by_ticker = df.groupby("ticker")["volume"]

    df["ma20"] = close_by_ticker.transform(lambda series: series.rolling(20).mean())
    df["ma60"] = close_by_ticker.transform(lambda series: series.rolling(60).mean())
    df["return_20d"] = close_by_ticker.transform(lambda series: series.pct_change(20))
    df["volume_ma20"] = volume_by_ticker.transform(
        lambda series: series.rolling(20).mean()
    )
    return df


def main():
    df = build_features()
    df.to_csv(FEATURE_DATA_PATH, index=False)
    print(df.head())
    print(df.columns)


if __name__ == "__main__":
    main()
