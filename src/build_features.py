import pandas as pd

from paths import FEATURE_DATA, PRICE_DATA

RENAME_COLUMNS = {
    "날짜": "date",
    "index": "date",
    "시가": "open",
    "고가": "high",
    "저가": "low",
    "종가": "close",
    "거래량": "volume",
    "등락률": "return",
}


def build_features():
    df = pd.read_csv(PRICE_DATA, dtype={"ticker": str}).rename(columns=RENAME_COLUMNS)
    df["ticker"] = df["ticker"].str.zfill(6)
    df = df.sort_values(["ticker", "date"])

    close_g = df.groupby("ticker")["close"]
    volume_g = df.groupby("ticker")["volume"]

    df["ma20"] = close_g.transform(lambda s: s.rolling(20).mean())
    df["ma60"] = close_g.transform(lambda s: s.rolling(60).mean())
    df["ma120"] = close_g.transform(lambda s: s.rolling(120).mean())
    df["return_20d"] = close_g.transform(lambda s: s.pct_change(20))
    df["return_60d"] = close_g.transform(lambda s: s.pct_change(60))
    df["return_120d"] = close_g.transform(lambda s: s.pct_change(120))

    df["volume_ma20"] = volume_g.transform(lambda s: s.rolling(20).mean())
    df["traded_value"] = df["close"] * df["volume"]
    df["traded_value_ma20"] = df.groupby("ticker")["traded_value"].transform(
        lambda s: s.rolling(20).mean()
    )
    df["volume_ratio"] = df["volume"] / df["volume_ma20"]
    df["volatility_20d"] = df.groupby("ticker")["return"].transform(
        lambda s: (s / 100).rolling(20).std()
    )

    rolling_high_120 = close_g.transform(lambda s: s.rolling(120).max())
    rolling_low_60 = close_g.transform(lambda s: s.rolling(60).min())
    rolling_low_120 = close_g.transform(lambda s: s.rolling(120).min())

    df["price_to_ma120"] = df["close"] / df["ma120"]
    df["price_to_ma60"] = df["close"] / df["ma60"]
    df["drawdown_from_high_120"] = (df["close"] / rolling_high_120) - 1
    df["rebound_from_low_60"] = (df["close"] / rolling_low_60) - 1
    df["rebound_from_low_120"] = (df["close"] / rolling_low_120) - 1
    return df


def main():
    df = build_features()
    df.to_csv(FEATURE_DATA, index=False)
    print(df.head())
    print(df.columns.tolist())


if __name__ == "__main__":
    main()
