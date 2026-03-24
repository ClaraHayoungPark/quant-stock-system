import pandas as pd

from paths import SCORED_DATA, TICKER_NAMES, TOP100, TOP_STOCKS


def _read_ticker_names() -> pd.DataFrame:
    """ticker_names.csv 우선, 없으면 top100_stocks.csv에서 이름 fallback."""
    if TICKER_NAMES.exists():
        df = pd.read_csv(TICKER_NAMES, dtype={"ticker": str})
    else:
        df = pd.read_csv(TOP100, dtype={"ticker": str})[["ticker", "name"]]
    df["ticker"] = df["ticker"].str.zfill(6)
    return df


def select_top_stocks():
    df = pd.read_csv(SCORED_DATA, dtype={"ticker": str})
    df["ticker"] = df["ticker"].str.zfill(6)
    latest_date = df["date"].max()

    top100 = pd.read_csv(TOP100, dtype={"ticker": str})[["ticker", "per", "pbr", "roe"]]
    top100["ticker"] = top100["ticker"].str.zfill(6)

    names = _read_ticker_names()

    top_stocks = (
        df[df["date"] == latest_date]
        .sort_values(
            ["score", "trend_score", "momentum_score", "entry_score"],
            ascending=False,
        )
        .head(5)
        .copy()
        .merge(names, on="ticker", how="left")
        .merge(top100, on="ticker", how="left")
    )
    return latest_date, top_stocks


def main():
    latest_date, top_stocks = select_top_stocks()
    top_stocks.to_csv(TOP_STOCKS, index=False)
    print("latest date:", latest_date)
    print()
    print(top_stocks[["ticker", "name", "close", "score", "trend_score",
                       "momentum_score", "entry_score", "stability_score",
                       "fundamental_score", "per", "pbr", "roe"]])


if __name__ == "__main__":
    main()
