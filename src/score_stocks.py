from pathlib import Path

import pandas as pd

FEATURE_DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "raw" / "feature_data.csv"
SCORED_DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "raw" / "scored_data.csv"


def score_stocks():
    df = pd.read_csv(FEATURE_DATA_PATH).sort_values(["ticker", "date"])
    # Each condition contributes one point to the daily score.
    df["daily_score"] = (
        (df["return_20d"] > 0).astype(int)
        + (df["close"] > df["ma20"]).astype(int)
        + (df["close"] > df["ma60"]).astype(int)
        + (df["volume"] > df["volume_ma20"]).astype(int)
    )
    df["score"] = df.groupby("ticker")["daily_score"].transform(
        lambda series: series.rolling(7).sum()
    )
    return df


def main():
    score_stocks().to_csv(SCORED_DATA_PATH, index=False)


if __name__ == "__main__":
    main()
