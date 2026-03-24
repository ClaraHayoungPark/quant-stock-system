import numpy as np
import pandas as pd

from paths import FEATURE_DATA, SCORED_DATA, TOP100


def compute_fundamental_scores(top100_df: pd.DataFrame) -> pd.DataFrame:
    per = top100_df["per"]
    pbr = top100_df["pbr"]
    roe = top100_df["roe"]

    per_valid = per.notna() & (per > 0)
    pbr_valid = pbr.notna() & (pbr > 0)
    roe_valid = roe.notna() & (roe > 0)

    per_score = np.select(
        [per_valid & (per <= 15), per_valid & (per <= 30), per_valid & (per <= 50)],
        [3, 2, 1], default=0,
    )
    pbr_score = np.select(
        [pbr_valid & (pbr < 1.0), pbr_valid & (pbr < 2.0), pbr_valid & (pbr < 4.0)],
        [3, 2, 1], default=0,
    )
    roe_score = np.select(
        [roe_valid & (roe >= 20), roe_valid & (roe >= 10), roe_valid & (roe >= 5)],
        [3, 2, 1], default=0,
    )

    return pd.DataFrame({
        "ticker": top100_df["ticker"],
        "fundamental_score": per_score + pbr_score + roe_score,
    })


def score_stocks():
    df = pd.read_csv(FEATURE_DATA, dtype={"ticker": str}).sort_values(["ticker", "date"])
    df["ticker"] = df["ticker"].str.zfill(6)

    top100 = pd.read_csv(TOP100, dtype={"ticker": str})
    top100["ticker"] = top100["ticker"].str.zfill(6)

    # 날짜별 cross-sectional rank
    df["momentum_20d_rank"] = df.groupby("date")["return_20d"].rank(pct=True)
    df["momentum_60d_rank"] = df.groupby("date")["return_60d"].rank(pct=True)
    df["momentum_120d_rank"] = df.groupby("date")["return_120d"].rank(pct=True)
    df["liquidity_rank"] = df.groupby("date")["traded_value_ma20"].rank(pct=True)
    df["stability_rank"] = df.groupby("date")["volatility_20d"].rank(pct=True, ascending=False)
    vol_80pct = df.groupby("date")["volatility_20d"].transform(lambda x: x.quantile(0.80))

    # 펀더멘탈 스코어
    df = df.merge(compute_fundamental_scores(top100), on="ticker", how="left")
    df["fundamental_score"] = df["fundamental_score"].fillna(0)

    # 1. Trend Score (0~4): MA 정배열 + 가격 위치
    #    중장기 상승 추세인지 확인
    df["trend_score"] = (
        (df["ma20"] > df["ma60"]).astype(int)       # 단기 > 중기
        + (df["ma60"] > df["ma120"]).astype(int)    # 중기 > 장기 (정배열)
        + (df["close"] > df["ma20"]).astype(int)    # 가격이 단기MA 위
        + (df["return_120d"] > 0.05).astype(int)    # 120일 수익률 +5% 이상
    )

    # 2. Momentum Score (0~3): 날짜별 상대 모멘텀 (cross-sectional rank)
    #    같은 날짜 내에서 얼마나 강한지 비교
    df["momentum_score"] = (
        (df["momentum_120d_rank"] >= 0.60).astype(int)  # 120일 모멘텀 상위 40%
        + (df["momentum_60d_rank"] >= 0.55).astype(int) # 60일 모멘텀 상위 45%
        + (df["momentum_20d_rank"] >= 0.50).astype(int) # 20일 모멘텀 상위 50%
    )

    # 3. Entry Score (0~4): 눌림목 진입 타이밍
    #    추세 내 조정 구간에서 매수 기회를 포착
    df["entry_score"] = (
        df["drawdown_from_high_120"].between(-0.30, -0.05).astype(int) * 2  # 고점 대비 5~30% 조정
        + df["price_to_ma60"].between(0.93, 1.03).astype(int)               # MA60 ±7% 이내
        + df["volume_ratio"].between(0.5, 1.8).astype(int)                  # 거래량 과열 없음
    )

    # 4. Stability Score (0~3): 안정성
    #    변동성 낮고 유동성 충분한 종목 선호
    df["stability_score"] = (
        (df["stability_rank"] >= 0.50).astype(int)  # 변동성 하위 50%
        + (df["liquidity_rank"] >= 0.50).astype(int) # 유동성 상위 50%
        + (df["return_60d"] > -0.10).astype(int)    # 60일 낙폭 -10% 이내
    )

    # 5. Fundamental Score (0~3): 기업 가치 (PER/PBR/ROE 기반 9점 → 3점 정규화)
    df["fundamental_score_norm"] = (df["fundamental_score"] / 9.0) * 3.0

    # 합산 (이론상 최대 ~13점)
    df["daily_score"] = (
        df["trend_score"]
        + df["momentum_score"]
        + df["entry_score"]
        + df["stability_score"]
        + df["fundamental_score_norm"]
    )

    # 페널티
    df.loc[df["volatility_20d"] > vol_80pct, "daily_score"] -= 2           # 변동성 상위 20%
    df.loc[df["drawdown_from_high_120"] < -0.45, "daily_score"] -= 2       # 고점 대비 -45% 이상 급락
    df.loc[df["return_20d"] > 0.20, "daily_score"] -= 2                    # 20일 +20% 이상 단기 급등
    df.loc[df["return_20d"] < -0.20, "daily_score"] -= 2                   # 20일 -20% 이상 단기 급락

    loss_tickers = top100.loc[top100["per"].notna() & (top100["per"] < 0), "ticker"].tolist()
    df.loc[df["ticker"].isin(loss_tickers), "daily_score"] -= 2            # 적자 기업

    # 3일 rolling mean (5일보다 빠른 반응, 노이즈 완화)
    df["score"] = df.groupby("ticker")["daily_score"].transform(
        lambda s: s.rolling(3, min_periods=2).mean()
    )

    return df


def main():
    score_stocks().to_csv(SCORED_DATA, index=False)


if __name__ == "__main__":
    main()
