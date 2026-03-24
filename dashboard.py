from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

SCORED_PATH = Path("data/raw/scored_data.csv")
TOP100_PATH = Path("data/raw/top100_stocks.csv")
TICKER_NAMES_PATH = Path("data/raw/ticker_names.csv")

MA_COLORS = {"종가": "#1f77b4", "MA20": "#ff7f0e", "MA60": "#2ca02c", "MA120": "#d62728"}


def _load_names() -> pd.DataFrame:
    src = TICKER_NAMES_PATH if TICKER_NAMES_PATH.exists() else TOP100_PATH
    df = pd.read_csv(src, dtype={"ticker": str})
    df["ticker"] = df["ticker"].str.zfill(6)
    return df[["ticker", "name"]] if "name" in df.columns else df[["ticker"]]


@st.cache_data
def load_data():
    scored = pd.read_csv(SCORED_PATH, dtype={"ticker": str})
    top100 = pd.read_csv(TOP100_PATH, dtype={"ticker": str})
    names = _load_names()

    scored["ticker"] = scored["ticker"].str.zfill(6)
    top100["ticker"] = top100["ticker"].str.zfill(6)
    scored["date"] = pd.to_datetime(scored["date"])

    latest_date = scored["date"].max()
    all_latest = (
        scored[scored["date"] == latest_date]
        .merge(names, on="ticker", how="left")
        .merge(top100[["ticker", "per", "pbr", "roe"]], on="ticker", how="left")
        .copy()
    )
    return scored, all_latest


# ── Tab 1: 종목 상세 분석 ─────────────────────────────────────────────────────
def tab_analysis(scored: pd.DataFrame, all_latest: pd.DataFrame, ticker: str) -> None:
    row = all_latest[all_latest["ticker"] == ticker].iloc[0]
    name = row.get("name", ticker) if pd.notna(row.get("name")) else ticker

    # ── 핵심 지표 카드 ──
    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    c1.metric("현재가",   f"{int(row['close']):,}원")
    c2.metric("Score",    f"{row['score']:.2f}" if pd.notna(row.get("score")) else "-")
    c3.metric("트렌드",   int(row["trend_score"])    if "trend_score"    in row.index else "-")
    c4.metric("눌림목",   int(row["pullback_score"])  if "pullback_score"  in row.index else "-")
    c5.metric("안정성",   int(row["stability_score"]) if "stability_score" in row.index else "-")
    c6.metric("PER",      f"{row['per']:.1f}"  if pd.notna(row.get("per"))  else "-")
    c7.metric("PBR",      f"{row['pbr']:.2f}"  if pd.notna(row.get("pbr"))  else "-")

    st.divider()

    # ── 가격 & 이동평균 ──
    df = scored[scored["ticker"] == ticker].sort_values("date")
    price_map = {"close": "종가", "ma20": "MA20", "ma60": "MA60", "ma120": "MA120"}
    melted = (
        df[["date"] + list(price_map)]
        .melt(id_vars="date", var_name="구분", value_name="가격")
    )
    melted["구분"] = melted["구분"].map(price_map)
    fig = px.line(
        melted, x="date", y="가격", color="구분",
        color_discrete_map=MA_COLORS, labels={"date": "날짜"},
        title="가격 & 이동평균",
    )
    fig.update_layout(height=380, margin=dict(t=40, b=10))
    st.plotly_chart(fig, use_container_width=True)

    # ── Score 추이 / 거래량 비율 ──
    latest = scored["date"].max()
    df30 = df[df["date"] >= latest - pd.Timedelta(days=30)].copy()

    col_l, col_r = st.columns(2)

    with col_l:
        fig2 = px.line(
            df30, x="date", y="score",
            labels={"date": "날짜", "score": "Score"},
            title="Score 추이 (최근 30일)",
        )
        fig2.update_traces(line_color="#EF553B", line_width=2)
        fig2.update_layout(height=300, margin=dict(t=40, b=10))
        st.plotly_chart(fig2, use_container_width=True)

    with col_r:
        fig3 = px.bar(
            df30, x="date", y="volume_ratio",
            labels={"date": "날짜", "volume_ratio": "거래량 비율"},
            title="거래량 비율 (최근 30일)",
        )
        fig3.add_hline(y=1.0, line_dash="dash", line_color="gray", annotation_text="기준(1.0)")
        fig3.update_layout(height=300, margin=dict(t=40, b=10))
        st.plotly_chart(fig3, use_container_width=True)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    st.set_page_config(page_title="퀀트 스톡 스크리너", layout="wide", page_icon="📈")
    scored, all_latest = load_data()
    latest_date = scored["date"].max().strftime("%Y-%m-%d")

    ticker_to_name = {
        t: (n if pd.notna(n) else t)
        for t, n in zip(all_latest["ticker"], all_latest.get("name", all_latest["ticker"]))
    }
    tickers_by_score = all_latest.sort_values("score", ascending=False)["ticker"].tolist()

    with st.sidebar:
        st.title("📈 퀀트 스톡")
        st.caption(f"기준일: {latest_date}")
        st.divider()
        selected_ticker = st.selectbox(
            "종목 선택",
            options=tickers_by_score,
            format_func=lambda t: f"{ticker_to_name.get(t, t)} ({t})",
        )

    selected_name = ticker_to_name.get(selected_ticker, selected_ticker)
    st.title(f"{selected_name}  `{selected_ticker}`")
    st.caption(f"기준일: {latest_date}")

    tab_analysis(scored, all_latest, selected_ticker)


main()
