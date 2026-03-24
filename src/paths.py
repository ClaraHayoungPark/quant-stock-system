from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"

PRICE_DATA = RAW / "price_data.csv"
FEATURE_DATA = RAW / "feature_data.csv"
SCORED_DATA = RAW / "scored_data.csv"
TOP_STOCKS = RAW / "top_stocks.csv"
TOP100 = RAW / "top100_stocks.csv"
TICKER_NAMES = RAW / "ticker_names.csv"
