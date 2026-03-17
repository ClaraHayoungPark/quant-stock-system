from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"


def run_script(script_name: str) -> None:
    script_path = SRC_DIR / script_name
    subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(SRC_DIR),
        check=True,
    )


with DAG(
    dag_id="quant_stock_pipeline",
    description="Fetches price data, builds features, scores stocks, selects picks, and sends the email report.",
    start_date=pendulum.datetime(2026, 3, 13, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["quant", "stocks", "daily"],
) as dag:
    fetch_data = PythonOperator(
        task_id="fetch_price_data",
        python_callable=run_script,
        op_kwargs={"script_name": "fetch_data.py"},
    )

    build_features = PythonOperator(
        task_id="build_features",
        python_callable=run_script,
        op_kwargs={"script_name": "build_features.py"},
    )

    score_stocks = PythonOperator(
        task_id="score_stocks",
        python_callable=run_script,
        op_kwargs={"script_name": "score_stocks.py"},
    )

    select_top_stocks = PythonOperator(
        task_id="select_top_stocks",
        python_callable=run_script,
        op_kwargs={"script_name": "select_stocks.py"},
    )

    send_email = PythonOperator(
        task_id="send_email_report",
        python_callable=run_script,
        op_kwargs={"script_name": "send_email.py"},
    )

    fetch_data >> build_features >> score_stocks >> select_top_stocks >> send_email
