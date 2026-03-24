from __future__ import annotations

import sys
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from fetch_data import main as fetch_main
from build_features import main as build_main
from score_stocks import main as score_main
from select_stocks import main as select_main
from send_email import main as send_main

with DAG(
    dag_id="quant_stock_pipeline",
    description="Fetches price data, builds features, scores stocks, selects picks, and sends the email report.",
    start_date=pendulum.datetime(2026, 3, 13, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["quant", "stocks", "daily"],
) as dag:
    t1 = PythonOperator(task_id="fetch_price_data", python_callable=fetch_main)
    t2 = PythonOperator(task_id="build_features", python_callable=build_main)
    t3 = PythonOperator(task_id="score_stocks", python_callable=score_main)
    t4 = PythonOperator(task_id="select_top_stocks", python_callable=select_main)
    t5 = PythonOperator(task_id="send_email_report", python_callable=send_main)

    t1 >> t2 >> t3 >> t4 >> t5
