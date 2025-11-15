# airflow/dags/ohlcv_minute.py
import os, datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator

SYMBOLS = os.getenv("SYMBOLS", "BTC/USDT,ETH/USDT").split(",")
EXCHANGE_NAME = os.getenv("EXCHANGE", "binance")

def task_fetch_and_upsert(**context):
    import ccxt
    from ingestion.common import fetch_last_closed_1m, upsert_ohlcv
    ex = getattr(ccxt, EXCHANGE_NAME)({"enableRateLimit": True})
    rows = []
    for sym in SYMBOLS:
        row = fetch_last_closed_1m(ex, sym.strip(), EXCHANGE_NAME)
        if row: rows.append(row)
    n = upsert_ohlcv(rows)
    return {"inserted": n, "symbols": SYMBOLS}

with DAG(
    dag_id="ohlcv_minute",
    start_date=dt.datetime(2025, 1, 1),
    schedule_interval="* * * * *",   # every minute
    catchup=False,
    max_active_runs=1,
    tags=["crypto","ingest","1m"],
) as dag:
    run = PythonOperator(
        task_id="fetch_last_closed",
        python_callable=task_fetch_and_upsert,
    )
