# airflow/dags/backfill_binance.py
import os, datetime as dt, json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

EXCHANGE_NAME = os.getenv("EXCHANGE", "binance")

def do_backfill(**context):
    import ccxt
    from ingestion.common import fetch_range_1m, upsert_ohlcv

    conf = context.get("dag_run").conf or {}
    symbol = conf.get("symbol", "BTC/USDT")
    days   = int(conf.get("days", 7))
    end    = conf.get("until_utc")  # optional ISO string

    until = dt.datetime.fromisoformat(end.replace("Z","+00:00")).timestamp()*1000 if end else dt.datetime.utcnow().timestamp()*1000
    since = until - days*24*60*60*1000

    ex = getattr(ccxt, EXCHANGE_NAME)({"enableRateLimit": True})
    total = 0
    for rows in fetch_range_1m(ex, symbol, int(since), int(until)):
        total += upsert_ohlcv(rows)
    return {"symbol": symbol, "inserted": total, "days": days}

with DAG(
    dag_id="backfill_binance",
    start_date=dt.datetime(2025, 1, 1),
    schedule_interval=None,   # manual/adhoc
    catchup=False,
    tags=["crypto","backfill"],
) as dag:
    backfill = PythonOperator(
        task_id="backfill_1m_range",
        python_callable=do_backfill,
    )
