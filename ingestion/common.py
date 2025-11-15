    # ingestion/common.py
import os, datetime as dt
import ccxt
import psycopg2
from psycopg2.extras import execute_values

PG = dict(
    host=os.getenv("PG_HOST", "timescaledb"),
    port=int(os.getenv("PG_PORT", "5432")),
    dbname=os.getenv("PG_DB", "crypto"),
    user=os.getenv("PG_USER", "postgres"),
    password=os.getenv("PG_PASS", "postgres"),
)

def get_conn():
    return psycopg2.connect(**PG)

def upsert_ohlcv(rows):
    if not rows: return 0
    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO ohlcv_1m (ts, symbol, open, high, low, close, volume, vwap, exchange)
            VALUES %s
            ON CONFLICT (symbol, ts) DO UPDATE SET
              open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
              close=EXCLUDED.close, volume=EXCLUDED.volume, vwap=EXCLUDED.vwap,
              exchange=EXCLUDED.exchange;
        """, rows)
    return len(rows)

def fetch_last_closed_1m(exchange, ccxt_symbol, vendor_name="binance"):
    ohlcv = exchange.fetch_ohlcv(ccxt_symbol, timeframe="1m", limit=2)
    if not ohlcv or len(ohlcv) < 2: return None
    ts_ms, o,h,l,c,v = ohlcv[-2]
    ts_iso = dt.datetime.utcfromtimestamp(ts_ms/1000).replace(tzinfo=dt.timezone.utc).isoformat()
    vwap = (h + l + c) / 3.0  # simple proxy (vendor VWAP not provided)
    return (ts_iso, ccxt_symbol, float(o), float(h), float(l), float(c), float(v), float(vwap), vendor_name)

def fetch_range_1m(exchange, ccxt_symbol, since_ms, until_ms):
    """
    Generator that yields contiguous 1m OHLCV rows between since_ms and until_ms (exclusive).
    Handles CCXT pagination (limit=1000 typical).
    """
    ms = since_ms
    while ms < until_ms:
        batch = exchange.fetch_ohlcv(ccxt_symbol, timeframe="1m", since=ms, limit=1000)
        if not batch: break
        rows, last = [], ms
        for ts_ms, o,h,l,c,v in batch:
            if ts_ms >= until_ms: break
            ts_iso = dt.datetime.utcfromtimestamp(ts_ms/1000).replace(tzinfo=dt.timezone.utc).isoformat()
            vwap = (h + l + c) / 3.0
            rows.append((ts_iso, ccxt_symbol, float(o), float(h), float(l), float(c), float(v), float(vwap), "binance"))
            last = ts_ms
        if not rows: break
        yield rows
        ms = last + 60_000  # next minute
