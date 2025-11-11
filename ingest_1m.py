import os, time, datetime as dt
import ccxt
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

PG = dict(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    dbname=os.getenv("PG_DB", "crypto"),
    user=os.getenv("PG_USER", "postgres"),
    password=os.getenv("PG_PASS", "postgres"),
)
SYMBOL   = os.getenv("SYMBOL", "BTC/USDT")
EXCHANGE = os.getenv("EXCHANGE", "binance")

# Use last closed 1m candle to avoid partials
def fetch_last_closed_kline(exchange, symbol):
    # limit=2 returns [prev, current forming]; we take the previous (closed)
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe="1m", limit=2)
    if not ohlcv or len(ohlcv) < 2:
        return None
    ts_ms, o, h, l, c, v = ohlcv[-2]
    # VWAP is not provided; use typical price * volume approx (simple proxy)
    vwap = ((h + l + c) / 3.0) if v == 0 else ((h + l + c) / 3.0)
    # normalize to UTC timestamptz string
    ts_iso = dt.datetime.utcfromtimestamp(ts_ms / 1000).replace(tzinfo=dt.timezone.utc).isoformat()
    return (ts_iso, SYMBOL, float(o), float(h), float(l), float(c), float(v), float(vwap), EXCHANGE)

def upsert_ohlcv(rows):
    if not rows:
        return
    conn = psycopg2.connect(**PG)
    conn.autocommit = True
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO ohlcv_1m (ts, symbol, open, high, low, close, volume, vwap, exchange)
            VALUES %s
            ON CONFLICT (symbol, ts) DO UPDATE SET
              open=EXCLUDED.open,
              high=EXCLUDED.high,
              low=EXCLUDED.low,
              close=EXCLUDED.close,
              volume=EXCLUDED.volume,
              vwap=EXCLUDED.vwap,
              exchange=EXCLUDED.exchange;
        """, rows)
    conn.close()

def align_to_next_minute():
    now = dt.datetime.utcnow()
    sleep_s = 60 - now.second
    if sleep_s <= 0: sleep_s = 60
    time.sleep(sleep_s)

def main_loop():
    ex = getattr(ccxt, EXCHANGE)({"enableRateLimit": True})
    # first run backfills just the last closed candle
    while True:
        try:
            row = fetch_last_closed_kline(ex, SYMBOL)
            if row:
                upsert_ohlcv([row])
                print(f"Upserted {row[1]} @ {row[0]}")
        except Exception as e:
            print("Error:", e)
        # wake up just after the next minute boundary to grab the last closed candle
        align_to_next_minute()

if __name__ == "__main__":
    main_loop()
