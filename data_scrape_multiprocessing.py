# 3.15 MINUTES

import requests
import psycopg2
import pandas as pd
import time
import os
import sys
import random
from multiprocessing import Pool, Manager
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from pathlib import Path
# from functions import *


# Configuration
BASE_URL = "https://mfdata.in/api/v1/schemes"
PLAN_TYPE = "regular"
CSV_FILE = "funds_data_mp.csv"
NUM_WORKERS = 4
CHUNK_SIZE = 100                         # limit per page (from script 1)
MAX_RETRIES = 5
RETRY_DELAY = 10
BACKOFF_MULTIPLIER = 2
MAX_CONSECUTIVE_FAILURES = 10
MAX_TOTAL_FAILURES = 40
# Rate limit: 30 req/min globally = 2s minimum between requests
MIN_INTERVAL = 2.0  # seconds

# neon db details
ENV_FILE = ".env"
NEON_URL_FILE = "neon_db_url.txt"

# DB connection
DB_CONFIG = {
    "host": "localhost",
    "database": "pc_mf_db",
    "user": "postgres",
    "password": "Mmxa106@",
    "port": 5432
}

DB_CONFIG_NEON = {
    "host": os.environ.get("NEON_DB_HOST", "localhost"),
    "database": os.environ.get("NEON_DB_NAME", "pc_mf_db"),
    "user": os.environ.get("NEON_DB_USER", "postgres"),
    "password": os.environ.get("NEON_DB_PASSWORD", ""),
    "port": int(os.environ.get("NEON_DB_PORT", 5432)),
    "sslmode": os.environ.get("NEON_DB_SSLMODE", "require")
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://mfdata.in/',
}

# ── Global rate limiter (shared across processes) ─────────────────────────────

class GlobalRateLimiter:
    def __init__(self, manager, min_interval=MIN_INTERVAL):
        self.lock = manager.Lock()
        self.last_request_time = manager.Value('d', 0.0)
        self.min_interval = min_interval

    def acquire(self):
        with self.lock:
            elapsed = time.time() - self.last_request_time.value
            wait = self.min_interval - elapsed
            if wait > 0:
                time.sleep(wait + random.uniform(0, 0.3))  # jitter
            self.last_request_time.value = time.time()

def get_db_connection():
    """Create a psycopg2 connection using local PostgreSQL settings."""
    return psycopg2.connect(**DB_CONFIG)


def get_neon_db_connection():
    """Create a psycopg2 connection using Neon env vars, local file, interactive input, or fallback."""
    neon_url = os.environ.get("NEON_DB_URL") or load_local_neon_url()
    source = None
    if neon_url:
        if "NEON_DB_URL" in os.environ:
            source = "environment"
        elif os.path.isfile(ENV_FILE):
            source = ENV_FILE
        elif os.path.isfile(NEON_URL_FILE):
            source = NEON_URL_FILE
        else:
            source = "interactive input"

        clean_url = normalize_db_url(neon_url)
        print(f"Using Neon connection string from {source}")
        return psycopg2.connect(clean_url)

    neon_url = prompt_for_neon_url()
    if neon_url:
        clean_url = normalize_db_url(neon_url)
        return psycopg2.connect(clean_url)

    print("No Neon DB URL found. Falling back to local PostgreSQL settings.")
    return psycopg2.connect(**DB_CONFIG)

def load_local_neon_url():
    """Load Neon connection string from a local file if NEON_DB_URL is not set."""
    if os.path.isfile(ENV_FILE):
        with open(ENV_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("NEON_DB_URL="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    if os.path.isfile(NEON_URL_FILE):
        with open(NEON_URL_FILE, encoding="utf-8") as f:
            return f.read().strip()
    return None


def normalize_db_url(db_url):
    """Remove unsupported query params from Neon connection string."""
    parsed = urlparse(db_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if "channel_binding" in query:
        query.pop("channel_binding")
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))
def is_interactive_session():
    return sys.stdin is not None and sys.stdin.isatty()


def prompt_for_neon_url():
    if not is_interactive_session():
        return None
    print("Neon DB URL not found in environment or local file.")
    print("Paste your Neon connection string now and press Enter.")
    print("You can also save it in .env or neon_db_url.txt for future runs.")
    neon_url = input("Neon DB URL: ").strip()
    return neon_url or None

# ── Worker ────────────────────────────────────────────────────────────────────

def fetch_page(args):
    """
    Fetch a single page using offset+limit params (from script 1).
    Returns (page_number, records_list | None)
    """
    page_number, rate_limiter, total_failures, failures_lock = args
    backoff = RETRY_DELAY

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            rate_limiter.acquire()

            # ← Script 1's URL style: limit + offset
            offset = (page_number - 1) * CHUNK_SIZE
            params = {
                "plan_type": PLAN_TYPE,
                "limit": CHUNK_SIZE,
                "offset": offset,
            }

            print(f"[Worker] Page {page_number} | offset={offset} | attempt {attempt}")
            resp = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=15)

            if resp.status_code in (429, 403):
                retry_after = int(resp.headers.get("Retry-After", 60))
                print(f"[!] Rate limited on page {page_number}. Sleeping {retry_after}s")
                time.sleep(retry_after)
                backoff *= BACKOFF_MULTIPLIER
                continue

            if resp.status_code == 503:
                print(f"[!] 503 on page {page_number}. Sleeping {backoff}s")
                time.sleep(backoff)
                backoff *= BACKOFF_MULTIPLIER
                continue

            if resp.status_code != 200:
                print(f"[!] HTTP {resp.status_code} on page {page_number}")
                time.sleep(backoff)
                backoff *= BACKOFF_MULTIPLIER
                continue

            data = resp.json()

            # Check for API-level error (from script 1)
            if isinstance(data, dict) and "error" in data:
                err = data["error"]
                if isinstance(err, dict) and err.get("code") == "IP_BANNED":
                    retry_after = err.get("retry_after", 3600)
                    print(f"[!!!] IP BANNED! Retry after {retry_after}s. Stopping.")
                    return page_number, "IP_BANNED"
                print(f"[!] API error on page {page_number}: {err}")
                return page_number, None

            # Unwrap records
            records = data.get("data", data) if isinstance(data, dict) else data
            if not isinstance(records, list):
                print(f"[!] Unexpected response format on page {page_number}")
                return page_number, []

            return page_number, records  # SUCCESS

        except requests.exceptions.Timeout:
            print(f"[!] Timeout page {page_number}, attempt {attempt}")
            time.sleep(backoff)
            backoff *= BACKOFF_MULTIPLIER

        except Exception as e:
            print(f"[!] Error page {page_number}: {e}")
            time.sleep(backoff)
            backoff *= BACKOFF_MULTIPLIER

    # All retries exhausted
    with failures_lock:
        total_failures.value += 1
        print(f"[✗] Page {page_number} failed permanently. Total failures: {total_failures.value}")

    return page_number, None


# ── Pagination ────────────────────────────────────────────────────────────────

def get_total_pages():
    """Use script 1's approach to get total pages via meta."""
    try:
        print("Checking API for total records...")
        resp = requests.get(BASE_URL, params={"plan_type": PLAN_TYPE}, headers=HEADERS, timeout=15)
        data = resp.json()
        if isinstance(data, dict):
            meta = data.get("meta", {})
            total = meta.get("total") or meta.get("count")
            if total:
                pages = (int(total) + CHUNK_SIZE - 1) // CHUNK_SIZE
                print(f"Total records: {total} → {pages} pages")
                return pages
    except Exception as e:
        print(f"Could not determine total pages: {e}")
    print("Falling back to 9999 pages (will stop on empty responses)")
    return 9999


# ── CSV + DB (table name from script 2) ──────────────────────────────────────

def save_to_csv(all_funds):
    if not all_funds:
        print("No data to save")
        return False
    try:
        df = pd.DataFrame(all_funds)
        df.to_csv(CSV_FILE, index=False, encoding='utf-8')
        print(f"Saved {len(all_funds)} funds to {CSV_FILE}")
        print(f"Columns: {df.columns.tolist()}")
        return True
    except Exception as e:
        print(f"Error saving CSV: {e}")
        return False


def create_table_from_csv():
    try:
        df = pd.read_csv(CSV_FILE)
        conn = get_neon_db_connection()
        cur = conn.cursor()

        table_name = "mutual_funds_mp"   # ← from script 2

        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")

        columns = []
        for col in df.columns:
            dtype = df[col].dtype
            if dtype == 'int64':
                sql_type = "BIGINT"
            elif dtype == 'float64':
                sql_type = "DOUBLE PRECISION"
            elif dtype == 'bool':
                sql_type = "BOOLEAN"
            else:
                sql_type = "TEXT"
            safe_col = col.lower().replace(' ', '_').replace('-', '_')
            columns.append(f"{safe_col} {sql_type}")

        cur.execute(f"""
            CREATE TABLE {table_name} (
                id SERIAL PRIMARY KEY,
                {', '.join(columns)}
            )
        """)
        conn.commit()
        print(f"Created table '{table_name}' with {len(columns)} columns")
        cur.close()
        conn.close()
        return table_name

    except Exception as e:
        print(f"Error creating table: {e}")
        return None


def load_csv_to_db(table_name):
    try:
        conn = get_neon_db_connection()
        cur = conn.cursor()
        df = pd.read_csv(CSV_FILE)
        columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
        columns_str = ', '.join(columns)

        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            cur.copy_expert(
                f"COPY {table_name} ({columns_str}) FROM STDIN WITH CSV HEADER", f
            )

        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        print(f"Loaded {count} records into '{table_name}'")
        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"Error loading to DB: {e}")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    start_time = time.time()

    print("=" * 60)
    print("FUND SCRAPER - Multiprocessing + offset/limit URL style")
    print("=" * 60)
    print(f"Workers: {NUM_WORKERS} | Chunk: {CHUNK_SIZE} | Rate: 30 req/min global")
    print("=" * 60 + "\n")

    total_pages = get_total_pages()

    with Manager() as manager:
        rate_limiter = GlobalRateLimiter(manager)
        total_failures = manager.Value('i', 0)
        failures_lock = manager.Lock()

        args = [
            (p, rate_limiter, total_failures, failures_lock)
            for p in range(1, total_pages + 1)
        ]

        all_funds = []
        consecutive_empty = 0
        ip_banned = False

        print(f"\nStarting pool with {NUM_WORKERS} workers...\n")

        with Pool(processes=NUM_WORKERS) as pool:
            for page_number, records in pool.imap_unordered(fetch_page, args):

                if records == "IP_BANNED":
                    print("IP banned! Terminating all workers.")
                    pool.terminate()
                    ip_banned = True
                    break

                if total_failures.value >= MAX_TOTAL_FAILURES:
                    print(f"Too many failures ({total_failures.value}). Terminating.")
                    pool.terminate()
                    break

                if records is None or len(records) == 0:
                    consecutive_empty += 1
                    print(f"Page {page_number}: empty (consecutive: {consecutive_empty})")
                    if consecutive_empty >= MAX_CONSECUTIVE_FAILURES:
                        print("Too many empty pages. Stopping.")
                        pool.terminate()
                        break
                    continue

                consecutive_empty = 0
                all_funds.extend(records)
                print(f"[✓] Page {page_number}: {len(records)} records | Total: {len(all_funds)}")

    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print(f"Total funds: {len(all_funds)}")
    print(f"Time: {elapsed/60:.1f} minutes")
    if ip_banned:
        print("STOPPED: IP banned. Wait 1+ hour before retrying.")
    print("=" * 60 + "\n")

    if not all_funds:
        return False

    if save_to_csv(all_funds):
        print("\nCreating database table...")
        table_name = create_table_from_csv()
        if table_name:
            print("\nLoading data into database...")
            if load_csv_to_db(table_name):
                print(f"\nSuccess! {len(all_funds)} funds stored in '{table_name}'")
                return True
    return False


# if __name__ == "__main__":
#     try:
#         main()
#     except KeyboardInterrupt:
#         print("\nInterrupted by user")
#     except Exception as e:
#         print(f"\nFatal error: {e}")
#         import traceback
#         traceback.print_exc()

if __name__ == "__main__":
    load_csv_to_db("mutual_funds_mp")  