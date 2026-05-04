# # import requests
# # import pandas as pd
# # import time
# # from multiprocessing import Pool, Manager
# # from data_scrape_multiprocessing import GlobalRateLimiter, get_db_connection

# # BASE_URL = "https://mfdata.in/api/v1/schemes"
# # HEADERS = {
# #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
# #     'Accept': 'application/json',
# #     'Referer': 'https://mfdata.in/',
# # }
# # MIN_INTERVAL = 3.0
# # NUM_WORKERS = 8


# # def fetch_scheme_detail(args):
# #     scheme_code, rate_limiter, total_failures, failures_lock = args

# #     for attempt in range(1, 6):
# #         try:
# #             rate_limiter.acquire()

# #             url = f"{BASE_URL}/{scheme_code}"
# #             resp = requests.get(url, headers=HEADERS, timeout=15)

# #             if resp.status_code in (429, 403):
# #                 retry_after = int(resp.headers.get("Retry-After", 60))
# #                 print(f"[!] Rate limited for {scheme_code}. Sleeping {retry_after}s")
# #                 time.sleep(retry_after)
# #                 continue

# #             if resp.status_code == 404:
# #                 print(f"[!] Not found: {scheme_code}")
# #                 return scheme_code, None

# #             if resp.status_code != 200:
# #                 print(f"[!] HTTP {resp.status_code} for {scheme_code}")
# #                 time.sleep(10 * attempt)
# #                 continue

# #             data = resp.json()

# #             # Unwrap if nested
# #             record = data.get("data", data) if isinstance(data, dict) else data

# #             # Always stamp the scheme_code on the record
# #             if isinstance(record, dict):
# #                 record["scheme_code"] = scheme_code
# #             elif isinstance(record, list) and record:
# #                 for r in record:
# #                     r["scheme_code"] = scheme_code

# #             print(f"[✓] Fetched: {scheme_code} (attempt {attempt})")
# #             return scheme_code, record

# #         except requests.exceptions.Timeout:
# #             print(f"[!] Timeout: {scheme_code}, attempt {attempt}")
# #             time.sleep(10 * attempt)

# #         except Exception as e:
# #             print(f"[!] Error {scheme_code}: {e}")
# #             time.sleep(10 * attempt)

# #     with failures_lock:
# #         total_failures.value += 1
# #     return scheme_code, None


# # def fetch_all_scheme_details(scheme_codes):
# #     all_details = []

# #     with Manager() as manager:
# #         rate_limiter = GlobalRateLimiter(manager)   # reuse your existing class
# #         total_failures = manager.Value('i', 0)
# #         failures_lock = manager.Lock()

# #         args = [
# #             (code, rate_limiter, total_failures, failures_lock)
# #             for code in scheme_codes
# #         ]

# #         print(f"\nFetching details for {len(scheme_codes)} schemes...\n")

# #         with Pool(processes=NUM_WORKERS) as pool:
# #             for scheme_code, record in pool.imap_unordered(fetch_scheme_detail, args):
# #                 if record is None:
# #                     continue
# #                 if isinstance(record, list):
# #                     all_details.extend(record)
# #                 else:
# #                     all_details.append(record)
# #                 print(f"Total fetched: {len(all_details)}")

# #     return all_details


# # df = pd.read_csv("funds_data_mp.csv")
# # print(df.columns.tolist())      # see all columns
# # print(df["amfi_code"].head())   # verify amfi_code exists

# # def create_scheme_details_table(df):
# #     conn = get_db_connection()
# #     cur = conn.cursor()

# #     table_name = "scheme_details"
# #     cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")

# #     columns = []
# #     for col in df.columns:
# #         dtype = df[col].dtype
# #         if dtype == 'int64':
# #             sql_type = "BIGINT"
# #         elif dtype == 'float64':
# #             sql_type = "DOUBLE PRECISION"
# #         elif dtype == 'bool':
# #             sql_type = "BOOLEAN"
# #         else:
# #             sql_type = "TEXT"
# #         safe_col = col.lower().replace(' ', '_').replace('-', '_')
# #         columns.append(f"{safe_col} {sql_type}")

# #     cur.execute(f"""
# #         CREATE TABLE {table_name} (
# #             id SERIAL PRIMARY KEY,
# #             {', '.join(columns)}
# #         )
# #     """)

# #     # Index on scheme_code for fast lookup
# #     cur.execute(f"CREATE INDEX idx_scheme_code ON {table_name} (scheme_code)")

# #     conn.commit()
# #     cur.close()
# #     conn.close()
# #     print(f"Created table '{table_name}' with index on scheme_code")
# #     return table_name


# # def save_scheme_details_to_db(all_details):
# #     df = pd.DataFrame(all_details)
# #     df.to_csv("scheme_details.csv", index=False)

# #     table_name = create_scheme_details_table(df)

# #     conn = get_db_connection()
# #     cur = conn.cursor()
# #     columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]

# #     with open("scheme_details.csv", 'r', encoding='utf-8') as f:
# #         cur.copy_expert(
# #             f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH CSV HEADER", f
# #         )

# #     conn.commit()
# #     cur.execute(f"SELECT COUNT(*) FROM {table_name}")
# #     print(f"Stored {cur.fetchone()[0]} records in '{table_name}'")
# #     cur.close()
# #     conn.close()



# # if __name__ == "__main__":
# #     # Load scheme codes from your existing CSV
# #     df = pd.read_csv("funds_data_mp.csv")
# #     scheme_codes = df["amfi_code"].dropna().astype(str).unique().tolist()
# #     print(f"Found {len(scheme_codes)} scheme codes")

# #     # Fetch details for all
# #     all_details = fetch_all_scheme_details(scheme_codes)

# #     # Store in DB
# #     if all_details:
# #         save_scheme_details_to_db(all_details)



# import asyncio
# import aiohttp
# import pandas as pd
# import time
# from data_scrape_multiprocessing import get_db_connection

# BASE_URL = "https://mfdata.in/api/v1/schemes"
# HEADERS = {
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
#     'Accept': 'application/json',
#     'Content-Type': 'application/json',
#     'Referer': 'https://mfdata.in/',
# }

# BATCH_SIZE = 50          # API supports up to 100 codes per request
# MAX_REQUESTS_PER_MINUTE = 30
# MIN_INTERVAL = 60 / MAX_REQUESTS_PER_MINUTE  # 2.0 seconds
# MAX_CONCURRENT = 5
# MAX_RETRIES = 5


# # ── Rate limiter ──────────────────────────────────────────────────────────────

# class AsyncRateLimiter:
#     def __init__(self, min_interval=MIN_INTERVAL):
#         self.min_interval = min_interval
#         self.last_request = 0.0
#         self.lock = asyncio.Lock()

#     async def acquire(self):
#         async with self.lock:
#             wait = self.min_interval - (time.monotonic() - self.last_request)
#             if wait > 0:
#                 await asyncio.sleep(wait)
#             self.last_request = time.monotonic()


# # ── Fetch one batch of up to 100 codes ───────────────────────────────────────

# async def fetch_batch(session, batch_codes, rate_limiter, semaphore, batch_num):
#     async with semaphore:
#         for attempt in range(1, MAX_RETRIES + 1):
#             try:
#                 await rate_limiter.acquire()

#                 url = f"{BASE_URL}/bulk"
#                 payload = {"scheme_codes": batch_codes}

#                 async with session.post(
#                     url,
#                     headers=HEADERS,
#                     json=payload,
#                     timeout=aiohttp.ClientTimeout(total=30)
#                 ) as resp:

#                     if resp.status in (429, 403):
#                         retry_after = int(resp.headers.get("Retry-After", 60))
#                         print(f"[!] Rate limited batch {batch_num}. Sleeping {retry_after}s")
#                         await asyncio.sleep(retry_after)
#                         continue

#                     if resp.status != 200:
#                         error_text = await resp.text()   # ← fix here
#                         print(f"[!] HTTP {resp.status} batch {batch_num}, attempt {attempt}: {error_text[:200]}")
#                         await asyncio.sleep(5 * attempt)
#                         continue

#                     data = await resp.json(content_type=None)  # ← content_type=None for safety

#                     records = data.get("data", data) if isinstance(data, dict) else data

#                     if not isinstance(records, list):
#                         print(f"[!] Unexpected format in batch {batch_num}: {str(data)[:200]}")
#                         return batch_num, []

#                     print(f"[✓] Batch {batch_num}: {len(records)} records (attempt {attempt})")
#                     return batch_num, records

#             except asyncio.TimeoutError:
#                 print(f"[!] Timeout batch {batch_num}, attempt {attempt}")
#                 await asyncio.sleep(5 * attempt)

#             except Exception as e:
#                 print(f"[!] Error batch {batch_num}: {e}")
#                 await asyncio.sleep(5 * attempt)

#         print(f"[✗] Batch {batch_num} failed permanently")
#         return batch_num, []


# # ── Fetch all in batches ──────────────────────────────────────────────────────

# async def fetch_all_scheme_details_async(scheme_codes):
#     # Split into chunks of 100
#     batches = [
#         scheme_codes[i:i + BATCH_SIZE]
#         for i in range(0, len(scheme_codes), BATCH_SIZE)
#     ]

#     print(f"Total codes: {len(scheme_codes)}")
#     print(f"Total batches: {len(batches)} (batch size: {BATCH_SIZE})")
#     print(f"Estimated time: ~{len(batches) / MAX_REQUESTS_PER_MINUTE:.1f} minutes\n")

#     rate_limiter = AsyncRateLimiter()
#     semaphore = asyncio.Semaphore(MAX_CONCURRENT)
#     all_details = []

#     async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=MAX_CONCURRENT)) as session:
#         tasks = [
#             fetch_batch(session, batch, rate_limiter, semaphore, i + 1)
#             for i, batch in enumerate(batches)
#         ]

#         for coro in asyncio.as_completed(tasks):
#             batch_num, records = await coro
#             all_details.extend(records)
#             print(f"Progress: {len(all_details)}/{len(scheme_codes)} records fetched")

#     print(f"\nTotal fetched: {len(all_details)}")
#     return all_details


# # ── DB ────────────────────────────────────────────────────────────────────────

# def create_scheme_details_table(df):
#     conn = get_db_connection()
#     cur = conn.cursor()
#     table_name = "scheme_details"

#     cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")

#     columns = []
#     for col in df.columns:
#         dtype = df[col].dtype
#         if dtype == 'int64':
#             sql_type = "BIGINT"
#         elif dtype == 'float64':
#             sql_type = "DOUBLE PRECISION"
#         elif dtype == 'bool':
#             sql_type = "BOOLEAN"
#         else:
#             sql_type = "TEXT"
#         safe_col = col.lower().replace(' ', '_').replace('-', '_')
#         columns.append(f"{safe_col} {sql_type}")

#     cur.execute(f"""
#         CREATE TABLE {table_name} (
#             id SERIAL PRIMARY KEY,
#             {', '.join(columns)}
#         )
#     """)
#     cur.execute(f"CREATE INDEX idx_scheme_code ON {table_name} (scheme_code)")
#     conn.commit()
#     cur.close()
#     conn.close()
#     print(f"Created table '{table_name}' with {len(columns)} columns")
#     return table_name


# def save_scheme_details_to_db(all_details):
#     df = pd.DataFrame(all_details)
#     df.to_csv("scheme_details.csv", index=False)
#     print(f"Saved {len(all_details)} records to scheme_details.csv")

#     table_name = create_scheme_details_table(df)
#     conn = get_db_connection()
#     cur = conn.cursor()
#     columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]

#     with open("scheme_details.csv", 'r', encoding='utf-8') as f:
#         cur.copy_expert(
#             f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH CSV HEADER", f
#         )

#     conn.commit()
#     cur.execute(f"SELECT COUNT(*) FROM {table_name}")
#     print(f"Stored {cur.fetchone()[0]} records in '{table_name}'")
#     cur.close()
#     conn.close()


# # ── Main ──────────────────────────────────────────────────────────────────────

# if __name__ == "__main__":
#     df = pd.read_csv("funds_data_mp.csv")
#     all_codes = df["amfi_code"].dropna().astype(str).unique().tolist()

#     # Separate numeric and non-numeric codes
#     numeric_codes = [c for c in all_codes if c.isdigit()]
#     skipped_codes = [c for c in all_codes if not c.isdigit()]

#     print(f"Total codes: {len(all_codes)}")
#     print(f"Numeric (will fetch): {len(numeric_codes)}")
#     print(f"Skipped non-numeric: {len(skipped_codes)} → {skipped_codes[:10]}")

#     start = time.time()
#     all_details = asyncio.run(fetch_all_scheme_details_async(numeric_codes))
#     elapsed = time.time() - start

#     print(f"\nTotal time: {elapsed/60:.1f} minutes")

#     if all_details:
#         save_scheme_details_to_db(all_details)


import asyncio
import aiohttp
import pandas as pd
import time
from data_scrape_multiprocessing import get_db_connection

BASE_URL = "https://mfdata.in/api/v1/schemes"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Referer': 'https://mfdata.in/',
}

BATCH_SIZE = 50
MAX_REQUESTS_PER_MINUTE = 10        # bulk endpoint limit
MIN_INTERVAL = 60 / MAX_REQUESTS_PER_MINUTE  # 6.0 seconds
MAX_CONCURRENT = 2                  # conservative concurrent connections
MAX_RETRIES = 5


# ── Rate limiter ──────────────────────────────────────────────────────────────

class AsyncRateLimiter:
    def __init__(self, min_interval=MIN_INTERVAL):
        self.min_interval = min_interval
        self.last_request = 0.0
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            wait = self.min_interval - (time.monotonic() - self.last_request)
            if wait > 0:
                await asyncio.sleep(wait)
            self.last_request = time.monotonic()


# ── Fetch one batch ───────────────────────────────────────────────────────────

async def fetch_batch(session, batch_codes, rate_limiter, semaphore, batch_num):
    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await rate_limiter.acquire()

                url = f"{BASE_URL}/bulk"
                payload = {"scheme_codes": batch_codes}

                async with session.post(
                    url,
                    headers=HEADERS,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:

                    if resp.status in (429, 403):
                        retry_after = int(resp.headers.get("Retry-After", 60))
                        print(f"[!] Rate limited batch {batch_num}. Sleeping {retry_after}s")

                        # If banned for 1 hour, stop everything
                        if retry_after >= 3600:
                            print(f"[!!!] IP BANNED for {retry_after}s. Stopping.")
                            return batch_num, "BANNED"

                        await asyncio.sleep(retry_after)
                        continue

                    if resp.status != 200:
                        error_text = await resp.text()
                        print(f"[!] HTTP {resp.status} batch {batch_num}, attempt {attempt}: {error_text[:200]}")
                        await asyncio.sleep(6 * attempt)  # respect 6s interval on retry
                        continue

                    data = await resp.json(content_type=None)
                    records = data.get("data", data) if isinstance(data, dict) else data

                    if not isinstance(records, list):
                        print(f"[!] Unexpected format batch {batch_num}: {str(data)[:200]}")
                        return batch_num, []

                    print(f"[✓] Batch {batch_num}: {len(records)} records (attempt {attempt})")
                    return batch_num, records

            except asyncio.TimeoutError:
                print(f"[!] Timeout batch {batch_num}, attempt {attempt}")
                await asyncio.sleep(6 * attempt)

            except Exception as e:
                print(f"[!] Error batch {batch_num}: {e}")
                await asyncio.sleep(6 * attempt)

        print(f"[✗] Batch {batch_num} failed permanently")
        return batch_num, []


# ── Fetch all in batches ──────────────────────────────────────────────────────

async def fetch_all_scheme_details_async(scheme_codes):
    batches = [
        scheme_codes[i:i + BATCH_SIZE]
        for i in range(0, len(scheme_codes), BATCH_SIZE)
    ]

    estimated_minutes = len(batches) / MAX_REQUESTS_PER_MINUTE
    print(f"Total codes:    {len(scheme_codes)}")
    print(f"Total batches:  {len(batches)} (batch size: {BATCH_SIZE})")
    print(f"Rate limit:     {MAX_REQUESTS_PER_MINUTE} req/min → {MIN_INTERVAL}s between requests")
    print(f"Estimated time: ~{estimated_minutes:.1f} minutes\n")

    rate_limiter = AsyncRateLimiter()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    all_details = []
    failed_batches = []
    banned = False

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            asyncio.ensure_future(
                fetch_batch(session, batch, rate_limiter, semaphore, i + 1)
            )
            for i, batch in enumerate(batches)
        ]

        for coro in asyncio.as_completed(tasks):
            batch_num, records = await coro

            # IP banned — cancel everything
            if records == "BANNED":
                print("\nIP banned! Cancelling all pending tasks.")
                banned = True
                for t in tasks:
                    t.cancel()
                break

            if not records:
                failed_batches.append(batch_num)
                continue

            all_details.extend(records)
            print(f"Progress: {len(all_details)}/{len(scheme_codes)} | "
                  f"Failed batches: {len(failed_batches)}")

    print(f"\nTotal fetched: {len(all_details)}")
    if failed_batches:
        print(f"Failed batches: {failed_batches}")
    if banned:
        print("STOPPED: IP banned. Wait 1 hour before retrying.")

    return all_details


# ── DB ────────────────────────────────────────────────────────────────────────

def create_scheme_details_table(df):
    conn = get_db_connection()
    cur = conn.cursor()
    table_name = "scheme_details"

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
    cur.execute(f"CREATE INDEX idx_scheme_code ON {table_name} (scheme_code)")
    conn.commit()
    cur.close()
    conn.close()
    print(f"Created table '{table_name}' with {len(columns)} columns")
    return table_name


def save_scheme_details_to_db(all_details):
    df = pd.DataFrame(all_details)
    df.to_csv("scheme_details.csv", index=False)
    print(f"Saved {len(all_details)} records to scheme_details.csv")

    table_name = create_scheme_details_table(df)
    conn = get_db_connection()
    cur = conn.cursor()
    columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]

    with open("scheme_details.csv", 'r', encoding='utf-8') as f:
        cur.copy_expert(
            f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH CSV HEADER", f
        )

    conn.commit()
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    print(f"Stored {cur.fetchone()[0]} records in '{table_name}'")
    cur.close()
    conn.close()


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    df = pd.read_csv("funds_data_mp.csv")
    all_codes = df["amfi_code"].dropna().astype(str).unique().tolist()

    numeric_codes = [c for c in all_codes if c.isdigit()]
    skipped_codes = [c for c in all_codes if not c.isdigit()]

    print(f"Total codes:          {len(all_codes)}")
    print(f"Numeric (fetching):   {len(numeric_codes)}")
    print(f"Skipped non-numeric:  {len(skipped_codes)} → {skipped_codes[:10]}\n")

    # Test with 2 batches (100 codes) first
    # numeric_codes = numeric_codes[:100]

    start = time.time()
    all_details = asyncio.run(fetch_all_scheme_details_async(numeric_codes))
    elapsed = time.time() - start

    print(f"Total time: {elapsed/60:.1f} minutes")

    if all_details:
        save_scheme_details_to_db(all_details)