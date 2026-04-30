import requests
import psycopg2
import pandas as pd
import csv
import time
import os
import sys
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import random


#4.30 minutes


# Configuration - CONSERVATIVE to avoid IP ban
BASE_URL = "https://mfdata.in/api/v1/schemes"
PLAN_TYPE = "regular"
LIMIT = 100  # Records per page
CSV_FILE = "funds_data.csv"

# Rate limiting - Based on official API limits
# /api/v1/schemes = 30 requests per minute
# = 1 request every 2 seconds EXACTLY
MIN_DELAY_BETWEEN_REQUESTS = 2.0   # 2 seconds minimum (30 requests/minute)
MAX_DELAY_BETWEEN_REQUESTS = 3.0   # 3 seconds maximum (adds randomness)
MAX_RETRIES = 3
RETRY_DELAY_BASE = 10  # 10 seconds before retry

# Request headers to avoid ban
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://mfdata.in/',
}

# DB connection
# For Neon, set NEON_DB_URL in your environment, or store it in .env / neon_db_url.txt.
DB_CONFIG = {
    "host": os.environ.get("NEON_DB_HOST", "localhost"),
    "database": os.environ.get("NEON_DB_NAME", "pc_mf_db"),
    "user": os.environ.get("NEON_DB_USER", "postgres"),
    "password": os.environ.get("NEON_DB_PASSWORD", ""),
    "port": int(os.environ.get("NEON_DB_PORT", 5432)),
    "sslmode": os.environ.get("NEON_DB_SSLMODE", "require")
}

ENV_FILE = ".env"
NEON_URL_FILE = "neon_db_url.txt"


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


def normalize_db_url(db_url):
    """Remove unsupported query params from Neon connection string."""
    parsed = urlparse(db_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if "channel_binding" in query:
        query.pop("channel_binding")
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def get_db_connection():
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


def check_ip_ban(response):
    """Check if response indicates IP ban"""
    if response.status_code == 403 or response.status_code == 429:
        try:
            data = response.json()
            if data.get("error", {}).get("code") == "IP_BANNED":
                retry_after = data.get("error", {}).get("retry_after", 3600)
                print(f"\nIP BANNED! Retry after {retry_after} seconds ({retry_after/60:.1f} minutes)")
                return True, retry_after
        except:
            pass
    return False, 0


def fetch_fund_data(page_number, attempt=1):
    """Fetch fund data for a specific page with extreme rate limiting"""
    
    # Wait before making request (except first call)
    if page_number > 1:
        delay = random.uniform(MIN_DELAY_BETWEEN_REQUESTS, MAX_DELAY_BETWEEN_REQUESTS)
        print(f"Waiting {delay:.0f}s before page {page_number}...")
        time.sleep(delay)
    
    try:
        offset = (page_number - 1) * LIMIT
        query_params = {
            "plan_type": PLAN_TYPE,
            "limit": LIMIT,
            "offset": offset
        }
        
        print(f"Fetching page {page_number}... (attempt {attempt})")
        response = requests.get(BASE_URL, params=query_params, timeout=15, headers=HEADERS)
        print(response.url)
        
        # Check for IP ban
        is_banned, retry_after = check_ip_ban(response)
        if is_banned:
            print(f"STOPPING: IP banned for {retry_after} seconds")
            return None, "IP_BANNED"
        
        # Handle other errors
        if response.status_code == 503:
            print(f"Service unavailable. Retrying in 120s...")
            time.sleep(120)
            if attempt < MAX_RETRIES:
                return fetch_fund_data(page_number, attempt + 1)
            return None, "SERVICE_UNAVAILABLE"
        
        if response.status_code != 200:
            print(f"HTTP {response.status_code} on page {page_number}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_BASE * attempt)
                return fetch_fund_data(page_number, attempt + 1)
            return None, f"HTTP_{response.status_code}"
        
        data = response.json()
        
        # Check if response is error
        if isinstance(data, dict) and "error" in data:
            print(f"API Error: {data.get('error')}")
            return None, "API_ERROR"
        
        return data, "SUCCESS"
    
    except requests.exceptions.Timeout:
        print(f"Timeout on page {page_number}")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY_BASE * attempt)
            return fetch_fund_data(page_number, attempt + 1)
        return None, "TIMEOUT"
    
    except Exception as e:
        print(f"Error fetching page {page_number}: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY_BASE * attempt)
            return fetch_fund_data(page_number, attempt + 1)
        return None, f"ERROR: {str(e)}"


def process_page_data(data):
    """Extract fund records from page data"""
    if not data:
        return []
    
    funds = []
    
    # Handle different API response structures
    records = data.get("data", data) if isinstance(data, dict) else data
    
    if isinstance(records, list):
        for fund in records:
            if isinstance(fund, dict):
                funds.append(fund)
    
    return funds


def get_total_pages():
    """Get estimated total pages"""
    try:
        print("Checking API for total pages...")
        query_params = {"plan_type": PLAN_TYPE}
        response = requests.get(BASE_URL, params=query_params, timeout=15, headers=HEADERS)
        data = response.json()
        
        if isinstance(data, dict):
            # Look for pagination info
            meta = data.get("meta", {})
            total = meta.get("total", meta.get("count"))
            if total:
                page_size = LIMIT  # Use the defined limit
                estimated_pages = (total // page_size) + 1
                print(f"API indicates ~{total} records ({estimated_pages} pages)")
                return estimated_pages
        
        print(f"Could not determine total, starting scrape...")
        return 999999  # Will stop when pages become empty
    
    except Exception as e:
        print(f"Error checking total: {e}")
        return 100  # Conservative default


def save_to_csv(all_funds):
    """Save fund data to CSV file"""
    if not all_funds:
        print("No data to save")
        return False
    
    try:
        df = pd.DataFrame(all_funds)
        df.to_csv(CSV_FILE, index=False, encoding='utf-8')
        print(f"Saved {len(all_funds)} funds to {CSV_FILE}")
        print(f"Columns: {', '.join(df.columns.tolist())}")
        return True
    except Exception as e:
        print(f"Error saving to CSV: {e}")
        return False


def create_table_from_csv():
    """Create database table from CSV schema"""
    try:
        df = pd.read_csv(CSV_FILE)
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        table_name = "mutual_funds"
        
        # Drop existing table if it exists
        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
        
        # Generate CREATE TABLE statement
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
        
        create_table_sql = f"""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            {', '.join(columns)}
        )
        """
        
        cur.execute(create_table_sql)
        conn.commit()
        
        print(f"Created table '{table_name}' with {len(columns)} columns")
        
        cur.close()
        conn.close()
        return table_name
    
    except Exception as e:
        print(f"Error creating table: {e}")
        return None


def load_csv_to_db(table_name):
    """Load CSV data into database"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        df = pd.read_csv(CSV_FILE)
        columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
        columns_str = ', '.join(columns)
        
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            cur.copy_expert(
                f"COPY {table_name} ({columns_str}) FROM STDIN WITH CSV HEADER",
                f
            )
        
        conn.commit()
        
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        print(f"Loaded {count} records into '{table_name}' table")
        
        cur.close()
        conn.close()
        return True
    
    except Exception as e:
        print(f"Error loading CSV to database: {e}")
        return False


def scrape_all_funds():
    """Main function - sequential (NO multiprocessing) to avoid IP ban"""
    
    start_time = time.time()
    
    print("=" * 60)
    print("FUND SCRAPER (Official API Rate Limits)")
    print("=" * 60)
    print(f"API Limit: 30 requests/minute for /api/v1/schemes")
    print(f"Delay: {MIN_DELAY_BETWEEN_REQUESTS}-{MAX_DELAY_BETWEEN_REQUESTS}s between requests")
    print(f"Speed: ~8000 funds = ~80 pages = ~5-8 minutes")
    print(f"Sequential requests (1 at a time)")
    print("=" * 60 + "\n")
    
    estimated_pages = get_total_pages()
    
    all_funds = []
    page_num = 1
    consecutive_empty = 0
    stop_reason = None
    
    while page_num <= estimated_pages:
        data, status = fetch_fund_data(page_num)
        
        if status == "IP_BANNED":
            stop_reason = "IP_BANNED"
            break
        
        if not data or status != "SUCCESS":
            consecutive_empty += 1
            print(f"Failed ({status}). Consecutive failures: {consecutive_empty}/3")
            
            if consecutive_empty >= 3:
                print("Stopping after 3 consecutive failures")
                stop_reason = "TOO_MANY_FAILURES"
                break
            
            page_num += 1
            continue
        
        consecutive_empty = 0
        page_funds = process_page_data(data)
        
        if page_funds:
            all_funds.extend(page_funds)
            print(f"Page {page_num}: Got {len(page_funds)} funds (total: {len(all_funds)})")
        else:
            print(f"Page {page_num}: No data")
        
        page_num += 1
        
        # Show progress every 10 pages
        if page_num % 10 == 0:
            print(f"\nProgress: {page_num} pages processed, {len(all_funds)} funds total\n")
    
    elapsed = time.time() - start_time
    
    print("\n" + "=" * 60)
    print(f"Scraping completed")
    print(f"Total funds fetched: {len(all_funds)}")
    print(f"Time taken: {elapsed/60:.1f} minutes")
    if stop_reason:
        print(f"Stopped due to: {stop_reason}")
    print("=" * 60 + "\n")
    
    if len(all_funds) == 0:
        print("No data collected. Your IP may still be banned. Wait 1+ hour before retrying.")
        return False
    
    # Save to CSV
    if save_to_csv(all_funds):
        # Create table and load data
        print("\nCreating database table...")
        table_name = create_table_from_csv()
        
        if table_name:
            print("\nLoading data into database...")
            if load_csv_to_db(table_name):
                print(f"\nSuccess! {len(all_funds)} funds stored in '{table_name}' table")
                return True
    
    return False


if __name__ == "__main__":
    try:
        print("\nIMPORTANT: If you got IP banned, WAIT 1 HOUR before running this!")
        print("This conservative version uses sequential requests only.\n")
        time.sleep(2)
        
        scrape_all_funds()
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user")
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()
