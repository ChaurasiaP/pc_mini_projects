import requests
import psycopg2
import pandas as pd
import time
from multiprocessing import Pool, Queue, Manager
import threading

# Configuration
BASE_URL = "https://mfdata.in/api/v1/schemes"
PLAN_TYPE = "regular"
CSV_FILE = "funds_data.csv"
NUM_WORKERS = 4  # Start LOW - increase cautiously (API has strict limits!)
CHUNK_SIZE = 100  # Fetch in chunks
RATE_LIMIT_DELAY = 10  # Seconds between requests (increase if getting 429/503)
MAX_RETRIES = 5
RETRY_DELAY = 3  # Seconds
BACKOFF_MULTIPLIER = 2  # Exponential backoff factor
MAX_CONSECUTIVE_FAILURES = 10  # Auto-stop if this many pages fail consecutively
MAX_TOTAL_FAILURES = 40  # Auto-stop if total failures exceed this

# DB connection
DB_CONFIG = {
    "host": "localhost",
    "database": "pc_mf_db",
    "user": "postgres",
    "password": "Mmxa106@",
    "port": 5432
}

# Thread-safe rate limiter
class RateLimiter:
    def __init__(self, delay):
        self.delay = delay
        self.last_call = 0
        self.lock = threading.Lock()
    
    def wait(self):
        with self.lock:
            elapsed = time.time() - self.last_call
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
            self.last_call = time.time()

rate_limiter = RateLimiter(RATE_LIMIT_DELAY)


def fetch_fund_data(page_number, shared_state):
    """Fetch fund data for a specific page with retry logic and backoff"""
    backoff_delay = RETRY_DELAY
    
    # Check if we should stop
    if shared_state['should_stop'].value:
        return None
    
    for attempt in range(MAX_RETRIES):
        try:
            rate_limiter.wait()
            
            params = {
                "plan_type": PLAN_TYPE,
                "page": page_number
            }
            
            response = requests.get(BASE_URL, params=params, timeout=10)
            
            # Handle rate limiting and service unavailable
            if response.status_code == 429:
                print(f"⏸️  Rate limited on page {page_number}. Waiting {backoff_delay}s...")
                time.sleep(backoff_delay)
                backoff_delay *= BACKOFF_MULTIPLIER
                continue
            
            if response.status_code == 503:
                print(f"⏸️  Service unavailable on page {page_number}. Waiting {backoff_delay}s...")
                time.sleep(backoff_delay)
                backoff_delay *= BACKOFF_MULTIPLIER
                continue
            
            response.raise_for_status()
            # Success - reset failure count
            with shared_state['lock']:
                shared_state['consecutive_failures'].value = 0
            return response.json()
        
        except requests.exceptions.RequestException as e:
            print(f"⚠️  Error on page {page_number} (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(backoff_delay)
                backoff_delay *= BACKOFF_MULTIPLIER
            else:
                # Update shared failure counters
                with shared_state['lock']:
                    shared_state['failure_count'].value += 1
                    shared_state['consecutive_failures'].value += 1
                    
                    # Check auto-stop conditions
                    if shared_state['consecutive_failures'].value >= MAX_CONSECUTIVE_FAILURES:
                        print(f"\n🛑 STOPPING: {MAX_CONSECUTIVE_FAILURES} consecutive failures reached!")
                        shared_state['should_stop'].value = True
                    elif shared_state['failure_count'].value >= MAX_TOTAL_FAILURES:
                        print(f"\n🛑 STOPPING: {MAX_TOTAL_FAILURES} total failures reached!")
                        shared_state['should_stop'].value = True
                
                print(f"❌ Failed to fetch page {page_number} after {MAX_RETRIES} retries")
                return None


def process_page_data(args):
    """Process a single page and return flattened fund records"""
    page_number, shared_state = args
    data = fetch_fund_data(page_number, shared_state)
    
    if not data:
        return []
    
    funds = []
    
    # Handle different API response structures
    records = data.get("data", data) if isinstance(data, dict) else data
    
    if isinstance(records, list):
        for fund in records:
            if isinstance(fund, dict):
                funds.append(fund)
    
    print(f"✅ Page {page_number}: Fetched {len(funds)} funds")
    return funds


def get_total_funds():
    """Get total count of funds to determine pagination"""
    try:
        response = requests.get(BASE_URL, params={"plan_type": PLAN_TYPE}, timeout=10)
        data = response.json()
        
        # Check for pagination info in response
        if isinstance(data, dict):
            total = data.get("total", data.get("count", data.get("pagination", {}).get("total")))
            if total:
                return total
            else:
                # Fallback: try to estimate by counting initial response
                records = data.get("data", data)
                if isinstance(records, list):
                    return len(records) * 100  # Rough estimate
        
        return 1000  # Default fallback
    except Exception as e:
        print(f"Error getting total funds: {e}")
        return 1000


def save_to_csv(all_funds):
    """Save fund data to CSV file"""
    if not all_funds:
        print("❌ No data to save")
        return False
    
    try:
        # Flatten nested structures if any
        flattened_funds = []
        for fund in all_funds:
            if isinstance(fund, dict):
                flattened_funds.append(fund)
        
        df = pd.DataFrame(flattened_funds)
        df.to_csv(CSV_FILE, index=False, encoding='utf-8')
        print(f"✅ Saved {len(flattened_funds)} funds to {CSV_FILE}")
        print(f"📊 Columns: {', '.join(df.columns.tolist())}")
        return True
    except Exception as e:
        print(f"❌ Error saving to CSV: {e}")
        return False


def create_table_from_csv():
    """Create database table from CSV schema"""
    try:
        df = pd.read_csv(CSV_FILE)
        
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Create table name from CSV
        table_name = "mutual_funds"
        
        # Drop existing table if it exists
        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
        
        # Generate CREATE TABLE statement
        columns = []
        for col in df.columns:
            # Map pandas dtypes to SQL types
            dtype = df[col].dtype
            if dtype == 'int64':
                sql_type = "BIGINT"
            elif dtype == 'float64':
                sql_type = "DOUBLE PRECISION"
            elif dtype == 'bool':
                sql_type = "BOOLEAN"
            else:
                sql_type = "TEXT"
            
            # Sanitize column names
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
        
        print(f"✅ Created table '{table_name}' with {len(columns)} columns")
        
        cur.close()
        conn.close()
        return table_name
    
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        return None


def load_csv_to_db(table_name):
    """Load CSV data into database using COPY (fastest method)"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Read CSV to get column names
        df = pd.read_csv(CSV_FILE)
        columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
        columns_str = ', '.join(columns)
        
        # Use COPY command for bulk insert (much faster)
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            cur.copy_expert(
                f"COPY {table_name} ({columns_str}) FROM STDIN WITH CSV HEADER",
                f
            )
        
        conn.commit()
        
        # Verify load
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        print(f"✅ Loaded {count} records into '{table_name}' table")
        
        cur.close()
        conn.close()
        return True
    
    except Exception as e:
        print(f"❌ Error loading CSV to database: {e}")
        return False


def scrape_all_funds():
    """Main function to scrape all funds using multiprocessing"""
    
    start_time = time.time()
    
    print("🔍 Determining total funds...")
    total_funds = get_total_funds()
    print(f"📈 Total funds to fetch: ~{total_funds}")
    
    # Calculate number of pages (assuming 20 per page or adjust based on actual API)
    page_size = 20
    num_pages = (total_funds // page_size) + 1
    
    print(f"📄 Number of pages to fetch: {num_pages}")
    print(f"⚙️  Using {NUM_WORKERS} workers with {RATE_LIMIT_DELAY}s rate limit per request")
    print(f"⏱️  Rate limit: {RATE_LIMIT_DELAY}s delay, {MAX_RETRIES} retries with exponential backoff")
    print(f"� Auto-stop: {MAX_CONSECUTIVE_FAILURES} consecutive or {MAX_TOTAL_FAILURES} total failures")
    print(f"💡 If getting 429/503 errors, increase RATE_LIMIT_DELAY in the script")
    
    # Shared state for multiprocessing
    with Manager() as manager:
        shared_state = {
            'failure_count': manager.Value('i', 0),
            'consecutive_failures': manager.Value('i', 0),
            'should_stop': manager.Value('i', 0),
            'lock': manager.Lock()
        }
        
        # Fetch data using multiprocessing
        print("\n🚀 Starting multiprocessing scrape...")
        all_funds = []
        
        # Create list of (page_number, shared_state) tuples
        page_args = [(page_num, shared_state) for page_num in range(1, num_pages + 1)]
        
        try:
            with Pool(NUM_WORKERS) as pool:
                results = pool.imap_unordered(process_page_data, page_args, chunksize=5)
                
                for page_funds in results:
                    all_funds.extend(page_funds)
                    
                    # Check if we should stop
                    if shared_state['should_stop'].value:
                        print("\n🛑 Terminating scrape due to max limits reached...")
                        pool.terminate()
                        pool.join()
                        break
        except Exception as e:
            print(f"Error during scraping: {e}")
        
        elapsed = time.time() - start_time
        total_failed = shared_state['failure_count'].value
    
    print(f"\n📊 Scraping completed in {elapsed:.2f} seconds")
    print(f"✅ Total funds fetched: {len(all_funds)}")
    print(f"❌ Total failed pages: {total_failed}")
    
    # Save to CSV
    if save_to_csv(all_funds):
        # Create table and load data
        print("\n📋 Creating database table...")
        table_name = create_table_from_csv()
        
        if table_name:
            print("\n💾 Loading data into database...")
            if load_csv_to_db(table_name):
                print(f"\n🎉 Success! All {len(all_funds)} funds scraped and stored in '{table_name}' table")
                print(f"⏱️  Total time: {elapsed:.2f} seconds")
                return True
    
    return False


if __name__ == "__main__":
    try:
        scrape_all_funds()
    except KeyboardInterrupt:
        print("\n⚠️  Scraping interrupted by user")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
