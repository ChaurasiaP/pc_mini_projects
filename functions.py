import pandas as pd

from data_scrape_multiprocessing import get_db_connection


def save_to_csv(all_funds, csv_file=CSV_FILE):
    """Takes a list of funds and saves to CSV. Can be called independently."""
    if not all_funds:
        print("No data to save.")
        return False
    try:
        df = pd.DataFrame(all_funds)
        df.to_csv(csv_file, index=False, encoding='utf-8')
        print(f"Saved {len(all_funds)} funds to {csv_file}")
        print(f"Columns: {df.columns.tolist()}")
        return True
    except Exception as e:
        print(f"Error saving CSV: {e}")
        return False


def create_table_from_csv(csv_file=CSV_FILE):
    """Reads CSV and creates the DB table. Can be called independently."""
    try:
        df = pd.read_csv(csv_file)
        conn = get_db_connection()
        cur = conn.cursor()

        table_name = "mutual_funds_mp"

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


def load_csv_to_db(table_name, csv_file=CSV_FILE):
    """Pushes an existing CSV into the DB table. Can be called independently."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        df = pd.read_csv(csv_file)
        columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
        columns_str = ', '.join(columns)

        with open(csv_file, 'r', encoding='utf-8') as f:
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


def scrape_and_save():
    """Scrapes + saves to CSV only. No DB."""
    funds = run_scraper()
    if funds:
        save_to_csv(funds)
    return funds


def csv_to_db():
    """Takes existing CSV and pushes to DB. No scraping."""
    table_name = create_table_from_csv()
    if table_name:
        load_csv_to_db(table_name)
