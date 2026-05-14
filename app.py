import os
from typing import List, Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, conlist
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

ENV_FILE = ".env"
NEON_URL_FILE = "neon_db_url.txt"

app = FastAPI(title="Mutual Funds Neon API")


class AmfiCodesRequest(BaseModel):
    amfi_codes: conlist(str, min_items=1, max_items=50)


class NeonError(Exception):
    pass


def load_local_neon_url() -> Optional[str]:
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


def normalize_db_url(db_url: str) -> str:
    parsed = urlparse(db_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if "channel_binding" in query:
        query.pop("channel_binding")
    return urlunparse(parsed._replace(query=urlencode(query, doseq=True)))


def get_neon_db_connection():
    neon_url = os.environ.get("NEON_DB_URL") or load_local_neon_url()
    if not neon_url:
        raise NeonError("Neon DB URL not found in environment or local file.")

    clean_url = normalize_db_url(neon_url)
    return psycopg2.connect(clean_url)


@app.post("/funds/by-amfi-codes")
def get_funds_by_amfi_codes(request: AmfiCodesRequest):
    amfi_codes = list(dict.fromkeys([code.strip() for code in request.amfi_codes if code.strip()]))

    query = "SELECT * FROM mutual_funds_mp WHERE amfi_code = ANY(%s)"

    try:
        conn = get_neon_db_connection()
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, (amfi_codes,))
                records = cur.fetchall()
    except NeonError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Database query failed: {exc}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not records:
        raise HTTPException(status_code=404, detail="No funds found for the provided AMFI codes.")

    return {
        "count": len(records),
        "funds": list(records),
    }


@app.get("/")
def root():
    return {"message": "FastAPI mutual funds Neon API is running"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
