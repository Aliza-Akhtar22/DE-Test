import csv
import io
import os
import re
from typing import Iterator, Dict, Any, Optional

import dlt
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import RedirectResponse
from sqlalchemy import create_engine, text


app = FastAPI(title="CSV to Postgres via dlt")


# ----------------------------
# Helpers
# ----------------------------

def slugify_table_name(filename: str) -> str:
    # iris.csv -> iris, AirPassengers.csv -> airpassengers
    base = filename.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
    base = re.sub(r"\.csv$", "", base, flags=re.IGNORECASE)
    base = base.strip().lower()
    base = re.sub(r"[^a-z0-9]+", "_", base)
    base = re.sub(r"_+", "_", base).strip("_")
    if not base:
        base = "uploaded_csv"
    if base[0].isdigit():
        base = f"t_{base}"
    return base


def normalize_header(h: Optional[str]) -> str:
    if not h:
        return "col"
    h = h.strip().lower()
    h = h.lstrip("#")
    h = re.sub(r"[^a-z0-9]+", "_", h)
    h = re.sub(r"_+", "_", h).strip("_")
    if not h:
        h = "col"
    if h[0].isdigit():
        h = f"c_{h}"
    return h


def decode_csv_bytes(file_bytes: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return file_bytes.decode(enc)
        except UnicodeDecodeError:
            continue
    return file_bytes.decode("utf-8", errors="replace")


def parse_csv_upload(file_bytes: bytes) -> Iterator[Dict[str, Any]]:
    text_data = decode_csv_bytes(file_bytes)
    reader = csv.DictReader(io.StringIO(text_data))

    if not reader.fieldnames:
        return iter(())

    normalized_fields = [normalize_header(f) for f in reader.fieldnames]

    for row in reader:
        out: Dict[str, Any] = {}
        for raw_key, norm_key in zip(reader.fieldnames, normalized_fields):
            val = row.get(raw_key)
            if isinstance(val, str):
                val = val.strip()
                if val == "":
                    val = None
            out[norm_key] = val
        yield out


def get_database_url() -> str:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set in Vercel")

    # dlt destination expects standard postgres URL, not sqlalchemy dialect
    # If you stored sqlalchemy style, convert:
    if db_url.startswith("postgresql+psycopg2://"):
        db_url = db_url.replace("postgresql+psycopg2://", "postgresql://", 1)

    return db_url


def ensure_schema_exists(db_url: str, schema_name: str) -> None:
    # Create schema if it doesn't exist (optional but helpful)
    engine = create_engine(db_url, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))


# ----------------------------
# Routes
# ----------------------------

@app.get("/")
def root():
    return RedirectResponse(url="/docs")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/load-csv")
async def load_csv(
    file: UploadFile = File(...),

    # which schema to load into (in Neon this appears as a schema on the left)
    db: str = Query(default="csv_demo", description="Schema/dataset name"),

    # replace is safest for your use-case
    mode: str = Query(default="replace", pattern="^(replace|append)$"),
):
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    table_name = slugify_table_name(file.filename)
    schema_name = normalize_header(db)

    db_url = get_database_url()
    ensure_schema_exists(db_url, schema_name)

    # IMPORTANT:
    # Use dlt postgres destination with credentials=db_url
    destination = dlt.destinations.postgres(credentials=db_url)

    pipeline = dlt.pipeline(
        pipeline_name="csv_uploader",      # keep stable
        destination=destination,           # uses DATABASE_URL
        dataset_name=schema_name,          # schema in Postgres/Neon
    )

    write_disposition = "replace" if mode == "replace" else "append"

    try:
        rows_iter = parse_csv_upload(content)

        # CRITICAL LINE:
        # table_name forces a distinct table per file
        load_info = pipeline.run(
            rows_iter,
            table_name=table_name,
            write_disposition=write_disposition,
        )

        return {
            "message": "Loaded successfully",
            "schema": schema_name,
            "table": table_name,
            "mode": mode,
            "load_info": str(load_info),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Load failed: {e}")
