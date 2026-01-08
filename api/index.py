import csv
import io
import os
import re
import uuid
from typing import Iterator, Dict, Any, Optional, List, Tuple

import dlt
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import RedirectResponse

from sqlalchemy import create_engine, text, inspect


app = FastAPI(title="CSV to Postgres via dlt")


# ----------------------------
# Naming & CSV parsing helpers
# ----------------------------

def slugify_table_name(filename: str) -> str:
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


def normalize_header(h: str) -> str:
    if h is None:
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


def extract_normalized_headers(file_bytes: bytes) -> List[str]:
    text = decode_csv_bytes(file_bytes)
    reader = csv.reader(io.StringIO(text))
    try:
        raw_headers = next(reader)
    except StopIteration:
        return []
    return [normalize_header(h) for h in raw_headers]


def parse_csv_upload(file_bytes: bytes) -> Iterator[Dict[str, Any]]:
    text = decode_csv_bytes(file_bytes)
    text_stream = io.StringIO(text)

    reader = csv.DictReader(text_stream)
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


# ----------------------------
# Postgres introspection helpers
# ----------------------------

def get_sqlalchemy_engine():
    # Put your Neon pooled connection string here (Vercel env var).
    # Example: postgresql+psycopg2://user:pass@host/db?sslmode=require
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    return create_engine(db_url, pool_pre_ping=True)


def table_exists_and_columns(engine, schema_name: str, table_name: str) -> Tuple[bool, List[str]]:
    insp = inspect(engine)
    exists = insp.has_table(table_name, schema=schema_name)
    if not exists:
        return False, []
    cols = insp.get_columns(table_name, schema=schema_name)
    col_names = [c["name"] for c in cols]
    return True, col_names


def columns_match(existing_cols: List[str], incoming_cols: List[str]) -> bool:
    # Match by exact set (case already normalized). Order not important.
    return set(existing_cols) == set(incoming_cols)


def choose_table_name_atomic(
    engine,
    schema_name: str,
    base_table_name: str,
    incoming_cols: List[str],
) -> Tuple[str, str]:
    """
    Concurrency-safe:
    - lock on (schema_name + base_table_name)
    - if exists & matches -> base
    - if exists & mismatch -> base__<random_suffix>
    - if not exists -> base
    """
    lock_key = f"{schema_name}.{base_table_name}"

    with engine.begin() as conn:
        # Advisory lock scoped to this transaction
        conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {"k": lock_key})

        exists, existing_cols = table_exists_and_columns(engine, schema_name, base_table_name)

        if not exists:
            return base_table_name, "created_new_table"

        if columns_match(existing_cols, incoming_cols):
            return base_table_name, "matched_existing_table"

        # mismatch -> guaranteed unique name (prevents concurrent collisions)
        suffix = uuid.uuid4().hex[:8]
        new_name = f"{base_table_name}__{suffix}"
        return new_name, "schema_mismatch_created_new_table"


# ----------------------------
# dlt resource factory
# ----------------------------

def make_resource(table_name: str, write_disposition: str):
    @dlt.resource(name=table_name, write_disposition=write_disposition)
    def csv_rows(rows: Iterator[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
        yield from rows
    return csv_rows


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

    # replace: overwrite target table
    # append: append into target table
    mode: str = Query(default="replace", pattern="^(replace|append)$"),

    # override base table name
    table: Optional[str] = Query(default=None, description="Optional base table name override"),

    # "new DB" equivalent: put data into a separate schema (dataset) inside the same Postgres database
    db: str = Query(default="csv_demo", description="Schema/dataset name (acts like logical DB)"),
):
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    incoming_cols = extract_normalized_headers(content)
    if not incoming_cols:
        raise HTTPException(status_code=400, detail="CSV has no header row")

    base_table_name = normalize_header(table) if table else slugify_table_name(file.filename)
    schema_name = normalize_header(db)  # keep schema name safe too

    # Decide table name based on schema matching rules (and do it safely under concurrency)
    try:
        engine = get_sqlalchemy_engine()
        final_table_name, decision = choose_table_name_atomic(engine, schema_name, base_table_name, incoming_cols)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB inspection failed: {e}")

    pipeline = dlt.pipeline(
        pipeline_name="csv_to_postgres_pipeline",
        destination="postgres",
        dataset_name=schema_name,   # maps to Postgres schema in dlt
    )

    write_disposition = "replace" if mode == "replace" else "append"

    # Important: if schema mismatch created a new table, we should not "append" into old.
    # We already changed table name; append/replace now applies to the chosen target table.
    try:
        rows_iter = parse_csv_upload(content)
        resource = make_resource(final_table_name, write_disposition)
        load_info = pipeline.run(resource(rows_iter))

        return {
            "message": "Loaded successfully",
            "dataset": schema_name,
            "base_table": base_table_name,
            "final_table": final_table_name,
            "decision": decision,
            "mode": mode,
            "columns": incoming_cols,
            "load_info": str(load_info),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Load failed: {e}")
