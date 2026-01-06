import csv
import io
import re
from typing import Iterator, Dict, Any, Optional

import dlt
from fastapi import FastAPI, UploadFile, File, HTTPException, Query

app = FastAPI(title="CSV to Postgres via dlt")


# ---------- Helpers ----------

def slugify_table_name(filename: str) -> str:
    """
    Convert filename -> safe table name:
    'Mall_Customers.csv' -> 'mall_customers'
    'AirPassengers.csv'  -> 'airpassengers'
    """
    base = filename.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]  # just in case
    base = re.sub(r"\.csv$", "", base, flags=re.IGNORECASE)
    base = base.strip().lower()

    # replace anything not alnum with underscore
    base = re.sub(r"[^a-z0-9]+", "_", base)
    base = re.sub(r"_+", "_", base).strip("_")

    if not base:
        base = "uploaded_csv"
    if base[0].isdigit():
        base = f"t_{base}"
    return base


def normalize_header(h: str) -> str:
    """
    Normalize CSV headers:
    'Month' -> 'month'
    '#Passengers' -> 'passengers'
    'Annual Income (k$)' -> 'annual_income_k'
    """
    if h is None:
        return "col"

    h = h.strip().lower()

    # remove leading # (common in AirPassengers)
    h = h.lstrip("#")

    # replace non-alnum with underscore
    h = re.sub(r"[^a-z0-9]+", "_", h)
    h = re.sub(r"_+", "_", h).strip("_")

    if not h:
        h = "col"
    if h[0].isdigit():
        h = f"c_{h}"
    return h


def decode_csv_bytes(file_bytes: bytes) -> str:
    # Handle utf-8 with BOM + fallback
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return file_bytes.decode(enc)
        except UnicodeDecodeError:
            continue
    # last resort
    return file_bytes.decode("utf-8", errors="replace")


def parse_csv_upload(file_bytes: bytes) -> Iterator[Dict[str, Any]]:
    text = decode_csv_bytes(file_bytes)
    text_stream = io.StringIO(text)

    reader = csv.DictReader(text_stream)
    if not reader.fieldnames:
        return iter(())

    # normalize headers once
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


def make_resource(table_name: str, write_disposition: str):
    """
    Create a dlt resource dynamically so table name changes per upload.
    """
    @dlt.resource(name=table_name, write_disposition=write_disposition)
    def csv_rows(rows: Iterator[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
        yield from rows

    return csv_rows


# ---------- API ----------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/load-csv")
async def load_csv(
    file: UploadFile = File(...),
    mode: str = Query(default="replace", pattern="^(replace|append)$"),
    table: Optional[str] = Query(default=None, description="Optional override table name"),
):
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    table_name = normalize_header(table) if table else slugify_table_name(file.filename)

    pipeline = dlt.pipeline(
        pipeline_name="csv_to_postgres_pipeline",
        destination="postgres",
        dataset_name="csv_demo",
    )

    write_disposition = "replace" if mode == "replace" else "append"

    try:
        rows_iter = parse_csv_upload(content)
        resource = make_resource(table_name, write_disposition)
        load_info = pipeline.run(resource(rows_iter))

        return {
            "message": "Loaded successfully",
            "table": f"csv_demo.{table_name}",
            "mode": mode,
            "load_info": str(load_info),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Load failed: {e}")
