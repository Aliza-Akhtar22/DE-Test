import csv
import io
import re
from typing import Iterator, Dict, Any

import dlt
from fastapi import FastAPI, UploadFile, File, HTTPException

app = FastAPI(title="CSV to Postgres via dlt")


def safe_table_name(filename: str) -> str:
    base = (filename or "").rsplit(".", 1)[0].lower().strip()
    base = re.sub(r"[^a-z0-9_]+", "_", base).strip("_")
    if not base:
        base = "uploaded_csv"
    if base[0].isdigit():
        base = f"t_{base}"
    return base


def parse_csv_upload(file_bytes: bytes) -> Iterator[Dict[str, Any]]:
    # utf-8 + BOM safe
    try:
        text = file_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = file_bytes.decode("utf-8", errors="replace")

    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise ValueError("CSV must have a header row (column names).")

    for row in reader:
        yield {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/load-csv")
async def load_csv(file: UploadFile = File(...)):
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    pipeline = dlt.pipeline(
        pipeline_name="csv_to_postgres_pipeline",
        destination="postgres",
        dataset_name="csv_demo",
    )

    table_name = safe_table_name(file.filename)

    try:
        rows_iter = parse_csv_upload(content)

        load_info = pipeline.run(
            rows_iter,
            table_name=table_name,
            write_disposition="append",
        )

        return {
            "message": "Loaded successfully",
            "schema": "csv_demo",
            "table": table_name,
            "full_table": f"csv_demo.{table_name}",
            "load_info": str(load_info),
        }
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Load failed: {e}")
