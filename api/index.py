import csv
import io
import os
import re
from typing import Iterator, Dict, Any, List

import dlt
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import RedirectResponse

app = FastAPI(title="CSV to Postgres via dlt")

# ----------------------------
# Helpers
# ----------------------------

def slugify_table_name(filename: str) -> str:
    name = filename.lower().replace(".csv", "")
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")

def decode_csv_bytes(file_bytes: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return file_bytes.decode(enc)
        except UnicodeDecodeError:
            continue
    return file_bytes.decode("utf-8", errors="replace")

def parse_csv(file_bytes: bytes) -> Iterator[Dict[str, Any]]:
    text = decode_csv_bytes(file_bytes)
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        yield {k.lower(): v for k, v in row.items()}

# ----------------------------
# Routes
# ----------------------------

@app.get("/")
def root():
    return RedirectResponse("/docs")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/load-csv")
async def load_csv(
    file: UploadFile = File(...),
    db: str = Query(default="csv_demo")
):
    if not file.filename.endswith(".csv"):
        raise HTTPException(400, "Only CSV files allowed")

    content = await file.read()
    table_name = slugify_table_name(file.filename)

    pipeline = dlt.pipeline(
        pipeline_name=f"csv_pipeline_{table_name}",
        destination="postgres",
        dataset_name=db,
        full_refresh=True  # ⬅️ CRITICAL
    )

    @dlt.resource(name=table_name, write_disposition="replace")
    def csv_resource():
        yield from parse_csv(content)

    load_info = pipeline.run(csv_resource())

    return {
        "message": "Loaded successfully",
        "schema": db,
        "table": table_name,
        "load_info": str(load_info)
    }
