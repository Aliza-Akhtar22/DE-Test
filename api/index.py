import csv
import io
from typing import Iterator, Dict, Any

import dlt
from fastapi import FastAPI, UploadFile, File, HTTPException

app = FastAPI(title="CSV to Postgres via dlt")

@dlt.resource(name="new_csv", write_disposition="append")
def student_rows(rows: Iterator[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
    # rows is already a generator of dicts
    yield from rows

def parse_csv_upload(file_bytes: bytes) -> Iterator[Dict[str, Any]]:
    # Parse CSV in-memory (no filesystem dependency)
    text_stream = io.StringIO(file_bytes.decode("utf-8"))
    reader = csv.DictReader(text_stream)
    for row in reader:
        # Optional: strip whitespace
        yield {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/load-csv")
async def load_csv(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    # Create pipeline (same as your script)
    pipeline = dlt.pipeline(
        pipeline_name="csv_to_postgres_pipeline",
        destination="postgres",
        dataset_name="csv_demo",
    )

    try:
        rows_iter = parse_csv_upload(content)
        load_info = pipeline.run(student_rows(rows_iter))
        # load_info is a dlt LoadInfo object; str(load_info) is usually enough for API output
        return {"message": "Loaded successfully", "load_info": str(load_info)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Load failed: {e}")
