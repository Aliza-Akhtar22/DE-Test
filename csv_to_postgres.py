import csv
from typing import Iterator, Dict, Any
import dlt


@dlt.resource(
    name="student",
    write_disposition="append"  
)
def student_csv(file_path: str) -> Iterator[Dict[str, Any]]:
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def main():
    pipeline = dlt.pipeline(
        pipeline_name="csv_to_postgres_pipeline",
        destination="postgres",
        dataset_name="csv_demo"
    )

    info = pipeline.run(
        student_csv("student_monnitoring_data.csv")
    )
    print(info)


if __name__ == "__main__":
    main()
