import json
import os
from typing import List, Dict, Any


RECORDS: List[Dict[str, Any]] = [
    {"id": 1, "created_at": "2026-01-08T00:00:00Z", "name": "Ada", "active": True, "amount": 34.0},
    {"id": 2, "created_at": "2026-01-08T12:30:00Z", "name": "Bob", "active": False, "amount": 66.6},
    {"id": 3, "created_at": "2026-01-09T08:15:00Z", "name": "Chloe", "active": True, "amount": 123.45},
]


def write_parquet(path: str):
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except Exception as e:
        raise SystemExit("pyarrow not installed. Install dependencies: pip install -e .") from e

    table = pa.Table.from_pylist(RECORDS)
    pq.write_table(table, path)
    print(f"Wrote Parquet: {path}")


def write_avro(path: str):
    try:
        from fastavro import writer, parse_schema
    except Exception as e:
        raise SystemExit("fastavro not installed. Install dependencies: pip install -e .") from e

    schema = {
        "name": "my_table",
        "type": "record",
        "fields": [
            {"name": "id", "type": ["null", "long"], "default": None},
            {"name": "created_at", "type": ["null", "string"], "default": None},
            {"name": "name", "type": ["null", "string"], "default": None},
            {"name": "active", "type": ["null", "boolean"], "default": None},
            {"name": "amount", "type": ["null", "double"], "default": None},
        ],
    }
    parsed = parse_schema(schema)
    with open(path, "wb") as f:
        writer(f, parsed, RECORDS)
    print(f"Wrote Avro: {path}")


if __name__ == "__main__":
    write_parquet(os.path.join(os.getcwd(), "my_table_parquet.parquet"))
    write_avro(os.path.join(os.getcwd(), "my_table_avro.avro"))
