import json
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from fastavro import writer, parse_schema


def make_records():
    return [
        {"id": 701, "updatedAt": "2026-01-10T09:00:00Z", "name": "Ada", "active": True, "amount": 50.0},
        {"id": 702, "updatedAt": "2026-01-10T12:30:00Z", "name": "Bob", "active": False, "amount": 75.5},
        {"id": 703, "updatedAt": "2026-01-11T08:15:00Z", "name": "Chloe", "active": True, "amount": 200.25},
    ]


def write_parquet(path: Path, records):
    table = pa.Table.from_pylist(records)
    pq.write_table(table, path)
    print(f"Wrote Parquet: {path}")


def write_avro(path: Path, records):
    avro_schema = {
        "name": "sample",
        "type": "record",
        "fields": [
            {"name": "id", "type": ["null", "int"]},
            {"name": "updatedAt", "type": ["null", "string"]},
            {"name": "name", "type": ["null", "string"]},
            {"name": "active", "type": ["null", "boolean"]},
            {"name": "amount", "type": ["null", "double"]},
        ]
    }
    parsed = parse_schema(avro_schema)
    with open(path, "wb") as f:
        writer(f, parsed, records)
    print(f"Wrote Avro: {path}")


def main():
    out_parquet = Path("my_table_parquet_increment.parquet")
    out_avro = Path("my_table_avro_increment.avro")
    recs = make_records()
    write_parquet(out_parquet, recs)
    write_avro(out_avro, recs)


if __name__ == "__main__":
    main()
