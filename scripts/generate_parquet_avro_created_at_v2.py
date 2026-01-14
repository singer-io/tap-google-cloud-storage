from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from fastavro import writer, parse_schema


def make_records():
    return [
        {"id": 1101, "created_at": "2026-01-09T10:00:00Z", "name": "Older", "active": False, "amount": 10.0},
        {"id": 1102, "created_at": "2026-01-11T08:15:00Z", "name": "Equal", "active": True, "amount": 20.0},
        {"id": 1103, "created_at": "2026-01-12T09:00:00Z", "name": "Newer", "active": True, "amount": 30.0},
    ]


def write_parquet(path: Path, records):
    table = pa.Table.from_pylist(records)
    pq.write_table(table, path)
    print(f"Wrote Parquet: {path}")


def write_avro(path: Path, records):
    avro_schema = {
        "name": "sample_v2",
        "type": "record",
        "fields": [
            {"name": "id", "type": ["null", "int"]},
            {"name": "created_at", "type": ["null", "string"]},
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
    out_parquet = Path("my_table_parquet_created_at_v2.parquet")
    out_avro = Path("my_table_avro_created_at_v2.avro")
    recs = make_records()
    write_parquet(out_parquet, recs)
    write_avro(out_avro, recs)


if __name__ == "__main__":
    main()
