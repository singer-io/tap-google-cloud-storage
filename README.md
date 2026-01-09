# tap-google-cloud-storage

Singer tap for discovering schemas from files in Google Cloud Storage.

Supported discovery formats:
- Delimited text: CSV, TSV, PSV, TXT (via `singer-encodings`)
- JSON Lines (`.jsonl`) and JSON arrays/objects (`.json`)
- Parquet (`.parquet`, via `pyarrow`)
- Avro (`.avro`, via `fastavro`)

Usage:
- Discover (writes Singer catalog to stdout):
	`tap-google-cloud-storage --config config.json --discover > catalog.json`

Config highlights:
- Provide service account fields inline plus `bucket` and optional `root_path`.
- `tables` is a JSON-encoded array of table specs, e.g.:
```
"tables": "[
	{\"search_prefix\":\"exports\",\"search_pattern\":\"my_table\\/.*\\.csv\",\"table_name\":\"my_table_csv\",\"delimiter\":\",\"},
	{\"search_prefix\":\"exports\",\"search_pattern\":\"my_table\\/.*\\.tsv\",\"table_name\":\"my_table_tsv\"},
	{\"search_prefix\":\"exports\",\"search_pattern\":\"my_table\\/.*\\.jsonl\",\"table_name\":\"my_table_jsonl\"},
	{\"search_prefix\":\"exports\",\"search_pattern\":\"my_table\\/.*\\.json\",\"table_name\":\"my_table_json\"},
	{\"search_prefix\":\"exports\",\"search_pattern\":\"my_table\\/.*\\.parquet\",\"table_name\":\"my_table_parquet\"},
	{\"search_prefix\":\"exports\",\"search_pattern\":\"my_table\\/.*\\.avro\",\"table_name\":\"my_table_avro\"}
]"
```

Notes:
- Discovery samples up to 10 files per table and every 100th row by default.