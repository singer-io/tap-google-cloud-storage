# tap-google-cloud-storage

Singer tap for discovering schemas from files in Google Cloud Storage.

Supported discovery formats:
- Delimited text: CSV, TSV, PSV, TXT (via `singer-encodings`)
- JSON Lines (`.jsonl`)
- Parquet (`.parquet`, via `pyarrow`)
- Avro (`.avro`, via `fastavro`)

Usage:
- Discover (writes Singer catalog to stdout):
	`tap-google-cloud-storage --config config.json --discover > catalog.json`

Config highlights:
- Provide service account fields inline plus `bucket` and optional `root_path`.
- `tables` is a JSON-encoded array of table specs, e.g.:
```
"tables": "[{"search_prefix":"exports/my_table","search_pattern":".\\.csv$","table_name":"my_table_csv","key_properties":"id","date_overrides":"created_at","delimiter":","},{"search_prefix":"exports/my_table","search_pattern":".\\.tsv$","table_name":"my_table_tsv","key_properties":"id","date_overrides":"created_at","delimiter":"\t"},{"search_prefix":"exports/my_table","search_pattern":".\\.psv$","table_name":"my_table_psv","key_properties":"id","date_overrides":"created_at","delimiter":"|"},{"search_prefix":"exports/my_table","search_pattern":".\\.txt$","table_name":"my_table_txt","key_properties":"id","date_overrides":"created_at","delimiter":","},{"search_prefix":"exports/my_table","search_pattern":".\\.jsonl$","table_name":"my_table_jsonl","key_properties":"id","date_overrides":"created_at"},{"search_prefix":"exports/my_table","search_pattern":".\\.parquet$","table_name":"my_table_parquet","key_properties":"id","date_overrides":"created_at"},{"search_prefix":"exports/my_table","search_pattern":".\\.avro$","table_name":"my_table_avro","key_properties":"id","date_overrides":"created_at"}]"
```

Notes:
- Discovery samples up to 10 files per table and every 100th row by default.