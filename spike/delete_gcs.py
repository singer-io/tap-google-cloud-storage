import argparse
import json
from typing import Iterable
from google.cloud import storage


def delete_blob(client: storage.Client, bucket_name: str, blob_name: str):
    b = client.bucket(bucket_name)
    blob = b.blob(blob_name)
    if blob.exists(client):
        print(f"Deleting gs://{bucket_name}/{blob_name}")
        blob.delete()
    else:
        print(f"Not found (skip): gs://{bucket_name}/{blob_name}")


def delete_prefix(client: storage.Client, bucket_name: str, prefix: str):
    prefix = (prefix or '').lstrip('/')
    it = client.list_blobs(bucket_name, prefix=prefix)
    found = False
    for blob in it:
        found = True
        print(f"Deleting gs://{bucket_name}/{blob.name}")
        blob.delete()
    if not found:
        print("No objects found under prefix.")


def main():
    p = argparse.ArgumentParser(description="Delete GCS object(s)")
    p.add_argument('--config', default='config.json', help='Path to config with service account and bucket')
    m = p.add_mutually_exclusive_group(required=True)
    m.add_argument('--blob', help='Exact object path to delete, e.g. exports/my_table/my_table_sample.csv')
    m.add_argument('--prefix', help='Delete all objects under this prefix (folder-like)')
    args = p.parse_args()

    cfg = json.load(open(args.config, 'r', encoding='utf-8'))
    bucket_name = cfg.get('bucket')
    if not bucket_name:
        raise SystemExit('bucket missing in config')

    client = storage.Client.from_service_account_info(cfg)

    if args.blob:
        delete_blob(client, bucket_name, args.blob.lstrip('/'))
    else:
        delete_prefix(client, bucket_name, args.prefix)


if __name__ == '__main__':
    main()
