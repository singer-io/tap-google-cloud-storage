import argparse
import json
import os
from typing import List

from google.cloud import storage


def load_config(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_client_from_config(config: dict) -> storage.Client:
    # Config is expected to contain service account fields directly
    return storage.Client.from_service_account_info(config)


def upload_files(config_path: str, prefix: str, files: List[str]):
    config = load_config(config_path)
    bucket_name = config.get('bucket')
    if not bucket_name:
        raise ValueError('bucket missing in config')

    client = get_client_from_config(config)
    bucket = client.bucket(bucket_name)

    # Normalize prefix (no leading slash, ensure trailing slash if non-empty)
    prefix = prefix.lstrip('/')
    if prefix and not prefix.endswith('/'):
        prefix = prefix + '/'

    for local_path in files:
        if not os.path.isfile(local_path):
            print(f"Skipping {local_path}: not a file")
            continue
        filename = os.path.basename(local_path)
        blob_name = f"{prefix}{filename}" if prefix else filename
        blob = bucket.blob(blob_name)
        print(f"Uploading {local_path} -> gs://{bucket_name}/{blob_name}")
        blob.upload_from_filename(local_path)
    print("Done.")


def main():
    parser = argparse.ArgumentParser(description='Upload local files to GCS for testing.')
    parser.add_argument('--config', default='config.json', help='Path to tap config.json (with service account fields)')
    parser.add_argument('--prefix', default='exports/my_table/', help='Destination prefix in bucket, e.g. exports/my_table/')
    parser.add_argument('files', nargs='+', help='Local file(s) to upload')
    args = parser.parse_args()

    upload_files(args.config, args.prefix, args.files)


if __name__ == '__main__':
    main()
