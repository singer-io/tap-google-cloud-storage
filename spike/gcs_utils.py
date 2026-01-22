"""
Unified GCS utility for managing bucket operations.

This module provides a command-line interface for Google Cloud Storage operations
including upload, list, and delete functionality.

Requirements:
    - google-cloud-storage Python package
    - A valid config.json file with service account credentials and bucket name

Configuration:
    The config.json file must include:
    - Service account credentials (project_id, private_key, client_email, etc.)
    - bucket: Name of the GCS bucket to operate on

Usage:
    python gcs_utils.py [--config CONFIG_FILE] COMMAND [OPTIONS]

Commands:
    upload      Upload local files to GCS bucket
    list        List objects in GCS bucket
    delete      Delete object(s) from GCS bucket

Examples:
    # Upload files to a specific prefix
    python gcs_utils.py upload --prefix exports/my_table/ file1.csv file2.csv

    # Upload with custom config file
    python gcs_utils.py --config my_custom_config.json upload --prefix exports/ file.csv

    # List all objects under a prefix
    python gcs_utils.py list --prefix exports/my_table/

    # List all objects in bucket
    python gcs_utils.py list

    # Delete a specific object
    python gcs_utils.py delete --blob exports/my_table/file1.csv

    # Delete all objects under a prefix
    python gcs_utils.py delete --prefix exports/my_table/

Notes:
    - Prefixes are automatically normalized (leading slashes removed)
    - Upload command preserves original filenames in destination
    - Delete with --prefix removes all matching objects (use with caution)
    - All operations require valid GCS credentials in config file
"""
import argparse
import json
import os
from typing import List
from google.cloud import storage


def load_config(path: str) -> dict:
    """Load configuration from JSON file."""
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_client_from_config(config: dict) -> storage.Client:
    """Create GCS client from service account config."""
    return storage.Client.from_service_account_info(config)


def cmd_upload(args):
    """Upload local files to GCS bucket."""
    config = load_config(args.config)
    bucket_name = config.get('bucket')
    if not bucket_name:
        raise ValueError('bucket missing in config')

    client = get_client_from_config(config)
    bucket = client.bucket(bucket_name)

    # Normalize prefix (no leading slash, ensure trailing slash if non-empty)
    prefix = args.prefix.lstrip('/')
    if prefix and not prefix.endswith('/'):
        prefix = prefix + '/'

    for local_path in args.files:
        if not os.path.isfile(local_path):
            print(f"Skipping {local_path}: not a file")
            continue
        filename = os.path.basename(local_path)
        blob_name = f"{prefix}{filename}" if prefix else filename
        blob = bucket.blob(blob_name)
        print(f"Uploading {local_path} -> gs://{bucket_name}/{blob_name}")
        blob.upload_from_filename(local_path)
    print("Upload complete.")


def cmd_list(args):
    """List objects in GCS bucket under a prefix."""
    config = load_config(args.config)
    bucket_name = config.get('bucket')
    if not bucket_name:
        raise SystemExit('bucket missing in config')

    client = get_client_from_config(config)
    # Normalize: strip leading slash
    prefix = (args.prefix or '').lstrip('/')

    total = 0
    for blob in client.list_blobs(bucket_name, prefix=prefix):
        print(blob.name)
        total += 1
    print(f"Total: {total}")


def cmd_delete(args):
    """Delete object(s) from GCS bucket."""
    config = load_config(args.config)
    bucket_name = config.get('bucket')
    if not bucket_name:
        raise SystemExit('bucket missing in config')

    client = get_client_from_config(config)

    if args.blob:
        # Delete single blob
        blob_name = args.blob.lstrip('/')
        b = client.bucket(bucket_name)
        blob = b.blob(blob_name)
        if blob.exists(client):
            print(f"Deleting gs://{bucket_name}/{blob_name}")
            blob.delete()
            print("Delete complete.")
        else:
            print(f"Not found (skip): gs://{bucket_name}/{blob_name}")
    else:
        # Delete by prefix
        prefix = (args.prefix or '').lstrip('/')
        it = client.list_blobs(bucket_name, prefix=prefix)
        found = False
        for blob in it:
            found = True
            print(f"Deleting gs://{bucket_name}/{blob.name}")
            blob.delete()
        if found:
            print("Delete complete.")
        else:
            print("No objects found under prefix.")


def main():
    parser = argparse.ArgumentParser(
        description='Unified GCS utility for bucket operations',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--config', default='config.json',
                       help='Path to config.json (with service account fields and bucket)')

    subparsers = parser.add_subparsers(dest='command', required=True, help='Command to execute')

    # Upload command
    upload_parser = subparsers.add_parser('upload', help='Upload local files to GCS')
    upload_parser.add_argument('--prefix', default='',
                              help='Destination prefix in bucket, e.g. exports/my_table/')
    upload_parser.add_argument('files', nargs='+', help='Local file(s) to upload')
    upload_parser.set_defaults(func=cmd_upload)

    # List command
    list_parser = subparsers.add_parser('list', help='List objects in GCS bucket')
    list_parser.add_argument('--prefix', default='', help='Filter by prefix')
    list_parser.set_defaults(func=cmd_list)

    # Delete command
    delete_parser = subparsers.add_parser('delete', help='Delete object(s) from GCS')
    delete_group = delete_parser.add_mutually_exclusive_group(required=True)
    delete_group.add_argument('--blob', help='Exact object path to delete')
    delete_group.add_argument('--prefix', help='Delete all objects under this prefix')
    delete_parser.set_defaults(func=cmd_delete)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
