import argparse
import json
from google.cloud import storage


def main():
    p = argparse.ArgumentParser(description="List GCS objects under a prefix")
    p.add_argument('--config', default='config.json')
    p.add_argument('--prefix', default='')
    args = p.parse_args()

    with open(args.config, 'r', encoding='utf-8') as f:
        cfg = json.load(f)

    bucket_name = cfg.get('bucket')
    if not bucket_name:
        raise SystemExit('bucket missing in config')

    client = storage.Client.from_service_account_info(cfg)
    # Normalize: strip leading slash
    prefix = (args.prefix or '').lstrip('/')

    total = 0
    for blob in client.list_blobs(bucket_name, prefix=prefix):
        print(blob.name)
        total += 1
    print(f"Total: {total}")


if __name__ == '__main__':
    main()
