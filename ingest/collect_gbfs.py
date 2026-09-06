"""
Snapshot Citi Bike GBFS feeds to S3.

Design notes:
  - The raw layer stays BYTE-IDENTICAL to what the API returned. No parsing,
    no cleaning, no reshaping. If we later discover we parsed something wrong,
    we can rebuild from these files instead of re-collecting (impossible, since
    the feed is ephemeral).
  - Our own UTC fetch timestamp goes in the S3 key and in object metadata.
    We do NOT trust the feed's `last_updated` for partitioning, because it can
    be stale/cached, and GitHub Actions cron fires at irregular intervals.
  - The GBFS spec version is part of the S3 key. Feed schemas change between
    spec versions, so a version bump must land in a separate partition rather
    than mixing two shapes of JSON in one prefix.
  - Files are gzipped. Snowflake reads .gz from an external stage natively.
  - Any failure exits non-zero so the Actions run shows red instead of
    silently collecting nothing for a week.
"""

import argparse
import gzip
import io
import os
import sys
from datetime import datetime, timezone

import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Pinned deliberately to 1.1. Lyft also publishes 2.3, which adds a
# vehicle_types_available array — but 1.1 already exposes num_ebikes_available,
# so the classic/electric split is derivable and the upgrade buys nothing for
# this analysis (though I do plan to switch to 2.3 after completing this project for 1.1). 
# Resolving the version dynamically from the discovery document
# would be worse: a silent spec bump mid-collection would change our schema
# without warning. Pinning makes a version change a decision, not a surprise.
GBFS_VERSION = "1.1"
_BASE = f"https://gbfs.lyft.com/gbfs/{GBFS_VERSION}/bkn/en"

FEEDS = {
    "station_status": f"{_BASE}/station_status.json",
    "station_information": f"{_BASE}/station_information.json",
}

REQUEST_TIMEOUT = 30
USER_AGENT = "citibike-rebalancing-project/0.1 (portfolio project; contact via github)"          # ethical when sending requests

# Reusable HTTP connection session
def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1.5,          # 0s, 1.5s, 3s, 6s, something I actually learned in my networks class :D
        status_forcelist=[429, 500, 502, 503, 504],          # 429 is "Too Many Requests", 500-504 are server errors
        allowed_methods=["GET"],           # only retry GET (since its idempotent (meaning it could be repeated without changing state beyond the first time))
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    return session

# Builds the S3 filename/key used in upload
def s3_key(feed_name: str, fetched_at: datetime) -> str:
    stamp = fetched_at.strftime("%Y%m%dT%H%M%SZ")
    return (
        f"raw/{feed_name}/"
        f"dt={fetched_at:%Y-%m-%d}/"
        f"hour={fetched_at:%H}/"
        f"{feed_name}_{stamp}.json.gz"
    )


def fetch(session: requests.Session, url: str) -> bytes:
    response = session.get(url, timeout=REQUEST_TIMEOUT)         #  sessions.get from requests.Session class, timeout needed or else it will wait forever
    response.raise_for_status()          # raises exception if response is 4xx or 5xx
    body = response.content  
    if not body.strip().startswith(b"{"):          # cheap way to check if it's JSON
        raise ValueError(f"Response from {url} does not look like JSON")
    return body

# compresses by gzip which is lossless
def gzip_bytes(raw: bytes) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as handle:          # mtime=0 keeps the gzip header deterministic, so identical payloads produce identical bytes, useful for diffing/debugging. 
        handle.write(raw)
    return buffer.getvalue()


def upload(client, bucket: str, key: str, payload: bytes, fetched_at: datetime, url: str) -> None:
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload,
        ContentType="application/json",
        ContentEncoding="gzip",
        Metadata={
            "fetched-at-utc": fetched_at.isoformat(),
            "source-url": url,
        },
    )


def collect(feed_names, bucket: str, dry_run: bool) -> int:
    session = build_session()
    client = None if dry_run else boto3.client("s3")          # boto3 is AWS SDK for Python
    failures = 0

    for name in feed_names:
        url = FEEDS[name]
        fetched_at = datetime.now(timezone.utc)
        key = s3_key(name, fetched_at)

        try:
            raw = fetch(session, url)
            packed = gzip_bytes(raw)

            if dry_run:
                print(f"[dry-run] would write s3://{bucket}/{key} "
                      f"({len(raw):,} B raw -> {len(packed):,} B gz)")
            else:
                upload(client, bucket, key, packed, fetched_at, url)
                print(f"wrote s3://{bucket}/{key} "
                      f"({len(raw):,} B raw -> {len(packed):,} B gz)")

        except (requests.RequestException, ValueError) as exc:
            print(f"FETCH FAILED {name}: {exc}", file=sys.stderr)
            failures += 1
        except (BotoCoreError, ClientError) as exc:
            print(f"UPLOAD FAILED {name}: {exc}", file=sys.stderr)
            failures += 1

    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description="Snapshot Citi Bike GBFS feeds to S3.")          # creates command line interface
    parser.add_argument(   
        "--feeds",
        nargs="+",
        choices=sorted(FEEDS),
        default=["station_status"],
        help="Which feeds to snapshot (default: station_status).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and compress but skip the S3 write. Useful for local testing.",
    )
    args = parser.parse_args()

    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        print("S3_BUCKET environment variable is not set", file=sys.stderr)
        sys.exit(1)

    failures = collect(args.feeds, bucket, args.dry_run)
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()