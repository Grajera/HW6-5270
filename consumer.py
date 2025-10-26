#!/usr/bin/env python3
import argparse
import json
import logging
import time
from typing import Dict, Any, Optional

import boto3
from botocore.exceptions import ClientError

def normalize_owner(owner: str) -> str:
    return owner.replace(" ", "-").lower()

def s3_key_for_widget(owner: str, widget_id: str) -> str:
    return f"widgets/{normalize_owner(owner)}/{widget_id}"

def flatten_widget(req: Dict[str, Any]) -> Dict[str, str]:
    """
    Convert a Create request JSON into a flat dict for storage.
    For DynamoDB we need each attribute as its own top-level field.
    We use the same flattened dict for S3 for simplicity.
    """
    out = {
        "widget_id": req["widgetId"],
        "owner": req["owner"],
    }
    if req.get("label") is not None and req.get("label") != "":
        out["label"] = req["label"]
    if req.get("description") is not None and req.get("description") != "":
        out["description"] = req["description"]
    for oa in (req.get("otherAttributes") or []):
        # Last write wins on collision; acceptable for HW6.
        out[oa["name"]] = oa["value"]
    return out

# --------- S3 request source (Bucket 2) ---------

def try_get_one_request(s3, bucket2: str) -> Optional[Dict[str, str]]:
    """
    Returns {"key": "<key>", "text": "<json>"} for the smallest lexicographic key,
    or None if bucket is empty.
    """
    resp = s3.list_objects_v2(Bucket=bucket2, MaxKeys=1)
    objs = resp.get("Contents", [])
    if not objs:
        return None
    key = objs[0]["Key"]
    get_resp = s3.get_object(Bucket=bucket2, Key=key)
    text = get_resp["Body"].read().decode("utf-8")
    return {"key": key, "text": text}

def delete_request(s3, bucket2: str, key: str) -> None:
    s3.delete_object(Bucket=bucket2, Key=key)

def store_widget_to_s3(s3, bucket3: str, widget: Dict[str, Any]) -> None:
    key = s3_key_for_widget(widget["owner"], widget["widget_id"])
    body = json.dumps(widget, separators=(",", ":"), ensure_ascii=False)
    s3.put_object(Bucket=bucket3, Key=key, Body=body, ContentType="application/json")

def store_widget_to_dynamo(ddb, table: str, widget: Dict[str, str]) -> None:
    item = {k: {"S": v} for k, v in widget.items()}
    ddb.put_item(TableName=table, Item=item)

def main():
    parser = argparse.ArgumentParser(description="HW6 Widget Consumer")
    parser.add_argument("--source-bucket", required=True, help="Bucket 2 name")
    parser.add_argument("--dest", choices=["s3", "dynamo"], default="s3", help="Destination storage")
    parser.add_argument("--dest-bucket", help="Bucket 3 name (required if dest=s3)")
    parser.add_argument("--dynamo-table", help="DynamoDB table (required if dest=dynamo)")
    parser.add_argument("--region", default=None, help="AWS region (optional)")
    parser.add_argument("--poll-ms", type=int, default=100, help="Poll delay when no work is found")
    parser.add_argument("--log-file", default="consumer.log", help="Log file path")
    parser.add_argument("--max-iterations", type=int, default=0, help="Stop after N loops. 0 means run forever.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(args.log_file), logging.StreamHandler()]
    )

    if args.dest == "s3" and not args.dest_bucket:
        parser.error("--dest-bucket is required when --dest s3")
    if args.dest == "dynamo" and not args.dynamo_table:
        parser.error("--dynamo-table is required when --dest dynamo")

    session_kwargs = {}
    if args.region:
        session_kwargs["region_name"] = args.region

    session = boto3.Session(**session_kwargs)
    s3 = session.client("s3")
    ddb = session.client("dynamodb")

    logging.info("Consumer starting. Source=%s Dest=%s", args.source_bucket, args.dest)

    iterations = 0
    try:
        while True:
            got = try_get_one_request(s3, args.source_bucket)
            if got is None:
                time.sleep(args.poll_ms / 1000.0)
            else:
                key = got["key"]
                text = got["text"]
                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    logging.error("Invalid JSON for key %s. Deleting to unblock.", key)
                    delete_request(s3, args.source_bucket, key)
                    iterations += 1
                    if args.max_iterations and iterations >= args.max_iterations:
                        logging.info("Reached max-iterations. Exiting.")
                        break
                    continue

                # Delete first to avoid duplicate processing after crashes
                try:
                    delete_request(s3, args.source_bucket, key)
                except ClientError as e:
                    logging.error("Failed to delete %s: %s", key, e)

            iterations += 1
            if args.max_iterations and iterations >= args.max_iterations:
                logging.info("Reached max-iterations. Exiting.")
                break
    except KeyboardInterrupt:
        logging.info("Shutting down on CTRL+C")

if __name__ == "__main__":
    main()
