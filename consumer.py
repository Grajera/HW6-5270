#!/usr/bin/env python3
"""
HW6 - Consumer
This program reads Widget Request JSON objects from an AWS S3 bucket (Bucket 2)
and processes each request.  For HW6, only "create" requests are implemented.
A created widget is stored either in another S3 bucket (Bucket 3) or
in a DynamoDB table, depending on command-line arguments.
The program polls the source bucket periodically (default 100 ms)
until interrupted or a maximum number of iterations is reached.

Future HW7 assignments will extend this design to support
"update" and "delete" requests and other data sources (queues).
"""
import argparse
import json
import logging
import time
from typing import Dict, Any, Optional

import boto3
from botocore.exceptions import ClientError

def normalize_owner(owner: str) -> str:
    """
    Replace spaces with dashes and convert to lowercase.
    Used to build predictable S3 keys.
    Input:  owner = "Alice Smith"
    Output: "alice-smith" 
    """
    return owner.replace(" ", "-").lower()

def s3_key_for_widget(owner: str, widget_id: str) -> str:
    """
    Build the key for a widget object in Bucket 3.
    Pattern: widgets/{owner}/{widget_id}
    """
    return f"widgets/{normalize_owner(owner)}/{widget_id}"

def flatten_widget(req: Dict[str, Any]) -> Dict[str, str]:
    """
    Convert a Widget Create Request into a flat dictionary where every
    attribute is a top-level key/value pair.  Required for DynamoDB storage.
    Skips empty string label/description fields.
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
        out[oa["name"]] = oa["value"]
    return out

# --------- S3 request source (Bucket 2) ---------

def try_get_one_request(s3, bucket2: str) -> Optional[Dict[str, str]]:
    """
    Retrieve exactly one JSON request from Bucket 2 in lexicographic key order.
    Returns a dictionary {"key": ..., "text": ...} or None if bucket is empty.
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
    """
    Delete the processed request from Bucket 2 so that it is not re-read.
    """
    s3.delete_object(Bucket=bucket2, Key=key)

def store_widget_to_s3(s3, bucket3: str, widget: Dict[str, Any]) -> None:
    """
    Serialize the widget dictionary to JSON and upload to Bucket 3
    using key pattern widgets/{owner}/{widget_id}.
    """
    key = s3_key_for_widget(widget["owner"], widget["widget_id"])
    body = json.dumps(widget, separators=(",", ":"), ensure_ascii=False)
    s3.put_object(Bucket=bucket3, Key=key, Body=body, ContentType="application/json")

def store_widget_to_dynamo(ddb, table: str, widget: Dict[str, str]) -> None:
    """
    Store the widget as a DynamoDB item with one attribute per key/value pair.
    """
    item = {k: {"S": v} for k, v in widget.items()}
    ddb.put_item(TableName=table, Item=item)

def process_request(
    data: Dict[str, Any],
    dest: str,
    s3,
    ddb,
    bucket3: Optional[str],
    table: Optional[str]
) -> None:
    """
    Handle one Widget Request object.
    For HW6 only the 'create' path is implemented.
    """
    t = str(data.get("type", "")).lower()
    if t == "create":
        widget_flat = flatten_widget(data)
        if dest == "s3":
            if not bucket3:
                raise ValueError("dest=s3 requires --dest-bucket")
            store_widget_to_s3(s3, bucket3, widget_flat)
        else:
            if not table:
                raise ValueError("dest=dynamo requires --dynamo-table")
            store_widget_to_dynamo(ddb, table, widget_flat)
    elif t in ("update", "delete"):
        # Will be implemented in HW7
        logging.warning("Request type '%s' seen. HW6 handles create only. Skipping.", t)
    else:
        logging.warning("Unknown request type '%s'. Skipping.", t)

def main():
    """
    Argument parsing, logger setup, and the simple polling loop.
    Loop:
      try to read one request
        if found: delete, process, loop
        if none: sleep poll-ms and loop
    """
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
    # Configure logging (console + file)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(args.log_file), logging.StreamHandler()]
    )
    # Validate parameters
    if args.dest == "s3" and not args.dest_bucket:
        parser.error("--dest-bucket is required when --dest s3")
    if args.dest == "dynamo" and not args.dynamo_table:
        parser.error("--dynamo-table is required when --dest dynamo")
    
    # Create SDK clients. Region can be provided or taken from environment or profile.
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
            # Try to get the next request
            got = try_get_one_request(s3, args.source_bucket)
            if got is None:
                # No requests. Sleep briefly and retry
                time.sleep(args.poll_ms / 1000.0)
            else:
                key = got["key"]
                text = got["text"]
                # Parse JSON. If invalid, delete to unblock the pipeline and continue.
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

                # Delete first to avoid duplicate processing
                try:
                    delete_request(s3, args.source_bucket, key)
                except ClientError as e:
                    logging.error("Failed to delete %s: %s", key, e)

                # Process the request (HW6: create only)
                try:
                    process_request(data, args.dest, s3, ddb, args.dest_bucket, args.dynamo_table)
                    logging.info("Processed request %s type=%s", data.get("requestId"), data.get("type"))
                except ClientError as e:
                    logging.exception("AWS error while processing %s: %s", key, e)
                except Exception as e:
                    logging.exception("Unhandled error while processing %s: %s", key, e)

            iterations += 1
            if args.max_iterations and iterations >= args.max_iterations:
                logging.info("Reached max-iterations. Exiting.")
                break
    except KeyboardInterrupt:
        logging.info("Shutting down on CTRL+C")

if __name__ == "__main__":
    main()
