import json
from botocore.stub import Stubber
import boto3
import io

from consumer import try_get_one_request, delete_request, store_widget_to_s3, s3_key_for_widget

def _s3():
    return boto3.client("s3", region_name="us-west-2")

def test_try_get_one_request_none_when_bucket_empty():
    s3 = _s3()
    with Stubber(s3) as stub:
        stub.add_response("list_objects_v2", {"IsTruncated": False, "KeyCount": 0}, {"Bucket": "b2", "MaxKeys": 1})
        got = try_get_one_request(s3, "b2")
        assert got is None

def test_try_get_one_request_returns_key_and_text():
    s3 = _s3()
    with Stubber(s3) as stub:
        stub.add_response(
            "list_objects_v2",
            {"IsTruncated": False, "KeyCount": 1, "Contents": [{"Key": "0001.json"}]},
            {"Bucket": "b2", "MaxKeys": 1}
        )
        body_bytes = json.dumps({"type":"create","requestId":"r1","widgetId":"w1","owner":"Alice"}).encode("utf-8")
        # For get_object, the Body must be a file-like object; botocore accepts StreamingBody but a bytes IO is okay through stubber
        stub.add_response(
            "get_object",
            {"Body": io.BytesIO(body_bytes)},
            {"Bucket": "b2", "Key": "0001.json"}
        )
        got = try_get_one_request(s3, "b2")
        assert got is not None
        assert got["key"] == "0001.json"
        assert '"widgetId":"w1"' in got["text"]

def test_delete_request_calls_delete_object():
    s3 = _s3()
    with Stubber(s3) as stub:
        stub.add_response("delete_object", {}, {"Bucket": "b2", "Key": "0001.json"})
        delete_request(s3, "b2", "0001.json")  # no exception means pass

def test_store_widget_to_s3_puts_expected_key_and_body(monkeypatch):
    s3 = _s3()
    widget = {"widget_id": "w1", "owner": "Alice Smith", "color": "red"}
    expected_key = s3_key_for_widget("Alice Smith", "w1")
    with Stubber(s3) as stub:
        stub.add_response(
            "put_object",
            {},
            {
                "Bucket": "b3",
                "Key": expected_key,
                "Body": json.dumps(widget, separators=(",", ":"), ensure_ascii=False),
                "ContentType": "application/json"
            }
        )
        store_widget_to_s3(s3, "b3", widget)
