from botocore.stub import Stubber
import boto3

from consumer import store_widget_to_dynamo

def _ddb():
    return boto3.client("dynamodb", region_name="us-west-2")

def test_store_widget_to_dynamo_flattened_attributes():
    ddb = _ddb()
    widget = {"widget_id": "w1", "owner": "Alice", "label": "L", "color": "red"}
    expected_item = {
        "widget_id": {"S": "w1"},
        "owner": {"S": "Alice"},
        "label": {"S": "L"},
        "color": {"S": "red"},
    }
    with Stubber(ddb) as stub:
        stub.add_response("put_item", {}, {"TableName": "widgets", "Item": expected_item})
        store_widget_to_dynamo(ddb, "widgets", widget)
