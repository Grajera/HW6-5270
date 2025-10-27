import json
import pytest
from consumer import normalize_owner, s3_key_for_widget, flatten_widget

def test_normalize_owner_spaces_to_dashes_lower():
    assert normalize_owner("Alice Smith") == "alice-smith"
    assert normalize_owner("Bob") == "bob"

def test_s3_key_for_widget_uses_normalized_owner():
    key = s3_key_for_widget("Alice Smith", "w-001")
    assert key == "widgets/alice-smith/w-001"

def test_flatten_widget_basic_fields_and_other_attrs():
    req = {
        "type": "create",
        "requestId": "r1",
        "widgetId": "w1",
        "owner": "Alice",
        "label": "L",
        "description": "D",
        "otherAttributes": [
            {"name": "color", "value": "red"},
            {"name": "size", "value": "M"}
        ]
    }
    flat = flatten_widget(req)
    assert flat["widget_id"] == "w1"
    assert flat["owner"] == "Alice"
    assert flat["label"] == "L"
    assert flat["description"] == "D"
    assert flat["color"] == "red"
    assert flat["size"] == "M"

def test_flatten_widget_empty_string_removal_for_label_and_description():
    req = {
        "type": "create",
        "requestId": "r2",
        "widgetId": "w2",
        "owner": "Alice",
        "label": "",
        "description": "",
        "otherAttributes": []
    }
    flat = flatten_widget(req)
    assert "label" not in flat
    assert "description" not in flat
