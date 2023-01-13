"""Tests standard tap features using the built-in SDK tests library."""
import json

from singer_sdk.testing import get_standard_tap_tests

from tap_quickbase_json.tap import TapQuickbaseJson

SAMPLE_CONFIG = {
    "qb_hostname": "somehost.quickbase.com",
    "qb_appid": "someid",
    "qb_user_token": "sometoken",
    "start_date": "2022-05-10T00:00:01Z",
    "table_catalog": ["my_table", "my_other_table"],
}

META_TABLES = {
    "qb_meta_tables",
    "qb_meta_fields",
}


def mock_tables_response_json(include=None):
    include = include or ["t1", "t2", "t3"]
    tables = [
        {
            "name": "My Table",
            "id": "t1",
        },
        {
            "name": "My Other Table",
            "id": "t2",
        },
        {
            "name": "My Excluded Table",
            "id": "t3",
        },
    ]
    return [table for table in tables if table["id"] in include]


def mock_fields_response_json(table_id, field_1=None):
    field_1 = field_1 or {
        "id": 1,
        "label": f"Field 1 from table {table_id}",
        "fieldType": "text",
    }

    return [
        field_1,
        {
            "id": 2,
            "label": "Record ID#",
            "fieldType": "recordid",
        },
        {
            "id": 3,
            "label": "Date Modified",
            "fieldType": "timestamp",
        },
    ]


def mock_query_response_json(table_id, field_1=None):
    field_1 = field_1 or {"value": f"Random text for field 1 from {table_id}"}
    return {
        "data": [
            {
                "1": field_1,
                "2": {"value": 1},
                "3": {"value": "2021-02-09T22:22:13Z"},
            }
        ],
        "metadata": {
            "totalRecords": 1,
            "numRecords": 1,
        },
    }


def match_request_query_body(table_id):
    def matcher(request):
        return table_id == request.json()["from"]

    return matcher


def mock_tables_request(requests_mock, include=None):
    requests_mock.get(
        "https://api.quickbase.com/v1/tables", json=mock_tables_response_json(include=include)
    )


def mock_fields_request(requests_mock, table_id, field_1=None):
    requests_mock.get(
        f"https://api.quickbase.com/v1/fields?tableId={table_id}&includeFieldPerms=False",
        json=mock_fields_response_json(table_id, field_1=field_1),
    )


def mock_query_request(requests_mock, table_id, field_1=None):
    requests_mock.post(
        "https://api.quickbase.com/v1/records/query",
        additional_matcher=match_request_query_body(table_id),
        json=mock_query_response_json(table_id, field_1),
    )


def mock_all_requests(requests_mock):
    mock_tables_request(requests_mock)
    for table_id in ["t1", "t2", "t3"]:
        mock_fields_request(requests_mock, table_id)
        mock_query_request(requests_mock, table_id)


def test_standard_tap_tests(requests_mock):
    """Run standard tap tests from the SDK."""
    mock_all_requests(requests_mock)

    tests = get_standard_tap_tests(TapQuickbaseJson, config=SAMPLE_CONFIG)
    for test in tests:
        test()


def test_table_catalog_restricted(requests_mock):
    """Tests that specifying table_catalog excludes unselected tables."""
    mock_all_requests(requests_mock)

    tap = TapQuickbaseJson(config=SAMPLE_CONFIG)
    assert set(tap.streams.keys()) == {"my_table", "my_other_table"} | META_TABLES


def test_table_catalog_not_restricted(requests_mock):
    """Tests that not specifying table_catalog selects all tables."""
    mock_all_requests(requests_mock)

    tap = TapQuickbaseJson(config={**SAMPLE_CONFIG, **{"table_catalog": []}})
    assert set(tap.streams.keys()) == {"my_table", "my_other_table", "my_excluded_table"} | META_TABLES


def test_empty_timestamps(requests_mock):
    """Tests that empty timestamp fields are converted to None/Null"""
    mock_tables_request(requests_mock, include="t1")
    mock_fields_request(
        requests_mock,
        "t1",
        field_1={
            "id": 1,
            "label": "Field 1",
            "fieldType": "timestamp",
        },
    )
    mock_query_request(requests_mock, "t1", field_1={"value": ""})

    tap = TapQuickbaseJson(config=SAMPLE_CONFIG)
    stream = tap.streams["my_table"]
    record = next(stream.get_records({}))
    assert record["field_1"] is None


def test_infinity_is_none(requests_mock):
    """Tests that Infinities are converted to None/Null"""
    mock_tables_request(requests_mock, include="t1")
    mock_fields_request(
        requests_mock,
        "t1",
        field_1={
            "id": 1,
            "label": "Field 1",
            "fieldType": "number",
        },
    )
    mock_query_request(requests_mock, "t1", field_1=json.loads('{"value": Infinity}'))

    tap = TapQuickbaseJson(config=SAMPLE_CONFIG)
    stream = tap.streams["my_table"]
    record = next(stream.get_records({}))
    assert record["field_1"] is None
