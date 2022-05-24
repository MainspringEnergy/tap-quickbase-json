"""Tests standard tap features using the built-in SDK tests library."""

import pytest
from httmock import urlmatch, response, HTTMock

from singer_sdk.helpers._util import read_json_file
from singer_sdk.testing import get_standard_tap_tests

from tap_quickbase_json.tap import TapQuickbaseJson

#SAMPLE_CONFIG = read_json_file(".secrets/config.tap.json")

SAMPLE_CONFIG = {
    "qb_hostname": "somehost.quickbase.com",
    "qb_appid": "someid",
    "qb_user_token": "sometoken",
    "start_date": "2022-05-10T00:00:01Z",
    "table_catalog": ["my_table"]
}


# Run standard built-in tap tests from the SDK:
# def test_standard_tap_tests():
#     """Run standard tap tests from the SDK."""
#     tests = get_standard_tap_tests(TapQuickbaseJson, config=SAMPLE_CONFIG)
#     for test in tests:
#         test()

def mock_tables_response_json():
    return [
        {
            'name': 'My Table',
            'id': 't1',
        },
        {
            'name': 'My Other Table',
            'id': 't2',
        },
        {
            'name': 'My Excluded Table',
            'id': 't3',
        },
    ]


def mock_fields_response_json(table_id):
    return [
        {
            'id': 1,
            'label': f'Field 1 from table {table_id}',
            'fieldType': 'text',
        },
        {
            'id': 2,
            'label': 'Record ID#',
            'fieldType': 'recordid',
        },
        {
            'id': 3,
            'label': 'Date Modified',
            'fieldType': 'timestamp',
        },
    ]


def generate_tables_fixture(path, response_json):
    @urlmatch(netloc=r'api\.quickbase\.com', path=path)
    def mocked(_url, _request):
        return response(200, response_json)

    with HTTMock(mocked):
        yield


@pytest.fixture
def get_tables():
    yield from generate_tables_fixture(
        path=r'/v1/tables',
        response_json=mock_tables_response_json()
    )


@pytest.mark.usefixtures('get_tables', 'get_fields')
def test_wtf():
    tap = TapQuickbaseJson(config=SAMPLE_CONFIG)
