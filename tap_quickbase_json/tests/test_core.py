"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.helpers._util import read_json_file
from singer_sdk.testing import get_standard_tap_tests

from tap_quickbase_json.tap import TapQuickbaseJson

SAMPLE_CONFIG = read_json_file(".secrets/config.tap.json")


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapQuickbaseJson, config=SAMPLE_CONFIG)
    for test in tests:
        test()
