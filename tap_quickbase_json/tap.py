"""quickbase-json tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
# TODO: Import your custom stream types here:
from tap_quickbase_json.streams import (
    QuickbaseJsonStream,
    LGensStream,
)
from tap_quickbase_json.client import QuickbaseConn

# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    LGensStream,
]


class TapQuickbaseJson(Tap):
    """quickbase-json tap class."""
    name = "tap-quickbase-json"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "qb_hostname",
            th.StringType,
            required=True,
            description="Quickbase Realm Hostname (like yoursubdomain.quickbase.com)"
        ),
        th.Property(
            "qb_appid",
            th.StringType,
            required=True,
            description="Quickbase App Id"
        ),
        th.Property(
            "qb_user_token",
            th.StringType,
            required=True,
            description="Quickbase User Token (Secret)"
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        conn = QuickbaseConn(config=self.config)
        tables = conn.get_tables()
        schema = {
            'type': 'SCHEMA',
            'stream': '',
            'schema': '',
            'key_properties': '',
        }
        schema = th.PropertiesList()
        schema.append(th.Property('blerg', th.StringType))

        return [QuickbaseJsonStream(tap=self, name=table['name'], schema=schema.to_dict()) for table in tables]

#        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
