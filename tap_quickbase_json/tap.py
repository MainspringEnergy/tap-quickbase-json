"""quickbase-json tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_quickbase_json.client import QuickbaseClient
from tap_quickbase_json.streams import QuickbaseJsonStream


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
        client = QuickbaseClient(config=self.config)
        tables = client.get_tables()

        streams = []
        for table in tables:
            stream = QuickbaseJsonStream(
                tap=self,
                name=table['name'],
                table=table,
            )
            streams.append(stream)

        return streams
