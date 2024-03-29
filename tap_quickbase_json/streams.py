"""Stream type classes for tap-quickbase-json."""

import datetime
from typing import Any, Dict, Iterable, List, Optional

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.streams import Stream

from tap_quickbase_json import json_clean_naninf, normalize_name
from tap_quickbase_json.client import QuickbaseClient


class TooManyKeyFieldsError(BaseException):
    """Raised when there are multiple key fields found."""


class NoKeyFieldError(BaseException):
    """Raised when there are no key fields found."""


class DateModifiedNotFoundError(BaseException):
    """Raised when there is no date modified field."""


class QuickbaseMetaTableStream(Stream):
    """quickbase-json table metadata stream class."""

    primary_keys: List = ["app_id", "table_id"]
    replication_method: str = "FULL_TABLE"

    def __init__(self, **kwargs) -> None:
        """Quickbase tables metadata stream.

        Args:
            tap: Singer Tap this stream belongs to.
            schema: JSON schema for records in this stream.
            name: Name of this stream.
        """
        super().__init__(**kwargs)
        self.logger = self._tap.logger

    @property
    def client(self) -> QuickbaseClient:
        """Quickbase API client."""
        if hasattr(self, "_client"):
            return self._client
        self._client: QuickbaseClient = QuickbaseClient(self.config, logger=self.logger)
        return self._client

    @client.setter
    def client(self, value) -> None:
        self._client = value

    @property
    def schema(self) -> dict:
        """Get schema.

        Returns:
            JSON Schema dictionary for this stream.

        """
        if hasattr(self, "_schema"):
            return self._schema

        schema_builder = th.PropertiesList()
        schema_builder.append(th.Property("app_id", th.StringType()))
        schema_builder.append(th.Property("query_at", th.DateTimeType()))
        schema_builder.append(th.Property("table_id", th.StringType()))
        schema_builder.append(th.Property("table_name", th.StringType()))
        schema_builder.append(th.Property("metadata", th.ArrayType(th.ObjectType())))
        self._schema = schema_builder.to_dict()
        return self._schema

    @schema.setter
    def schema(self, value) -> None:
        self._schema = value

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        Yields:
            One item per (possibly processed) record in the API.
        """
        query_at = datetime.datetime.now()
        tables = self.client.request_tables().json()

        for table in tables:
            table_name = normalize_name(table["name"])
            record = {
                "app_id": self.config["qb_appid"],
                "query_at": query_at,
                "table_id": table["id"],
                "table_name": table_name,
                "metadata": table,
            }
            yield record


class QuickbaseMetaFieldStream(Stream):
    """quickbase-json field metadata stream class."""

    primary_keys = ["app_id", "table_id", "field_id"]
    replication_method: str = "FULL_TABLE"

    def __init__(self, table_catalog: List[str] = None, **kwargs) -> None:
        """Quickbase field metadata stream.

        Args:
            tap: Singer Tap this stream belongs to.
            schema: JSON schema for records in this stream.
            name: Name of this stream.
            table_catalog: Only includes fields from tables in the catalog.
        """
        super().__init__(**kwargs)
        self.logger = self._tap.logger
        self.table_catalog = table_catalog or []

    @property
    def client(self) -> QuickbaseClient:
        """Quickbase API client."""
        if hasattr(self, "_client"):
            return self._client
        self._client: QuickbaseClient = QuickbaseClient(self.config, logger=self.logger)
        return self._client

    @client.setter
    def client(self, value) -> None:
        self._client = value

    @property
    def schema(self) -> dict:
        """Get schema.

        Returns:
            JSON Schema dictionary for this stream.

        """
        if hasattr(self, "_schema"):
            return self._schema

        schema_builder = th.PropertiesList()
        schema_builder.append(th.Property("app_id", th.StringType()))
        schema_builder.append(th.Property("query_at", th.DateTimeType()))
        schema_builder.append(th.Property("table_id", th.StringType()))
        schema_builder.append(th.Property("table_name", th.StringType()))
        schema_builder.append(th.Property("field_id", th.StringType()))
        schema_builder.append(th.Property("field_name", th.StringType()))
        schema_builder.append(th.Property("metadata", th.ArrayType(th.ObjectType())))
        self._schema = schema_builder.to_dict()
        return self._schema

    @schema.setter
    def schema(self, value) -> None:
        self._schema = value

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        Yields:
            One item per (possibly processed) record in the API.
        """
        query_at = datetime.datetime.now()
        tables = self.client.request_tables().json()
        for table in tables:
            table_name = normalize_name(table["name"])

            if table_name not in self.table_catalog and len(self.table_catalog) > 0:
                continue

            self.logger.info("Fetching field metadata for table %s (%s)", table_name, table["id"])

            fields = self.client.request_fields(table_id=table["id"]).json()

            for field in fields:
                record = {
                    "app_id": self.config["qb_appid"],
                    "query_at": query_at,
                    "table_id": table["id"],
                    "table_name": table_name,
                    "field_id": field["id"],
                    "field_name": normalize_name(field["label"]),
                    "metadata": field,
                }
                yield record


class QuickbaseJsonStream(Stream):
    """quickbase-json record stream class."""

    is_sorted = True
    STATE_MSG_FREQUENCY = 2000

    def __init__(self, table: dict, **kwargs) -> None:
        """Init Quickbase tap stream.

        Args:
            tap: Singer Tap this stream belongs to.
            schema: JSON schema for records in this stream.
            name: Name of this stream.
            table: Dictionary of Quickbase table properties (id, name)

        """
        self.table = table
        super().__init__(**kwargs)
        self.logger = self._tap.logger

    @property
    def client(self) -> QuickbaseClient:
        """Quickbase API client."""
        if hasattr(self, "_client"):
            return self._client
        self._client: QuickbaseClient = QuickbaseClient(self.config, logger=self.logger)
        return self._client

    @client.setter
    def client(self, value) -> None:
        self._client = value

    @property
    def fields(self) -> List[Dict]:
        """Quickbase fields for this stream's table."""
        if hasattr(self, "_fields"):
            return self._fields

        client_fields = self.client.request_fields(table_id=self.table["id"]).json()
        self._fields: List[Dict] = [
            {
                "id": field["id"],
                "name": normalize_name(field["label"]),
                "fieldType": field["fieldType"],
            }
            for field in client_fields
        ]
        return self._fields

    def _field_lookup(self) -> dict:
        return {field["id"]: field["name"] for field in self.fields}

    def _field_type_lookup(self) -> dict:
        return {field["id"]: field["fieldType"] for field in self.fields}

    @staticmethod
    def _type_lookup(qb_type: str) -> object:
        return {
            "checkbox": th.BooleanType,
            "currency": th.NumberType,
            "date": th.DateType,
            "duration": th.IntegerType,
            "numeric": th.NumberType,
            "percent": th.NumberType,
            "rating": th.NumberType,
            "timestamp": th.DateTimeType,
            "datetime": th.DateTimeType,
            "timeofday": th.TimeType,
            "recordid": th.IntegerType,
            "multitext": th.ArrayType(th.StringType),
            "user": th.ObjectType(
                th.Property("email", th.StringType()),
                th.Property("id", th.StringType()),
                th.Property("name", th.StringType()),
                th.Property("userName", th.StringType()),
            ),
            "multiuser": th.ArrayType(
                th.ObjectType(
                    th.Property("email", th.StringType()),
                    th.Property("id", th.StringType()),
                    th.Property("name", th.StringType()),
                    th.Property("userName", th.StringType()),
                )
            ),
            "file": th.StringType,
        }.get(qb_type, th.StringType)

    @property
    def schema(self) -> dict:
        """Get schema.

        Returns:
            JSON Schema dictionary for this stream.

        """
        if hasattr(self, "_schema"):
            return self._schema

        schema_builder = th.PropertiesList()
        for field in self.fields:
            schema_builder.append(th.Property(field["name"], self._type_lookup(field["fieldType"])))
        self._schema = schema_builder.to_dict()
        return self._schema

    @schema.setter
    def schema(self, value) -> None:
        self._schema = value

    @property
    def primary_keys(self) -> List:
        """Get primary keys.

        Returns:
            List of primary keys

        """
        # parent Stream class initializes this one to None
        if hasattr(self, "_primary_keys") and self._primary_keys is not None:
            return self._primary_keys

        id_fields = [field["name"] for field in self.fields if field["fieldType"] == "recordid"]
        if len(id_fields) > 1:
            raise TooManyKeyFieldsError(
                f'In table {self.table["id"]}, found multiple key fields: {id_fields}'
            )
        if len(id_fields) < 1:
            raise NoKeyFieldError(f'No key fields defined found for table {self.table["id"]}')
        self._primary_keys = id_fields
        return self._primary_keys

    @primary_keys.setter
    def primary_keys(self, value: List) -> None:
        self._primary_keys = value

    @property
    def replication_key(self) -> str:
        """Get replication key.

        Returns:
            Name of replication key

        """
        # parent Stream class initializes this one to None
        if hasattr(self, "_replication_key") and self._replication_key is not None:
            return self._replication_key

        if "date_modified" not in [field["name"] for field in self.fields]:
            raise DateModifiedNotFoundError(f'No `date_modified` field found for table {self.table["id"]}')

        self._replication_key = "date_modified"
        return self._replication_key

    @replication_key.setter
    def replication_key(self, value) -> None:
        self._replication_key = value

    def _request_records(self, context: Optional[dict]) -> Iterable[dict]:
        skip = 0
        total_records = 0
        finished = False

        date_modified_id = list(filter(lambda field: field["name"] == "date_modified", self.fields))[0][
            "id"
        ]

        self.logger.info("Fetching data for table %s (%s)", self.table["name"], self.table["id"])
        while not finished:
            request = self.client.request_records(
                table_id=self.table["id"],
                table_name=self.table["name"],
                field_ids=sorted([field["id"] for field in self.fields]),
                date_modified_id=date_modified_id,
                last_date_modified=self.get_starting_replication_key_value(context),
                skip=skip,
            )

            metadata = request.json()["metadata"]
            total_records = metadata["totalRecords"]
            skip = skip + metadata["numRecords"]
            self.logger.info("%s: Retrieved %s/%s records", self.table["name"], skip, total_records)

            finished = skip >= total_records

            yield from self._process_record_data(request.json()["data"])

    @staticmethod
    def _json_clean_values(value: Any, field_type: str) -> Any:
        value = json_clean_naninf(value)
        if field_type in ["date", "datetime", "timestamp"] and len(value) == 0:
            value = None
        return value

    def _process_record_data(self, data: List) -> Iterable[dict]:
        field_lookup = self._field_lookup()
        field_type_lookup = self._field_type_lookup()
        for record in data:
            processed = {
                field_lookup[int(field_id)]: self._json_clean_values(
                    value["value"], field_type_lookup[int(field_id)]
                )
                for field_id, value in record.items()
            }
            yield processed

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self._request_records(context):
            yield record
