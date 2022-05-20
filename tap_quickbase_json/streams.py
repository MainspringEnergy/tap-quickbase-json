"""Stream type classes for tap-quickbase-json."""

from typing import Optional, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.streams import Stream

from tap_quickbase_json.client import QuickbaseClient
from tap_quickbase_json import (
    normalize_name,
    json_clean_num,
)


class TooManyKeyFieldsError(BaseException):
    pass


class NoKeyFieldError(BaseException):
    pass


class DateModifiedNotFoundError(BaseException):
    pass


class QuickbaseJsonStream(Stream):
    """quickbase-json stream class."""

    def __init__(self, table: dict = None, **kwargs) -> None:
        self.table = table
        super().__init__(**kwargs)
        self.logger = self._tap.logger

    @property
    def client(self) -> QuickbaseClient:
        if hasattr(self, '_client'):
            return self._client
        self._client = QuickbaseClient(self.config, logger=self.logger)
        return self._client

    @client.setter
    def client(self, value) -> None:
        self._client = value

    @property
    def fields(self) -> dict:
        if hasattr(self, '_fields'):
            return self._fields

        client_fields = self.client.request_fields(self.table['id']).json()
        self._fields = [
            {
                'id': field['id'],
                'name': normalize_name(field['label']),
                'fieldType': field['fieldType'],
            }
            for field in client_fields
        ]
        return self._fields

    def _field_lookup(self) -> dict:
        return {
            field['id']: field['name']
            for field in self.fields
        }

    @staticmethod
    def type_lookup(qb_type: str) -> th.JSONTypeHelper:
        return {
            'checkbox': th.BooleanType,
            'currency': th.NumberType,
            'date': th.DateType,
            'duration': th.IntegerType,
            'numeric': th.NumberType,
            'percent': th.NumberType,
            'rating': th.NumberType,
            'timestamp': th.DateTimeType,
            'datetime': th.DateTimeType,
            'timeofday': th.TimeType,
            'recordid': th.IntegerType,
            'multitext': th.ArrayType(th.StringType),
            'user': th.ObjectType(
                th.Property('email', th.StringType()),
                th.Property('id', th.StringType()),
                th.Property('name', th.StringType()),
                th.Property('userName', th.StringType()),
            ),
            'multiuser': th.ArrayType(th.ObjectType(
                th.Property('email', th.StringType()),
                th.Property('id', th.StringType()),
                th.Property('name', th.StringType()),
                th.Property('userName', th.StringType()),
            )),
            'file': th.StringType,
        }.get(qb_type, th.StringType)

    @property
    def schema(self) -> dict:
        if hasattr(self, '_schema'):
            return self._schema

        schema_builder = th.PropertiesList()
        for field in self.fields:
            schema_builder.append(
                th.Property(
                    field['name'],
                    self.type_lookup(field['fieldType'])
                )
            )
        self._schema = schema_builder.to_dict()
        return self._schema

    @schema.setter
    def schema(self, value) -> None:
        self._schema = value

    @property
    def primary_keys(self) -> List:
        # parent Stream class initializes this one to None
        if hasattr(self, '_primary_keys') and self._primary_keys is not None:
            return self._primary_keys

        id_fields = [
            field['name']
            for field in self.fields
            if field['fieldType'] == 'recordid'
        ]
        if len(id_fields) > 1:
            raise TooManyKeyFieldsError(
                f'In table {self.table["id"]}, found multiple key fields: {id_fields}'
            )
        if len(id_fields) < 1:
            raise NoKeyFieldError(
                f'No key fields defined found for table {self.table["id"]}'
            )
        self._primary_keys = id_fields
        return self._primary_keys

    @primary_keys.setter
    def primary_keys(self, value: List) -> None:
        self._primary_keys = value

    @property
    def replication_key(self) -> str:
        # parent Stream class initializes this one to None
        if hasattr(self, '_replication_key') and self._replication_key is not None:
            return self._replication_key

        if 'date_modified' not in [field['name'] for field in self.fields]:
            raise DateModifiedNotFoundError(
                f'No `date_modified` field found for table {self.table["id"]}'
            )

        self._replication_key = 'date_modified'
        return self._replication_key

    @replication_key.setter
    def replication_key(self, value) -> None:
        self._replication_key = value

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        skip = 0
        total_records = 0
        finished = False

        date_modified_id = list(filter(
            lambda field: field['name'] == 'date_modified',
            self.fields
        ))[0]['id']

        self.logger.info('Fetching data for table %s (%s)', self.table['id'], self.table['name'])
        while not finished:
            request = self.client.request_records(
                table_id=self.table['id'],
                field_ids=sorted([field['id'] for field in self.fields]),
                date_modified_id=date_modified_id,
                last_date_modified=self.get_starting_replication_key_value(context),
                skip=skip,
            )

            metadata = request.json()['metadata']
            total_records = metadata['totalRecords']
            skip = skip + metadata['numRecords']
            self.logger.info('Retrieved %s/%s records', skip, total_records)

            finished = skip >= total_records
            yield from self.process_record_data(request.json()['data'])

    def process_record_data(self, data: List) -> Iterable[dict]:
        field_lookup = self._field_lookup()
        for record in data:
            processed = {
                field_lookup[int(field_id)]: json_clean_num(value['value'])
                for field_id, value in record.items()
            }
            yield processed

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            yield record
