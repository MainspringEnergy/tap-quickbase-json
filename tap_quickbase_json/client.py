"""REST client handling, including QuickbaseJsonStream base class."""
import re
import logging
import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from memoization import cached

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import Stream
from singer_sdk import typing as th  # JSON schema typing helpers

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class TooManyKeyFieldsError(BaseException):
    pass


class NoKeyFieldError(BaseException):
    pass


class DateModifiedNotFoundError(BaseException):
    pass


def normalize_name(name: str) -> str:
    name = name.replace('#', ' nbr ')
    name = name.replace('&', ' and ')
    name = name.replace('@', ' at ')
    name = name.replace('*', ' star ')
    name = name.replace('$', ' dollar ')
    name = name.replace('?', ' q ')
    name = str(name).strip()
    name = re.sub(r'\s+', ' ', name).lower()
    name = re.sub(r'[^a-z0-9]+', '_', name)
    return name


def raise_for_status_w_message(request: requests.Request) -> None:
    try:
        request.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise requests.exceptions.HTTPError(request.text) from err



class QuickbaseApi():
    def __init__(self, config: Dict, logger: [None, logging.Logger] = None):
        self.config = config
        self.logger = logger or logging.Logger

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {
            "QB-Realm-Hostname": self.config.get("qb_hostname"),
            "Authorization": f"QB-USER-TOKEN {self.config.get('qb_user_token')}",
        }

        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def request_tables(self) -> requests.Request:
        params = {
            'appId': self.config['qb_appid']
        }

        request = requests.get(
            'https://api.quickbase.com/v1/tables',
            params=params,
            headers=self.http_headers,
        )
        raise_for_status_w_message(request)
        return request

    def get_tables(self) -> dict:
        tables = self.request_tables().json()

        return [
            {
                'name': normalize_name(table['name']),
                'label': table['name'],
                'id': table['id'],
            }
            for table in tables
            # TODO: remove debug
            if table['name'] in [
                    'WO Tags',
                    'Cost Centers',
                    'Approvals',
                    'Assemblies',
            ]
        ]

    @cached
    def request_fields(self, table_id: str) -> requests.Request:
        params = {
            'tableId': table_id,
            'includeFieldPerms': False
        }

        request = requests.get(
            'https://api.quickbase.com/v1/fields',
            params=params,
            headers=self.http_headers
        )
        raise_for_status_w_message(request)
        return request

    def request_records(
        self,
        table_id: str,
        field_ids: list,
        date_modified_id: int,
        last_date_modified: str = '1970-01-01',
        skip: int = 0,
    ) -> requests.Request:

        options = {
            'skip': skip
        }

        #TODO: Doc - What the flark!  Quickbase does not allow you to query on datetimes,
        # only dates.  So we're not going to be able to incrementally stream data at
        # a granularity more than daily.

        body = {
            'from': table_id,
            'select': field_ids,
            'options': options,
            # Quickbase weird query language - https://help.quickbase.com/api-guide/componentsquery.html
            #   OAF - On or after
            'where': f"{{'{date_modified_id}'.OAF.'{last_date_modified}'}}",
            # Hard-coding the sortBy field to be based on the last modified date
            #  This seems like a standard Quickbase field so I don't think it needs to be configurable
            'sortBy': [{
                'fieldId': date_modified_id,
                'order': 'ASC',
            }],
        }

        self.logger.info('Sending record request to Quickbase: %s', body)

        request = requests.post(
            'https://api.quickbase.com/v1/records/query',
            headers=self.http_headers,
            json=body
        )
        raise_for_status_w_message(request)
        return request

class QuickbaseJsonStream(Stream):
    """quickbase-json stream class."""

    def __init__(self, table: dict = None, **kwargs) -> None:
        self.table = table
        super().__init__(**kwargs)
        self.logger = self._tap.logger

    @property
    def api(self) -> QuickbaseApi:
        if hasattr(self, '_api'):
            return self._api
        self._api = QuickbaseApi(self.config, logger=self.logger)
        return self._api

    @api.setter
    def api(self, value) -> None:
        self._api = value

    @property
    def fields(self) -> dict:
        if hasattr(self, '_fields'):
            return self._fields

        api_fields = self.api.request_fields(self.table['id']).json()
        self._fields = [
            {
                'id': field['id'],
                'name': normalize_name(field['label']),
                'fieldType': field['fieldType'],
            }
            for field in api_fields
            # TODO: remove debuging
#            if field['id'] <= 5
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
            'user': th.ObjectType(),
            'multiuser': th.ArrayType(th.ObjectType()),
            'file': th.ObjectType,
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
            request = self.api.request_records(
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
                field_lookup[int(field_id)]: value['value']
                for field_id, value in record.items()
                # TODO: remove debug
#                if int(field_id) <= 5
            }
            yield processed

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            yield record
