"""REST client handling, including QuickbaseJsonStream base class."""
import re

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


class QuickbaseApi():
    def __init__(self, config: Dict):
        self.config = config

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

    def request_tables(self) -> dict:
        params = {
            'appId': self.config['qb_appid']
        }

        request = requests.get(
            'https://api.quickbase.com/v1/tables',
            params=params,
            headers=self.http_headers,
        )
        request.raise_for_status()
        return request.json()

    def get_tables(self) -> dict:
        tables = self.request_tables()

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
#                    'Cost Centers',
            ]
        ]

    @cached
    def request_fields(self, table_id: str) -> dict:
        params = {
            'tableId': table_id,
            'includeFieldPerms': False
        }

        request = requests.get(
            'https://api.quickbase.com/v1/fields',
            params=params,
            headers=self.http_headers
        )
        request.raise_for_status()
        return request.json()



class QuickbaseJsonStream(Stream):
    """quickbase-json stream class."""

    def __init__(self, table: dict = None, **kwargs) -> None:
        self.table = table
        super().__init__(**kwargs)

    @property
    def api(self) -> QuickbaseApi:
        if hasattr(self, '_api'):
            return self._api
        self._api = QuickbaseApi(self.config)
        return self._api

    @api.setter
    def api(self, value) -> None:
        self._api = value

    @property
    def fields(self) -> dict:
        if hasattr(self, '_fields'):
            return self._fields

        api_fields = self.api.request_fields(self.table['id'])
        self._fields = [
            {
                'id': field['id'],
                'name': normalize_name(field['label']),
                'fieldType': field['fieldType'],
            }
            for field in api_fields
        ]
        return self._fields

    def field_lookup(self) -> dict:
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
            'duration': th.DurationType,
            'numeric': th.NumberType,
            'percent': th.NumberType,
            'rating': th.NumberType,
            'timestamp': th.DateTimeType,
            'datetime': th.DateTimeType,
            'timeofday': th.TimeType,
            'recordid': th.NumberType,
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


    # TODO: I don't know if I need this
    # @property
    # def replication_key(self) -> str:
    #     # parent Stream class initializes this one to None
    #     if hasattr(self, '_replication_key') and self._replication_key is not None:
    #         return self._replication_key

    #     if 'date_modified' not in [field['name'] for field in self.fields]:
    #         raise DateModifiedNotFoundError(
    #             f'No `date_modified` field found for table {self.table["id"]}'
    #         )

    #     self._replication_key = 'date_modified'
    #     return self._replication_key

    # @replication_key.setter
    # def replication_key(self, value) -> None:
    #     self._replication_key = value



    def get_records(self, partition: Optional[dict] = None) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        yield {'blerg': 'doh'}
