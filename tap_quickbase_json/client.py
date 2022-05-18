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

    def _request_tables(self) -> dict:
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
        tables = self._request_tables()

        return [
            {
                'name': normalize_name(table['name']),
                'label': table['name'],
                'id': table['id'],
            }
            for table in tables
            # TODO: remove debug
            if table['name'] in ['WO Tags', 'Cost Centers']
        ]

    def _request_fields(self, table_id: str) -> dict:
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

    @cached
    def get_fields(self, table_id: str) -> dict:
        fields = self._request_fields(table_id)
        return {
            field['id']: {
                'name': normalize_name(field['label']),
                'fieldType': field['fieldType']
            }
            for field in fields
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
        }.get(qb_type, th.StringType)

    @cached
    def get_schema(self, table_id) -> dict:
        schema = th.PropertiesList()
        for _id, field in self.get_fields(table_id).items():
            schema.append(
                th.Property(
                    field['name'],
                    self.type_lookup(field['fieldType'])
                )
            )
        return schema.to_dict()



class QuickbaseJsonStream(Stream):
    """quickbase-json stream class."""

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
    def table(self) -> dict:
        if hasattr(self, '_table'):
            return self._table

        self._table = {}
        return self._table

    @table.setter
    def table(self, value) -> None:
        self._table = value

    def get_records(self, partition: Optional[dict] = None) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        yield {'blerg': 'doh'}
