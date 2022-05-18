"""REST client handling, including QuickbaseJsonStream base class."""
import re

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from memoization import cached

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import Stream
from singer_sdk.authenticators import APIKeyAuthenticator


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


def normalize_name(name: str) -> str:
    name = str(name).strip()
    name = re.sub(r'\s+', '_', name).lower()
    name = re.sub(r'[^a-z0-9_]', '_', name)
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

    @property
    def url_params(self) -> dict:
        """Return a dictionary of values to be used in URL parameterization."""
        params = {
            'appId': self.config['qb_appid']
        }
        return params

    def _request_tables(self) -> dict:
        print(f'params: {self.url_params}')
        print(f'headers: {self.http_headers}')
        request = requests.get(
            'https://api.quickbase.com/v1/tables',
            params=self.url_params,
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
