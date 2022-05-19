"""Quickbase client handling."""
import logging
import requests
from memoization import cached

from tap_quickbase_json import normalize_name


def raise_for_status_w_message(request: requests.Request) -> None:
    try:
        request.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise requests.exceptions.HTTPError(request.text) from err


class QuickbaseClient():
    def __init__(self, config: dict, logger: [None, logging.Logger] = None):
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

        body = {
            'from': table_id,
            'select': field_ids,
            'options': {
                'skip': skip
            },
            # Quickbase weird query language
            #   * https://help.quickbase.com/api-guide/componentsquery.html
            #   * OAF - On or after
            'where': f"{{'{date_modified_id}'.OAF.'{last_date_modified}'}}",
            # Hard-coding the sortBy field to be based on the last modified date
            #  This seems like a standard Quickbase field so it shouldn't need to be configurable
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
