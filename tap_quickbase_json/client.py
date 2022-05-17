"""REST client handling, including QuickbaseJsonStream base class."""
import re

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from memoization import cached

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIKeyAuthenticator


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


def normalize_name(name: str) -> str:
    name = str(name).strip()
    name = re.sub(r'\s+', '_', name).lower()
    name = re.sub(r'[^a-z0-9_]', '_', name)
    return name


class QuickbaseConn():
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
            if table['name'] == 'WO Tags'
        ]







class QuickbaseJsonStream(RESTStream):
    """quickbase-json stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["qb_url"]

    # TODO: WTF?
    records_jsonpath = "$[*]"  # Or override `parse_response`.
    next_page_token_jsonpath = "$.next_page"  # Or override `get_next_page_token`.

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object."""
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="Authorization",
            value=f"QB-USER-TOKEN {self.config.get('qb_user_token')}",
            location="header"
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {
            "QB-Realm-Hostname": self.config.get("qb_url")
        }

        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        # TODO: If pagination is required, return a token which can be used to get the
        #       next page. If this is the final page, return "None" to end the
        #       pagination loop.
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        else:
            next_page_token = response.headers.get("X-Next-Page", None)

        return next_page_token

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = {
            'appId': self.config['qb_appid']
        }
        return params

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).
        """
        # TODO: Delete this method if no payload is required. (Most REST APIs.)
        return None

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        # TODO: Delete this method if not needed.
        return row
