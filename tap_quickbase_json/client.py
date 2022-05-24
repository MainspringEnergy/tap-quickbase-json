"""Quickbase client handling."""
import logging
import time
from functools import lru_cache
from typing import Any, Dict, List, Mapping, Optional

import requests

from tap_quickbase_json import normalize_name


def raise_for_status_w_message(response: requests.Response) -> None:
    """Raises on bad requests and prints error messages."""
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        msg = response.text
        if "quota" in response.text.lower():
            msg += (
                ' x-ratelimit-remaining: {response.headers.get("x-ratelimit-remaining", None)},'
                ' x-ratelimit-reset: {response.headers.get("x-ratelimit-reset", None)}'
            )
        raise requests.exceptions.HTTPError(msg) from err


def wait_for_rate_limit(response: requests.Response) -> None:
    """Waits some time if rate limiting is applied."""
    if int(response.headers.get("x-ratelimit-remaining", 0)) <= 2:
        time.sleep(int(response.headers.get("x-ratelimit-reset", 0)) * 0.001)


class QuickbaseClient:
    """QuickbaseClient class for interface with the API."""

    def __init__(self, config: Mapping[str, Any], logger: Optional[logging.Logger] = None):
        """Init Quickbase client.

        Args:
            config: Tap configuration parameters.
            logger: logger to be used for printing useful info.

        """
        self.config = config
        self.logger = logger or logging.getLogger()

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

    def request_tables(self) -> requests.Response:
        """Returns a response object containing results of a GET tables request."""
        params = {"appId": self.config["qb_appid"]}

        response = requests.get(
            "https://api.quickbase.com/v1/tables",
            params=params,
            headers=self.http_headers,
        )
        raise_for_status_w_message(response)
        wait_for_rate_limit(response)
        return response

    def get_tables(self) -> List[Dict]:
        """Gets a list of tables available to the API."""
        tables = self.request_tables().json()

        return [
            {
                "name": normalize_name(table["name"]),
                "label": table["name"],
                "id": table["id"],
            }
            for table in tables
        ]

    @lru_cache
    def request_fields(self, table_id: str) -> requests.Response:
        """Returns a response object containing all fields for a given table id."""
        params = {"tableId": table_id, "includeFieldPerms": False}

        response = requests.get(
            "https://api.quickbase.com/v1/fields",
            params=params,
            headers=self.http_headers,
        )
        raise_for_status_w_message(response)
        wait_for_rate_limit(response)
        return response

    def request_records(
        self,
        table_id: str,
        field_ids: list,
        date_modified_id: int,
        last_date_modified: str = "1970-01-01",
        skip: int = 0,
    ) -> requests.Response:
        """Request records for a table.

        Args:
            table_id: Id of the table.
            field_ids: List of field ids.
            date_modified_id: Field id of the date modified field.
            last_date_modified: Only returns records modified on or after this date (YYYY-MM-DD).
            skip: Number of records to skip initially.

        """
        body = {
            "from": table_id,
            "select": field_ids,
            "options": {
                "skip": skip,
            },
            # Quickbase weird query language
            #   * https://help.quickbase.com/api-guide/componentsquery.html
            #   * OAF - On or after
            "where": f"{{'{date_modified_id}'.OAF.'{last_date_modified}'}}",
            # Hard-coding the sortBy field to be based on the last modified date
            #  This seems like a standard Quickbase field so it shouldn't need to be configurable
            "sortBy": [
                {
                    "fieldId": date_modified_id,
                    "order": "ASC",
                }
            ],
        }

        self.logger.info("Sending record request to Quickbase: %s", body)

        response = requests.post(
            "https://api.quickbase.com/v1/records/query",
            headers=self.http_headers,
            json=body,
        )
        raise_for_status_w_message(response)
        wait_for_rate_limit(response)
        return response
