"""REST client handling, including TikTokStream base class."""

import json
import requests
from typing import Any, Dict, Iterable, Optional
from urllib.parse import parse_qs, urlparse

from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import LegacyStreamPaginator
from singer_sdk.streams import RESTStream

DATE_FORMAT = "%Y-%m-%d"
RETRIABLE_CODES = {40100, 50000, 50002}


class TikTokPaginator(LegacyStreamPaginator):
    def continue_if_empty(self, response: requests.Response) -> bool:
        return True


class TikTokStream(RESTStream):
    url_base = "https://business-api.tiktok.com/open_api/v1.3"

    records_jsonpath = "$.data.list[*]"

    def get_new_paginator(self) -> TikTokPaginator:
        return TikTokPaginator(self)

    def validate_response(self, response: requests.Response) -> None:
        super().validate_response(response)
        body = response.json()
        code = body.get("code", 0)
        if code == 0:
            return
        url = urlparse(response.request.url)
        advertiser_id = parse_qs(url.query).get("advertiser_id", ["?"])[0]
        msg = f"TikTok API error {code} on {url.path} for advertiser {advertiser_id}: {body.get('message')}"
        if code in RETRIABLE_CODES:
            raise RetriableAPIError(msg, response)
        raise FatalAPIError(msg)

    def _configured_advertiser_ids(self) -> list[str]:
        """Return advertiser IDs from config (supports advertiser_ids or advertiser_id)."""
        advertiser_ids = self.config.get("advertiser_ids")
        if isinstance(advertiser_ids, list) and advertiser_ids:
            return [str(x) for x in advertiser_ids if x is not None and str(x).strip()]

        advertiser_id = self.config.get("advertiser_id")
        if advertiser_id is not None and str(advertiser_id).strip():
            return [str(advertiser_id)]

        raise RuntimeError("Missing advertiser_id(s). Set `advertiser_ids` (list) or `advertiser_id` (string).")

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Run the stream once per advertiser_id (simple multi-account support)."""
        base_ctx = dict(context or {})
        for advertiser_id in self._configured_advertiser_ids():
            local_ctx = dict(base_ctx)
            local_ctx["advertiser_id"] = advertiser_id
            yield from super().get_records(local_ctx)

    def _resolve_advertiser_id(self, context: Optional[dict]) -> str:
        """Prefer context; fall back to first configured id."""
        if context and context.get("advertiser_id"):
            return str(context["advertiser_id"])
        return self._configured_advertiser_ids()[0]

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        headers["Content-Type"] = "application/json"
        headers["Access-Token"] = self.config["access_token"].__str__()
        return headers

    @staticmethod
    def _get_page_info(json_path, json):
        page_matches = extract_jsonpath(json_path, json)
        return next(iter(page_matches), None)

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[Any]) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        current_page = self._get_page_info("$.data.page_info.page", response.json()) or 0
        total_pages = self._get_page_info("$.data.page_info.total_page", response.json()) or 0
        if current_page < total_pages:
            return current_page + 1
        return None

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {"advertiser_id": self._resolve_advertiser_id(context)}
        if next_page_token:
            params["page"] = next_page_token
        params["filtering"] = json.dumps(
            {"primary_status": "STATUS_ALL" if self.config.get("include_deleted") else "STATUS_NOT_DELETE"}
        )
        params["page_size"] = 10
        return params


class TikTokReportsStream(TikTokStream):
    url_base = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"

    records_jsonpath = "$.data.list[*]"
    next_page_token_jsonpath = "$.page_info.page"

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        return {**row["dimensions"], **row["metrics"]}

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[Any]) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        page_matches = extract_jsonpath("$.data.page_info.page", response.json())
        page_match = next(iter(page_matches), None)
        current_page = page_match
        total_pages_matches = extract_jsonpath("$.data.page_info.total_page", response.json())
        total_pages_match = next(iter(total_pages_matches), None)
        total_pages = total_pages_match
        if current_page < total_pages:
            return current_page + 1
        return None
