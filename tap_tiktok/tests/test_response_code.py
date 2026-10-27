"""TikTok returns API errors with HTTP 200 and a non-zero `code` in the body."""

import json

import pytest
import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from tap_tiktok.tap import TapTikTok


def _response(code: int) -> requests.Response:
    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps({"code": code, "message": "msg", "data": {}}).encode()
    response.request = requests.Request("GET", "https://business-api.tiktok.com/open_api/v1.3/campaign/get/").prepare()
    return response


@pytest.fixture
def stream():
    tap = TapTikTok(
        config={"access_token": "x", "advertiser_ids": ["1"], "start_date": "2026-01-01T00:00:00Z"},
        parse_env_config=False,
    )
    return tap.streams["campaigns"]


def test_ok_code_passes(stream):
    stream.validate_response(_response(0))


def test_rate_limit_is_retried(stream):
    with pytest.raises(RetriableAPIError, match="40100"):
        stream.validate_response(_response(40100))


def test_other_codes_fail(stream):
    with pytest.raises(FatalAPIError, match="40002"):
        stream.validate_response(_response(40002))
