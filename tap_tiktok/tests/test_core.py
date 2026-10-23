"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_tap_test_class

from tap_tiktok.tap import TapTikTok

SAMPLE_CONFIG = {
    "start_date": (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)
    ).strftime("%Y-%m-%dT00:00:00Z"),
}

TestTapTikTok = get_tap_test_class(
    tap_class=TapTikTok,
    config=SAMPLE_CONFIG,
    parse_env_config=True,
)
