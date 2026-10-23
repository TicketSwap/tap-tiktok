"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import SuiteConfig, get_tap_test_class

from tap_tiktok.tap import TapTikTok

SAMPLE_CONFIG = {
    "start_date": (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)
    ).strftime("%Y-%m-%dT00:00:00Z"),
}

TestTapTikTok = get_tap_test_class(
    tap_class=TapTikTok,
    config=SAMPLE_CONFIG,
    suite_config=SuiteConfig(
        ignore_no_records_for_streams=[
            "ads_attribute_metrics",
            "campaigns_attribute_metrics",
        ],
    ),
    parse_env_config=True,
)
