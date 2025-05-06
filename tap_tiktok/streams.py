"""Stream type classes for tap-tiktok."""
import copy
import json
import datetime
import dateutil
import requests
from typing import Any, Dict, Iterable, Optional
from urllib.parse import urlparse
from urllib.parse import parse_qs

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_tiktok.client import TikTokStream
from tap_tiktok.client import TikTokReportsStream


class AdAccountsStream(TikTokStream):
    name = "ad_accounts"
    path = "/advertiser/info/"
    primary_keys = ["advertiser_id"]
    records_jsonpath = "$.data.list[*]"
    replication_key = None
    schema = th.PropertiesList(
        th.Property("advertiser_id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("company", th.StringType),
        th.Property("contacter", th.StringType),
        th.Property("promotion_area", th.StringType),
        th.Property("balance", th.NumberType),
        th.Property("license_province", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("promotion_center_city", th.StringType),
        th.Property("display_timezone", th.StringType),
        th.Property("email", th.StringType),
        th.Property("telephone_number", th.StringType),
        th.Property("cellphone_number", th.StringType),
        th.Property("language", th.StringType),
        th.Property("industry", th.StringType),
        th.Property("create_time", th.IntegerType),
        th.Property("address", th.StringType),
        th.Property("role", th.StringType),
        th.Property("rejection_reason", th.StringType),
        th.Property("promotion_center_province", th.StringType),
        th.Property("timezone", th.StringType),
        th.Property("license_url", th.StringType),
        th.Property("country", th.StringType),
        th.Property("status", th.StringType),
        th.Property("brand", th.StringType),
        th.Property("license_city", th.StringType),
        th.Property("description", th.StringType),
        th.Property("license_no", th.StringType),
        th.Property("owner_bc_id", th.StringType),
        th.Property("advertiser_account_type", th.StringType),
    ).to_dict()
    schema["additionalProperties"] = True

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params: dict = {"advertiser_ids": "{advertiser_ids}".format(advertiser_ids=json.dumps([str(self.config["advertiser_id"])]))}
        if next_page_token:
            params["page"] = next_page_token
        return params

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        return None


class CampaignsStream(TikTokStream):
    name = "campaigns"
    path = "/campaign/get/"
    primary_keys = ["campaign_id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("campaign_id", th.StringType),
        th.Property("campaign_name", th.StringType),
        th.Property("campaign_type", th.StringType),
        th.Property("objective", th.StringType),
        th.Property("objective_type", th.StringType),
        th.Property("advertiser_id", th.StringType),
        th.Property("budget_mode", th.StringType),
        th.Property("deep_bid_type", th.StringType),
        th.Property("status", th.StringType),
        th.Property("create_time", th.DateTimeType),
        th.Property("modify_time", th.DateTimeType),
        th.Property("opt_status", th.StringType),
        th.Property("is_new_structure", th.BooleanType),
        th.Property("split_test_variable", th.StringType),
        th.Property("budget", th.NumberType),
        th.Property("roas_bid", th.NumberType),
    ).to_dict()


class AdGroupsStream(TikTokStream):
    name = "ad_groups"
    path = "/adgroup/get/"
    primary_keys = ["adgroup_id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("adgroup_id", th.StringType),
        th.Property("age", th.ArrayType(th.StringType)),
        th.Property("display_mode", th.StringType),
        th.Property("operation_system", th.ArrayType(th.StringType)),
        th.Property("creative_material_mode", th.StringType),
        th.Property("excluded_audience", th.ArrayType(th.IntegerType)),
        th.Property("optimize_goal", th.StringType),
        th.Property("budget_mode", th.StringType),
        th.Property("brand_safety_partner", th.StringType),
        th.Property("enable_inventory_filter", th.BooleanType),
        th.Property("conversion_bid", th.NumberType),
        th.Property("interest_keywords", th.ArrayType(th.NumberType)),
        th.Property("scheduled_budget", th.NumberType),
        th.Property("deep_external_action", th.StringType),
        th.Property("modify_time", th.DateTimeType),
        th.Property("placement_type", th.StringType),
        th.Property("is_comment_disable", th.IntegerType),
        th.Property("audience", th.ArrayType(th.IntegerType)),
        th.Property("schedule_end_time", th.DateTimeType),
        th.Property("brand_safety", th.StringType),
        th.Property("daily_retention_ratio", th.NumberType),
        th.Property("languages", th.ArrayType(th.StringType)),
        th.Property("rf_predict_cpr", th.NumberType),
        th.Property("deep_bid_type", th.StringType),
        th.Property("skip_learning_phase", th.BooleanType),
        th.Property("rf_predict_frequency", th.NumberType),
        th.Property("external_type", th.StringType),
        th.Property("gender", th.StringType),
        th.Property("category", th.IntegerType),
        th.Property("pixel_id", th.StringType),
        th.Property("frequency_schedule", th.IntegerType),
        th.Property("keywords", th.ArrayType(th.NumberType)),
        th.Property("enable_search_result", th.BooleanType),
        th.Property("bid", th.NumberType),
        th.Property("app_type", th.StringType),
        th.Property("app_download_url", th.StringType),
        th.Property("create_time", th.DateTimeType),
        th.Property("frequency", th.IntegerType),
        th.Property("include_custom_actions", th.ArrayType(th.StringType)),
        th.Property("ios14_quota_type", th.StringType),
        th.Property("bid_type", th.StringType),
        th.Property("app_id", th.StringType),
        th.Property("split_test_adgroup_ids", th.ArrayType(th.StringType)),
        th.Property("advertiser_id", th.StringType),
        th.Property("buy_reach", th.IntegerType),
        th.Property("dayparting", th.StringType),
        th.Property("buy_impression", th.IntegerType),
        th.Property("automated_targeting", th.StringType),
        th.Property("pacing", th.StringType),
        th.Property("statistic_type", th.StringType),
        th.Property("is_hfss", th.BooleanType),
        th.Property("conversion_id", th.IntegerType),
        th.Property("campaign_name", th.StringType),
        th.Property("rf_buy_type", th.StringType),
        th.Property("device_models", th.ArrayType(th.IntegerType)),
        th.Property("campaign_id", th.StringType),
        th.Property("device_price", th.ArrayType(th.NumberType)),
        th.Property("schedule_start_time", th.DateTimeType),
        th.Property("placement", th.ArrayType(th.StringType)),
        th.Property("opt_status", th.StringType),
        th.Property("status", th.StringType),
        th.Property("external_action", th.StringType),
        th.Property("conversion_window", th.IntegerType),
        th.Property("adgroup_name", th.StringType),
        th.Property("billing_event", th.StringType),
        th.Property("location", th.ArrayType(th.IntegerType)),
        th.Property("budget", th.NumberType),
        th.Property("video_download", th.StringType),
        th.Property("deep_cpabid", th.NumberType),
        th.Property("is_share_disable", th.BooleanType),
        th.Property("feed_type", th.StringType),
        th.Property("schedule_infos", th.StringType),
        th.Property("exclude_custom_actions", th.ArrayType(th.StringType)),
        th.Property("action_v2", th.ArrayType(
            th.ObjectType(
                th.Property("action_categories", th.ArrayType(th.IntegerType)),
                th.Property("action_period", th.IntegerType),
                th.Property("action_scene", th.StringType),
                th.Property("user_actions", th.ArrayType(th.StringType)),
            ))
        ),
        th.Property("is_new_structure", th.BooleanType),
        th.Property("schedule_type", th.StringType),
        th.Property("interest_category_v2", th.ArrayType(th.IntegerType)),
        th.Property("connection_type", th.ArrayType(th.StringType)),
    ).to_dict()


class AdsStream(TikTokStream):
    name = "ads"
    path = "/ad/get/"
    primary_keys = ["ad_id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("ad_id", th.StringType),
        th.Property("ad_format", th.StringType),
        th.Property("campaign_name", th.StringType),
        th.Property("external_action", th.StringType),
        th.Property("identity_type", th.StringType),
        th.Property("campaign_id", th.StringType),
        th.Property("impression_tracking_url", th.StringType),
        th.Property("brand_safety_postbid_partner", th.StringType),
        th.Property("ad_name", th.StringType),
        th.Property("avatar_icon_web_uri", th.StringType),
        th.Property("ad_texts", th.StringType),
        th.Property("vast_moat", th.BooleanType),
        th.Property("is_creative_authorized", th.BooleanType),
        th.Property("adgroup_id", th.StringType),
        th.Property("image_ids", th.ArrayType(th.StringType)),
        th.Property("product_ids", th.ArrayType(th.IntegerType)),
        th.Property("call_to_action_id", th.StringType),
        th.Property("video_id", th.StringType),
        th.Property("adgroup_name", th.StringType),
        th.Property("advertiser_id", th.StringType),
        th.Property("open_url_type", th.StringType),
        th.Property("creative_type", th.StringType),
        th.Property("landing_page_url", th.StringType),
        th.Property("modify_time", th.DateTimeType),
        th.Property("profile_image", th.StringType),
        th.Property("call_to_action", th.StringType),
        th.Property("identity_id", th.StringType),
        th.Property("page_id", th.NumberType),
        th.Property("display_name", th.StringType),
        th.Property("opt_status", th.StringType),
        th.Property("playable_url", th.StringType),
        th.Property("is_new_structure", th.BooleanType),
        th.Property("click_tracking_url", th.StringType),
        th.Property("ad_text", th.StringType),
        th.Property("landing_page_urls", th.StringType),
        th.Property("is_aco", th.BooleanType),
        th.Property("open_url", th.StringType),
        th.Property("app_name", th.StringType),
        th.Property("status", th.StringType),
        th.Property("create_time", th.DateTimeType),
        th.Property("card_id", th.StringType),
        th.Property("fallback_type", th.StringType),
    ).to_dict()


DATE_FORMAT = "%Y-%m-%d"
STEP_NUM_DAYS = 30


class AdsMetricsByDayStream(TikTokReportsStream):
    tiktok_metrics = []
    data_level = "AUCTION_AD"
    dimensions = ["ad_id", "stat_time_day"]

    status_field = "ad_status"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        if isinstance(next_page_token, dict) and next_page_token["start_date"] is not None:
            start_date = datetime.datetime.strptime(next_page_token["start_date"], DATE_FORMAT)
        else:
            start_date = self.get_starting_timestamp(context)

            # picking up where we left off on the last run (or first run), adjust for lookback if set
            lookback_window = self.config["lookback"]
            if lookback_window > 0:
                # if lookback is configured, we want to refetch data for the entire lookback window
                # (or as far back as the configured start date, whichever is the most recent date)
                start_date = max(
                    min(start_date, datetime.datetime.now(tz=start_date.tzinfo) - datetime.timedelta(days=lookback_window)),
                    dateutil.parser.isoparse(self.config["start_date"]),
                )

        yesterday = datetime.datetime.now(tz=start_date.tzinfo) - datetime.timedelta(days=1)
        end_date = min(start_date + datetime.timedelta(days=STEP_NUM_DAYS), yesterday)
        params: dict = {
            "page_size": 10,
            "advertiser_id": self.config.get("advertiser_id"),
            "service_type": "AUCTION",
            "report_type": "BASIC",
            "data_level": self.data_level,
            "dimensions": json.dumps(self.dimensions),
            "metrics": json.dumps(self.tiktok_metrics),
            "start_date": start_date.strftime(DATE_FORMAT),
            "end_date": end_date.strftime(DATE_FORMAT),
            "filtering": json.dumps([
                {
                    'field_name': self.status_field,
                    'filter_type': 'IN',
                    'filter_value': json.dumps(["STATUS_ALL" if self.config.get("include_deleted") else "STATUS_NOT_DELETE"]),
                }
            ])
        }
        if next_page_token:
            params["page"] = next_page_token["page"]
        
        return params

    @staticmethod
    def _get_page_info(json_path, json):
        page_matches = extract_jsonpath(json_path, json)
        return next(iter(page_matches), None)

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        current_page = self._get_page_info("$.data.page_info.page", response.json()) or 0
        total_pages = self._get_page_info("$.data.page_info.total_page", response.json()) or 0
        start_date = datetime.datetime.strptime(parse_qs(urlparse(response.request.url).query)['start_date'][0],DATE_FORMAT)
        yesterday = datetime.datetime.now(tz=start_date.tzinfo) - datetime.timedelta(days=1)
        end_date = datetime.datetime.strptime(parse_qs(urlparse(response.request.url).query)['end_date'][0], DATE_FORMAT)
        if current_page < total_pages:
            return {
                "page": current_page + 1,
                "start_date": previous_token["start_date"] if previous_token else None
            }
        elif end_date.date() < yesterday.date():
            return {
                "page": 1,
                "start_date": min(end_date + datetime.timedelta(days=1), yesterday).strftime(DATE_FORMAT)
            }
        return None


class CampaignMetricsByDayStream(AdsMetricsByDayStream):
    data_level = "AUCTION_CAMPAIGN"
    dimensions = ["campaign_id", "stat_time_day"]
    status_field = "campaign_status"

ATTRIBUTE_METRICS = [
    "campaign_name", "objective_type", "campaign_id", "adgroup_name", "placement", "adgroup_id", "ad_name", "ad_text",
    "tt_app_id", "tt_app_name", "mobile_app_id", "promotion_type", "dpa_target_audience_type"
]


class AdsAttributeMetricsStream(AdsMetricsByDayStream):
    name = "ads_attribute_metrics"
    tiktok_metrics = ATTRIBUTE_METRICS
    path = "/"
    primary_keys = ["ad_id"]
    replication_key = None
    properties = [
        th.Property("ad_id", th.StringType),
    ]
    properties += [th.Property(metric, th.StringType if metric in ["campaign_id", "adgroup_id"] else th.StringType) for metric in ATTRIBUTE_METRICS]
    schema = th.PropertiesList(*properties).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {
            "page_size": 10,
            "advertiser_id": self.config.get("advertiser_id"),
            "service_type": "AUCTION",
            "report_type": "BASIC",
            "data_level": "AUCTION_AD",
            "dimensions": json.dumps(["ad_id", "stat_time_day"]),
            "metrics": json.dumps(self.tiktok_metrics)
        }
        if next_page_token:
            params["page"] = next_page_token["page"]
        return params

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        current_page = self._get_page_info("$.data.page_info.page", response.json()) or 0
        total_pages = self._get_page_info("$.data.page_info.total_page", response.json()) or 0
        if current_page < total_pages:
            return {"page": current_page + 1}
        return None


class CampaignsAttributeMetricsStream(CampaignMetricsByDayStream):
    name = "campaigns_attribute_metrics"
    status_field = "campaign_status"
    tiktok_metrics = ATTRIBUTE_METRICS
    path = "/"
    primary_keys = ["campaign_id"]
    replication_key = None
    properties = [
        th.Property("campaign_id", th.IntegerType),
    ]
    properties += [th.Property(metric, th.StringType if metric in ["campaign_id", "adgroup_id"] else th.StringType) for metric in ATTRIBUTE_METRICS]
    schema = th.PropertiesList(*properties).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {
            "page_size": 10,
            "advertiser_id": self.config.get("advertiser_id"),
            "service_type": "AUCTION",
            "report_type": "BASIC",
            "data_level": "AUCTION_CAMPAIGN",
            "dimensions": json.dumps(["campaign_id", "stat_time_day"]),
            "metrics": json.dumps(self.tiktok_metrics)
        }
        if next_page_token:
            params["page"] = next_page_token["page"]
        return params

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        current_page = self._get_page_info("$.data.page_info.page", response.json()) or 0
        total_pages = self._get_page_info("$.data.page_info.total_page", response.json()) or 0
        if current_page < total_pages:
            return {"page": current_page + 1}
        return None


BASIC_DATA_METRICS = [
    "spend",
    # "cash_spend", "voucher_spend", # unsupported
    "cpc", "cpm", "impressions", "clicks", "ctr", "reach", "cost_per_1000_reached",
    "conversion", "cost_per_conversion", "conversion_rate", "real_time_conversion", "real_time_cost_per_conversion",
    "real_time_conversion_rate", "result", "cost_per_result", "result_rate", "real_time_result", "real_time_cost_per_result",
    "real_time_result_rate", "secondary_goal_result", "cost_per_secondary_goal_result", "secondary_goal_result_rate",
    "frequency"
]


class AdsBasicDataMetricsByDayStream(AdsMetricsByDayStream):
    name = "ads_basic_data_metrics_by_day"
    tiktok_metrics = BASIC_DATA_METRICS
    path = "/"
    primary_keys = ["ad_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("ad_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in BASIC_DATA_METRICS]
    schema = th.PropertiesList(*properties).to_dict()


class CampaignsBasicDataMetricsByDayStream(CampaignMetricsByDayStream):
    name = "campaigns_basic_data_metrics_by_day"
    status_field = "campaign_status"
    tiktok_metrics = BASIC_DATA_METRICS
    path = "/"
    primary_keys = ["campaign_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("campaign_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in BASIC_DATA_METRICS]
    schema = th.PropertiesList(*properties).to_dict()


VIDEO_PLAY_METRICS = [
    "video_play_actions", "video_watched_2s", "video_watched_6s", "average_video_play", "average_video_play_per_user",
    "video_views_p25", "video_views_p50", "video_views_p75", "video_views_p100"
]


class AdsVideoPlayMetricsByDayStream(AdsMetricsByDayStream):
    name = "ads_video_play_metrics_by_day"
    tiktok_metrics = VIDEO_PLAY_METRICS
    path = "/"
    primary_keys = ["ad_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("ad_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in VIDEO_PLAY_METRICS]
    schema = th.PropertiesList(*properties).to_dict()


class CampaignsVideoPlayMetricsByDayStream(CampaignMetricsByDayStream):
    name = "campaigns_video_play_metrics_by_day"
    status_field = "campaign_status"
    tiktok_metrics = VIDEO_PLAY_METRICS
    path = "/"
    primary_keys = ["campaign_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("campaign_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in VIDEO_PLAY_METRICS]
    schema = th.PropertiesList(*properties).to_dict()


ENGAGEMENT_METRICS = [
    "profile_visits", "profile_visits_rate", "likes", "comments", "shares", "follows", "clicks_on_music_disc"
]


class AdsEngagementMetricsByDayStream(AdsMetricsByDayStream):
    name = "ads_engagement_metrics_by_day"
    tiktok_metrics = ENGAGEMENT_METRICS
    path = "/"
    primary_keys = ["ad_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("ad_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in ENGAGEMENT_METRICS]
    schema = th.PropertiesList(*properties).to_dict()


class CampaignsEngagementMetricsByDayStream(CampaignMetricsByDayStream):
    name = "campaigns_engagement_metrics_by_day"
    status_field = "campaign_status"
    tiktok_metrics = ENGAGEMENT_METRICS
    path = "/"
    primary_keys = ["campaign_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("campaign_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in ENGAGEMENT_METRICS]
    schema = th.PropertiesList(*properties).to_dict()


ATTRIBUTION_METRICS = [
    "vta_app_install", "vta_conversion", "cost_per_vta_conversion", "vta_registration", "cost_per_vta_registration",
    "vta_purchase", "cost_per_vta_purchase", "cta_app_install", "cta_conversion", "cost_per_cta_conversion",
    "cta_registration", "cost_per_cta_registration", "cta_purchase", "cost_per_cta_purchase"
]


class AdsAttributionMetricsByDayStream(AdsMetricsByDayStream):
    name = "ads_attribution_metrics_by_day"
    tiktok_metrics = ATTRIBUTION_METRICS
    path = "/"
    primary_keys = ["ad_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("ad_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in ATTRIBUTION_METRICS]
    schema = th.PropertiesList(*properties).to_dict()


class CampaignsAttributionMetricsByDayStream(CampaignMetricsByDayStream):
    name = "campaigns_attribution_metrics_by_day"
    status_field = "campaign_status"
    tiktok_metrics = ATTRIBUTION_METRICS
    path = "/"
    primary_keys = ["campaign_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("campaign_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in ATTRIBUTION_METRICS]
    schema = th.PropertiesList(*properties).to_dict()


PAGE_EVENT_METRICS = [
    "complete_payment_roas",  # Complete Payment ROAS
    "complete_payment", "cost_per_complete_payment", "complete_payment_rate", "value_per_complete_payment", "total_complete_payment_rate",  # Complete Payment
    "page_browse_view", "cost_per_page_browse_view", "page_browse_view_rate", "total_page_browse_view_value", "value_per_page_browse_view",  # Page View
    "button_click", "cost_per_button_click", "button_click_rate", "value_per_button_click", "total_button_click_value",  # Click Button
    "online_consult", "cost_per_online_consult", "online_consult_rate", "value_per_online_consult", "total_online_consult_value",  # Contact
    "user_registration", "cost_per_user_registration", "user_registration_rate", "value_per_user_registration", "total_user_registration_value",  # Complete Registration
    "product_details_page_browse", "cost_per_product_details_page_browse", "product_details_page_browse_rate", "value_per_product_details_page_browse", "total_product_details_page_browse_value",  # View Content
    "web_event_add_to_cart", "cost_per_web_event_add_to_cart", "web_event_add_to_cart_rate", "value_per_web_event_add_to_cart", "total_web_event_add_to_cart_value",  # Add to Cart
    "on_web_order", "cost_per_on_web_order", "on_web_order_rate", "value_per_on_web_order", "total_on_web_order_value",  # Place an Order
    "initiate_checkout", "cost_per_initiate_checkout", "initiate_checkout_rate", "value_per_initiate_checkout", "total_initiate_checkout_value",  # Initiate Checkout
    "add_billing", "cost_per_add_billing", "add_billing_rate", "value_per_add_billing", "total_add_billing_value",  # Add Payment Info
    "page_event_search", "cost_per_page_event_search", "page_event_search_rate", "value_per_page_event_search", "total_page_event_search_value",  # Search
    "form", "cost_per_form", "form_rate", "value_per_form", "total_form_value",  # Submit Form
    "download_start", "cost_per_download_start", "download_start_rate", "value_per_download_start", "total_download_start_value",  # Download
    "on_web_add_to_wishlist", "cost_per_on_web_add_to_wishlist", "on_web_add_to_wishlist_per_click", "value_per_on_web_add_to_wishlist", "total_on_web_add_to_wishlist_value",  # Add to Wishlist
    "on_web_subscribe", "cost_per_on_web_subscribe", "on_web_subscribe_per_click", "value_per_on_web_subscribe", "total_on_web_subscribe_value"  # Subscribe
]


class AdsPageEventMetricsByDayStream(AdsMetricsByDayStream):
    name = "ads_page_event_metrics_by_day"
    tiktok_metrics = PAGE_EVENT_METRICS
    path = "/"
    primary_keys = ["ad_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("ad_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in PAGE_EVENT_METRICS]
    schema = th.PropertiesList(*properties).to_dict()


class CampaignsPageEventMetricsByDayStream(CampaignMetricsByDayStream):
    name = "campaigns_page_event_metrics_by_day"
    status_field = "campaign_status"
    tiktok_metrics = PAGE_EVENT_METRICS
    path = "/"
    primary_keys = ["campaign_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("campaign_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in PAGE_EVENT_METRICS]
    schema = th.PropertiesList(*properties).to_dict()


IN_APP_EVENT_METRICS = [
    "real_time_app_install", "real_time_app_install_cost",  # Real-time App Install
    "app_install", "cost_per_app_install",  # App Install
    "registration", "cost_per_registration", "registration_rate", "total_registration", "cost_per_total_registration",  # Registration
    "purchase", "cost_per_purchase", "purchase_rate", "total_purchase", "cost_per_total_purchase", "value_per_total_purchase", "total_purchase_value", "total_active_pay_roas",  # Purchase
    "app_event_add_to_cart", "cost_per_app_event_add_to_cart", "app_event_add_to_cart_rate", "total_app_event_add_to_cart", "cost_per_total_app_event_add_to_cart", "value_per_total_app_event_add_to_cart", "total_app_event_add_to_cart_value",  # Add to Cart
    "checkout", "cost_per_checkout", "checkout_rate", "total_checkout", "cost_per_total_checkout", "value_per_checkout", "total_checkout_value",  # Checkout
    "view_content", "cost_per_view_content", "view_content_rate", "total_view_content", "cost_per_total_view_content", "value_per_total_view_content", "total_view_content_value",  # View Content
    "next_day_open", "cost_per_next_day_open", "next_day_open_rate", "total_next_day_open", "cost_per_total_next_day_open",  # Day 2 Retention
    "add_payment_info", "cost_per_add_payment_info", "add_payment_info_rate", "total_add_payment_info", "cost_total_add_payment_info",  # Add Payment Info
    "add_to_wishlist", "cost_per_add_to_wishlist", "add_to_wishlist_rate", "total_add_to_wishlist", "cost_per_total_add_to_wishlist", "value_per_total_add_to_wishlist", "total_add_to_wishlist_value",  # Add to Wishlist
    "launch_app", "cost_per_launch_app", "launch_app_rate", "total_launch_app", "cost_per_total_launch_app",  # Launch App
    "complete_tutorial", "cost_per_complete_tutorial", "complete_tutorial_rate", "total_complete_tutorial", "cost_per_total_complete_tutorial", "value_per_total_complete_tutorial", "total_complete_tutorial_value",  # Complete Tutorial
    "create_group", "cost_per_create_group", "create_group_rate", "total_create_group", "cost_per_total_create_group", "value_per_total_create_group", "total_create_group_value",  # Create Group
    "join_group", "cost_per_join_group", "join_group_rate", "total_join_group", "cost_per_total_join_group", "value_per_total_join_group", "total_join_group_value",  # Join Group
    "create_gamerole", "cost_per_create_gamerole", "create_gamerole_rate", "total_create_gamerole", "cost_per_total_create_gamerole", "value_per_total_create_gamerole", "total_create_gamerole_value",  # Create Role
    "spend_credits", "cost_per_spend_credits", "spend_credits_rate", "total_spend_credits", "cost_per_total_spend_credits", "value_per_total_spend_credits", "total_spend_credits_value",  # Spend Credit
    "achieve_level", "cost_per_achieve_level", "achieve_level_rate", "total_achieve_level", "cost_per_total_achieve_level", "value_per_total_achieve_level", "total_achieve_level_value",  # Achieve Level
    "unlock_achievement", "cost_per_unlock_achievement", "unlock_achievement_rate", "total_unlock_achievement", "cost_per_total_unlock_achievement", "value_per_total_unlock_achievement", "total_unlock_achievement_value",  # Unlock Achievement
    "sales_lead", "cost_per_sales_lead", "sales_lead_rate", "total_sales_lead", "cost_per_total_sales_lead", "value_per_total_sales_lead", "total_sales_lead_value",  # Generate Lead
    "in_app_ad_click", "cost_per_in_app_ad_click", "in_app_ad_click_rate", "total_in_app_ad_click", "cost_per_total_in_app_ad_click", "value_per_total_in_app_ad_click", "total_in_app_ad_click_value",  # In-App Ad Click
    "in_app_ad_impr", "cost_per_in_app_ad_impr", "in_app_ad_impr_rate", "total_in_app_ad_impr", "cost_per_total_in_app_ad_impr", "value_per_total_in_app_ad_impr", "total_in_app_ad_impr_value",  # In-App Ad Impression
    "loan_apply", "cost_per_loan_apply", "loan_apply_rate", "total_loan_apply", "cost_per_total_loan_apply",  # Loan Apply
    "loan_credit", "cost_per_loan_credit", "loan_credit_rate", "total_loan_credit", "cost_per_total_loan_credit",  # Loan Approval
    "loan_disbursement", "cost_per_loan_disbursement", "loan_disbursement_rate", "total_loan_disbursement", "cost_per_total_loan_disbursement",  # Loan Disbursal
    "login", "cost_per_login", "login_rate", "total_login", "cost_per_total_login",  # Login
    "ratings", "cost_per_ratings", "ratings_rate", "total_ratings", "cost_per_total_ratings", "value_per_total_ratings", "total_ratings_value",  # Rate
    "search", "cost_per_search", "search_rate", "total_search", "cost_per_total_search",  # Search
    "start_trial", "cost_per_start_trial", "start_trial_rate", "total_start_trial", "cost_per_total_start_trial",  # Start Trial
    "subscribe", "cost_per_subscribe", "subscribe_rate", "total_subscribe", "cost_per_total_subscribe", "value_per_total_subscribe", "total_subscribe_value"  # Subscribe
]


class AdsInAppEventMetricsByDayStream(AdsMetricsByDayStream):
    name = "ads_in_app_event_metrics_by_day"
    path = "/"
    primary_keys = ["ad_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("ad_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in IN_APP_EVENT_METRICS]
    schema = th.PropertiesList(*properties).to_dict()

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self._request)

        while not finished:
            rows = {}
            for i in range(2):
                chunk_size = len(IN_APP_EVENT_METRICS) // 2
                self.tiktok_metrics = IN_APP_EVENT_METRICS[chunk_size * i: (i + 1) * chunk_size]
                prepared_request = self.prepare_request(
                    context, next_page_token=next_page_token
                )
                resp = decorated_request(prepared_request, context)
                for row in self.parse_response(resp):
                    primary_key = tuple(row['dimensions'][key] for key in self.primary_keys)
                    if rows.get(primary_key) is None:
                        rows[primary_key] = row
                    else:
                        rows[primary_key]['metrics'] = {**row['metrics'], **rows[primary_key]['metrics']}
            for row in rows.values():
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token


class CampaignsInAppEventMetricsByDayStream(CampaignMetricsByDayStream):
    name = "campaigns_in_app_event_metrics_by_day"
    status_field = "campaign_status"
    path = "/"
    primary_keys = ["campaign_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("campaign_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in IN_APP_EVENT_METRICS]
    schema = th.PropertiesList(*properties).to_dict()

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self._request)

        while not finished:
            rows = {}
            for i in range(2):
                chunk_size = len(IN_APP_EVENT_METRICS) // 2
                self.tiktok_metrics = IN_APP_EVENT_METRICS[chunk_size * i: (i + 1) * chunk_size]
                prepared_request = self.prepare_request(
                    context, next_page_token=next_page_token
                )
                resp = decorated_request(prepared_request, context)
                for row in self.parse_response(resp):
                    primary_key = tuple(row['dimensions'][key] for key in self.primary_keys)
                    if rows.get(primary_key) is None:
                        rows[primary_key] = row
                    else:
                        rows[primary_key]['metrics'] = {**row['metrics'], **rows[primary_key]['metrics']}
            for row in rows.values():
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token
