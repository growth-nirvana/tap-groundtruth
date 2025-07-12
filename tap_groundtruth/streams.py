"""Stream type classes for tap-groundtruth."""

from __future__ import annotations

import typing as t
from importlib import resources
import requests
import logging
import datetime

from singer_sdk import typing as th  # JSON Schema typing helpers
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from tap_groundtruth.client import GroundTruthStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.

RESOURCE_API_BASE = "https://ads.groundtruth.com/api"

LOGGER = logging.getLogger("tap_groundtruth")


def is_retryable_response(response):
    # Retry on 5xx or 429
    return response.status_code >= 500 or response.status_code == 429

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True,
)
def get_with_retry(*args, **kwargs):
    response = requests.get(*args, **kwargs)
    if is_retryable_response(response):
        response.raise_for_status()
    return response


class CreativeStatsStream(GroundTruthStream):
    """Stream for creative stats per campaign per day."""

    name = "creative_stats"
    path = None  # Will be set dynamically per campaign
    primary_keys = ["id"]  # Adjust if the real primary key is different
    # replication_key = "date"
    schema = th.PropertiesList(
        th.Property("date", th.StringType),
        th.Property("organization_name", th.StringType),
        th.Property("account_id", th.IntegerType),
        th.Property("account_name", th.StringType),
        th.Property("campaign_id", th.IntegerType),
        th.Property("campaign_name", th.StringType),
        th.Property("adgroup_id", th.IntegerType),
        th.Property("adgroup_name", th.StringType),
        th.Property("creative_id", th.IntegerType),
        th.Property("creative_name", th.StringType),
        th.Property("creative_size", th.StringType),
        th.Property("creative_url", th.StringType),
        th.Property("campaign_start_date", th.StringType),
        th.Property("campaign_end_date", th.StringType),
        th.Property("advertiser_bid_type", th.StringType),
        th.Property("budget_type", th.StringType),
        th.Property("impressions", th.IntegerType),
        th.Property("clicks", th.IntegerType),
        th.Property("daily_reach", th.IntegerType),
        th.Property("ctr", th.NumberType),
        th.Property("cpm", th.NumberType),
        th.Property("cpv", th.NumberType),
        th.Property("visits", th.IntegerType),
        th.Property("video_start", th.IntegerType),
        th.Property("video_first_quartile", th.IntegerType),
        th.Property("video_midpoint", th.IntegerType),
        th.Property("video_third_quartile", th.IntegerType),
        th.Property("video_end", th.IntegerType),
        th.Property("total_sa", th.IntegerType),
        th.Property("coupon", th.IntegerType),
        th.Property("website", th.IntegerType),
        th.Property("moreinfo", th.IntegerType),
        th.Property("directions", th.IntegerType),
        th.Property("click_to_call", th.IntegerType),
        th.Property("sar", th.IntegerType),
        th.Property("spend", th.NumberType),
        th.Property("market", th.StringType),
        th.Property("contract_id", th.StringType),
        th.Property("campaign_budget", th.NumberType),
        th.Property("total_imp_contracted", th.IntegerType),
        th.Property("external_adgroup_id", th.IntegerType),
        th.Property("oh_vst", th.IntegerType),
        th.Property("prj_vst", th.IntegerType),
    ).to_dict()

    def _parse_date(self, date_str):
        """Parse a date string in either ISO 8601 or YYYY-MM-DD format to a date object."""
        try:
            # Try full ISO 8601 format (e.g. '2025-05-01T00:00:00Z')
            return datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ").date()
        except ValueError:
            # Fallback to just date
            return datetime.datetime.strptime(date_str[:10], "%Y-%m-%d").date()

    def get_child_context(self, record, context):
        return None

    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        if self.config.get("start_date"):
            params["start_date"] = self.config["start_date"]
        if self.config.get("end_date"):
            params["end_date"] = self.config["end_date"]
        return params

    def get_records(self, context):
        org_id = self.config["organization_id"]
        accounts_url = f"{RESOURCE_API_BASE}/organizations/{org_id}/accounts"
        headers = self.http_headers.copy()
        headers["Accept"] = "application/json"
        headers["User-Agent"] = "curl/7.79.1"
        headers["Accept-Encoding"] = "gzip, deflate"
        LOGGER.info("Accounts URL: %s", accounts_url)
        LOGGER.info("Request headers: %s", dict(headers))
        response = get_with_retry(accounts_url, headers=headers)
        LOGGER.info("Response status code: %s", response.status_code)
        LOGGER.info("Response headers: %s", dict(response.headers))
        LOGGER.info("Response text (first 500 chars): %s", response.text[:500])
        response.raise_for_status()
        try:
            accounts = response.json().get("accounts", [])
        except Exception as e:
            LOGGER.error("Failed to decode accounts response as JSON: %s", e)
            LOGGER.error("Raw response: %s", response.text)
            raise
        # Filter accounts if account_ids config is provided (comma-separated string)
        account_ids_config = set()
        account_ids_str = self.config.get("account_ids")
        if account_ids_str:
            account_ids_config = set(s.strip() for s in account_ids_str.split(",") if s.strip())
        for account in accounts:
            account_id = account.get("id")
            if not account_id:
                continue
            if account_ids_config and str(account_id) not in account_ids_config:
                continue
            campaigns_url = f"{RESOURCE_API_BASE}/campaigns"
            campaign_headers = headers.copy()
            campaign_headers["X-GT-ACCOUNT-ID"] = str(account_id)
            LOGGER.info("Campaigns URL: %s", campaigns_url)
            LOGGER.info("Request headers: %s", dict(campaign_headers))
            campaign_response = get_with_retry(campaigns_url, headers=campaign_headers)
            LOGGER.info("Response status code: %s", campaign_response.status_code)
            LOGGER.info("Response headers: %s", dict(campaign_response.headers))
            LOGGER.info("Response text (first 500 chars): %s", campaign_response.text[:500])
            campaign_response.raise_for_status()
            try:
                campaigns = campaign_response.json().get("campaigns", [])
            except Exception as e:
                LOGGER.error("Failed to decode campaigns response as JSON: %s", e)
                LOGGER.error("Raw response: %s", campaign_response.text)
                raise
            for campaign in campaigns:
                campaign_id = campaign.get("id")
                if not campaign_id:
                    continue
                stats_url = f"{self.url_base.rstrip('/')}/creatives/{campaign_id}/daily"
                stats_headers = self.http_headers.copy()
                stats_headers["Accept"] = "application/json"
                stats_headers["User-Agent"] = "curl/7.79.1"
                stats_headers["Accept-Encoding"] = "gzip, deflate"
                # Determine start_date for incremental sync
                starting_timestamp = self.get_starting_timestamp(context)
                if starting_timestamp:
                    start_date = starting_timestamp.date()
                else:
                    start_date = self._parse_date(self.config["start_date"])
                # Always require end_date, default to today if not provided
                end_date_str = self.config.get("end_date")
                if end_date_str:
                    end_date = self._parse_date(end_date_str)
                else:
                    end_date = datetime.date.today()
                # Walk through the date range in 7-day chunks
                chunk_size = datetime.timedelta(days=7)
                current_start = start_date
                while current_start <= end_date:
                    current_end = min(current_start + chunk_size - datetime.timedelta(days=1), end_date)
                    params = {
                        "start_date": current_start.strftime("%Y-%m-%d"),
                        "end_date": current_end.strftime("%Y-%m-%d")
                    }
                    LOGGER.info("Stats URL: %s", stats_url)
                    LOGGER.info("Request headers: %s", dict(stats_headers))
                    LOGGER.info("Request params: %s", params)
                    stats_response = get_with_retry(stats_url, headers=stats_headers, params=params)
                    LOGGER.info("Response status code: %s", stats_response.status_code)
                    LOGGER.info("Response headers: %s", dict(stats_response.headers))
                    LOGGER.info("Response text (first 500 chars): %s", stats_response.text[:500])
                    stats_response.raise_for_status()
                    try:
                        for record in stats_response.json():
                            yield record
                    except Exception as e:
                        LOGGER.error("Failed to decode stats response as JSON: %s", e)
                        LOGGER.error("Raw response: %s", stats_response.text)
                        raise
                    current_start = current_end + datetime.timedelta(days=1)

    def post_process(self, row, context=None):
        # Coerce 'date' to YYYY-MM-DD format for incremental sync compatibility
        if row.get("date") and isinstance(row["date"], str):
            row["date"] = row["date"][:10]
        return row


class CampaignsStream(GroundTruthStream):
    """Stream for all campaigns per account."""

    name = "campaigns"
    path = None  # Will be set dynamically per account
    primary_keys = ["id"]
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("status", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("name", th.StringType),
        )),
        th.Property("start_date", th.StringType),
        th.Property("end_date", th.StringType),
        th.Property("pacing", th.IntegerType),
        th.Property("users", th.ObjectType()),
        th.Property("salesforce_number", th.StringType),
        th.Property("budget_type", th.IntegerType),
        th.Property("account_id", th.IntegerType),
    ).to_dict()

    def get_records(self, context):
        org_id = self.config["organization_id"]
        accounts_url = f"{RESOURCE_API_BASE}/organizations/{org_id}/accounts"
        headers = self.http_headers.copy()
        headers["Accept"] = "application/json"
        headers["User-Agent"] = "curl/7.79.1"
        headers["Accept-Encoding"] = "gzip, deflate"
        LOGGER.info("Accounts URL: %s", accounts_url)
        LOGGER.info("Request headers: %s", dict(headers))
        response = get_with_retry(accounts_url, headers=headers)
        LOGGER.info("Response status code: %s", response.status_code)
        LOGGER.info("Response headers: %s", dict(response.headers))
        LOGGER.info("Response text (first 500 chars): %s", response.text[:500])
        response.raise_for_status()
        try:
            accounts = response.json().get("accounts", [])
        except Exception as e:
            LOGGER.error("Failed to decode accounts response as JSON: %s", e)
            LOGGER.error("Raw response: %s", response.text)
            raise
        # Filter accounts if account_ids config is provided (comma-separated string)
        account_ids_config = set()
        account_ids_str = self.config.get("account_ids")
        if account_ids_str:
            account_ids_config = set(s.strip() for s in account_ids_str.split(",") if s.strip())
        for account in accounts:
            account_id = account.get("id")
            if not account_id:
                continue
            if account_ids_config and str(account_id) not in account_ids_config:
                continue
            campaigns_url = f"{RESOURCE_API_BASE}/campaigns"
            campaign_headers = headers.copy()
            campaign_headers["X-GT-ACCOUNT-ID"] = str(account_id)
            LOGGER.info("Campaigns URL: %s", campaigns_url)
            LOGGER.info("Request headers: %s", dict(campaign_headers))
            campaign_response = get_with_retry(campaigns_url, headers=campaign_headers)
            LOGGER.info("Response status code: %s", campaign_response.status_code)
            LOGGER.info("Response headers: %s", dict(campaign_response.headers))
            LOGGER.info("Response text (first 500 chars): %s", campaign_response.text[:500])
            campaign_response.raise_for_status()
            try:
                campaigns = campaign_response.json().get("campaigns", [])
            except Exception as e:
                LOGGER.error("Failed to decode campaigns response as JSON: %s", e)
                LOGGER.error("Raw response: %s", campaign_response.text)
                raise
            for campaign in campaigns:
                # Add account_id to each campaign record
                campaign["account_id"] = account_id
                yield campaign


class AccountsStream(GroundTruthStream):
    """Stream for all accounts in the organization."""

    name = "accounts"
    path = None  # Will be set dynamically
    primary_keys = ["id"]
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("status", th.StringType),  # status is a JSON object, but flatten as string for now
        th.Property("timezone", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("account_vertical", th.IntegerType),
        th.Property("domain", th.StringType),
        th.Property("accountType", th.StringType),  # accountType is a JSON object, flatten as string for now
    ).to_dict()

    def get_records(self, context):
        org_id = self.config["organization_id"]
        accounts_url = f"{RESOURCE_API_BASE}/organizations/{org_id}/accounts"
        headers = self.http_headers.copy()
        headers["Accept"] = "application/json"
        headers["User-Agent"] = "curl/7.79.1"
        headers["Accept-Encoding"] = "gzip, deflate"
        LOGGER.info("Accounts URL: %s", accounts_url)
        LOGGER.info("Request headers: %s", dict(headers))
        response = get_with_retry(accounts_url, headers=headers)
        LOGGER.info("Response status code: %s", response.status_code)
        LOGGER.info("Response headers: %s", dict(response.headers))
        LOGGER.info("Response text (first 500 chars): %s", response.text[:500])
        response.raise_for_status()
        try:
            accounts = response.json().get("accounts", [])
        except Exception as e:
            LOGGER.error("Failed to decode accounts response as JSON: %s", e)
            LOGGER.error("Raw response: %s", response.text)
            raise
        # Filter accounts if account_ids config is provided (comma-separated string)
        account_ids_config = set()
        account_ids_str = self.config.get("account_ids")
        if account_ids_str:
            account_ids_config = set(s.strip() for s in account_ids_str.split(",") if s.strip())
        for account in accounts:
            account_id = account.get("id")
            if not account_id:
                continue
            if account_ids_config and str(account_id) not in account_ids_config:
                continue
            # Flatten status and accountType fields as string for now
            account_out = dict(account)
            if isinstance(account_out.get("status"), dict):
                account_out["status"] = str(account_out["status"])
            if isinstance(account_out.get("accountType"), dict):
                account_out["accountType"] = str(account_out["accountType"])
            yield account_out
