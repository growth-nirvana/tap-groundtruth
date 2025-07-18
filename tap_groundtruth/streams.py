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
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        LOGGER.error("HTTP error: %s", e)
        LOGGER.error("Response body: %s", response.text)
        if response.status_code >= 500 or response.status_code == 429:
            raise  # Will be retried by tenacity
        else:
            raise
    return response


class CreativeStatsStream(GroundTruthStream):
    """Stream for creative stats per campaign per day."""

    name = "creative_stats"
    path = None  # Will be set dynamically per campaign
    primary_keys = ["id"]  # Adjust if the real primary key is different
    replication_key = "date"
    schema = th.PropertiesList(
        th.Property("date", th.DateTimeType),
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
        th.Property("imp", th.IntegerType),
        th.Property("clks", th.IntegerType),
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
        th.Property("vcr", th.NumberType),
        th.Property("ltr", th.NumberType),
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
                stats_url = f"{self.url_base}/creatives/{campaign_id}/daily"
                stats_headers = self.http_headers.copy()
                stats_headers["Accept"] = "application/json"
                stats_headers["User-Agent"] = "curl/7.79.1"
                stats_headers["Accept-Encoding"] = "gzip, deflate"
                # Determine start_date for incremental sync with lookback
                lookback_days = int(self.config.get("lookback_days", 7))
                starting_timestamp = self.get_starting_timestamp(context)
                if starting_timestamp:
                    start_date = starting_timestamp.date() - datetime.timedelta(days=lookback_days)
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
                            LOGGER.info(f"Record Date: {record['date']}")
                            # Coerce 'date' to a Python date object for Singer SDK compatibility
                            if "date" in record and isinstance(record["date"], str):
                                try:
                                    record["date"] = datetime.datetime.strptime(record["date"], "%Y-%m-%d").date()
                                except Exception as e:
                                    LOGGER.error(f"Failed to parse date field: {record['date']}")
                                    raise
                            # Ensure 'date' is always a string for Singer SDK compatibility
                            if "date" in record and isinstance(record["date"], (datetime.date, datetime.datetime)):
                                record["date"] = record["date"].isoformat()
                            yield record
                    except Exception as e:
                        LOGGER.error("Failed to decode stats response as JSON: %s", e)
                        LOGGER.error("Raw response: %s", stats_response.text)
                        raise
                    current_start = current_end + datetime.timedelta(days=1)


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


class LocationStatsStreamBase(GroundTruthStream):
    """Base stream for campaign location stats by location_type."""

    name = None  # To be set by subclass
    location_type = None  # To be set by subclass
    path = None
    primary_keys = ["campaign_id", "zip", "date"]  # Subclass may override
    replication_key = "date"
    schema = th.PropertiesList(
        th.Property("date", th.DateTimeType),
        th.Property("account_name", th.StringType),
        th.Property("campaign_id", th.IntegerType),
        th.Property("campaign_name", th.StringType),
        th.Property("zip", th.StringType),
        th.Property("latitude", th.NumberType),
        th.Property("longitude", th.NumberType),
        th.Property("dma", th.IntegerType),
        th.Property("city", th.StringType),
        th.Property("state", th.StringType),
        th.Property("country", th.StringType),
        th.Property("impressions", th.IntegerType),
        th.Property("clicks", th.IntegerType),
        th.Property("spend", th.NumberType),
        th.Property("ctr", th.NumberType),
        th.Property("secondary_actions", th.IntegerType),
        th.Property("visits", th.IntegerType),
        th.Property("coupon", th.IntegerType),
        th.Property("website", th.IntegerType),
        th.Property("moreinfo", th.IntegerType),
        th.Property("directions", th.IntegerType),
        th.Property("click_to_call", th.IntegerType),
        th.Property("vcr", th.NumberType),
        th.Property("ltr", th.NumberType),
    ).to_dict()

    def get_records(self, context):
        org_id = self.config["organization_id"]
        accounts_url = f"{RESOURCE_API_BASE}/organizations/{org_id}/accounts"
        headers = self.http_headers.copy()
        headers["Accept"] = "application/json"
        headers["User-Agent"] = "curl/7.79.1"
        headers["Accept-Encoding"] = "gzip, deflate"
        response = get_with_retry(accounts_url, headers=headers)
        response.raise_for_status()
        accounts = response.json().get("accounts", [])
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
            campaign_response = get_with_retry(campaigns_url, headers=campaign_headers)
            campaign_response.raise_for_status()
            campaigns = campaign_response.json().get("campaigns", [])
            for campaign in campaigns:
                campaign_id = campaign.get("id")
                if not campaign_id:
                    continue
                # Determine start_date for incremental sync with lookback
                lookback_days = int(self.config.get("lookback_days", 7))
                starting_timestamp = self.get_starting_timestamp(context)
                if starting_timestamp:
                    start_date = starting_timestamp.date() - datetime.timedelta(days=lookback_days)
                else:
                    start_date = datetime.datetime.strptime(self.config["start_date"], "%Y-%m-%d").date()
                end_date_str = self.config.get("end_date")
                if end_date_str:
                    end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
                else:
                    end_date = datetime.date.today()
                # 1-day chunking
                chunk_size = datetime.timedelta(days=1)
                current_start = start_date
                while current_start <= end_date:
                    current_end = current_start  # Only one day per chunk
                    stats_url = f"{self.url_base}/campaign/locations/{campaign_id}"
                    stats_headers = self.http_headers.copy()
                    stats_headers["Accept"] = "application/json"
                    stats_headers["User-Agent"] = "curl/7.79.1"
                    stats_headers["Accept-Encoding"] = "gzip, deflate"
                    params = {
                        "start_date": current_start.strftime("%Y-%m-%d"),
                        "end_date": current_end.strftime("%Y-%m-%d"),
                        "location_type": self.location_type,
                        # Optionally add metric and key if needed
                    }
                    stats_response = get_with_retry(stats_url, headers=stats_headers, params=params)
                    stats_response.raise_for_status()
                    for record in stats_response.json():
                        # Add derived date field as a datetime object for Singer SDK compatibility
                        record["date"] = datetime.datetime.combine(current_start, datetime.time.min)
                        # Ensure 'date' is always a string for Singer SDK compatibility
                        if "date" in record and isinstance(record["date"], (datetime.date, datetime.datetime)):
                            record["date"] = record["date"].isoformat()
                        yield record
                    current_start = current_start + chunk_size

class LocationZipcodeStatsStream(LocationStatsStreamBase):
    name = "location_zipcode_stats"
    location_type = "zipcode"
    primary_keys = ["campaign_id", "zip", "date"]

class LocationDMAStatsStream(LocationStatsStreamBase):
    name = "location_dma_stats"
    location_type = "dma"
    primary_keys = ["campaign_id", "dma", "date"]


class CampaignPublisherStatsStream(GroundTruthStream):
    """Stream for campaign-level publisher stats per campaign per day."""

    name = "campaign_publisher_stats"
    path = None  # Set dynamically per campaign
    primary_keys = ["campaign_id", "publisher_name", "date"]
    replication_key = "date"
    schema = th.PropertiesList(
        th.Property("date", th.DateTimeType),
        th.Property("campaign_id", th.IntegerType),
        th.Property("campaign_name", th.StringType),
        th.Property("publisher_name", th.StringType),
        th.Property("imp", th.IntegerType),
        th.Property("video_start", th.IntegerType),
        th.Property("video_first_quartile", th.IntegerType),
        th.Property("video_midpoint", th.IntegerType),
        th.Property("video_third_quartile", th.IntegerType),
        th.Property("video_end", th.IntegerType),
        th.Property("vcr", th.IntegerType),
    ).to_dict()

    @property
    def url_base(self) -> str:
        return "https://reporting.groundtruth.com/demand/v2"

    def get_records(self, context):
        org_id = self.config["organization_id"]
        accounts_url = f"{RESOURCE_API_BASE}/organizations/{org_id}/accounts"
        headers = self.http_headers.copy()
        headers["Accept"] = "application/json"
        headers["User-Agent"] = "curl/7.79.1"
        headers["Accept-Encoding"] = "gzip, deflate"
        response = get_with_retry(accounts_url, headers=headers)
        response.raise_for_status()
        accounts = response.json().get("accounts", [])
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
            campaign_response = get_with_retry(campaigns_url, headers=campaign_headers)
            campaign_response.raise_for_status()
            campaigns = campaign_response.json().get("campaigns", [])
            for campaign in campaigns:
                campaign_id = campaign.get("id")
                campaign_name = campaign.get("name")
                if not campaign_id:
                    continue
                # Determine start_date for incremental sync with lookback
                lookback_days = int(self.config.get("lookback_days", 7))
                starting_timestamp = self.get_starting_timestamp(context)
                if starting_timestamp:
                    start_date = starting_timestamp.date() - datetime.timedelta(days=lookback_days)
                else:
                    start_date = datetime.datetime.strptime(self.config["start_date"], "%Y-%m-%d").date()
                end_date_str = self.config.get("end_date")
                if end_date_str:
                    end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
                else:
                    end_date = datetime.date.today()
                # 1-day chunking
                chunk_size = datetime.timedelta(days=1)
                current_start = start_date
                while current_start <= end_date:
                    current_end = current_start  # 1-day window
                    stats_url = f"{self.url_base}/campaign/publisher/{campaign_id}"
                    stats_headers = self.http_headers.copy()
                    stats_headers["Accept"] = "application/json"
                    stats_headers["User-Agent"] = "curl/7.79.1"
                    stats_headers["Accept-Encoding"] = "gzip, deflate"
                    params = {
                        "start_date": current_start.strftime("%Y-%m-%d"),
                        "end_date": current_end.strftime("%Y-%m-%d")
                    }
                    stats_response = get_with_retry(stats_url, headers=stats_headers, params=params)
                    stats_response.raise_for_status()
                    try:
                        records = stats_response.json()
                        if isinstance(records, dict):
                            records = [records]
                        for record in records:
                            record["date"] = datetime.datetime.combine(current_start, datetime.time.min)
                            record["campaign_id"] = campaign_id
                            record["campaign_name"] = campaign_name
                            # Ensure 'date' is always a string for Singer SDK compatibility
                            if "date" in record and isinstance(record["date"], (datetime.date, datetime.datetime)):
                                record["date"] = record["date"].isoformat()
                            yield record
                    except Exception as e:
                        LOGGER.error("Failed to decode publisher stats response as JSON: %s", e)
                        LOGGER.error("Raw response: %s", stats_response.text)
                        raise
                    current_start += chunk_size


class CampaignDemographicStatsStream(GroundTruthStream):
    """Stream for campaign-level demographic stats per campaign per day (age & gender)."""

    name = "campaign_demographic_stats"
    path = None  # Set dynamically per campaign
    primary_keys = ["campaign_id", "adgroup_id", "age", "gender", "date"]
    replication_key = "date"
    schema = th.PropertiesList(
        th.Property("date", th.DateTimeType),
        th.Property("account_name", th.StringType),
        th.Property("campaign_id", th.IntegerType),
        th.Property("campaign_name", th.StringType),
        th.Property("adgroup_id", th.IntegerType),
        th.Property("adgroup_name", th.StringType),
        th.Property("age", th.StringType),
        th.Property("gender", th.StringType),
        th.Property("impressions", th.IntegerType),
        th.Property("clicks", th.IntegerType),
        th.Property("spend", th.NumberType),
        th.Property("ctr", th.NumberType),
        th.Property("cpm", th.NumberType),
        th.Property("total_sa", th.IntegerType),
        th.Property("sar", th.IntegerType),
        th.Property("click_to_call", th.IntegerType),
        th.Property("directions", th.IntegerType),
        th.Property("website", th.IntegerType),
        th.Property("moreinfo", th.IntegerType),
        th.Property("coupon", th.IntegerType),
        th.Property("video", th.IntegerType),
        th.Property("cumulative_reach", th.IntegerType),
        th.Property("visits", th.IntegerType),
        th.Property("open_hour_visits", th.IntegerType),
        th.Property("projected_visits", th.IntegerType),
        th.Property("projected_open_hour_visits", th.IntegerType),
        th.Property("vcr", th.NumberType),
        th.Property("ltr", th.NumberType),
    ).to_dict()

    def get_records(self, context):
        org_id = self.config["organization_id"]
        accounts_url = f"{RESOURCE_API_BASE}/organizations/{org_id}/accounts"
        headers = self.http_headers.copy()
        headers["Accept"] = "application/json"
        headers["User-Agent"] = "curl/7.79.1"
        headers["Accept-Encoding"] = "gzip, deflate"
        response = get_with_retry(accounts_url, headers=headers)
        response.raise_for_status()
        accounts = response.json().get("accounts", [])
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
            campaign_response = get_with_retry(campaigns_url, headers=campaign_headers)
            campaign_response.raise_for_status()
            campaigns = campaign_response.json().get("campaigns", [])
            for campaign in campaigns:
                campaign_id = campaign.get("id")
                campaign_name = campaign.get("name")
                if not campaign_id:
                    continue
                # Determine start_date for incremental sync with lookback
                lookback_days = int(self.config.get("lookback_days", 7))
                starting_timestamp = self.get_starting_timestamp(context)
                if starting_timestamp:
                    start_date = starting_timestamp.date() - datetime.timedelta(days=lookback_days)
                else:
                    start_date = datetime.datetime.strptime(self.config["start_date"], "%Y-%m-%d").date()
                end_date_str = self.config.get("end_date")
                if end_date_str:
                    end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
                else:
                    end_date = datetime.date.today()
                # 1-day chunking
                chunk_size = datetime.timedelta(days=1)
                current_start = start_date
                while current_start <= end_date:
                    current_end = current_start  # 1-day window
                    stats_url = f"{self.url_base}/campaign/demographic/{campaign_id}"
                    stats_headers = self.http_headers.copy()
                    stats_headers["Accept"] = "application/json"
                    stats_headers["User-Agent"] = "curl/7.79.1"
                    stats_headers["Accept-Encoding"] = "gzip, deflate"
                    params = {
                        "start_date": current_start.strftime("%Y-%m-%d"),
                        "end_date": current_end.strftime("%Y-%m-%d"),
                        "all_adgroups": 1,
                        "key": 3,
                    }
                    stats_response = get_with_retry(stats_url, headers=stats_headers, params=params)
                    stats_response.raise_for_status()
                    try:
                        records = stats_response.json()
                        if isinstance(records, dict):
                            records = [records]
                        for record in records:
                            record["date"] = datetime.datetime.combine(current_start, datetime.time.min)
                            record["campaign_id"] = campaign_id
                            record["campaign_name"] = campaign_name
                            # Ensure 'date' is always a string for Singer SDK compatibility
                            if "date" in record and isinstance(record["date"], (datetime.date, datetime.datetime)):
                                record["date"] = record["date"].isoformat()
                            yield record
                    except Exception as e:
                        LOGGER.error("Failed to decode demographic stats response as JSON: %s", e)
                        LOGGER.error("Raw response: %s", stats_response.text)
                        raise
                    current_start += chunk_size
