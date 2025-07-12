"""GroundTruth tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_groundtruth import streams


class TapGroundTruth(Tap):
    """GroundTruth tap class."""

    name = "tap-groundtruth"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="The earliest record date to sync",
        ),
        th.Property(
            "api_url",
            th.StringType(nullable=False),
            title="API URL",
            default="https://api.mysample.com",
            description="The url for the API service",
        ),
        th.Property(
            "user_agent",
            th.StringType(nullable=True),
            description=(
                "A custom User-Agent header to send with each request. Default is "
                "'<tap_name>/<tap_version>'"
            ),
        ),
        th.Property(
            "user_id",
            th.StringType(nullable=False),
            required=True,
            secret=False,
            title="User ID",
            description="The user ID for API authentication",
        ),
        th.Property(
            "api_key",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="API Key",
            description="The API key for authentication",
        ),
        th.Property(
            "organization_id",
            th.StringType(nullable=False),
            required=True,
            title="Organization ID",
            description="The organization ID to fetch accounts for",
        ),
        th.Property(
            "account_ids",
            th.StringType(),
            required=False,
            title="Account IDs",
            description="Optional comma-separated list of account IDs to sync. If not provided, all accounts will be synced.",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.GroundTruthStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.CreativeStatsStream(self),
            streams.CampaignsStream(self),
            streams.AccountsStream(self),
        ]


if __name__ == "__main__":
    TapGroundTruth.cli()
