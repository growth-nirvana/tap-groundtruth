# tap-groundtruth

A [Singer](https://www.singer.io/) tap for extracting data from the GroundTruth API, built with the [Meltano Singer SDK](https://sdk.meltano.com/).

## Features
- Extracts campaign, creative, location, publisher, and demographic stats from GroundTruth.
- Supports incremental sync with configurable lookback window.
- Robust error handling and retry logic for API requests.
- Flexible account selection and stream discovery.

---

## Requirements
- Python 3.8+
- [Meltano](https://meltano.com/) (recommended) or standalone Singer-compatible runner
- Access to the GroundTruth API (user ID and API key)

---

## Installation

### With Meltano (recommended)
1. Install [Meltano](https://docs.meltano.com/getting-started/installation/):
   ```bash
   pipx install meltano
   # or
   pip install meltano
   ```
2. Add the tap to your Meltano project:
   ```bash
   meltano add extractor tap-groundtruth
   ```

### Standalone (for development)
1. Clone this repo:
   ```bash
   git clone <repo-url>
   cd tap-groundtruth
   ```
2. Install dependencies:
   ```bash
   pip install -e .
   ```

---

## Configuration

Create a `meltano.yml` or `config.json` with the following required settings:

| Setting           | Description                                 | Required |
|-------------------|---------------------------------------------|----------|
| `user_id`         | Your GroundTruth user ID                     | Yes      |
| `api_key`         | Your GroundTruth API key                     | Yes      |
| `organization_id` | Organization ID to fetch accounts for        | Yes      |
| `start_date`      | Earliest record date to sync (YYYY-MM-DD)    | Yes      |
| `account_ids`     | Comma-separated list of account IDs to sync  | No       |
| `lookback_days`   | Days to look back from bookmark (default: 7) | No       |

**Example `config.json`:**
```json
{
  "user_id": "your_user_id",
  "api_key": "your_api_key",
  "organization_id": "your_org_id",
  "start_date": "2025-05-01",
  "lookback_days": 7
}
```

---

## Usage

### With Meltano

1. **Discover available streams:**
   ```bash
   meltano select tap-groundtruth
   ```
2. **Run a sync:**
   ```bash
   meltano run tap-groundtruth target-jsonl
   ```
3. **Select specific streams:**
   In `meltano.yml`:
   ```yaml
   select:
     - creative_stats
     - campaign_publisher_stats
     - location_zipcode_stats
     - location_dma_stats
     - campaign_demographic_stats
   ```

### Standalone Singer

```bash
# Discover catalog
./tap-groundtruth --config config.json --discover > catalog.json
# Run sync
./tap-groundtruth --config config.json --catalog catalog.json --state state.json
```

---

## Streams
- `creative_stats`
- `campaign_publisher_stats`
- `campaign_demographic_stats`
- `location_zipcode_stats`
- `location_dma_stats`
- `accounts`
- `campaigns`

---

## Incremental Sync & Lookback
- All reporting streams support incremental sync using a `date` replication key.
- The `lookback_days` config (default: 7) ensures each sync re-fetches the last N days for late-arriving data.
- Bookmarks are managed automatically by Meltano/Singer.

---

## Error Handling & Retries
- All HTTP requests use robust retry logic (5 attempts, exponential backoff) for 5xx and 429 errors.
- Errors and failed responses are logged with details for troubleshooting.

---

## Troubleshooting
- **500 Server Error:** Usually a GroundTruth API issue. The tap will retry automatically. If persistent, check your parameters or contact GroundTruth support.
- **Type errors on `date`:** Ensure you are using the latest version; all date fields are now ISO8601 strings.
- **No data:** Check your `account_ids`, `start_date`, and stream selection.
- **Debug logs:** Enable more verbose logging by setting the `LOG_LEVEL` environment variable:
  ```bash
  export LOG_LEVEL=DEBUG
  ```

---

## Contributing
Pull requests and issues are welcome! Please open an issue to discuss major changes first.

---

## License
MIT
