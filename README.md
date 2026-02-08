# MOT Data Runner

A data pipeline that ingests UK MOT vehicle test data from the [GOV.UK MOT History API](https://documentation.history.mot.api.gov.uk/), processes it into BigQuery, and powers ML models for pass/fail prediction and test volume forecasting.

## Architecture

```
GOV.UK MOT API
      |
      v
 DataPuller          Authenticate (OAuth2/MSAL) and download bulk/delta files
      |
      v
 DataProcessor       Unzip, decompress .json.gz, convert to NDJSON
      |
      v
 GCPUploader         Load to BigQuery, MERGE deltas (create/update/delete)
      |
      v
 BigQuery Views      Enriched view, daily counts
      |
      v
 BigQuery ML         Logistic regression (pass/fail) + ARIMA+ (30-day forecast)
      |
      v
 Looker Studio       Dashboards for predictions and trends
```

## Project Structure

```
runner/
├── main.py                  # Entry point — daily delta pipeline
├── .env.template            # Required environment variables
├── data/                    # Downloaded and processed data files
├── data_runner/             # Core pipeline modules
│   ├── _base.py             # Config and env loading
│   ├── auth.py              # OAuth2 auth via MSAL
│   ├── data_puller.py       # API download (bulk + delta)
│   ├── data_processor.py    # Decompress and transform to NDJSON
│   ├── gcp_upload.py        # BigQuery load and MERGE
│   └── requirements.txt     # Python dependencies
├── bigquery/                # SQL models and views
│   ├── create_main_enriched_view.sql
│   ├── create_daily_counts.sql
│   ├── train_logreg_model.sql
│   ├── train_arimap_model.sql
│   └── create_forecast_view.sql
├── looker_studio/           # Dashboard queries
│   ├── logreg_model.sql
│   └── reg_lookup.sql
└── killswitch/              # GCP billing safety Cloud Function
    ├── main.py
    └── requirements.txt
```

## Setup

### Prerequisites

- Python 3.12+
- A [GOV.UK MOT History API](https://documentation.history.mot.api.gov.uk/) account (trade access)
- A Google Cloud project with BigQuery enabled
- A GCP service account key with BigQuery Data Editor permissions

### Environment Variables

Copy `.env.template` to `.env` and fill in the values:

```bash
cp .env.template .env
```

| Variable | Description |
|---|---|
| `MOT_CLIENT_ID` | Azure AD client ID for MOT API |
| `MOT_CLIENT_SECRET` | Azure AD client secret |
| `MOT_API_KEY` | MOT API key |
| `MOT_SCOPE_URL` | OAuth scope URL |
| `MOT_TOKEN_URL` | Azure AD token endpoint |
| `API_EMAIL` | Registered email address |
| `GCP_PROJECT` | Google Cloud project ID |
| `GCP_SERVICE_CREDENTIALS` | Path to GCP service account JSON key |

### Install Dependencies

```bash
pip install -r data_runner/requirements.txt
```

You'll also need `google-cloud-bigquery` and `google-auth`:

```bash
pip install google-cloud-bigquery google-auth
```

## Usage

### Daily Delta Run

The standard daily job pulls the latest delta, processes it, and merges it into BigQuery:

```bash
python main.py
```

This runs three steps:
1. **Pull** the latest delta file from the MOT API
2. **Process** it (unzip, decompress, convert to NDJSON)
3. **Merge** into BigQuery (insert new records, update existing, delete removed)

### Initial Bulk Load

To create the table from a full bulk download, use `GCPUploader` directly:

```python
from data_runner.data_puller import DataPuller
from data_runner.gcp_upload import GCPUploader

# Download all bulk files
puller = DataPuller()
puller.download_bulk()

# Create table from bulk file
uploader = GCPUploader(dataset_id="motwot_v2", table_id="motwot_v2")
uploader.create_table("data/bulk-light-vehicle_02-02-2026.json")
```

## BigQuery Analytics

### Views

- **`motwot_v2_enriched`** — Adds computed fields: last test result, mileage, vehicle age, pass/fail counts
- **`daily_counts`** — Aggregated PASSED/FAILED test counts per day
- **`prediction_output`** — Blends 30 days of historical data with 30-day forecast

### ML Models

- **`pass_fail_logreg_model`** — Logistic regression predicting MOT pass/fail based on mileage, vehicle age, make, model, and colour
- **`pass_fail_forecast_model`** — ARIMA+ time series forecasting daily test pass/fail volumes 30 days ahead, with UK holiday adjustments

## Billing Killswitch

The `killswitch/` directory contains a Google Cloud Function that monitors project spend via Pub/Sub budget alerts. If costs exceed the budget threshold, it automatically disables billing on the project to prevent runaway charges.

Deploy as a Cloud Function triggered by a Pub/Sub topic linked to a GCP budget alert.

## Tech Stack

- **Python** — Pipeline orchestration
- **MSAL** — OAuth2 authentication against GOV.UK's Azure AD
- **Google BigQuery** — Data warehouse and ML (BQML)
- **Looker Studio** — Dashboards and visualisation
- **Google Cloud Functions** — Billing killswitch