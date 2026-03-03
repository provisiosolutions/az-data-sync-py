# Azure Blob Sync

A highly efficient **one-way sync tool** that securely pulls files from an Azure Blob Storage container to your local machine.

## What is "One-Way Sync"?

This tool strictly downloads data **from Azure to your local machine**. It will **never** upload local changes back to Azure, and it will **never** delete files from your Azure container. It only downloads new or changed files based on an ETag/Last-Modified manifest, making it safe to run repeatedly.

## Common Use Cases

- **Scheduled Backups**: Run via a cron job to keep a localized, up-to-date replica of a cloud container.
- **Large Dataset Retrieval**: Sync machine learning training sets or large media libraries before processing them locally.
- **Offline Content Delivery**: Pull down localized media and configuration files for edge devices, kiosks, or disconnected laptops.
- **Cloud Migration**: Use this as the "download" step before uploading the retrieved files to another cloud provider (e.g., AWS S3).
- **CI/CD Artifact Fetching**: Retrieve build artifacts or release binaries from a storage container onto a deployment server.

## Prerequisites

- Python 3.10+
- An Azure Storage account with at least one blob container

## Installation

First, create and activate a virtual environment:

```bash
# Windows
python -m venv .venv
.venv\Scripts\activate

# macOS/Linux
python -m venv .venv
source .venv/bin/activate
```

Then install the dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

You can configure the tool three ways (in order of precedence):

### 1. CLI flags (highest priority)

```bash
python sync.py \
  --connection-string "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net" \
  --container my-container \
  --local-dir ./output
```

### 2. JSON config file

```bash
cp config.example.json config.json
```

Edit `config.json` with your values:

```json
{
  "connection_string": "DefaultEndpointsProtocol=https;AccountName=YOUR_ACCOUNT;AccountKey=YOUR_KEY;EndpointSuffix=core.windows.net",
  "container_name": "my-container",
  "local_dir": "./synced-files",
  "prefix": "",
  "delete_orphaned": false
}
```

Then run with:

```bash
python sync.py --config config.json
```

### 3. Environment variable (lowest priority)

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;..."
python sync.py --container my-container
```

> **Note:** CLI flags override config file values, which override env vars.

## Usage Examples

```bash
# Basic sync using a config file
python sync.py --config config.json

# Sync only blobs under a specific path
python sync.py -c config.json --prefix "images/2025/"

# Sync and delete local files that were removed from Azure
python sync.py -c config.json --delete-orphaned

# Verbose output for debugging
python sync.py -c config.json -v

# Override output directory
python sync.py -c config.json --local-dir /data/backup
```

## CLI Reference

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--config` | `-c` | — | Path to JSON config file |
| `--connection-string` | | — | Azure Storage connection string |
| `--container` | | — | Blob container name |
| `--local-dir` | | `./synced-files` | Local directory to sync into |
| `--prefix` | | `""` | Only sync blobs matching this prefix |
| `--delete-orphaned` | | `false` | Remove local files deleted from Azure |
| `--verbose` | `-v` | `false` | Enable debug-level logging |

## How It Works

1. **List** — queries all blobs in the container (filtered by `--prefix` if set)
2. **Compare** — checks each blob's `etag` and `last_modified` against a local `.sync_manifest.json`
3. **Download** — only pulls blobs that are new or changed
4. **Clean up** *(optional)* — with `--delete-orphaned`, removes local files that no longer exist in the container
5. **Save manifest** — writes `.sync_manifest.json` so the next run knows what's already synced

The manifest is stored inside your `--local-dir`. Deleting it will cause a full re-download on the next run.

## Running Tests

```bash
pip install pytest
pytest test_sync.py -v
```

All Azure SDK calls are mocked — no real Azure connection needed.

## Finding Your Connection String

1. Go to the [Azure Portal](https://portal.azure.com)
2. Navigate to your **Storage Account** → **Access keys**
3. Click **Show** next to Key 1 and copy the **Connection string**

> **Security:** Never commit `config.json` to source control — it's in `.gitignore` by default.
