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
  "container_name": "raw-sessions",
  "local_dir": "./raw-sessions",
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
| `--delete-orphaned` | | `false` | Delete orphaned local files on pull or orphaned blobs on `--push` |
| `--verify-local` | | `false` | Re-download blobs missing on disk even if manifest ETag matches |
| `--push` | | `false` | Push local files to Azure Blob Storage instead of pulling |
| `--classifier-root` | | — | Path to `audio-classifier-hear-me` root to push/pull dataset, profile heads, and model weights |
| `--stage-from-classifier` | | — | Stage raw Kirk WAV sessions and trained speaker heads locally |
| `--raw-sessions-dir` | | `./raw-sessions` | Local staging path for direct raw Kirk sessions |
| `--trained-heads-dir` | | `./trained-heads` | Local staging path for trained speaker heads |
| `--include-mp4` | | `false` | Include `.mp4` videos from `Edited_Clips` when staging raw sessions |
| `--skip-exports` | | `false` | Skip heavy Core ML / ONNX export bundles when syncing `--classifier-root` |
| `--dry-run` | | `false` | Preview files to upload without transferring data |
| `--verbose` | `-v` | `false` | Enable debug-level logging |
| `--tui` | | `false` | Launch the interactive Textual sync dashboard |

## TUI Quickstart (`tui.py` / `--tui`)

Launch the interactive dashboard to inspect datasets, preview diffs, and sync buckets without long CLI flags:

```bash
python sync.py --tui
# Or run directly:
python tui.py
```

1. **Copy Config:** Run `cp config.example.json config.json` and paste your Azure connection string.
2. **Pick a Bucket:** Press `Up` or `Down` to select `raw-sessions`, `trained-heads`, or `sessions (SWA)`.
3. **Inspect Words, Files, and Audio:** Press `/` to filter words or filenames (`Esc` clears), press `V` (or `1`/`2`/`3`) to switch `Words`, `Files`, and `Sync Diff` tabs, press `Enter` on a word to view matching files, and press `Space` to play/stop audio clips with a live waveform.
4. **Sync Safely:** Press `G` to pull files, `S` to stage new clips from `../audio-classifier-hear-me`, `P` to push after preview, or `?` for the quickstart guide. (`sessions` stays Pull-Only.)

## Three-Bucket Session & Trained-Head Layout

Keep HearMe SWA web sessions, direct raw audio sessions, and trained speaker heads in separate directories and Azure containers:

| Local Directory | Azure Container | Contents |
| :--- | :--- | :--- |
| `./synced-files/` | `sessions` | HearMe SWA web-app sessions (`metadata.json` + `.wav`) |
| `./raw-sessions/` | `raw-sessions` | `snake_case` Kirk WAV sessions (`session_2026_08_25`, `session_2026_09_25`, `session_2026_11_18_edited_clips`), `manifest.jsonl`, `lexicon.json` |
| `./trained-heads/` | `trained-heads` | Trained speaker heads (`kirk/`, `kirk_core/`) without audio symlinks |

Stage and preview uploads before you push to Azure:

```bash
# 1. Pull HearMe SWA sessions into ./synced-files/ when real recordings arrive
python sync.py -c config.json --container sessions --local-dir ./synced-files

# 2. Stage snake_case raw WAV sessions, manifest.jsonl, lexicon.json, and heads locally
python sync.py --stage-from-classifier ../audio-classifier-hear-me

# 3. Pull or preview raw-sessions (default in config.json) and trained-heads
python sync.py -c config.json
python sync.py -c config.json --container raw-sessions --local-dir ./raw-sessions --push --delete-orphaned --dry-run
python sync.py -c config.json --container trained-heads --local-dir ./trained-heads --push --dry-run

# 4. Push to Azure when ready (creates containers on live upload)
python sync.py -c config.json --container raw-sessions --local-dir ./raw-sessions --push --delete-orphaned
python sync.py -c config.json --container trained-heads --local-dir ./trained-heads --push
```

## Backing Up & Restoring HearMe Classifier Audio & Weights

Push or restore the `audio-classifier-hear-me` dataset (`dataset/train`, `manifest.jsonl`, `splits/`), speaker profile heads (`profiles/kirk/head.pt`, `profiles/kirk_core/head.pt`), and model weights (`results/best_model`, `results/best_model_core`, Core ML `.mlpackage`, ONNX) under the `classifier/` prefix:

```bash
# Dry-run preview of classifier files to upload
python sync.py -c config.json --push --classifier-root ../audio-classifier-hear-me --dry-run

# Push dataset, profile heads, and best_model + exports to Azure Blob Storage
python sync.py -c config.json --push --classifier-root ../audio-classifier-hear-me

# Restore dataset, profile heads, and model weights from Azure Blob Storage onto a new machine
python sync.py -c config.json --classifier-root ../audio-classifier-hear-me
```

## Reviewing & Exporting Sessions (`review_sessions.py`)

Generate a self-contained `synced-files/review.html` page (supporting legacy GUID folders, standalone `{friendlyName}/` folders, and nested `{sessionCode}/{friendlyName}/` folders) or export recordings into canonical HearMe classifier folders:

```bash
# Generate synced-files/review.html and open in browser
python review_sessions.py

# Generate review.html without opening browser
python review_sessions.py --no-open

# Export valid recordings into HearMe canonical phrase folders + export_manifest.jsonl
python review_sessions.py --no-open --export-classifier ./classifier-export
```

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
pytest test_sync.py test_tui.py -v
```

All Azure SDK calls are mocked — no real Azure connection needed.

## Finding Your Connection String

1. Go to the [Azure Portal](https://portal.azure.com)
2. Navigate to your **Storage Account** → **Access keys**
3. Click **Show** next to Key 1 and copy the **Connection string**

> **Security:** Never commit `config.json` to source control — it's in `.gitignore` by default.
