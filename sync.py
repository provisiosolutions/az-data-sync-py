"""
Azure Blob Storage One-Way Sync
===============================
Pulls files from an Azure Blob Storage container to a local directory.
Only downloads blobs that are new or have changed since the last sync
using an ETag/Last-Modified manifest to minimize bandwidth.

Usage Scenarios:
----------------
1. Basic Sync using a config file:
    python sync.py --config config.json

2. Sync with connection string override and 16 concurrent workers:
    python sync.py -c config.json --connection-string "..." --workers 16

3. Sync specific folder/prefix only, and delete local files not in Azure:
    python sync.py -c config.json --prefix "images/" --delete-orphaned

4. Override the local download directory for a specific run:
    python sync.py -c config.json --local-dir /data/backup
"""

import argparse
import concurrent.futures
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from azure.storage.blob import BlobServiceClient, ContainerClient


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("azure-sync")

# ---------------------------------------------------------------------------
# Manifest helpers (tracks what we already have locally)
# ---------------------------------------------------------------------------
MANIFEST_FILE = ".sync_manifest.json"


def load_manifest(local_dir: Path) -> dict:
    """Load the sync manifest that tracks etag/md5 of previously synced blobs."""
    manifest_path = local_dir / MANIFEST_FILE
    if manifest_path.exists():
        with open(manifest_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_manifest(local_dir: Path, manifest: dict) -> None:
    """Persist the sync manifest."""
    manifest_path = local_dir / MANIFEST_FILE
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, default=str)


# ---------------------------------------------------------------------------
# Core sync logic
# ---------------------------------------------------------------------------

def should_download(blob_props, manifest_entry: dict | None) -> bool:
    """Decide whether a blob needs to be (re-)downloaded."""
    if manifest_entry is None:
        return True  # new file

    # Compare by etag first (most reliable)
    remote_etag = blob_props.etag
    if remote_etag and manifest_entry.get("etag") == remote_etag:
        return False

    # Fall back to last-modified comparison
    remote_mtime = blob_props.last_modified
    local_mtime_str = manifest_entry.get("last_modified")
    if remote_mtime and local_mtime_str:
        local_mtime = datetime.fromisoformat(local_mtime_str)
        if remote_mtime <= local_mtime:
            return False

    return True


def _download_blob(container_client: ContainerClient, blob_name: str, dest_path: Path):
    """Worker function to download a single blob from Azure."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    blob_client = container_client.get_blob_client(blob_name)
    log.info("DOWN  %s → %s", blob_name, dest_path)

    with open(dest_path, "wb") as f:
        stream = blob_client.download_blob()
        stream.readinto(f)


def sync_container(
    container_client: ContainerClient,
    local_dir: Path,
    prefix: str = "",
    delete_orphaned: bool = False,
    max_workers: int = 8,
) -> dict:
    """
    One-way sync: Azure Blob → local filesystem using concurrent workers.

    Returns a summary dict with counts of downloaded / skipped / deleted files.
    """
    local_dir.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(local_dir)
    new_manifest: dict = {}

    stats = {"downloaded": 0, "skipped": 0, "deleted": 0, "errors": 0}

    log.info("Listing blobs in container (prefix=%r) …", prefix or "(none)")
    blobs = list(container_client.list_blobs(name_starts_with=prefix or None))

    log.info("Found %d blobs matching prefix. Determining sync actions…", len(blobs))

    # Determine actions needed: (blobs to skip, blobs to download)
    to_download = []
    
    for blob in blobs:
        blob_name: str = blob.name
        
        # Skip "directory" markers (zero-byte blobs ending with /)
        if blob_name.endswith("/"):
            continue

        entry = manifest.get(blob_name)

        if not should_download(blob, entry):
            log.debug("SKIP  %s  (unchanged)", blob_name)
            new_manifest[blob_name] = entry  # carry forward
            stats["skipped"] += 1
        else:
            to_download.append((blob, entry))

    # Process downloads concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks
        future_to_blob = {
            executor.submit(
                _download_blob, container_client, blob.name, local_dir / blob.name
            ): (blob, entry)
            for blob, entry in to_download
        }

        # Handle results as they complete
        for future in concurrent.futures.as_completed(future_to_blob):
            blob, entry = future_to_blob[future]
            blob_name = blob.name
            
            try:
                future.result()  # Will raise if _download_blob threw an exception
                new_manifest[blob_name] = {
                    "etag": blob.etag,
                    "last_modified": blob.last_modified.isoformat() if blob.last_modified else None,
                    "size": blob.size,
                    "synced_at": datetime.now(timezone.utc).isoformat(),
                }
                stats["downloaded"] += 1
            except Exception as exc:
                log.error("FAIL  %s  → %s", blob_name, exc)
                # Keep the old manifest entry so we retry next run
                if entry:
                    new_manifest[blob_name] = entry
                stats["errors"] += 1

    # Optionally remove local files that no longer exist in the container
    if delete_orphaned:
        orphaned = set(manifest.keys()) - set(new_manifest.keys())
        for orphan in orphaned:
            orphan_path = local_dir / orphan
            if orphan_path.exists():
                log.info("DEL   %s  (orphaned)", orphan)
                orphan_path.unlink()
                stats["deleted"] += 1

    log.info("Saving manifest (tracked files: %d) …", len(new_manifest))
    save_manifest(local_dir, new_manifest)
    
    return stats


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_config(path: str) -> dict:
    """Load JSON config file."""
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    required = ["connection_string", "container_name"]
    for key in required:
        if key not in cfg or not cfg[key]:
            raise ValueError(f"Config is missing required key: {key}")
    return cfg


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="One-way sync: Azure Blob Storage → local filesystem",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--config", "-c",
        help="Path to a JSON config file containing default settings (e.g., config.json).",
    )
    p.add_argument(
        "--connection-string",
        help="Azure Storage connection string or SAS URL. Overrides config file if provided.",
    )
    p.add_argument(
        "--container",
        help="Name of the remote Azure Blob container to sync from. Overrides config file.",
    )
    p.add_argument(
        "--local-dir",
        default="./synced-files",
        help="Local directory path to sync into. Default is './synced-files'.",
    )
    p.add_argument(
        "--prefix",
        default="",
        help="Virtual folder prefix limit: only sync blobs whose name starts with this string.",
    )
    p.add_argument(
        "--delete-orphaned",
        action="store_true",
        help="Clean up: delete local files that no longer exist in the remote container.",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of concurrent download threads to use. Default is 8.",
    )
    p.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug-level logging for detailed troubleshooting.",
    )
    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # ── Resolve configuration ───────────────────────────────────
    cfg: dict = {}
    if args.config:
        cfg = load_config(args.config)

    # CLI flags override config file values
    connection_string = (
        args.connection_string
        or cfg.get("connection_string")
        or os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    )
    container_name = args.container or cfg.get("container_name")
    
    # Intelligently resolve local dir overrides
    local_dir_arg = args.local_dir
    local_dir_cfg = cfg.get("local_dir", "./synced-files")
    resolved_dir = local_dir_arg if local_dir_arg != "./synced-files" else local_dir_cfg
    local_dir = Path(resolved_dir)
    
    prefix = args.prefix or cfg.get("prefix", "")
    delete_orphaned = args.delete_orphaned or cfg.get("delete_orphaned", False)
    workers = getattr(args, "workers", cfg.get("workers", 8))

    if not connection_string:
        log.error("No connection string provided. Use --connection-string, config file, or AZURE_STORAGE_CONNECTION_STRING env var.")
        sys.exit(1)
    if not container_name:
        log.error("No container name provided. Use --container or config file.")
        sys.exit(1)

    # ── Connect & sync ──────────────────────────────────────────
    log.info("Connecting to Azure Blob Storage …")
    log.info("Container : %s", container_name)
    log.info("Local dir : %s", local_dir.resolve())
    log.info("Workers   : %d", workers)

    if connection_string.startswith("http://") or connection_string.startswith("https://"):
        blob_service = BlobServiceClient(account_url=connection_string)
    else:
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        
    container_client = blob_service.get_container_client(container_name)

    stats = sync_container(
        container_client,
        local_dir,
        prefix=prefix,
        delete_orphaned=delete_orphaned,
        max_workers=workers,
    )

    # ── Summary ─────────────────────────────────────────────────
    log.info("── Sync complete ──────────────────────────────────")
    log.info("  Downloaded : %d", stats["downloaded"])
    log.info("  Skipped    : %d", stats["skipped"])
    log.info("  Deleted    : %d", stats["deleted"])
    log.info("  Errors     : %d", stats["errors"])

    if stats["errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

