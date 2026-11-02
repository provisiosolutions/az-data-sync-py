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


from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn, BarColumn, TextColumn
from rich.panel import Panel
from rich.table import Table

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True, markup=True)]
)
log = logging.getLogger("azure-sync")
# Suppress noisy azure SDK
logging.getLogger("azure").setLevel(logging.WARNING)

console = Console()

# ---------------------------------------------------------------------------
# Manifest helpers (tracks what we already have locally / pushed remotely)
# ---------------------------------------------------------------------------
MANIFEST_FILE = ".sync_manifest.json"
PUSH_MANIFEST_FILE = ".push_manifest.json"
_IGNORED_FILENAMES = {MANIFEST_FILE, PUSH_MANIFEST_FILE, ".DS_Store", "Thumbs.db"}


def load_manifest(local_dir: Path, filename: str = MANIFEST_FILE) -> dict:
    """Load the sync manifest that tracks etag/md5 of previously synced blobs."""
    manifest_path = local_dir / filename
    if manifest_path.exists():
        with open(manifest_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_manifest(local_dir: Path, manifest: dict, filename: str = MANIFEST_FILE) -> None:
    """Persist the sync manifest."""
    local_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = local_dir / filename
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
    log.debug("DOWN  %s", blob_name)

    with open(dest_path, "wb") as f:
        stream = blob_client.download_blob()
        stream.readinto(f)


def sync_container(
    container_client: ContainerClient,
    local_dir: Path,
    prefix: str = "",
    delete_orphaned: bool = False,
    max_workers: int = 8,
    max_size_bytes: int = 50 * 1024**3,
    verify_local_files: bool = False,
    strip_prefix: str = "",
) -> dict:
    """
    One-way sync: Azure Blob → local filesystem using concurrent workers.

    Returns a summary dict with counts of downloaded / skipped / deleted files.
    """
    local_dir.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(local_dir)
    new_manifest: dict = {}

    # Preserve manifest entries outside the active prefix so partial syncs
    # neither wipe unrelated manifest records nor delete unrelated local files.
    if prefix:
        for key, val in manifest.items():
            if not key.startswith(prefix):
                new_manifest[key] = val

    stats = {"downloaded": 0, "skipped": 0, "deleted": 0, "errors": 0}

    log.info("Listing blobs in container (prefix=%r) …", prefix or "(none)")
    blobs = list(container_client.list_blobs(name_starts_with=prefix or None))

    log.info("Found %d blobs matching prefix. Determining sync actions…", len(blobs))

    # Determine actions needed: (blobs to skip, blobs to download)
    to_download = []
    
    resolved_local_dir = local_dir.resolve()

    for blob in blobs:
        blob_name: str = blob.name
        
        # Skip "directory" markers (zero-byte blobs ending with /)
        if blob_name.endswith("/"):
            continue

        rel_name = (
            blob_name[len(strip_prefix) :]
            if strip_prefix and blob_name.startswith(strip_prefix)
            else blob_name
        )
        if not rel_name:
            continue

        # SECURITY: Check for directory traversal / escape attacks
        dest_path = (local_dir / rel_name).resolve()
        if not dest_path.is_relative_to(resolved_local_dir):
            log.warning("SKIP  %s  (SECURITY: Path escapes local directory)", blob_name)
            stats["skipped"] += 1
            continue

        # SECURITY: Check size limits
        if blob.size and blob.size > max_size_bytes:
            log.warning("SKIP  %s  (SECURITY: Exceeds max size limit of %d bytes)", blob_name, max_size_bytes)
            stats["skipped"] += 1
            continue

        entry = manifest.get(blob_name)
        local_missing = verify_local_files and not dest_path.exists()

        if not local_missing and not should_download(blob, entry):
            log.debug("SKIP  %s  (unchanged)", blob_name)
            new_manifest[blob_name] = entry  # carry forward
            stats["skipped"] += 1
        else:
            to_download.append((blob, entry, dest_path))

    if to_download:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task("[cyan]Downloading files...", total=len(to_download))

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all download tasks
                future_to_blob = {
                    executor.submit(
                        _download_blob, container_client, blob.name, dest_path
                    ): (blob, entry)
                    for blob, entry, dest_path in to_download
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
                    
                    progress.advance(task_id)

    # Optionally remove local files that no longer exist in the container
    if delete_orphaned:
        orphaned = set(manifest.keys()) - set(new_manifest.keys())
        for orphan in orphaned:
            orphan_path = (local_dir / orphan).resolve()
            if orphan_path.is_relative_to(resolved_local_dir) and orphan_path.exists():
                log.info("DEL   %s  (orphaned)", orphan)
                orphan_path.unlink()
                stats["deleted"] += 1

    log.info("Saving manifest (tracked files: %d) …", len(new_manifest))
    save_manifest(local_dir, new_manifest)
    
    return stats


# ---------------------------------------------------------------------------
# Upload / Push & HearMe Classifier Artifact Backup
# ---------------------------------------------------------------------------

def should_upload(local_path: Path, push_entry: dict | None, remote_blob=None) -> bool:
    """Decide whether a local file needs to be uploaded to Azure Blob Storage."""
    stat = local_path.stat()
    if push_entry is not None:
        if (
            push_entry.get("size") == stat.st_size
            and push_entry.get("mtime_ns") == stat.st_mtime_ns
        ):
            if remote_blob is None or (
                not push_entry.get("etag") or remote_blob.etag == push_entry.get("etag")
            ):
                return False

    if remote_blob is not None and push_entry is None:
        remote_mtime = getattr(remote_blob, "last_modified", None)
        local_mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        if getattr(remote_blob, "size", None) == stat.st_size and remote_mtime and remote_mtime >= local_mtime:
            return False

    return True


def _upload_blob(container_client: ContainerClient, blob_name: str, local_path: Path) -> str | None:
    """Worker function to upload a single local file to Azure Blob Storage."""
    blob_client = container_client.get_blob_client(blob_name)
    log.debug("UP    %s", blob_name)
    with open(local_path, "rb") as f:
        result = blob_client.upload_blob(f, overwrite=True)
    if isinstance(result, dict):
        return result.get("etag")
    return getattr(result, "etag", None)


def push_directory(
    container_client: ContainerClient,
    local_dir: Path,
    remote_prefix: str = "",
    max_workers: int = 8,
    max_size_bytes: int = 50 * 1024**3,
    dry_run: bool = False,
) -> dict:
    """
    One-way push: local directory → Azure Blob Storage using concurrent workers.
    Skips symbolic links so deduplicated audio directories are not uploaded twice.
    """
    stats = {"uploaded": 0, "skipped": 0, "deleted": 0, "errors": 0}
    if not local_dir.exists():
        log.warning("Local directory %s does not exist; nothing to push.", local_dir)
        return stats

    norm_prefix = remote_prefix.strip("/")
    if norm_prefix:
        norm_prefix += "/"

    manifest = load_manifest(local_dir, PUSH_MANIFEST_FILE)
    new_manifest = dict(manifest)

    remote_blobs: dict[str, object] = {}
    try:
        for b in container_client.list_blobs(name_starts_with=norm_prefix or None):
            remote_blobs[b.name] = b
    except Exception as exc:
        log.debug("Could not pre-list remote blobs (%s); relying on local push manifest.", exc)

    to_upload: list[tuple[Path, str, os.stat_result, dict | None]] = []

    for root, dirs, files in os.walk(local_dir, followlinks=False):
        root_path = Path(root)
        # Filter out symlinked directories and cache folders
        dirs[:] = [
            d for d in sorted(dirs)
            if d != "__pycache__" and not (root_path / d).is_symlink()
        ]
        for fname in sorted(files):
            if fname in _IGNORED_FILENAMES or fname.endswith(".tmp"):
                continue
            fpath = root_path / fname
            if fpath.is_symlink():
                continue
            fstat = fpath.stat()
            if fstat.st_size > max_size_bytes:
                log.warning("SKIP  %s  (Exceeds max size limit of %d bytes)", fpath, max_size_bytes)
                stats["skipped"] += 1
                continue

            rel_posix = fpath.relative_to(local_dir).as_posix()
            blob_name = f"{norm_prefix}{rel_posix}"
            entry = manifest.get(blob_name)
            remote_blob = remote_blobs.get(blob_name)

            if not should_upload(fpath, entry, remote_blob=remote_blob):
                if entry is None and remote_blob is not None:
                    new_manifest[blob_name] = {
                        "size": fstat.st_size,
                        "mtime_ns": fstat.st_mtime_ns,
                        "etag": getattr(remote_blob, "etag", None),
                        "uploaded_at": datetime.now(timezone.utc).isoformat(),
                    }
                stats["skipped"] += 1
            else:
                to_upload.append((fpath, blob_name, fstat, entry))

    if dry_run:
        stats["uploaded"] = len(to_upload)
        return stats

    if to_upload:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task("[cyan]Uploading files...", total=len(to_upload))
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_item = {
                    executor.submit(_upload_blob, container_client, blob_name, fpath): (
                        fpath,
                        blob_name,
                        fstat,
                        entry,
                    )
                    for fpath, blob_name, fstat, entry in to_upload
                }
                for future in concurrent.futures.as_completed(future_to_item):
                    fpath, blob_name, fstat, entry = future_to_item[future]
                    try:
                        etag = future.result()
                        new_manifest[blob_name] = {
                            "size": fstat.st_size,
                            "mtime_ns": fstat.st_mtime_ns,
                            "etag": etag,
                            "uploaded_at": datetime.now(timezone.utc).isoformat(),
                        }
                        stats["uploaded"] += 1
                    except Exception as exc:
                        log.error("FAIL  %s  → %s", blob_name, exc)
                        if entry:
                            new_manifest[blob_name] = entry
                        stats["errors"] += 1
                    progress.advance(task_id)

    save_manifest(local_dir, new_manifest, PUSH_MANIFEST_FILE)
    return stats


def sync_classifier_artifacts(
    container_client: ContainerClient,
    classifier_root: Path,
    direction: str = "push",
    remote_base_prefix: str = "classifier",
    include_exports: bool = True,
    max_workers: int = 8,
    max_size_bytes: int = 50 * 1024**3,
    dry_run: bool = False,
) -> dict:
    """
    Push or pull HearMe audio-classifier dataset (106 MB), profile heads,
    and model checkpoints/exports (best_model, Core ML, ONNX) to/from Azure Blob Storage.
    """
    base_prefix = remote_base_prefix.strip("/")
    targets: list[tuple[Path, str]] = [
        (classifier_root / "dataset", f"{base_prefix}/dataset"),
        (classifier_root / "profiles", f"{base_prefix}/profiles"),
        (classifier_root / "results" / "best_model", f"{base_prefix}/results/best_model"),
    ]
    if include_exports:
        targets.append(
            (
                classifier_root / "results" / "HearMeClassifier_flexible.mlpackage",
                f"{base_prefix}/results/HearMeClassifier_flexible.mlpackage",
            )
        )

    totals = {"uploaded": 0, "downloaded": 0, "skipped": 0, "deleted": 0, "errors": 0}

    if direction == "push":
        for local_target, remote_pref in targets:
            if local_target.is_dir():
                sub = push_directory(
                    container_client,
                    local_target,
                    remote_prefix=remote_pref,
                    max_workers=max_workers,
                    max_size_bytes=max_size_bytes,
                    dry_run=dry_run,
                )
                for k in ("uploaded", "skipped", "errors"):
                    totals[k] += sub.get(k, 0)

        # Also push single-file exports (HearMeClassifier.onnx, metadata, parity_baseline)
        if include_exports:
            results_dir = classifier_root / "results"
            for single_name in (
                "HearMeClassifier.onnx",
                "HearMeClassifier_metadata.json",
                "parity_baseline.json",
            ):
                fpath = results_dir / single_name
                if fpath.is_file():
                    blob_name = f"{base_prefix}/results/{single_name}"
                    if dry_run:
                        totals["uploaded"] += 1
                    else:
                        try:
                            _upload_blob(container_client, blob_name, fpath)
                            totals["uploaded"] += 1
                        except Exception as exc:
                            log.error("FAIL  %s  → %s", blob_name, exc)
                            totals["errors"] += 1
    else:
        # Pull classifier prefix into classifier_root, stripping the remote base_prefix
        sub = sync_container(
            container_client,
            classifier_root,
            prefix=f"{base_prefix}/",
            strip_prefix=f"{base_prefix}/",
            delete_orphaned=False,
            max_workers=max_workers,
            max_size_bytes=max_size_bytes,
            verify_local_files=True,
        )
        for k in ("downloaded", "skipped", "deleted", "errors"):
            totals[k] += sub.get(k, 0)

        # Ensure deduplicated symlink profiles/kirk/audio_data -> ../../dataset/train exists
        kirk_dir = classifier_root / "profiles" / "kirk"
        kirk_audio = kirk_dir / "audio_data"
        train_dir = classifier_root / "dataset" / "train"
        if kirk_dir.is_dir() and train_dir.is_dir() and not kirk_audio.exists() and not kirk_audio.is_symlink():
            kirk_audio.symlink_to(Path("../../dataset/train"))

    return totals


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
        "--verify-local",
        action="store_true",
        help="Re-download blobs if the local file is missing on disk even when manifest ETag matches.",
    )
    p.add_argument(
        "--push",
        action="store_true",
        help="Push local files to Azure Blob Storage instead of pulling.",
    )
    p.add_argument(
        "--classifier-root",
        type=Path,
        default=None,
        help="Path to audio-classifier-hear-me root to push/pull dataset (106 MB), profile heads, and model weights.",
    )
    p.add_argument(
        "--skip-exports",
        action="store_true",
        help="When syncing --classifier-root, skip heavy Core ML / ONNX export bundles.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview files to upload without transferring data.",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of concurrent download/upload threads to use. Default is 8.",
    )
    p.add_argument(
        "--max-size-gb",
        type=float,
        default=50.0,
        help="Maximum allowed file size in GB. Larger files are skipped. Default is 50.0.",
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
    verify_local = getattr(args, "verify_local", False) or cfg.get("verify_local", False)
    workers = getattr(args, "workers", cfg.get("workers", 8))
    max_size_gb = getattr(args, "max_size_gb", cfg.get("max_size_gb", 50.0))

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
    log.info("Size Limit: %.1f GB", max_size_gb)

    try:
        if connection_string.startswith("http://") or connection_string.startswith("https://"):
            blob_service = BlobServiceClient(account_url=connection_string)
        else:
            blob_service = BlobServiceClient.from_connection_string(connection_string)
    except Exception as exc:
        log.error("Failed to connect to Azure or parse connection string. Please verify your configuration.")
        sys.exit(1)
        
    container_client = blob_service.get_container_client(container_name)
    max_size_bytes = int(max_size_gb * 1024**3)

    if args.classifier_root is not None:
        direction = "push" if args.push else "pull"
        stats = sync_classifier_artifacts(
            container_client,
            args.classifier_root,
            direction=direction,
            remote_base_prefix=prefix or "classifier",
            include_exports=not args.skip_exports,
            max_workers=workers,
            max_size_bytes=max_size_bytes,
            dry_run=args.dry_run,
        )
    elif args.push:
        stats = push_directory(
            container_client,
            local_dir,
            remote_prefix=prefix,
            max_workers=workers,
            max_size_bytes=max_size_bytes,
            dry_run=args.dry_run,
        )
    else:
        stats = sync_container(
            container_client,
            local_dir,
            prefix=prefix,
            delete_orphaned=delete_orphaned,
            max_workers=workers,
            max_size_bytes=max_size_bytes,
            verify_local_files=verify_local,
        )

    # ── Summary ─────────────────────────────────────────────────
    table = Table(title="Sync Complete")
    table.add_column("Action", style="cyan")
    table.add_column("Count", justify="right", style="green")

    if "uploaded" in stats and (args.push or stats.get("uploaded", 0) > 0):
        table.add_row("Uploaded", str(stats.get("uploaded", 0)))
    if "downloaded" in stats and (not args.push or stats.get("downloaded", 0) > 0):
        table.add_row("Downloaded", str(stats.get("downloaded", 0)))
    table.add_row("Skipped", str(stats.get("skipped", 0)))
    table.add_row("Deleted", str(stats.get("deleted", 0)))
    table.add_row("Errors", f"[red]{stats['errors']}[/red]" if stats.get("errors", 0) > 0 else "0")

    console.print()
    console.print(Panel(table, expand=False, border_style="blue"))

    if stats["errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

