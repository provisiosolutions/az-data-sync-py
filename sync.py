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
from contextlib import nullcontext
import hashlib
import json
import logging
import os
import re
import shutil
import sys
from typing import Callable
import uuid
import wave
from datetime import datetime, timezone
from pathlib import Path

from azure.core.exceptions import ResourceExistsError
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


def _normalize_etag(etag: str | None) -> str | None:
    """Strip surrounding HTTP double quotes so upload_blob and list_blobs ETags match."""
    if not etag or not isinstance(etag, str):
        return None
    return etag.strip('"') or None


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
    remote_etag = _normalize_etag(getattr(blob_props, "etag", None))
    if remote_etag and _normalize_etag(manifest_entry.get("etag")) == remote_etag:
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
    quiet: bool = False,
    progress_callback: Callable[[int, int, str], None] | None = None,
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
            if entry is not None and "mtime_ns" not in entry and dest_path.exists():
                new_manifest[blob_name] = {**entry, "mtime_ns": dest_path.stat().st_mtime_ns}
            else:
                new_manifest[blob_name] = entry  # carry forward
            stats["skipped"] += 1
        else:
            to_download.append((blob, entry, dest_path))

    if to_download:
        total_items = len(to_download)
        done_count = 0
        progress_ctx = (
            nullcontext(None)
            if quiet
            else Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                console=console,
            )
        )
        with progress_ctx as progress:
            task_id = (
                progress.add_task("[cyan]Downloading files...", total=total_items)
                if progress is not None
                else None
            )

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all download tasks
                future_to_blob = {
                    executor.submit(
                        _download_blob, container_client, blob.name, dest_path
                    ): (blob, entry, dest_path)
                    for blob, entry, dest_path in to_download
                }

                # Handle results as they complete
                for future in concurrent.futures.as_completed(future_to_blob):
                    blob, entry, dest_path = future_to_blob[future]
                    blob_name = blob.name

                    try:
                        future.result()  # Will raise if _download_blob threw an exception
                        new_manifest[blob_name] = {
                            "etag": blob.etag,
                            "last_modified": blob.last_modified.isoformat() if blob.last_modified else None,
                            "size": blob.size,
                            "mtime_ns": dest_path.stat().st_mtime_ns if dest_path.exists() else None,
                            "synced_at": datetime.now(timezone.utc).isoformat(),
                        }
                        stats["downloaded"] += 1
                    except Exception as exc:
                        log.error("FAIL  %s  → %s", blob_name, exc)
                        # Keep the old manifest entry so we retry next run
                        if entry:
                            new_manifest[blob_name] = entry
                        stats["errors"] += 1

                    done_count += 1
                    if progress is not None and task_id is not None:
                        progress.advance(task_id)
                    if progress_callback is not None:
                        progress_callback(done_count, total_items, blob_name)

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

def should_upload(
    local_path: Path,
    push_entry: dict | None,
    remote_blob=None,
    sync_entry: dict | None = None,
) -> bool:
    """Decide whether a local file needs to be uploaded to Azure Blob Storage."""
    stat = local_path.stat()
    if push_entry is not None:
        if (
            push_entry.get("size") == stat.st_size
            and push_entry.get("mtime_ns") == stat.st_mtime_ns
        ):
            saved_etag = _normalize_etag(push_entry.get("etag"))
            remote_etag = _normalize_etag(getattr(remote_blob, "etag", None))
            if remote_blob is None or not saved_etag or remote_etag == saved_etag:
                return False

    if sync_entry is not None and remote_blob is not None:
        sync_mtime_ns = sync_entry.get("mtime_ns")
        if sync_entry.get("size") == stat.st_size and (
            sync_mtime_ns is None or sync_mtime_ns == stat.st_mtime_ns
        ):
            saved_sync_etag = _normalize_etag(sync_entry.get("etag"))
            remote_etag = _normalize_etag(getattr(remote_blob, "etag", None))
            if saved_sync_etag and remote_etag == saved_sync_etag:
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
        return _normalize_etag(result.get("etag"))
    return _normalize_etag(getattr(result, "etag", None))


def push_directory(
    container_client: ContainerClient,
    local_dir: Path,
    remote_prefix: str = "",
    delete_orphaned: bool = False,
    max_workers: int = 8,
    max_size_bytes: int = 50 * 1024**3,
    dry_run: bool = False,
    quiet: bool = False,
    progress_callback: Callable[[int, int, str], None] | None = None,
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
    sync_manifest = load_manifest(local_dir, MANIFEST_FILE)
    new_manifest = dict(manifest)

    remote_blobs: dict[str, object] = {}
    try:
        for b in container_client.list_blobs(name_starts_with=norm_prefix or None):
            remote_blobs[b.name] = b
    except Exception as exc:
        log.debug("Could not pre-list remote blobs (%s); relying on local push manifest.", exc)

    to_upload: list[tuple[Path, str, os.stat_result, dict | None]] = []
    seen_local_blobs: set[str] = set()

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

            rel_posix = fpath.relative_to(local_dir).as_posix()
            blob_name = f"{norm_prefix}{rel_posix}"
            seen_local_blobs.add(blob_name)

            fstat = fpath.stat()
            if fstat.st_size > max_size_bytes:
                log.warning("SKIP  %s  (Exceeds max size limit of %d bytes)", fpath, max_size_bytes)
                stats["skipped"] += 1
                continue

            entry = manifest.get(blob_name)
            sync_entry = sync_manifest.get(blob_name)
            remote_blob = remote_blobs.get(blob_name)

            if not should_upload(fpath, entry, remote_blob=remote_blob, sync_entry=sync_entry):
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

    remote_orphans: list[str] = []
    if delete_orphaned:
        remote_orphans = sorted(
            name
            for name in remote_blobs
            if not name.endswith("/") and name not in seen_local_blobs
        )

    if dry_run:
        stats["uploaded"] = len(to_upload)
        stats["deleted"] = len(remote_orphans)
        return stats

    if to_upload:
        try:
            container_client.create_container()
        except ResourceExistsError:
            pass
        except Exception as exc:
            log.debug("Container create skipped or already exists (%s).", exc)

        total_items = len(to_upload)
        done_count = 0
        progress_ctx = (
            nullcontext(None)
            if quiet
            else Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                console=console,
            )
        )
        with progress_ctx as progress:
            task_id = (
                progress.add_task("[cyan]Uploading files...", total=total_items)
                if progress is not None
                else None
            )
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
                    done_count += 1
                    if progress is not None and task_id is not None:
                        progress.advance(task_id)
                    if progress_callback is not None:
                        progress_callback(done_count, total_items, blob_name)

    if delete_orphaned:
        for k in list(new_manifest):
            if (not norm_prefix or k.startswith(norm_prefix)) and k not in seen_local_blobs:
                new_manifest.pop(k, None)
        for orphan_name in remote_orphans:
            try:
                log.info("DEL   %s  (orphaned on local)", orphan_name)
                container_client.delete_blob(orphan_name)
                new_manifest.pop(orphan_name, None)
                stats["deleted"] += 1
            except Exception as exc:
                log.error("FAIL  DEL %s  → %s", orphan_name, exc)
                stats["errors"] += 1

    save_manifest(local_dir, new_manifest, PUSH_MANIFEST_FILE)
    return stats


_TAKE_SUFFIX_RE = re.compile(r"(?:\s*\(\d+\)|\s*-\s*\d+)+$")
_APOSTROPHES_TABLE = str.maketrans("", "", "'’")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_PRESERVED_DISPLAY_TOKENS = {
    "i": "I",
    "i'm": "I'm",
    "i'll": "I'll",
    "i'd": "I'd",
    "i've": "I've",
    "rockstar": "Rockstar",
}


def to_snake_case(raw: str) -> str:
    """Convert arbitrary phrase or identifier text into strict lowercase snake_case."""
    cleaned = raw.translate(_APOSTROPHES_TABLE).lower()
    slug = _NON_ALNUM_RE.sub("_", cleaned).strip("_")
    return slug or "clip"


def _normalize_display_text(raw: str) -> str:
    """Normalize display casing deterministically across clip filename variants."""
    tokens = raw.replace("_", " ").strip().split()
    if not tokens:
        return "Clip"
    normalized: list[str] = []
    for idx, token in enumerate(tokens):
        lower = token.lower()
        if lower in _PRESERVED_DISPLAY_TOKENS:
            normalized.append(_PRESERVED_DISPLAY_TOKENS[lower])
        elif idx == 0:
            normalized.append(token[0].upper() + token[1:].lower())
        else:
            normalized.append(lower)
    return " ".join(normalized)


def _load_classifier_lexicon_map(classifier_root: Path) -> dict[str, str]:
    """Load canonical snake_case label -> display_text mappings from dataset/lexicon.json and base_lexicon.txt."""
    lexicon_map: dict[str, str] = {}
    lexicon_json = classifier_root / "dataset" / "lexicon.json"
    if lexicon_json.is_file():
        try:
            payload = json.loads(lexicon_json.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            payload = {}
        for item in payload.get("phrases", []) if isinstance(payload, dict) else []:
            if isinstance(item, dict) and item.get("id") and item.get("display_text"):
                lexicon_map[to_snake_case(str(item["id"]))] = str(item["display_text"])

    base_lexicon = classifier_root / "base_lexicon.txt"
    if base_lexicon.is_file():
        for line in base_lexicon.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if text and not text.startswith("#"):
                lexicon_map.setdefault(to_snake_case(text), text)

    return lexicon_map


def parse_raw_clip_stem(
    stem: str,
    lexicon_map: dict[str, str] | None = None,
) -> tuple[str, str]:
    """Parse a raw clip filename stem into `(snake_case_label, canonical_display_text)`."""
    cleaned = _TAKE_SUFFIX_RE.sub("", stem.rstrip(".")).strip() or stem.strip()
    label = to_snake_case(cleaned)
    if label == "silence":
        return "silence", (lexicon_map or {}).get("silence", "Silence")
    if lexicon_map and label in lexicon_map:
        return label, lexicon_map[label]
    return label, _normalize_display_text(cleaned)


def _probe_wav_metadata(path: Path) -> dict[str, int | float | None]:
    """Read sample_rate, channels, and duration_s from PCM WAV headers."""
    if path.suffix.lower() != ".wav":
        return {"sample_rate": None, "channels": None, "duration_s": None}
    try:
        with wave.open(str(path), "rb") as wf:
            sr = wf.getframerate()
            ch = wf.getnchannels()
            nframes = wf.getnframes()
            dur = round(nframes / sr, 6) if sr > 0 else 0.0
            return {"sample_rate": sr, "channels": ch, "duration_s": dur}
    except (wave.Error, EOFError, OSError):
        return {"sample_rate": None, "channels": None, "duration_s": None}


def _copy_if_changed(src: Path, dst: Path) -> bool:
    """Copy src to dst only when dst is missing or file size/mtime differ."""
    src_stat = src.stat()
    if dst.exists():
        dst_stat = dst.stat()
        if dst_stat.st_size == src_stat.st_size and dst_stat.st_mtime_ns == src_stat.st_mtime_ns:
            return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return True


def _write_text_if_changed(dst: Path, content: str) -> None:
    """Write UTF-8 text to dst only when missing or content differs."""
    if dst.is_file() and dst.read_text(encoding="utf-8") == content:
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(content, encoding="utf-8")


def _sha256_file(path: Path) -> str:
    """Compute SHA-256 hex digest of a file using streaming reads."""
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def stage_classifier_raw_sessions(
    classifier_root: Path,
    dest_dir: Path,
    edited_clips_dir: Path | None = None,
    include_mp4: bool = False,
) -> dict:
    """
    Stage Kirk direct raw audio sessions from audio-classifier-hear-me into dest_dir
    using strict snake_case paths, self-contained training manifests, and lexicon metadata.
    """
    stats: dict = {
        "copied": 0,
        "unchanged": 0,
        "skipped_mp4": 0,
        "removed_stale": 0,
        "sessions": {},
    }
    clips_dir = (
        edited_clips_dir if edited_clips_dir is not None else (classifier_root / "Edited_Clips")
    )
    has_raw_edited_clips = clips_dir.is_dir() and any(
        p.is_file() and p.suffix.lower() == ".wav" for p in clips_dir.iterdir()
    )

    lexicon_map = _load_classifier_lexicon_map(classifier_root)
    expected_rel_paths: set[str] = set()
    all_manifest_rows: list[dict] = []
    observed_phrases: dict[str, str] = {}

    manifest_path = classifier_root / "dataset" / "manifest.jsonl"
    if manifest_path.is_file():
        grouped: dict[str, list[dict]] = {}
        for line in manifest_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            session_id = str(row.get("session_id") or "unknown")
            grouped.setdefault(session_id, []).append(row)

        for session_id in sorted(grouped):
            if session_id == "session_2026-11-18" and has_raw_edited_clips:
                continue

            snake_session_id = to_snake_case(session_id)
            rows = sorted(
                grouped[session_id],
                key=lambda r: (
                    to_snake_case(str(r.get("label") or "unknown")),
                    str(r.get("path") or ""),
                ),
            )
            staged_rows: list[dict] = []
            for row in rows:
                rel_path = str(row.get("path") or "")
                label = to_snake_case(str(row.get("label") or "unknown"))
                if not rel_path:
                    continue
                src_wav = classifier_root / "dataset" / rel_path
                if not src_wav.is_file():
                    continue

                orig_path = Path(rel_path)
                snake_filename = f"{to_snake_case(orig_path.stem)}{orig_path.suffix.lower()}"
                rel_dest = f"{snake_session_id}/{label}/{snake_filename}"
                if _copy_if_changed(src_wav, dest_dir / rel_dest):
                    stats["copied"] += 1
                else:
                    stats["unchanged"] += 1
                expected_rel_paths.add(rel_dest)

                display_text = str(
                    row.get("display_text")
                    or lexicon_map.get(label)
                    or _normalize_display_text(label)
                )
                observed_phrases[label] = display_text

                take_id = str(row.get("take_id") or "")
                if not take_id:
                    take_match = re.search(r"_(\d+)$", Path(snake_filename).stem)
                    take_id = take_match.group(1) if take_match else "01"

                wav_meta = None
                sample_rate = row.get("sample_rate")
                channels = row.get("channels")
                duration_s = row.get("duration_s")
                if sample_rate is None or channels is None or duration_s is None:
                    wav_meta = _probe_wav_metadata(src_wav)
                    if sample_rate is None:
                        sample_rate = wav_meta["sample_rate"]
                    if channels is None:
                        channels = wav_meta["channels"]
                    if duration_s is None:
                        duration_s = wav_meta["duration_s"]

                record: dict[str, object] = {
                    "path": rel_dest,
                    "session_id": snake_session_id,
                    "label": label,
                    "display_text": display_text,
                    "take_id": take_id,
                    "original_filename": orig_path.name,
                    "sample_rate": sample_rate,
                    "channels": channels,
                    "duration_s": duration_s,
                    "size": src_wav.stat().st_size,
                    "sha256": str(row.get("sha256") or _sha256_file(src_wav)),
                }
                for opt_key in ("recorded_at", "rms", "peak", "silence_ratio"):
                    if opt_key in row and row[opt_key] is not None:
                        record[opt_key] = row[opt_key]
                staged_rows.append(record)
                all_manifest_rows.append(record)

            stats["sessions"][snake_session_id] = len(staged_rows)
            session_manifest_rel = f"{snake_session_id}/session_manifest.jsonl"
            expected_rel_paths.add(session_manifest_rel)
            manifest_content = (
                "\n".join(json.dumps(r, sort_keys=True) for r in staged_rows) + "\n"
                if staged_rows
                else ""
            )
            _write_text_if_changed(dest_dir / session_manifest_rel, manifest_content)

    if has_raw_edited_clips:
        edited_session_id = "session_2026_11_18_edited_clips"
        edited_records: list[dict] = []
        label_counts: dict[str, int] = {}

        for fpath in sorted(clips_dir.iterdir(), key=lambda p: (p.name.lower(), p.name)):
            if fpath.name in _IGNORED_FILENAMES or fpath.is_symlink() or not fpath.is_file():
                continue
            suffix = fpath.suffix.lower()
            if suffix == ".mp4":
                if not include_mp4:
                    stats["skipped_mp4"] += 1
                    continue
            elif suffix != ".wav":
                continue

            label, display_text = parse_raw_clip_stem(fpath.stem, lexicon_map)
            label_counts[label] = label_counts.get(label, 0) + 1
            take_id = f"{label_counts[label]:02d}"
            snake_filename = f"{label}_{take_id}{suffix}"
            rel_dest = f"{edited_session_id}/{label}/{snake_filename}"

            if _copy_if_changed(fpath, dest_dir / rel_dest):
                stats["copied"] += 1
            else:
                stats["unchanged"] += 1
            expected_rel_paths.add(rel_dest)

            wav_meta = _probe_wav_metadata(fpath)
            observed_phrases[label] = display_text
            record = {
                "path": rel_dest,
                "session_id": edited_session_id,
                "label": label,
                "display_text": display_text,
                "take_id": take_id,
                "original_filename": fpath.name,
                "sample_rate": wav_meta["sample_rate"],
                "channels": wav_meta["channels"],
                "duration_s": wav_meta["duration_s"],
                "size": fpath.stat().st_size,
                "sha256": _sha256_file(fpath),
            }
            edited_records.append(record)
            all_manifest_rows.append(record)

        edited_records.sort(key=lambda r: (str(r["label"]), str(r["take_id"])))
        stats["sessions"][edited_session_id] = len(edited_records)
        edited_manifest_rel = f"{edited_session_id}/session_manifest.jsonl"
        expected_rel_paths.add(edited_manifest_rel)
        edited_manifest = (
            "\n".join(json.dumps(r, sort_keys=True) for r in edited_records) + "\n"
            if edited_records
            else ""
        )
        _write_text_if_changed(dest_dir / edited_manifest_rel, edited_manifest)

    if all_manifest_rows:
        all_manifest_rows.sort(
            key=lambda r: (
                str(r["session_id"]),
                str(r["label"]),
                str(r["take_id"]),
                str(r["path"]),
            )
        )
        root_manifest_content = (
            "\n".join(json.dumps(r, sort_keys=True) for r in all_manifest_rows) + "\n"
        )
        _write_text_if_changed(dest_dir / "manifest.jsonl", root_manifest_content)
        expected_rel_paths.add("manifest.jsonl")

        lexicon_payload = {
            "phrases": [
                {"id": key, "display_text": observed_phrases[key]}
                for key in sorted(observed_phrases)
            ]
        }
        _write_text_if_changed(
            dest_dir / "lexicon.json",
            json.dumps(lexicon_payload, indent=2) + "\n",
        )
        expected_rel_paths.add("lexicon.json")

    if dest_dir.is_dir():
        for root, _dirs, files in os.walk(dest_dir, topdown=False, followlinks=False):
            root_path = Path(root)
            for fname in files:
                if fname in {MANIFEST_FILE, PUSH_MANIFEST_FILE}:
                    continue
                fpath = root_path / fname
                rel_posix = fpath.relative_to(dest_dir).as_posix()
                if rel_posix not in expected_rel_paths:
                    fpath.unlink()
                    stats["removed_stale"] += 1
            if root_path != dest_dir and not any(root_path.iterdir()):
                root_path.rmdir()

    return stats


def _make_phrase_list_payload(
    *,
    seed_key: str,
    title: str,
    slug: str,
    description: str,
    tags: list[str],
    phrases: list[tuple[str, str]],
    timestamp_iso: str,
) -> dict:
    """Build one HearMe SWA PhraseList JSON payload with deterministic UUIDv5 keys."""
    list_id = str(uuid.uuid5(uuid.NAMESPACE_URL, seed_key))
    return {
        "id": list_id,
        "title": title,
        "slug": slug,
        "description": description,
        "difficulty": "standard",
        "isPublished": True,
        "createdAt": timestamp_iso,
        "updatedAt": timestamp_iso,
        "createdBy": None,
        "modifiedBy": None,
        "publishedAt": timestamp_iso,
        "publishedBy": None,
        "tags": tags,
        "allowedUsers": [],
        "phrases": [
            {
                "id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"hearme:phrase:{list_id}:{phrase_id}")),
                "text": display_text,
                "orderIndex": idx,
                "audioUrl": None,
                "audioSource": None,
                "notes": None,
                "createdAt": timestamp_iso,
            }
            for idx, (phrase_id, display_text) in enumerate(phrases)
        ],
    }


def build_swa_phrase_seeds(
    lexicon_path: Path,
    core_labels_path: Path | None = None,
    timestamp_iso: str | None = None,
) -> dict:
    """
    Build SWA PhraseList (`phrase-lists/{id}/list.json`) and PhraseSet (`phrases/*.json`)
    seed payloads from a classifier lexicon.json and optional core labels list.
    """
    ts = timestamp_iso or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    raw_lexicon = json.loads(lexicon_path.read_text(encoding="utf-8"))
    raw_items = raw_lexicon.get("phrases", []) if isinstance(raw_lexicon, dict) else []

    full_phrases: list[tuple[str, str]] = []
    display_by_id: dict[str, str] = {}
    for item in raw_items:
        if not isinstance(item, dict) or not item.get("id"):
            continue
        phrase_id = to_snake_case(str(item["id"]))
        if phrase_id == "silence" or phrase_id in display_by_id:
            continue
        display_text = str(item.get("display_text") or _normalize_display_text(phrase_id))
        display_by_id[phrase_id] = display_text
        full_phrases.append((phrase_id, display_text))

    phrase_lists: dict[str, dict] = {}
    phrase_sets: dict[str, dict] = {}

    if full_phrases:
        full_title = f"Kirk - Full Classifier Lexicon ({len(full_phrases)})"
        full_slug = "kirk-full-classifier-lexicon"
        full_list = _make_phrase_list_payload(
            seed_key="hearme:phrase-list:kirk-full-lexicon-v1",
            title=full_title,
            slug=full_slug,
            description="Complete Kirk classifier spoken phrase lexicon",
            tags=["kirk", "lexicon"],
            phrases=full_phrases,
            timestamp_iso=ts,
        )
        phrase_lists[f"{full_list['id']}/list.json"] = full_list
        phrase_sets["kirk-full-lexicon.json"] = {
            "topic": full_title,
            "slug": full_slug,
            "createdAt": ts,
            "phrases": [text for _, text in full_phrases],
            "phrasesPerSession": 20,
        }

    core_phrases: list[tuple[str, str]] = []
    if core_labels_path is not None and core_labels_path.is_file():
        raw_core = json.loads(core_labels_path.read_text(encoding="utf-8"))
        seen_core: set[str] = set()
        for raw_label in raw_core if isinstance(raw_core, list) else []:
            if not isinstance(raw_label, str) or not raw_label.strip():
                continue
            phrase_id = to_snake_case(raw_label)
            if phrase_id == "silence" or phrase_id in seen_core:
                continue
            seen_core.add(phrase_id)
            display_text = display_by_id.get(phrase_id) or _normalize_display_text(phrase_id)
            core_phrases.append((phrase_id, display_text))

    if core_phrases:
        core_title = f"Kirk - Core Session Phrases ({len(core_phrases)})"
        core_slug = "kirk-core-session-phrases"
        core_list = _make_phrase_list_payload(
            seed_key="hearme:phrase-list:kirk-core-phrases-v1",
            title=core_title,
            slug=core_slug,
            description="Kirk core spoken phrase lexicon",
            tags=["kirk", "core"],
            phrases=core_phrases,
            timestamp_iso=ts,
        )
        phrase_lists[f"{core_list['id']}/list.json"] = core_list
        phrase_sets["kirk-core-phrases.json"] = {
            "topic": core_title,
            "slug": core_slug,
            "createdAt": ts,
            "phrases": [text for _, text in core_phrases],
            "phrasesPerSession": len(core_phrases),
        }

    return {"phrase_lists": phrase_lists, "phrase_sets": phrase_sets}


def stage_trained_heads(classifier_root: Path, dest_dir: Path) -> dict:
    """
    Stage trained speaker heads and profile metadata from classifier_root/profiles/
    into dest_dir while skipping audio_data directories and symlinks.
    """
    stats: dict = {"copied": 0, "unchanged": 0, "profiles": {}}
    profiles_root = classifier_root / "profiles"
    if not profiles_root.is_dir():
        return stats

    for root, dirs, files in os.walk(profiles_root, followlinks=False):
        root_path = Path(root)
        dirs[:] = [
            d
            for d in sorted(dirs)
            if d not in {"__pycache__", "audio_data"} and not (root_path / d).is_symlink()
        ]
        for fname in sorted(files):
            if fname in _IGNORED_FILENAMES or fname.endswith(".tmp"):
                continue
            fpath = root_path / fname
            if fpath.is_symlink():
                continue
            rel = fpath.relative_to(profiles_root)
            profile_name = rel.parts[0]
            if _copy_if_changed(fpath, dest_dir / rel):
                stats["copied"] += 1
            else:
                stats["unchanged"] += 1
            stats["profiles"][profile_name] = stats["profiles"].get(profile_name, 0) + 1

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
    and model checkpoints/exports (best_model, best_model_core, Core ML, ONNX) to/from Azure Blob Storage.
    """
    base_prefix = remote_base_prefix.strip("/")
    targets: list[tuple[Path, str]] = [
        (classifier_root / "dataset", f"{base_prefix}/dataset"),
        (classifier_root / "profiles", f"{base_prefix}/profiles"),
        (classifier_root / "results" / "best_model", f"{base_prefix}/results/best_model"),
        (classifier_root / "results" / "best_model_core", f"{base_prefix}/results/best_model_core"),
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

        # Ensure per-class symlinks profiles/kirk_core/audio_data/<label> -> ../../../dataset/train/<label> exist
        kirk_core_dir = classifier_root / "profiles" / "kirk_core"
        kirk_core_labels = kirk_core_dir / "labels.json"
        if kirk_core_labels.is_file() and train_dir.is_dir():
            try:
                labels = json.loads(kirk_core_labels.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                labels = []
            if isinstance(labels, list):
                kirk_core_audio = kirk_core_dir / "audio_data"
                kirk_core_audio.mkdir(parents=True, exist_ok=True)
                for label in labels:
                    if not isinstance(label, str) or not label:
                        continue
                    target_label_dir = train_dir / label
                    link_path = kirk_core_audio / label
                    if target_label_dir.is_dir() and not link_path.exists() and not link_path.is_symlink():
                        link_path.symlink_to(Path("../../../dataset/train") / label)

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
        "--stage-from-classifier",
        type=Path,
        default=None,
        help="Stage Kirk raw session WAVs into --raw-sessions-dir and trained speaker heads into --trained-heads-dir from audio-classifier-hear-me.",
    )
    p.add_argument(
        "--raw-sessions-dir",
        type=Path,
        default=Path("./raw-sessions"),
        help="Local staging directory for raw Kirk sessions. Default: ./raw-sessions.",
    )
    p.add_argument(
        "--trained-heads-dir",
        type=Path,
        default=Path("./trained-heads"),
        help="Local staging directory for trained speaker heads. Default: ./trained-heads.",
    )
    p.add_argument(
        "--include-mp4",
        action="store_true",
        help="Include .mp4 video files from Edited_Clips when staging raw sessions.",
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
    p.add_argument(
        "--tui",
        action="store_true",
        help="Launch the interactive Textual terminal dashboard.",
    )
    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.tui:
        from tui import run_tui

        run_tui(
            config_path=Path(args.config) if args.config else Path("config.json"),
            classifier_root=args.classifier_root
            or args.stage_from_classifier
            or Path("../audio-classifier-hear-me"),
        )
        return

    if (
        args.stage_from_classifier is not None
        and not args.push
        and not args.container
        and not args.config
    ):
        log.info(
            "Staging raw sessions and trained heads from %s …",
            args.stage_from_classifier.resolve(),
        )
        raw_stats = stage_classifier_raw_sessions(
            args.stage_from_classifier,
            args.raw_sessions_dir,
            include_mp4=args.include_mp4,
        )
        head_stats = stage_trained_heads(
            args.stage_from_classifier,
            args.trained_heads_dir,
        )
        stage_table = Table(title="Local Staging Complete")
        stage_table.add_column("Bucket", style="cyan")
        stage_table.add_column("Details", style="green")
        raw_sessions_summary = (
            ", ".join(f"{sid}: {cnt}" for sid, cnt in sorted(raw_stats["sessions"].items()))
            or "none"
        )
        stage_table.add_row(
            f"raw-sessions ({args.raw_sessions_dir})",
            f"copied={raw_stats['copied']}, unchanged={raw_stats['unchanged']}, "
            f"skipped_mp4={raw_stats['skipped_mp4']}, removed_stale={raw_stats['removed_stale']} | {raw_sessions_summary}",
        )
        heads_summary = (
            ", ".join(f"{pname}: {cnt}" for pname, cnt in sorted(head_stats["profiles"].items()))
            or "none"
        )
        stage_table.add_row(
            f"trained-heads ({args.trained_heads_dir})",
            f"copied={head_stats['copied']}, unchanged={head_stats['unchanged']} | {heads_summary}",
        )
        console.print()
        console.print(Panel(stage_table, expand=False, border_style="blue"))
        return

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
            delete_orphaned=delete_orphaned,
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

