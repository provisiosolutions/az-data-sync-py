"""Fast, cached local & Azure diff inspection helpers for the HearMe Sync TUI."""

from __future__ import annotations

import concurrent.futures
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

from sync import (
    MANIFEST_FILE,
    PUSH_MANIFEST_FILE,
    _IGNORED_FILENAMES,
    _normalize_etag,
    load_manifest,
)

_ACCOUNT_NAME_RE = re.compile(r"(?:^|;)AccountName=([^;]+)", re.IGNORECASE)
_SKIP_SCAN_FILES = _IGNORED_FILENAMES | {"review.html"}


@dataclass(frozen=True)
class BucketSpec:
    """Configuration for one managed local <-> Azure storage bucket."""

    key: str
    title: str
    container: str
    local_dir: Path
    pull_only: bool
    delete_orphaned_on_push: bool
    subtitle: str


@dataclass
class LocalScanResult:
    """Snapshot of all real files inside a local bucket directory."""

    exists: bool
    files: dict[str, tuple[int, int]] = field(default_factory=dict)
    total_bytes: int = 0
    latest_mtime: float | None = None

    @property
    def file_count(self) -> int:
        return len(self.files)

    @property
    def latest_iso(self) -> str | None:
        if self.latest_mtime is None:
            return None
        return datetime.fromtimestamp(self.latest_mtime, tz=timezone.utc).isoformat()


@dataclass
class DatasetSummary:
    """Training corpus summary parsed from raw-sessions manifest & lexicon."""

    has_manifest: bool = False
    total_clips: int = 0
    phrase_count: int = 0
    total_duration_s: float = 0.0
    by_session: dict[str, dict] = field(default_factory=dict)


@dataclass
class HeadsAndUpstreamSummary:
    """Summary of staged speaker heads plus upstream classifier drift."""

    profiles: dict[str, dict] = field(default_factory=dict)
    classifier_present: bool = False
    needs_staging: bool = False
    staging_notes: list[str] = field(default_factory=list)


@dataclass
class BucketDiff:
    """Comparison between local files and remote Azure blobs for one bucket."""

    spec: BucketSpec
    local_count: int
    local_bytes: int
    local_latest_iso: str | None
    remote_count: int | None = None
    remote_bytes: int | None = None
    remote_latest_iso: str | None = None
    to_upload: list[str] = field(default_factory=list)
    to_download: list[str] = field(default_factory=list)
    to_delete_remote: list[str] = field(default_factory=list)
    error: str | None = None
    offline: bool = False

    @property
    def status_label(self) -> str:
        if self.error:
            return "⚠ Error"
        if self.offline:
            return "○ Offline"
        up = len(self.to_upload)
        down = len(self.to_download)
        if up == 0 and down == 0 and len(self.to_delete_remote) == 0:
            if self.local_count == 0 and (self.remote_count or 0) == 0:
                return "● Empty (Ready)"
            return "● In Sync"
        if up > 0 and down > 0:
            return f"⇅ {up}↑ / {down}↓"
        if up > 0:
            return f"↑ {up} to push"
        if down > 0:
            return f"↓ {down} to pull"
        return f"✕ {len(self.to_delete_remote)} to prune"


_DATASET_CACHE: dict[Path, tuple[tuple[int, int, int, int], DatasetSummary]] = {}


def default_bucket_specs(repo_root: Path) -> list[BucketSpec]:
    """Return the three canonical HearMe storage buckets in recommended order."""
    return [
        BucketSpec(
            key="raw-sessions",
            title="raw-sessions",
            container="raw-sessions",
            local_dir=repo_root / "raw-sessions",
            pull_only=False,
            delete_orphaned_on_push=True,
            subtitle="Kirk snake_case WAV corpus + manifest.jsonl & lexicon.json",
        ),
        BucketSpec(
            key="trained-heads",
            title="trained-heads",
            container="trained-heads",
            local_dir=repo_root / "trained-heads",
            pull_only=False,
            delete_orphaned_on_push=False,
            subtitle="Trained speaker heads (kirk 137-class & kirk_core 30-class)",
        ),
        BucketSpec(
            key="sessions",
            title="sessions (SWA)",
            container="sessions",
            local_dir=repo_root / "synced-files",
            pull_only=True,
            delete_orphaned_on_push=False,
            subtitle="HearMe web-app participant recordings (Pull-Only)",
        ),
    ]


def format_bytes(num_bytes: int | None) -> str:
    """Format byte count concisely for TUI cards and tables."""
    if num_bytes is None:
        return "—"
    value = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{int(value)} B" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{num_bytes} B"


def format_short_time(iso_str: str | None) -> str:
    """Format an ISO timestamp into short YYYY-MM-DD HH:MM UTC/local display."""
    if not iso_str:
        return "—"
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return iso_str[:16]


def scan_local_dir(local_dir: Path) -> LocalScanResult:
    """Scan local_dir iteratively with os.scandir() skipping symlinks and manifests."""
    if not local_dir.is_dir():
        return LocalScanResult(exists=False)

    files: dict[str, tuple[int, int]] = {}
    total_bytes = 0
    latest_mtime: float | None = None
    stack: list[tuple[Path, str]] = [(local_dir, "")]

    while stack:
        cur_dir, rel_prefix = stack.pop()
        try:
            with os.scandir(cur_dir) as it:
                entries = list(it)
        except OSError:
            continue

        for entry in entries:
            name = entry.name
            if name == "__pycache__" or entry.is_symlink():
                continue
            rel_posix = f"{rel_prefix}/{name}" if rel_prefix else name
            if entry.is_dir(follow_symlinks=False):
                stack.append((Path(entry.path), rel_posix))
                continue
            if not entry.is_file(follow_symlinks=False):
                continue
            if name in _SKIP_SCAN_FILES or name.endswith(".tmp"):
                continue
            stat = entry.stat(follow_symlinks=False)
            files[rel_posix] = (stat.st_size, stat.st_mtime_ns)
            total_bytes += stat.st_size
            if latest_mtime is None or stat.st_mtime > latest_mtime:
                latest_mtime = stat.st_mtime

    return LocalScanResult(
        exists=True,
        files=files,
        total_bytes=total_bytes,
        latest_mtime=latest_mtime,
    )


def get_dataset_summary(raw_sessions_dir: Path) -> DatasetSummary:
    """Parse raw-sessions/manifest.jsonl & lexicon.json with mtime-keyed caching."""
    manifest_path = raw_sessions_dir / "manifest.jsonl"
    lexicon_path = raw_sessions_dir / "lexicon.json"
    if not manifest_path.is_file():
        return DatasetSummary(has_manifest=False)

    m_stat = manifest_path.stat()
    l_stat = lexicon_path.stat() if lexicon_path.is_file() else None
    cache_key = (
        m_stat.st_size,
        m_stat.st_mtime_ns,
        l_stat.st_size if l_stat else 0,
        l_stat.st_mtime_ns if l_stat else 0,
    )
    resolved = raw_sessions_dir.resolve()
    cached = _DATASET_CACHE.get(resolved)
    if cached and cached[0] == cache_key:
        return cached[1]

    by_session: dict[str, dict] = {}
    labels: set[str] = set()
    total_clips = 0
    total_duration = 0.0

    for line in manifest_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        sid = str(row.get("session_id") or "unknown")
        label = str(row.get("label") or "")
        if label:
            labels.add(label)
        dur = float(row.get("duration_s") or 0.0)
        total_clips += 1
        total_duration += dur
        bucket = by_session.setdefault(
            sid,
            {"count": 0, "duration_s": 0.0, "sample_rate": row.get("sample_rate"), "channels": row.get("channels")},
        )
        bucket["count"] += 1
        bucket["duration_s"] += dur

    phrase_count = len(labels)
    if lexicon_path.is_file():
        try:
            lex_data = json.loads(lexicon_path.read_text(encoding="utf-8"))
            if isinstance(lex_data, dict) and isinstance(lex_data.get("phrases"), list):
                phrase_count = len(lex_data["phrases"])
        except (json.JSONDecodeError, OSError):
            pass

    summary = DatasetSummary(
        has_manifest=True,
        total_clips=total_clips,
        phrase_count=phrase_count,
        total_duration_s=total_duration,
        by_session=dict(sorted(by_session.items())),
    )
    _DATASET_CACHE[resolved] = (cache_key, summary)
    return summary


def get_heads_and_upstream_summary(
    trained_heads_dir: Path,
    raw_sessions_dir: Path,
    classifier_root: Path,
) -> HeadsAndUpstreamSummary:
    """Inspect staged speaker heads and check classifier_root for un-staged updates."""
    profiles: dict[str, dict] = {}
    if trained_heads_dir.is_dir():
        for child in sorted(trained_heads_dir.iterdir()):
            if not child.is_dir() or child.name.startswith("."):
                continue
            head_path = child / "head.pt"
            labels_path = child / "labels.json"
            if not head_path.is_file() and not labels_path.is_file():
                continue
            label_count = 0
            if labels_path.is_file():
                try:
                    loaded = json.loads(labels_path.read_text(encoding="utf-8"))
                    if isinstance(loaded, list):
                        label_count = len(loaded)
                except (json.JSONDecodeError, OSError):
                    pass
            h_stat = head_path.stat() if head_path.is_file() else None
            profiles[child.name] = {
                "label_count": label_count,
                "head_size_bytes": h_stat.st_size if h_stat else 0,
                "head_mtime_iso": (
                    datetime.fromtimestamp(h_stat.st_mtime, tz=timezone.utc).isoformat()
                    if h_stat
                    else None
                ),
            }

    if not classifier_root.is_dir():
        return HeadsAndUpstreamSummary(profiles=profiles, classifier_present=False)

    notes: list[str] = []
    edited_dir = classifier_root / "Edited_Clips"
    staged_edited_manifest = raw_sessions_dir / "session_2026_11_18_edited_clips" / "session_manifest.jsonl"
    if edited_dir.is_dir():
        edited_wavs = [
            p for p in edited_dir.iterdir() if p.is_file() and p.suffix.lower() == ".wav"
        ]
        if edited_wavs and not staged_edited_manifest.is_file():
            notes.append(f"{len(edited_wavs)} Edited_Clips WAVs not yet staged")
        elif edited_wavs and staged_edited_manifest.is_file():
            staged_sizes: dict[str, int] = {}
            for ln in staged_edited_manifest.read_text(encoding="utf-8").splitlines():
                ln = ln.strip()
                if not ln:
                    continue
                row = json.loads(ln)
                orig = str(row.get("original_filename") or "")
                if orig:
                    staged_sizes[orig] = int(row.get("size") or 0)
            source_sizes = {p.name: p.stat().st_size for p in edited_wavs}
            if source_sizes != staged_sizes:
                notes.append(
                    f"Edited_Clips changed ({len(source_sizes)} source vs {len(staged_sizes)} staged)"
                )

    upstream_profiles = classifier_root / "profiles"
    if upstream_profiles.is_dir():
        changed_heads = 0
        for root, dirs, files in os.walk(upstream_profiles, followlinks=False):
            root_path = Path(root)
            dirs[:] = [
                d for d in dirs if d not in {"__pycache__", "audio_data"} and not (root_path / d).is_symlink()
            ]
            for fname in files:
                if fname in _IGNORED_FILENAMES or fname.endswith(".tmp"):
                    continue
                src = root_path / fname
                if src.is_symlink():
                    continue
                dst = trained_heads_dir / src.relative_to(upstream_profiles)
                src_stat = src.stat()
                if not dst.is_file():
                    changed_heads += 1
                else:
                    dst_stat = dst.stat()
                    if dst_stat.st_size != src_stat.st_size or src_stat.st_mtime_ns > dst_stat.st_mtime_ns:
                        changed_heads += 1
        if changed_heads > 0:
            notes.append(f"{changed_heads} updated profile head file(s) ready to stage")

    return HeadsAndUpstreamSummary(
        profiles=profiles,
        classifier_present=True,
        needs_staging=bool(notes),
        staging_notes=notes,
    )


def _needs_upload_fast(
    file_size: int,
    file_mtime_ns: int,
    push_entry: dict | None,
    sync_entry: dict | None,
    remote_blob,
) -> bool:
    if push_entry is not None:
        if push_entry.get("size") == file_size and push_entry.get("mtime_ns") == file_mtime_ns:
            saved_etag = _normalize_etag(push_entry.get("etag"))
            remote_etag = _normalize_etag(getattr(remote_blob, "etag", None))
            if remote_blob is None or not saved_etag or remote_etag == saved_etag:
                return False

    if sync_entry is not None and remote_blob is not None:
        sync_mtime_ns = sync_entry.get("mtime_ns")
        if sync_entry.get("size") == file_size and (
            sync_mtime_ns is None or sync_mtime_ns == file_mtime_ns
        ):
            saved_sync_etag = _normalize_etag(sync_entry.get("etag"))
            remote_etag = _normalize_etag(getattr(remote_blob, "etag", None))
            if saved_sync_etag and remote_etag == saved_sync_etag:
                return False

    if remote_blob is not None and push_entry is None:
        remote_mtime = getattr(remote_blob, "last_modified", None)
        local_mtime = datetime.fromtimestamp(file_mtime_ns / 1e9, tz=timezone.utc)
        if getattr(remote_blob, "size", None) == file_size and remote_mtime and remote_mtime >= local_mtime:
            return False

    return True


def _needs_download_fast(
    blob,
    local_exists: bool,
    sync_entry: dict | None,
    push_entry: dict | None,
    local_meta: tuple[int, int] | None,
) -> bool:
    remote_etag = _normalize_etag(getattr(blob, "etag", None))
    if local_exists and local_meta is not None and push_entry is not None:
        file_size, file_mtime_ns = local_meta
        if (
            push_entry.get("size") == file_size
            and push_entry.get("mtime_ns") == file_mtime_ns
            and remote_etag
            and _normalize_etag(push_entry.get("etag")) == remote_etag
        ):
            return False

    if not local_exists:
        return True
    if sync_entry is None:
        return True
    if remote_etag and _normalize_etag(sync_entry.get("etag")) == remote_etag:
        return False

    remote_mtime = getattr(blob, "last_modified", None)
    local_mtime_str = sync_entry.get("last_modified")
    if remote_mtime and local_mtime_str:
        if remote_mtime <= datetime.fromisoformat(local_mtime_str):
            return False
    return True


def compute_bucket_diff(
    spec: BucketSpec,
    local_scan: LocalScanResult,
    remote_blobs: list | None,
    error: str | None = None,
) -> BucketDiff:
    """Compute exact pending uploads, downloads, and remote orphans for one bucket."""
    diff = BucketDiff(
        spec=spec,
        local_count=local_scan.file_count,
        local_bytes=local_scan.total_bytes,
        local_latest_iso=local_scan.latest_iso,
        error=error,
        offline=(remote_blobs is None and error is None),
    )
    if remote_blobs is None:
        return diff

    sync_manifest = load_manifest(spec.local_dir, MANIFEST_FILE) if spec.local_dir.is_dir() else {}
    push_manifest = load_manifest(spec.local_dir, PUSH_MANIFEST_FILE) if spec.local_dir.is_dir() else {}

    remote_by_name: dict[str, object] = {}
    remote_bytes = 0
    latest_remote: datetime | None = None
    for blob in remote_blobs:
        if blob.name.endswith("/"):
            continue
        remote_by_name[blob.name] = blob
        remote_bytes += int(getattr(blob, "size", 0) or 0)
        lm = getattr(blob, "last_modified", None)
        if lm is not None and (latest_remote is None or lm > latest_remote):
            latest_remote = lm

    diff.remote_count = len(remote_by_name)
    diff.remote_bytes = remote_bytes
    diff.remote_latest_iso = latest_remote.isoformat() if latest_remote else None

    for blob_name in sorted(remote_by_name):
        blob = remote_by_name[blob_name]
        local_meta = local_scan.files.get(blob_name)
        if _needs_download_fast(
            blob,
            local_exists=local_meta is not None,
            sync_entry=sync_manifest.get(blob_name),
            push_entry=push_manifest.get(blob_name),
            local_meta=local_meta,
        ):
            diff.to_download.append(blob_name)

    if not spec.pull_only:
        for rel_path in sorted(local_scan.files):
            file_size, file_mtime_ns = local_scan.files[rel_path]
            if _needs_upload_fast(
                file_size,
                file_mtime_ns,
                push_entry=push_manifest.get(rel_path),
                sync_entry=sync_manifest.get(rel_path),
                remote_blob=remote_by_name.get(rel_path),
            ):
                diff.to_upload.append(rel_path)

        if spec.delete_orphaned_on_push:
            diff.to_delete_remote = sorted(
                name for name in remote_by_name if name not in local_scan.files
            )

    return diff


def extract_account_label(connection_string: str) -> str:
    """Return non-secret storage account label from a connection string or URL."""
    match = _ACCOUNT_NAME_RE.search(connection_string)
    if match:
        return match.group(1)
    if connection_string.startswith(("http://", "https://")):
        return urlparse(connection_string).netloc.split(".")[0] or "azure-sas"
    return "azure-storage"


def resolve_blob_service(config_path: Path) -> tuple[BlobServiceClient | None, str, str | None]:
    """Create a reusable BlobServiceClient from config_path or env without raising."""
    conn_str = ""
    if config_path.is_file():
        try:
            cfg = json.loads(config_path.read_text(encoding="utf-8"))
            conn_str = str(cfg.get("connection_string") or "")
        except (json.JSONDecodeError, OSError) as exc:
            return None, "Config Error", f"Invalid {config_path.name}: {exc}"

    if not conn_str:
        conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")

    if not conn_str:
        return (
            None,
            "Offline (No config.json)",
            "Copy config.example.json -> config.json (or set AZURE_STORAGE_CONNECTION_STRING) to sync with Azure.",
        )

    account_label = extract_account_label(conn_str)
    try:
        if conn_str.startswith(("http://", "https://")):
            client = BlobServiceClient(account_url=conn_str)
        else:
            client = BlobServiceClient.from_connection_string(conn_str)
        return client, account_label, None
    except Exception as exc:
        return None, f"{account_label} (Error)", f"Azure client error: {exc}"


def fetch_all_remote_diffs(
    blob_service: BlobServiceClient,
    specs: list[BucketSpec],
    local_scans: dict[str, LocalScanResult],
) -> dict[str, BucketDiff]:
    """Fetch remote blob listings concurrently across all buckets and compute diffs."""

    def _query_one(spec: BucketSpec) -> tuple[str, list | None, str | None]:
        try:
            cc = blob_service.get_container_client(spec.container)
            return spec.key, list(cc.list_blobs()), None
        except ResourceNotFoundError:
            return spec.key, [], None
        except Exception as exc:
            return spec.key, None, str(exc)

    results: dict[str, BucketDiff] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(specs))) as pool:
        futs = [pool.submit(_query_one, spec) for spec in specs]
        spec_by_key = {s.key: s for s in specs}
        for fut in concurrent.futures.as_completed(futs):
            key, blobs, err = fut.result()
            results[key] = compute_bucket_diff(spec_by_key[key], local_scans[key], blobs, error=err)
    return results
