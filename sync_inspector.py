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

from review_sessions import canonicalize_phrase_id, discover_sessions
from sync import (
    MANIFEST_FILE,
    PUSH_MANIFEST_FILE,
    _IGNORED_FILENAMES,
    _normalize_etag,
    load_manifest,
    parse_raw_clip_stem,
)

_ACCOUNT_NAME_RE = re.compile(r"(?:^|;)AccountName=([^;]+)", re.IGNORECASE)
_SKIP_SCAN_FILES = _IGNORED_FILENAMES | {"review.html", "hearme_dashboard.html", "hearme_powerbi.xlsx"}
_SESSION_DATE_RE = re.compile(r"^session_\d{4}_(\d{2}_\d{2})(?:_|$)")
_HEAD_FILE_ROLES = {
    "head.pt": "Head Weights",
    "labels.json": "Class Labels",
    "profile_manifest.json": "Profile Manifest",
    "speaker_model.json": "Speaker Centroid",
    "calibration.json": "Calibration",
    "decision_model.json": "Decision Model",
    "take_provenance.json": "Provenance",
}


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


@dataclass(frozen=True)
class RawWordStat:
    """Per-phrase corpus metrics parsed from raw-sessions manifest & lexicon."""

    label: str
    display_text: str
    clip_count: int
    total_duration_s: float
    sessions: tuple[str, ...]
    sample_rates: tuple[int, ...]


@dataclass(frozen=True)
class ClipMeta:
    """Compact audio metadata for one local raw-session WAV file."""

    label: str
    display_text: str
    session_id: str
    duration_s: float
    sample_rate: int
    channels: int


@dataclass(frozen=True)
class HeadWordStat:
    """Per-class summary across staged trained speaker heads."""

    label: str
    display_text: str
    profiles: tuple[str, ...]
    take_count: int
    threshold: float | None


@dataclass(frozen=True)
class SwaWordStat:
    """Per-phrase summary aggregated across pulled HearMe SWA sessions."""

    phrase_id: str
    display_text: str
    recording_count: int
    session_count: int
    sample_feedback: str


@dataclass
class SwaSummary:
    """Summary of pulled HearMe SWA participant sessions."""

    session_count: int = 0
    recording_count: int = 0
    words: list[SwaWordStat] = field(default_factory=list)
    file_phrases: dict[str, tuple[str, str]] = field(default_factory=dict)


@dataclass(frozen=True)
class FileInspectorRow:
    """Uniform row model for the right-pane Files table across all buckets."""

    path: str
    category: str
    details: str
    size_bytes: int
    sync_state: str


@dataclass
class DatasetSummary:
    """Training corpus summary parsed from raw-sessions manifest & lexicon."""

    has_manifest: bool = False
    total_clips: int = 0
    phrase_count: int = 0
    recorded_phrase_count: int = 0
    total_duration_s: float = 0.0
    by_session: dict[str, dict] = field(default_factory=dict)
    words: list[RawWordStat] = field(default_factory=list)
    clips_by_path: dict[str, ClipMeta] = field(default_factory=dict)


@dataclass
class HeadsAndUpstreamSummary:
    """Summary of staged speaker heads plus upstream classifier drift."""

    profiles: dict[str, dict] = field(default_factory=dict)
    words: list[HeadWordStat] = field(default_factory=list)
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
    remote_files: dict[str, tuple[int, str | None]] = field(default_factory=dict)
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
_HEADS_CACHE: dict[Path, tuple[tuple[tuple[str, int, int], ...], dict[str, dict], list[HeadWordStat]]] = {}
_SWA_CACHE: dict[Path, tuple[tuple[int, int, float | None], SwaSummary]] = {}


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


def _short_session_tag(session_id: str) -> str:
    """Shorten a session_YYYY_MM_DD[_...] identifier for compact word tables."""
    match = _SESSION_DATE_RE.match(session_id)
    if match:
        return match.group(1)
    return session_id.removeprefix("session_") or session_id


def _label_to_display(label: str) -> str:
    """Convert a snake_case phrase label into readable display text."""
    cleaned = label.replace("_", " ").strip()
    return cleaned.capitalize() if cleaned else "Unknown"


def _safe_read_json(path: Path):
    """Read and parse JSON if file exists and is valid, otherwise return None."""
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


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

    lex_display: dict[str, str] = {}
    lex_data = _safe_read_json(lexicon_path)
    if isinstance(lex_data, dict) and isinstance(lex_data.get("phrases"), list):
        for entry in lex_data["phrases"]:
            if isinstance(entry, dict) and entry.get("id"):
                pid = str(entry["id"])
                lex_display[pid] = str(entry.get("display_text") or _label_to_display(pid))

    by_session: dict[str, dict] = {}
    clips_by_path: dict[str, ClipMeta] = {}
    word_agg: dict[str, dict] = {}
    total_clips = 0
    total_duration = 0.0

    for line in manifest_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        rel_path = str(row.get("path") or "")
        sid = str(row.get("session_id") or "unknown")
        label = str(row.get("label") or "")
        display_text = str(row.get("display_text") or lex_display.get(label) or _label_to_display(label))
        sr = int(row.get("sample_rate") or 0)
        ch = int(row.get("channels") or 0)
        dur = float(row.get("duration_s") or 0.0)
        total_clips += 1
        total_duration += dur

        bucket = by_session.setdefault(
            sid,
            {"count": 0, "duration_s": 0.0, "sample_rate": row.get("sample_rate"), "channels": row.get("channels")},
        )
        bucket["count"] += 1
        bucket["duration_s"] += dur

        if rel_path:
            clips_by_path[rel_path] = ClipMeta(
                label=label,
                display_text=display_text,
                session_id=sid,
                duration_s=dur,
                sample_rate=sr,
                channels=ch,
            )

        if label:
            wb = word_agg.setdefault(
                label,
                {
                    "display_text": lex_display.get(label) or display_text,
                    "clip_count": 0,
                    "duration_s": 0.0,
                    "sessions": set(),
                    "sample_rates": set(),
                },
            )
            wb["clip_count"] += 1
            wb["duration_s"] += dur
            wb["sessions"].add(_short_session_tag(sid))
            if sr > 0:
                wb["sample_rates"].add(sr)

    words: list[RawWordStat] = [
        RawWordStat(
            label=label,
            display_text=word_agg[label]["display_text"],
            clip_count=word_agg[label]["clip_count"],
            total_duration_s=word_agg[label]["duration_s"],
            sessions=tuple(sorted(word_agg[label]["sessions"])),
            sample_rates=tuple(sorted(word_agg[label]["sample_rates"])),
        )
        for label in sorted(word_agg)
    ]
    for label in sorted(lex_display):
        if label not in word_agg:
            words.append(
                RawWordStat(
                    label=label,
                    display_text=lex_display[label],
                    clip_count=0,
                    total_duration_s=0.0,
                    sessions=(),
                    sample_rates=(),
                )
            )

    summary = DatasetSummary(
        has_manifest=True,
        total_clips=total_clips,
        phrase_count=len(lex_display) if lex_display else len(word_agg),
        recorded_phrase_count=len(word_agg),
        total_duration_s=total_duration,
        by_session=dict(sorted(by_session.items())),
        words=words,
        clips_by_path=clips_by_path,
    )
    _DATASET_CACHE[resolved] = (cache_key, summary)
    return summary


def _load_heads_profiles_and_words(trained_heads_dir: Path) -> tuple[dict[str, dict], list[HeadWordStat]]:
    """Parse staged speaker profile metadata and per-class head thresholds with mtime caching."""
    if not trained_heads_dir.is_dir():
        return {}, []

    profile_dirs = [
        child
        for child in sorted(trained_heads_dir.iterdir())
        if child.is_dir() and not child.name.startswith(".")
        and ((child / "head.pt").is_file() or (child / "labels.json").is_file())
    ]
    if not profile_dirs:
        return {}, []

    fingerprint: list[tuple[str, int, int]] = []
    meta_files = ("head.pt", "labels.json", "profile_manifest.json", "calibration.json")
    for pdir in profile_dirs:
        for fname in meta_files:
            fpath = pdir / fname
            if fpath.is_file():
                st = fpath.stat()
                fingerprint.append((f"{pdir.name}/{fname}", st.st_size, st.st_mtime_ns))

    resolved = trained_heads_dir.resolve()
    fp_tuple = tuple(fingerprint)
    cached = _HEADS_CACHE.get(resolved)
    if cached and cached[0] == fp_tuple:
        return cached[1], cached[2]

    profiles: dict[str, dict] = {}
    class_agg: dict[str, dict] = {}

    for pdir in profile_dirs:
        pname = pdir.name
        head_path = pdir / "head.pt"
        labels_data = _safe_read_json(pdir / "labels.json")
        manifest_data = _safe_read_json(pdir / "profile_manifest.json")
        calib_data = _safe_read_json(pdir / "calibration.json")

        labels_list = [str(x) for x in labels_data] if isinstance(labels_data, list) else []
        lex_display: dict[str, str] = {}
        take_metadata: dict = {}
        display_name = pname
        active_ckpt = ""
        if isinstance(manifest_data, dict):
            display_name = str(manifest_data.get("display_name") or pname)
            active_ckpt = str(manifest_data.get("active_checkpoint") or "")
            raw_ld = manifest_data.get("lexicon_display")
            if isinstance(raw_ld, dict):
                lex_display = {str(k): str(v) for k, v in raw_ld.items()}
            raw_tm = manifest_data.get("take_metadata")
            if isinstance(raw_tm, dict):
                take_metadata = raw_tm
            if not labels_list and isinstance(manifest_data.get("lexicon"), list):
                labels_list = [str(x) for x in manifest_data["lexicon"]]

        default_thresh: float | None = None
        per_class_thresh: dict[str, float] = {}
        if isinstance(calib_data, dict):
            if isinstance(calib_data.get("default_threshold"), (int, float)):
                default_thresh = float(calib_data["default_threshold"])
            raw_pct = calib_data.get("per_class_thresholds")
            if isinstance(raw_pct, dict):
                per_class_thresh = {
                    str(k): float(v)
                    for k, v in raw_pct.items()
                    if isinstance(v, (int, float))
                }

        h_stat = head_path.stat() if head_path.is_file() else None
        profiles[pname] = {
            "label_count": len(labels_list),
            "head_size_bytes": h_stat.st_size if h_stat else 0,
            "head_mtime_iso": (
                datetime.fromtimestamp(h_stat.st_mtime, tz=timezone.utc).isoformat()
                if h_stat
                else None
            ),
            "display_name": display_name,
            "active_checkpoint": active_ckpt,
        }

        for label in labels_list:
            entry = class_agg.setdefault(
                label,
                {
                    "display_text": lex_display.get(label) or _label_to_display(label),
                    "profiles": [],
                    "takes": set(),
                    "threshold": None,
                    "threshold_from_core": False,
                },
            )
            if pname not in entry["profiles"]:
                entry["profiles"].append(pname)
            if label in lex_display and lex_display[label]:
                entry["display_text"] = lex_display[label]
            class_takes = take_metadata.get(label)
            if isinstance(class_takes, dict):
                entry["takes"].update(str(k) for k in class_takes)
            thresh = per_class_thresh.get(label, default_thresh)
            if thresh is not None and (entry["threshold"] is None or (pname == "kirk_core" and not entry["threshold_from_core"])):
                entry["threshold"] = thresh
                if pname == "kirk_core":
                    entry["threshold_from_core"] = True

    def _to_stat(label: str) -> HeadWordStat:
        item = class_agg[label]
        ordered_profiles = tuple(
            sorted(item["profiles"], key=lambda p: (0 if p == "kirk_core" else 1, p))
        )
        return HeadWordStat(
            label=label,
            display_text=item["display_text"],
            profiles=ordered_profiles,
            take_count=len(item["takes"]),
            threshold=item["threshold"],
        )

    core_labels = sorted(lbl for lbl, info in class_agg.items() if "kirk_core" in info["profiles"])
    other_labels = sorted(lbl for lbl, info in class_agg.items() if "kirk_core" not in info["profiles"])
    words = [_to_stat(lbl) for lbl in core_labels] + [_to_stat(lbl) for lbl in other_labels]

    _HEADS_CACHE[resolved] = (fp_tuple, profiles, words)
    return profiles, words


def get_heads_and_upstream_summary(
    trained_heads_dir: Path,
    raw_sessions_dir: Path,
    classifier_root: Path,
) -> HeadsAndUpstreamSummary:
    """Inspect staged speaker heads and check classifier_root for un-staged updates."""
    profiles, words = _load_heads_profiles_and_words(trained_heads_dir)

    if not classifier_root.is_dir():
        return HeadsAndUpstreamSummary(profiles=profiles, words=words, classifier_present=False)

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
        words=words,
        classifier_present=True,
        needs_staging=bool(notes),
        staging_notes=notes,
    )


def get_swa_summary(
    synced_dir: Path,
    local_scan: LocalScanResult | None = None,
) -> SwaSummary:
    """Aggregate pulled HearMe SWA sessions and phrases with fast empty-dir bypass."""
    if not synced_dir.is_dir():
        return SwaSummary()
    scan = local_scan if local_scan is not None else scan_local_dir(synced_dir)
    if scan.file_count == 0:
        return SwaSummary()

    resolved = synced_dir.resolve()
    cache_key = (scan.file_count, scan.total_bytes, scan.latest_mtime)
    cached = _SWA_CACHE.get(resolved)
    if cached and cached[0] == cache_key:
        return cached[1]

    sessions = discover_sessions(synced_dir=synced_dir)
    word_agg: dict[str, dict] = {}
    file_phrases: dict[str, tuple[str, str]] = {}
    total_recordings = 0

    for session in sessions:
        session_tag = session.session_code or session.short_id
        file_phrases[f"{session.folder}/metadata.json"] = (
            "Session Metadata",
            f"{session.phrase_count} phrases · {session_tag}",
        )
        for rec in session.recordings:
            total_recordings += 1
            pid = canonicalize_phrase_id(rec.phrase or rec.phrase_id)
            display = rec.phrase or rec.phrase_id
            wb = word_agg.setdefault(
                pid,
                {
                    "display_text": display,
                    "recording_count": 0,
                    "sessions": set(),
                    "feedbacks": set(),
                },
            )
            wb["recording_count"] += 1
            wb["sessions"].add(session.folder)
            if rec.feedback:
                wb["feedbacks"].add(rec.feedback)

            detail = (
                f"feedback: {rec.feedback}"
                if rec.feedback
                else (rec.transcription or f"session {session_tag}")
            )
            file_phrases[f"{session.folder}/{rec.phrase_id}.wav"] = (display, detail)

    words = [
        SwaWordStat(
            phrase_id=pid,
            display_text=word_agg[pid]["display_text"],
            recording_count=word_agg[pid]["recording_count"],
            session_count=len(word_agg[pid]["sessions"]),
            sample_feedback=", ".join(sorted(word_agg[pid]["feedbacks"])) or "—",
        )
        for pid in sorted(word_agg)
    ]
    summary = SwaSummary(
        session_count=len(sessions),
        recording_count=total_recordings,
        words=words,
        file_phrases=file_phrases,
    )
    _SWA_CACHE[resolved] = (cache_key, summary)
    return summary


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
    remote_files: dict[str, tuple[int, str | None]] = {}
    remote_bytes = 0
    latest_remote: datetime | None = None
    for blob in remote_blobs:
        if blob.name.endswith("/"):
            continue
        blob_size = int(getattr(blob, "size", 0) or 0)
        lm = getattr(blob, "last_modified", None)
        remote_by_name[blob.name] = blob
        remote_files[blob.name] = (blob_size, lm.isoformat() if lm else None)
        remote_bytes += blob_size
        if lm is not None and (latest_remote is None or lm > latest_remote):
            latest_remote = lm

    diff.remote_count = len(remote_by_name)
    diff.remote_bytes = remote_bytes
    diff.remote_latest_iso = latest_remote.isoformat() if latest_remote else None
    diff.remote_files = remote_files

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


def derive_remote_raw_words(remote_files: dict[str, tuple[int, str | None]]) -> list[RawWordStat]:
    """Derive per-word clip counts from Azure blob paths before raw-sessions is pulled."""
    word_agg: dict[str, dict] = {}
    for rel_path in sorted(remote_files):
        if not rel_path.lower().endswith(".wav"):
            continue
        parts = Path(rel_path).parts
        if len(parts) >= 3:
            session_id = parts[0]
            label = parts[1]
            display = _label_to_display(label)
        elif len(parts) == 2:
            session_id = parts[0]
            label, display = parse_raw_clip_stem(Path(parts[1]).stem)
        else:
            session_id = "remote"
            label, display = parse_raw_clip_stem(Path(rel_path).stem)

        entry = word_agg.setdefault(
            label,
            {"display_text": display, "clip_count": 0, "sessions": set()},
        )
        entry["clip_count"] += 1
        entry["sessions"].add(_short_session_tag(session_id))

    return [
        RawWordStat(
            label=label,
            display_text=word_agg[label]["display_text"],
            clip_count=word_agg[label]["clip_count"],
            total_duration_s=0.0,
            sessions=tuple(sorted(word_agg[label]["sessions"])),
            sample_rates=(),
        )
        for label in sorted(word_agg)
    ]


def _describe_raw_file(
    rel_path: str,
    dataset_summary: DatasetSummary,
    remote_info: tuple[int, str | None] | None,
) -> tuple[str, str]:
    parts = Path(rel_path).parts
    if rel_path == "manifest.jsonl":
        return "Corpus Manifest", f"{dataset_summary.total_clips} clips" if dataset_summary.has_manifest else "Azure index"
    if rel_path == "lexicon.json":
        return "Phrase Lexicon", f"{dataset_summary.phrase_count} phrases" if dataset_summary.has_manifest else "Azure lexicon"
    if len(parts) >= 2 and parts[-1] == "session_manifest.jsonl":
        return "Session Manifest", parts[0]

    meta = dataset_summary.clips_by_path.get(rel_path)
    if meta is not None:
        ch_str = "mono" if meta.channels == 1 else "stereo" if meta.channels == 2 else "wav"
        sr_str = f"{meta.sample_rate // 1000}k" if meta.sample_rate >= 1000 else f"{meta.sample_rate}Hz"
        return f"{meta.display_text} ({meta.label})", f"{meta.duration_s:.2f}s · {sr_str} {ch_str}"

    if len(parts) >= 3 and rel_path.lower().endswith(".wav"):
        category = f"{_label_to_display(parts[1])} ({parts[1]})"
        details = _short_session_tag(parts[0])
        if remote_info and remote_info[1]:
            details = f"{details} · {format_short_time(remote_info[1])}"
        return category, details

    return "Session File", parts[0] if parts else rel_path


def _describe_head_file(
    rel_path: str,
    heads_summary: HeadsAndUpstreamSummary,
) -> tuple[str, str]:
    parts = Path(rel_path).parts
    profile = parts[0] if len(parts) >= 2 else "profile"
    fname = parts[-1]
    pinfo = heads_summary.profiles.get(profile, {})
    label_count = pinfo.get("label_count")

    if fname in _HEAD_FILE_ROLES:
        role = _HEAD_FILE_ROLES[fname]
        category = f"{profile} · {role}"
        if fname in {"head.pt", "labels.json"} and label_count:
            return category, f"{label_count} classes"
        ckpt = pinfo.get("active_checkpoint")
        if fname == "profile_manifest.json" and ckpt:
            return category, str(ckpt)
        return category, "Artifact"
    if len(parts) >= 3 and parts[1] == "checkpoints":
        return f"{profile} · Checkpoint", Path(fname).stem
    return f"{profile} · Artifact", fname


def _describe_swa_file(
    rel_path: str,
    swa_summary: SwaSummary,
    remote_info: tuple[int, str | None] | None,
) -> tuple[str, str]:
    known = swa_summary.file_phrases.get(rel_path)
    if known is not None:
        return known
    parts = Path(rel_path).parts
    if parts and parts[-1] == "metadata.json":
        return "Session Metadata", "/".join(parts[:-1]) or "root"
    if rel_path.lower().endswith(".wav"):
        details = "/".join(parts[:-1]) or "SWA recording"
        if remote_info and remote_info[1]:
            details = f"{details} · {format_short_time(remote_info[1])}"
        return f"Recording ({Path(rel_path).stem})", details
    return "SWA Artifact", "/".join(parts[:-1]) or "root"


def build_bucket_file_rows(
    spec: BucketSpec,
    local_scan: LocalScanResult,
    diff: BucketDiff,
    dataset_summary: DatasetSummary,
    heads_summary: HeadsAndUpstreamSummary,
    swa_summary: SwaSummary,
) -> list[FileInspectorRow]:
    """Build merged local + remote file rows for the selected bucket inspector."""
    all_paths = sorted(set(local_scan.files) | set(diff.remote_files))
    up_set = set(diff.to_upload)
    down_set = set(diff.to_download)
    del_set = set(diff.to_delete_remote)

    rows: list[FileInspectorRow] = []
    for rel_path in all_paths:
        if diff.offline or diff.error:
            sync_state = "○ Local"
        elif rel_path in up_set:
            sync_state = "↑ Push"
        elif rel_path in del_set:
            sync_state = "✕ Prune"
        elif rel_path in down_set:
            sync_state = "↓ Pull"
        else:
            sync_state = "✓ Synced"

        local_meta = local_scan.files.get(rel_path)
        remote_info = diff.remote_files.get(rel_path)
        size_bytes = local_meta[0] if local_meta is not None else (remote_info[0] if remote_info is not None else 0)

        if spec.key == "raw-sessions":
            category, details = _describe_raw_file(rel_path, dataset_summary, remote_info)
        elif spec.key == "trained-heads":
            category, details = _describe_head_file(rel_path, heads_summary)
        else:
            category, details = _describe_swa_file(rel_path, swa_summary, remote_info)

        rows.append(
            FileInspectorRow(
                path=rel_path,
                category=category,
                details=details,
                size_bytes=size_bytes,
                sync_state=sync_state,
            )
        )
    return rows
