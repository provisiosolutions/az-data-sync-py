#!/usr/bin/env python3
"""
Sync Session Reviewer
=====================
Reads the synced-files directory, ingests each session's metadata.json,
cross-references the .sync_manifest.json, and generates a self-contained
HTML page for easy human review of all recording sessions.

Usage:
    python review_sessions.py            # generate + open in browser
    python review_sessions.py --no-open  # generate only

Template files live in ./templates/ and are inlined at generation time
so the output remains a single portable HTML file.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import unicodedata
import webbrowser
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html import escape as html_escape
from pathlib import Path
from string import Template

# ── Paths ─────────────────────────────────────────────────────────────────────

_ROOT = Path(__file__).parent
SYNCED_DIR = _ROOT / "synced-files"
MANIFEST_PATH = SYNCED_DIR / ".sync_manifest.json"
OUTPUT_PATH = SYNCED_DIR / "review.html"
TEMPLATE_DIR = _ROOT / "templates"

_PHRASE_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_APOSTROPHES = str.maketrans("", "", "'’")
_UNSAFE_RUN = re.compile(r"[^a-z0-9_-]+")
_SEPARATOR_RUN = re.compile(r"[_-]{2,}")


def canonicalize_phrase_id(display_text: str) -> str:
    """Convert phrase display text into a deterministic HearMe canonical phrase ID."""
    cleaned = (display_text or "").strip()
    if not cleaned:
        return "unknown_phrase"
    if _PHRASE_ID_PATTERN.fullmatch(cleaned):
        return cleaned[:128]
    normalized = unicodedata.normalize("NFKD", cleaned).encode("ascii", "ignore").decode("ascii")
    normalized = normalized.lower().translate(_APOSTROPHES)
    phrase_id = _UNSAFE_RUN.sub("_", normalized)
    phrase_id = _SEPARATOR_RUN.sub("_", phrase_id).strip("_-")
    return (phrase_id or "unknown_phrase")[:128]


# ── Data Models ───────────────────────────────────────────────────────────────


@dataclass
class WavFile:
    filename: str
    size: int = 0
    synced_at: str = ""
    last_modified: str = ""


@dataclass
class Recording:
    phrase_id: str
    phrase: str
    transcription: str = ""
    feedback: str = ""
    timestamp: str = ""


@dataclass
class Session:
    session_id: str
    folder: str
    start_time: str = ""
    end_time: str = ""
    session_code: str = ""
    recordings: list[Recording] = field(default_factory=list)
    wav_files: list[WavFile] = field(default_factory=list)

    @property
    def phrase_count(self) -> int:
        return len(self.recordings)

    @property
    def file_count(self) -> int:
        return len(self.wav_files)

    @property
    def total_size(self) -> int:
        return sum(w.size for w in self.wav_files)

    @property
    def phrase_preview(self) -> str:
        """First 3 phrases joined for a compact preview."""
        phrases = [r.phrase for r in self.recordings[:3]]
        preview = " · ".join(phrases)
        if len(self.recordings) > 3:
            preview += " …"
        return preview

    @property
    def short_id(self) -> str:
        """First 8 chars of the GUID."""
        return self.session_id[:8] if self.session_id else "—"


# ── Helpers ───────────────────────────────────────────────────────────────────

_NATURAL_RE = re.compile(r"(\d+)")


def _natural_sort_key(filename: str) -> list:
    """Sort '1.wav', '2.wav', ... '10.wav' naturally."""
    return [int(c) if c.isdigit() else c.lower() for c in _NATURAL_RE.split(filename)]


def format_size(size_bytes: int) -> str:
    """Human-readable file size."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    return f"{size_bytes / (1024 * 1024):.1f} MB"


def format_timestamp(iso_str: str) -> str:
    """Friendly timestamp from ISO string."""
    if not iso_str:
        return "—"
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.strftime("%b %d, %Y  %I:%M %p")
    except (ValueError, TypeError):
        return iso_str


# ── Data Loading ──────────────────────────────────────────────────────────────


def load_manifest(manifest_path: Path | None = None) -> dict:
    """Load the sync manifest, returning an empty dict if missing."""
    path = manifest_path or MANIFEST_PATH
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def discover_sessions(
    synced_dir: Path | None = None,
    manifest_path: Path | None = None,
) -> list[Session]:
    """Recursively walk every session folder, load metadata, and enrich with manifest or disk WAVs."""
    root_dir = synced_dir or SYNCED_DIR
    if not root_dir.exists():
        return []

    effective_manifest_path = manifest_path or (
        root_dir / ".sync_manifest.json" if synced_dir is not None else MANIFEST_PATH
    )
    manifest = load_manifest(effective_manifest_path)
    sessions: list[Session] = []

    for meta_path in sorted(root_dir.rglob("metadata.json")):
        session_dir = meta_path.parent
        rel_parts = session_dir.relative_to(root_dir).parts
        rel_folder = "/".join(rel_parts)

        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue

        session_code = str(meta.get("sessionCode", "") or "")
        if not session_code and len(rel_parts) >= 2:
            session_code = rel_parts[0]

        recordings = [
            Recording(
                phrase_id=str(r.get("phraseId", "?")),
                phrase=str(r.get("phrase", "")),
                transcription=str(r.get("transcription", "")),
                feedback=str(r.get("transcriptionFeedback", "")),
                timestamp=str(r.get("timestamp", "")),
            )
            for r in meta.get("recordings", [])
        ]

        prefix = f"{rel_folder}/"
        wav_by_name: dict[str, WavFile] = {}
        for key, info in manifest.items():
            if key.startswith(prefix) and key.endswith(".wav"):
                rel_wav = key[len(prefix) :]
                if "/" not in rel_wav:
                    wav_by_name[rel_wav] = WavFile(
                        filename=rel_wav,
                        size=int(info.get("size", 0) or 0),
                        synced_at=str(info.get("synced_at", "") or ""),
                        last_modified=str(info.get("last_modified", "") or ""),
                    )

        # Supplement with on-disk .wav files when manifest is missing or partial
        for wav_path in session_dir.glob("*.wav"):
            if wav_path.name not in wav_by_name:
                stat = wav_path.stat()
                wav_by_name[wav_path.name] = WavFile(
                    filename=wav_path.name,
                    size=stat.st_size,
                    last_modified=datetime.fromtimestamp(
                        stat.st_mtime, tz=timezone.utc
                    ).isoformat(),
                )

        wav_files = sorted(
            wav_by_name.values(),
            key=lambda w: _natural_sort_key(w.filename),
        )

        sessions.append(
            Session(
                session_id=str(meta.get("sessionId", session_dir.name)),
                folder=rel_folder,
                start_time=str(meta.get("startTime", "")),
                end_time=str(meta.get("endTime", "")),
                session_code=session_code,
                recordings=recordings,
                wav_files=wav_files,
            )
        )

    sessions.sort(key=lambda s: s.start_time, reverse=True)
    return sessions


def export_sessions_for_classifier(
    sessions: list[Session],
    synced_dir: Path | None = None,
    output_dir: Path | None = None,
    include_bad_feedback: bool = False,
) -> dict:
    """Export synced session WAVs into HearMe canonical phrase folders and manifest.jsonl."""
    root_dir = synced_dir or SYNCED_DIR
    dest_dir = output_dir or (_ROOT / "classifier-export")
    dest_dir.mkdir(parents=True, exist_ok=True)

    exported_files = 0
    skipped_bad = 0
    missing_wav = 0
    manifest_rows: list[dict] = []

    for session in sessions:
        session_slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", session.folder).strip("_") or session.short_id
        for rec in session.recordings:
            if not include_bad_feedback and rec.feedback.lower() == "bad":
                skipped_bad += 1
                continue

            src_wav = root_dir / session.folder / f"{rec.phrase_id}.wav"
            if not src_wav.exists():
                missing_wav += 1
                continue

            label = canonicalize_phrase_id(rec.phrase or rec.phrase_id)
            label_dir = dest_dir / label
            label_dir.mkdir(parents=True, exist_ok=True)

            safe_phrase_id = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(rec.phrase_id)).strip("_") or "take"
            dest_filename = f"{session_slug}_{safe_phrase_id}.wav"
            dest_wav = label_dir / dest_filename
            shutil.copy2(src_wav, dest_wav)
            exported_files += 1

            manifest_rows.append(
                {
                    "session_id": session.session_id,
                    "session_code": session.session_code,
                    "folder": session.folder,
                    "phrase_id": rec.phrase_id,
                    "canonical_label": label,
                    "display_text": rec.phrase,
                    "transcription": rec.transcription,
                    "feedback": rec.feedback,
                    "timestamp": rec.timestamp or session.start_time,
                    "audio_path": f"{label}/{dest_filename}",
                }
            )

    manifest_out = dest_dir / "export_manifest.jsonl"
    manifest_out.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in manifest_rows)
        + ("\n" if manifest_rows else ""),
        encoding="utf-8",
    )

    return {
        "exported_files": exported_files,
        "skipped_bad": skipped_bad,
        "missing_wav": missing_wav,
        "manifest_path": str(manifest_out),
    }


# ── HTML Rendering ────────────────────────────────────────────────────────────
#
# The page shell and CSS live in ./templates/ as plain files.
# They're loaded once, inlined into the output so the final review.html
# remains a single self-contained file (works from file:// with no server).
#


def _load_template() -> Template:
    """Read the HTML shell template from disk."""
    html_path = TEMPLATE_DIR / "review.html"
    return Template(html_path.read_text(encoding="utf-8"))


def _load_css() -> str:
    """Read the CSS stylesheet from disk."""
    css_path = TEMPLATE_DIR / "review.css"
    return css_path.read_text(encoding="utf-8")


def _render_phrase_row(rec: Recording, session: Session) -> str:
    """Render a single <tr> for the phrase table."""
    fb_class = "good" if rec.feedback == "good" else "bad"
    fb_icon = "✓" if rec.feedback == "good" else "✗"
    audio_path = f"{session.folder}/{rec.phrase_id}.wav"

    return f"""<tr>
  <td class="phrase-id">#{html_escape(rec.phrase_id)}</td>
  <td class="phrase-text">{html_escape(rec.phrase)}</td>
  <td class="transcription">{html_escape(rec.transcription or '—')}</td>
  <td class="feedback {fb_class}">{fb_icon} {html_escape(rec.feedback or '—')}</td>
  <td class="audio-cell"><audio-player src="{html_escape(audio_path)}"></audio-player></td>
  <td class="ts">{format_timestamp(rec.timestamp)}</td>
</tr>"""


def _render_file_chip(wav: WavFile) -> str:
    """Render a single audio file chip."""
    return f"""<div class="file-chip">
  <span class="file-icon">🎤</span>
  <span class="file-name">{html_escape(wav.filename)}</span>
  <span class="file-size">{format_size(wav.size)}</span>
</div>"""


def _render_session_card(session: Session, idx: int, total: int) -> str:
    """Render a full session card with header, preview, and expandable body."""
    label_date = format_timestamp(session.start_time)
    preview_html = (
        html_escape(session.phrase_preview)
        if session.phrase_preview
        else "<em>No phrases</em>"
    )
    code_badge = (
        f'<span class="session-id-badge" title="Session Code">{html_escape(session.session_code)}</span>'
        if session.session_code
        else ""
    )

    phrase_rows = "\n".join(
        _render_phrase_row(r, session) for r in session.recordings
    )
    file_chips = "\n".join(
        _render_file_chip(w) for w in session.wav_files
    ) or "<em>No audio files in manifest</em>"

    return f"""<session-card class="session-card" id="session-{idx}">
  <div class="card-header">
    <div class="header-left">
      <span class="session-number">Session {total - idx}</span>
      <span class="session-date">{label_date}</span>
      {code_badge}
      <span class="session-id-badge" title="{html_escape(session.session_id)}">{html_escape(session.short_id)}</span>
    </div>
    <div class="header-right">
      <span class="stat-pill">{session.phrase_count} phrases</span>
      <span class="stat-pill">{session.file_count} files</span>
      <span class="stat-pill">{format_size(session.total_size)}</span>
      <span class="chevron" id="chevron-{idx}">▾</span>
    </div>
  </div>
  <div class="phrase-preview">{preview_html}</div>
  <div class="card-body" id="body-{idx}" style="display:none;">
    <h4>📝 Phrases &amp; Transcriptions</h4>
    <table class="phrase-table">
      <thead>
        <tr>
          <th>#</th><th>Phrase</th><th>Transcription</th>
          <th>Feedback</th><th>Audio</th><th>Recorded</th>
        </tr>
      </thead>
      <tbody>{phrase_rows}</tbody>
    </table>

    <h4>🎙️ Audio Files</h4>
    <div class="file-grid">{file_chips}</div>

    <div class="meta-footer">
      <span>Folder: <code>{html_escape(session.folder)}</code></span>
      <span>Session ID: <code>{html_escape(session.session_id)}</code></span>
      <span>Start: {format_timestamp(session.start_time)}</span>
      <span>End: {format_timestamp(session.end_time)}</span>
    </div>
  </div>
</session-card>"""


def generate_html(sessions: list[Session]) -> str:
    """Assemble the full HTML page from external template + rendered cards."""
    template = _load_template()
    css = _load_css()

    cards = "\n".join(
        _render_session_card(s, idx, len(sessions))
        for idx, s in enumerate(sessions)
    )

    return template.substitute(
        css=css,
        session_count=len(sessions),
        total_phrases=sum(s.phrase_count for s in sessions),
        total_files=sum(s.file_count for s in sessions),
        total_size=format_size(sum(s.total_size for s in sessions)),
        generated_at=datetime.now().strftime("%b %d, %Y  %I:%M %p"),
        cards_html=cards,
    )


# ── CLI Entry Point ──────────────────────────────────────────────────────────


def build_review_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Generate review.html for synced recording sessions and optionally export for HearMe classifier."
    )
    p.add_argument(
        "--synced-dir",
        type=Path,
        default=None,
        help="Path to synced-files directory (defaults to ./synced-files).",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output path for review.html (defaults to <synced-dir>/review.html).",
    )
    p.add_argument(
        "--export-classifier",
        type=Path,
        default=None,
        help="Export valid session WAVs into HearMe canonical phrase folders at this directory.",
    )
    p.add_argument(
        "--include-bad",
        action="store_true",
        help="Include recordings marked with 'bad' feedback when exporting for the classifier.",
    )
    p.add_argument(
        "--no-open",
        action="store_true",
        help="Do not automatically open review.html in the default browser.",
    )
    return p


def main(argv: list[str] | None = None) -> None:
    parser = build_review_parser()
    args = parser.parse_args(argv)

    synced_dir = args.synced_dir or SYNCED_DIR
    output_path = args.output or (
        synced_dir / "review.html" if args.synced_dir is not None else OUTPUT_PATH
    )

    print(f"📂 Scanning {synced_dir} directory…")
    sessions = discover_sessions(synced_dir=synced_dir)
    print(f"   Found {len(sessions)} sessions")

    html = generate_html(sessions)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    print(f"✅ Review page written to: {output_path}")

    if args.export_classifier is not None:
        export_stats = export_sessions_for_classifier(
            sessions=sessions,
            synced_dir=synced_dir,
            output_dir=args.export_classifier,
            include_bad_feedback=args.include_bad,
        )
        print(
            f"📦 Exported {export_stats['exported_files']} WAVs for classifier "
            f"(skipped_bad={export_stats['skipped_bad']}, missing_wav={export_stats['missing_wav']}) "
            f"→ {export_stats['manifest_path']}"
        )

    if not args.no_open:
        webbrowser.open(output_path.resolve().as_uri())


if __name__ == "__main__":
    main()
