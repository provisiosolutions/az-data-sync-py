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

import json
import re
import webbrowser
from dataclasses import dataclass, field
from datetime import datetime
from html import escape as html_escape
from pathlib import Path
from string import Template

# ── Paths ─────────────────────────────────────────────────────────────────────

_ROOT = Path(__file__).parent
SYNCED_DIR = _ROOT / "synced-files"
MANIFEST_PATH = SYNCED_DIR / ".sync_manifest.json"
OUTPUT_PATH = SYNCED_DIR / "review.html"
TEMPLATE_DIR = _ROOT / "templates"

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


def load_manifest() -> dict:
    """Load the sync manifest, returning an empty dict if missing."""
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    return {}


def discover_sessions() -> list[Session]:
    """Walk every session folder, load metadata, and enrich with manifest."""
    manifest = load_manifest()
    sessions: list[Session] = []

    for entry in sorted(SYNCED_DIR.iterdir()):
        if not entry.is_dir():
            continue
        meta_path = entry / "metadata.json"
        if not meta_path.exists():
            continue

        meta = json.loads(meta_path.read_text(encoding="utf-8"))

        recordings = [
            Recording(
                phrase_id=r.get("phraseId", "?"),
                phrase=r.get("phrase", ""),
                transcription=r.get("transcription", ""),
                feedback=r.get("transcriptionFeedback", ""),
                timestamp=r.get("timestamp", ""),
            )
            for r in meta.get("recordings", [])
        ]

        wav_files = sorted(
            [
                WavFile(
                    filename=key.split("/", 1)[1],
                    size=info.get("size", 0),
                    synced_at=info.get("synced_at", ""),
                    last_modified=info.get("last_modified", ""),
                )
                for key, info in manifest.items()
                if key.startswith(f"{entry.name}/") and key.endswith(".wav")
            ],
            key=lambda w: _natural_sort_key(w.filename),
        )

        sessions.append(
            Session(
                session_id=meta.get("sessionId", entry.name),
                folder=entry.name,
                start_time=meta.get("startTime", ""),
                end_time=meta.get("endTime", ""),
                recordings=recordings,
                wav_files=wav_files,
            )
        )

    sessions.sort(key=lambda s: s.start_time, reverse=True)
    return sessions


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


def main() -> None:
    import sys

    print("📂 Scanning synced-files directory…")
    sessions = discover_sessions()
    print(f"   Found {len(sessions)} sessions")

    html = generate_html(sessions)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"✅ Review page written to: {OUTPUT_PATH}")

    if "--no-open" not in sys.argv:
        webbrowser.open(OUTPUT_PATH.as_uri())


if __name__ == "__main__":
    main()
