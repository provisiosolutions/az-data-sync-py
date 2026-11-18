"""Performant Textual TUI dashboard for HearMe audio & trained-head Azure sync."""

from __future__ import annotations

import argparse
import logging
import os
import threading
import time
import webbrowser
from datetime import datetime
from pathlib import Path

from rich.markup import escape
from rich.text import Text
from textual import events, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    RichLog,
    Static,
    TabbedContent,
    TabPane,
)

from review_sessions import discover_sessions, generate_html
from sync import (
    push_directory,
    stage_classifier_raw_sessions,
    stage_trained_heads,
    sync_container,
)
from sync_inspector import (
    BucketDiff,
    BucketSpec,
    DatasetSummary,
    HeadsAndUpstreamSummary,
    LocalScanResult,
    SwaSummary,
    build_bucket_file_rows,
    compute_bucket_diff,
    default_bucket_specs,
    derive_remote_raw_words,
    fetch_all_remote_diffs,
    format_bytes,
    format_short_time,
    get_dataset_summary,
    get_heads_and_upstream_summary,
    get_swa_summary,
    resolve_blob_service,
    scan_local_dir,
)

APP_CSS = """
Screen {
    layout: vertical;
    background: $surface;
}

#workflow-banner {
    height: auto;
    padding: 0 2;
    margin: 0 1 1 1;
    border: round $primary;
    background: $panel;
}

#main-grid {
    height: 1fr;
    margin: 0 1;
}

#left-pane {
    width: 38%;
    min-width: 44;
    height: 1fr;
    margin-right: 1;
}

#right-pane {
    width: 1fr;
    height: 1fr;
}

.pane-title {
    text-style: bold;
    color: $accent;
    padding: 0 1;
}

#buckets-table {
    height: 7;
    border: round $primary-lighten-2;
    background: $panel;
    margin-bottom: 1;
}

#dataset-scroll {
    height: auto;
    max-height: 12;
    border: round $primary-lighten-2;
    background: $panel;
    padding: 0 1;
    margin-bottom: 1;
}

#activity-log {
    height: 1fr;
    min-height: 6;
    border: round $primary-lighten-2;
    background: $panel;
    padding: 0 1;
}

#inspector-header {
    height: 3;
    align: left middle;
}

#inspector-title {
    width: auto;
    margin-right: 2;
}

#inspector-filter {
    width: 1fr;
}

#inspector-tabs {
    height: 1fr;
    border: round $primary-lighten-2;
    background: $panel;
}

#inspector-tabs ContentSwitcher {
    height: 1fr;
}

#inspector-tabs TabPane {
    height: 1fr;
    padding: 0;
}

#words-table, #files-table, #diff-scroll {
    height: 1fr;
}

#diff-scroll {
    padding: 1 2;
}

#inspector-detail {
    height: auto;
    min-height: 2;
    border: round $primary-lighten-2;
    background: $panel;
    padding: 0 1;
    margin-top: 1;
}

ConfirmSyncModal, StudentGuideModal {
    align: center middle;
}

#confirm-dialog, #guide-dialog {
    width: 72;
    max-width: 92%;
    max-height: 85%;
    border: thick $accent;
    background: $panel;
    padding: 1 2;
}

#guide-scroll {
    height: 1fr;
    max-height: 22;
    margin: 1 0;
}

#modal-buttons {
    height: 3;
    align: right middle;
    margin-top: 1;
}

#modal-buttons Button {
    margin-left: 1;
}
"""


class _TuiLogHandler(logging.Handler):
    """Forward sync module log records safely into the TUI RichLog."""

    def __init__(self, app: "HearMeSyncApp") -> None:
        super().__init__(level=logging.INFO)
        self._app = app

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self._app.post_log(msg, level=record.levelname)
        except Exception:
            pass


class ConfirmSyncModal(ModalScreen[bool]):
    """Safety modal confirming uploads and orphan deletions before pushing to Azure."""

    BINDINGS = [
        Binding("y", "confirm", "Confirm"),
        Binding("enter", "confirm", "Confirm"),
        Binding("n", "cancel", "Cancel"),
        Binding("escape", "cancel", "Cancel"),
    ]

    def __init__(self, title: str, body: str, confirm_label: str = "Push Now \\[Y]") -> None:
        super().__init__()
        self._title = title
        self._body = body
        self._confirm_label = confirm_label

    def compose(self) -> ComposeResult:
        with Vertical(id="confirm-dialog"):
            yield Static(f"[bold yellow]{escape(self._title)}[/bold yellow]")
            yield Static(self._body, id="confirm-body")
            with Horizontal(id="modal-buttons"):
                yield Button("Cancel \\[Esc]", id="cancel-btn", variant="default")
                yield Button(self._confirm_label, id="confirm-btn", variant="warning")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "confirm-btn")

    def action_confirm(self) -> None:
        self.dismiss(True)

    def action_cancel(self) -> None:
        self.dismiss(False)


class StudentGuideModal(ModalScreen[None]):
    """Quick onboarding & Pandas/PyTorch usage cheat sheet for students."""

    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("enter", "close", "Close"),
        Binding("q", "close", "Close"),
    ]

    def compose(self) -> ComposeResult:
        guide = "\n".join(
            [
                "[bold cyan]1. Three-Bucket Layout[/bold cyan]",
                "  • [bold]raw-sessions[/bold]   → Kirk snake_case WAV clips + manifest.jsonl & lexicon.json",
                "  • [bold]trained-heads[/bold]  → Lightweight speaker heads (kirk 137-class, kirk_core 30-class)",
                "  • [bold]sessions (SWA)[/bold] → Web app participant recordings (Pull-Only safety lock)",
                "",
                "[bold cyan]2. First-Time Setup on Your Laptop[/bold cyan]",
                "  1) Copy [bold]config.example.json[/bold] to [bold]config.json[/bold] and add the Azure connection string.",
                "  2) Select [bold]raw-sessions[/bold] and press [bold]G (Pull)[/bold] to download training audio.",
                "  3) Select [bold]trained-heads[/bold] and press [bold]G (Pull)[/bold] to download head weights.",
                "",
                "[bold cyan]3. Browse Words, Phrases, and Files[/bold cyan]",
                "  • Press [bold]/[/bold] to filter words, class labels, or WAV filenames live ([bold]Esc[/bold] clears).",
                "  • Press [bold]V[/bold] (or [bold]1[/bold]/[bold]2[/bold]/[bold]3[/bold]) to switch Words, Files, and Sync Diff tabs.",
                "  • Press [bold]Enter[/bold] on a word row to jump straight to its matching WAV/profile files.",
                "",
                "[bold cyan]4. Load Raw Audio & Labels in Python / Pandas[/bold cyan]",
                '  [green]import pandas as pd[/green]',
                '  [green]df = pd.read_json("raw-sessions/manifest.jsonl", lines=True)[/green]',
                '  [green]print(df.groupby(["session_id", "sample_rate"]).size())[/green]',
                "",
                "[bold cyan]5. Updating From audio-classifier-hear-me[/bold cyan]",
                "  • Press [bold]S (Stage)[/bold] to copy new clips and heads locally in strict snake_case.",
                "  • Press [bold]P (Push)[/bold] to preview exact upload and prune counts before confirming.",
            ]
        )
        with Vertical(id="guide-dialog"):
            yield Static("[bold]HearMe Data Sync — Student Quickstart Guide[/bold]")
            with VerticalScroll(id="guide-scroll"):
                yield Static(guide)
            with Horizontal(id="modal-buttons"):
                yield Button("Close \\[Esc]", id="close-guide-btn", variant="primary")

    def on_button_pressed(self, _event: Button.Pressed) -> None:
        self.dismiss(None)

    def action_close(self) -> None:
        self.dismiss(None)


class HearMeSyncApp(App):
    """Interactive Textual studio for HearMe three-bucket Azure Blob synchronization."""

    TITLE = "HearMe Data Sync Studio"
    CSS = APP_CSS
    BINDINGS = [
        Binding("g", "pull_selected", "Pull"),
        Binding("p", "push_selected", "Push"),
        Binding("s", "stage_classifier", "Stage"),
        Binding("slash", "focus_filter", "Filter", key_display="/"),
        Binding("v", "cycle_view", "View"),
        Binding("o", "open_review", "Review"),
        Binding("r", "refresh_all", "Refresh"),
        Binding("question_mark", "show_guide", "Guide", key_display="?"),
        Binding("q", "quit", "Quit"),
        Binding("1", "activate_tab('tab-words')", "Words", show=False),
        Binding("2", "activate_tab('tab-files')", "Files", show=False),
        Binding("3", "activate_tab('tab-diff')", "Diff", show=False),
        Binding("d", "activate_tab('tab-diff')", "Diff", show=False),
    ]

    _TAB_ORDER = ("tab-words", "tab-files", "tab-diff")

    def __init__(
        self,
        repo_root: Path | None = None,
        config_path: Path | None = None,
        classifier_root: Path | None = None,
        blob_service=None,
        auto_connect: bool = True,
    ) -> None:
        super().__init__()
        self.repo_root = (repo_root or Path(__file__).parent).resolve()
        self.config_path = config_path or (self.repo_root / "config.json")
        if not self.config_path.is_absolute():
            self.config_path = self.repo_root / self.config_path
        self.classifier_root = classifier_root or (self.repo_root.parent / "audio-classifier-hear-me")
        if not self.classifier_root.is_absolute():
            self.classifier_root = (self.repo_root / self.classifier_root).resolve()

        self._specs: list[BucketSpec] = default_bucket_specs(self.repo_root)
        self._spec_by_key: dict[str, BucketSpec] = {s.key: s for s in self._specs}
        self._selected_key: str = self._specs[0].key
        self._col_keys: list = []
        self._words_col_mode: str = ""

        self._local_scans: dict[str, LocalScanResult] = {}
        self._diffs: dict[str, BucketDiff] = {}
        self._dataset_summary = DatasetSummary()
        self._heads_summary = HeadsAndUpstreamSummary()
        self._swa_summary = SwaSummary()

        self._filter_query: str = ""
        self._word_detail_by_key: dict[str, str] = {}
        self._word_drill_filter: dict[str, str] = {}
        self._file_detail_by_key: dict[str, str] = {}

        self._blob_service = blob_service
        self._auto_connect = auto_connect
        if blob_service is not None:
            self._account_label = "Mock Azure"
        elif auto_connect:
            self._account_label = "Checking…"
        else:
            self._account_label = "Offline (Local Only)"
        self._connection_hint: str | None = None
        self._busy_transfer = False

        self._ui_thread_id = threading.get_ident()
        self._log_handler = _TuiLogHandler(self)
        self._saved_root_handlers: list[logging.Handler] = []
        self._sync_logger_had_handler = False

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(id="workflow-banner")
        with Horizontal(id="main-grid"):
            with Vertical(id="left-pane"):
                yield Static("1. Storage Buckets (↑/↓ to select)", classes="pane-title")
                yield DataTable(id="buckets-table", cursor_type="row")
                yield Static("2. Bucket Summary", classes="pane-title")
                with VerticalScroll(id="dataset-scroll"):
                    yield Static(id="dataset-card")
                yield Static("Activity Log", classes="pane-title")
                yield RichLog(id="activity-log", highlight=True, markup=True)
            with Vertical(id="right-pane"):
                with Horizontal(id="inspector-header"):
                    yield Static("3. Bucket Inspector", id="inspector-title", classes="pane-title")
                    yield Input(
                        placeholder="[/] Filter words, phrases, or files… (Esc clears)",
                        id="inspector-filter",
                    )
                with TabbedContent(id="inspector-tabs", initial="tab-words"):
                    with TabPane("Words & Phrases", id="tab-words"):
                        yield DataTable(id="words-table", cursor_type="row", zebra_stripes=True)
                    with TabPane("Files", id="tab-files"):
                        yield DataTable(id="files-table", cursor_type="row", zebra_stripes=True)
                    with TabPane("Sync Diff", id="tab-diff"):
                        with VerticalScroll(id="diff-scroll"):
                            yield Static(id="diff-view")
                yield Static("Select a word or file row for details.", id="inspector-detail")
        yield Footer()

    def on_mount(self) -> None:
        self._attach_log_capture()
        buckets_table = self.query_one("#buckets-table", DataTable)
        self._col_keys = list(buckets_table.add_columns("Bucket", "Local", "Azure", "Size", "Status"))
        for spec in self._specs:
            buckets_table.add_row(
                Text(spec.title),
                Text("…"),
                Text("…"),
                Text("…"),
                Text("Scanning…"),
                key=spec.key,
            )

        files_table = self.query_one("#files-table", DataTable)
        files_table.add_columns("File Path", "Phrase / Role", "Details", "Size", "Sync")

        t0 = time.perf_counter()
        self._scan_local_state()
        self._render_all()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        self.post_log(f"Loaded local workspace in {elapsed_ms:.1f} ms.", level="INFO")
        buckets_table.focus()

        if self._blob_service is not None or self._auto_connect:
            self.refresh_remote_worker()

    def on_unmount(self) -> None:
        self._detach_log_capture()

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        """Enable or gray out Footer shortcuts dynamically based on bucket & state."""
        online = self._blob_service is not None
        spec = self._spec_by_key.get(self._selected_key)
        if action == "pull_selected":
            return True if (online and not self._busy_transfer) else None
        if action == "push_selected":
            if not online or self._busy_transfer or (spec is not None and spec.pull_only):
                return None
            return True
        if action == "stage_classifier":
            return True if (self._heads_summary.classifier_present and not self._busy_transfer) else None
        if action == "open_review":
            return True if self._swa_summary.session_count > 0 else None
        if action == "refresh_all":
            return True if not self._busy_transfer else None
        return True

    def _attach_log_capture(self) -> None:
        root_logger = logging.getLogger()
        self._saved_root_handlers = list(root_logger.handlers)
        root_logger.handlers = [
            h for h in self._saved_root_handlers if type(h).__name__ != "RichHandler"
        ]
        sync_logger = logging.getLogger("azure-sync")
        self._sync_logger_had_handler = self._log_handler in sync_logger.handlers
        if not self._sync_logger_had_handler:
            sync_logger.addHandler(self._log_handler)

    def _detach_log_capture(self) -> None:
        sync_logger = logging.getLogger("azure-sync")
        if self._log_handler in sync_logger.handlers:
            sync_logger.removeHandler(self._log_handler)
        root_logger = logging.getLogger()
        if self._saved_root_handlers:
            root_logger.handlers = self._saved_root_handlers

    def post_log(self, message: str, level: str = "INFO") -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        color = "red" if level in ("ERROR", "CRITICAL") else "yellow" if level == "WARNING" else "cyan"
        formatted = f"[dim]{ts}[/dim] [{color}]{escape(message)}[/{color}]"
        if threading.get_ident() == self._ui_thread_id:
            self._write_log_line(formatted)
        elif self.is_running:
            try:
                self.call_from_thread(self._write_log_line, formatted)
            except RuntimeError:
                pass

    def _write_log_line(self, formatted: str) -> None:
        try:
            self.query_one("#activity-log", RichLog).write(formatted)
        except Exception:
            pass

    def _scan_local_state(self) -> None:
        for spec in self._specs:
            scan = scan_local_dir(spec.local_dir)
            self._local_scans[spec.key] = scan
            if spec.key not in self._diffs:
                self._diffs[spec.key] = compute_bucket_diff(spec, scan, remote_blobs=None)
            else:
                prev = self._diffs[spec.key]
                self._diffs[spec.key] = BucketDiff(
                    spec=spec,
                    local_count=scan.file_count,
                    local_bytes=scan.total_bytes,
                    local_latest_iso=scan.latest_iso,
                    remote_count=prev.remote_count,
                    remote_bytes=prev.remote_bytes,
                    remote_latest_iso=prev.remote_latest_iso,
                    remote_files=prev.remote_files,
                    to_upload=prev.to_upload,
                    to_download=prev.to_download,
                    to_delete_remote=prev.to_delete_remote,
                    error=prev.error,
                    offline=prev.offline,
                )

        self._dataset_summary = get_dataset_summary(self.repo_root / "raw-sessions")
        self._heads_summary = get_heads_and_upstream_summary(
            self.repo_root / "trained-heads",
            self.repo_root / "raw-sessions",
            self.classifier_root,
        )
        self._swa_summary = get_swa_summary(
            self.repo_root / "synced-files",
            local_scan=self._local_scans.get("sessions"),
        )

    @work(thread=True, exclusive=True, group="refresh")
    def refresh_remote_worker(self) -> None:
        local_scans = {spec.key: scan_local_dir(spec.local_dir) for spec in self._specs}
        dataset_summary = get_dataset_summary(self.repo_root / "raw-sessions")
        heads_summary = get_heads_and_upstream_summary(
            self.repo_root / "trained-heads",
            self.repo_root / "raw-sessions",
            self.classifier_root,
        )
        swa_summary = get_swa_summary(
            self.repo_root / "synced-files",
            local_scan=local_scans.get("sessions"),
        )

        blob_service = self._blob_service
        account_label = self._account_label
        conn_hint = self._connection_hint

        if blob_service is None and self._auto_connect:
            blob_service, account_label, conn_hint = resolve_blob_service(self.config_path)

        if blob_service is not None:
            t0 = time.perf_counter()
            diffs = fetch_all_remote_diffs(blob_service, self._specs, local_scans)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            log_msg = f"Synced Azure metadata ({account_label}) across 3 containers in {elapsed_ms:.0f} ms."
        else:
            diffs = {
                spec.key: compute_bucket_diff(spec, local_scans[spec.key], remote_blobs=None)
                for spec in self._specs
            }
            log_msg = conn_hint or "Running in local offline mode."

        def _apply() -> None:
            self._blob_service = blob_service
            self._account_label = account_label
            self._connection_hint = conn_hint
            self._local_scans = local_scans
            self._dataset_summary = dataset_summary
            self._heads_summary = heads_summary
            self._swa_summary = swa_summary
            self._diffs = diffs
            self._render_all()
            self._write_log_line(f"[dim]{datetime.now().strftime('%H:%M:%S')}[/dim] [green]{escape(log_msg)}[/green]")

        if self.is_running:
            try:
                self.call_from_thread(_apply)
            except RuntimeError:
                pass

    def _render_all(self) -> None:
        self._render_banner()
        self._render_table()
        self._render_dataset_card()
        self._render_inspector()
        self.refresh_bindings()

    def _render_banner(self) -> None:
        if self._blob_service is not None:
            conn_badge = f"[bold green]● Azure: {escape(self._account_label)}[/bold green]"
        else:
            conn_badge = f"[bold yellow]○ {escape(self._account_label)}[/bold yellow]"

        if not self._heads_summary.classifier_present:
            cls_badge = "[dim]Classifier: Not adjacent[/dim]"
        elif self._heads_summary.needs_staging:
            cls_badge = "[bold yellow]⚠ Classifier: Updates ready[/bold yellow]"
        else:
            cls_badge = "[green]✓ Classifier: Staged[/green]"

        ds = self._dataset_summary
        if self._connection_hint and self._blob_service is None:
            tail = f"[yellow]{escape(self._connection_hint)}[/yellow]"
        else:
            tail = (
                f"[dim]Corpus: {ds.total_clips} clips · "
                f"{ds.phrase_count} phrases · "
                f"{len(self._heads_summary.profiles)} heads[/dim]"
            )

        self.query_one("#workflow-banner", Static).update(
            f"[bold]HearMe Data Sync Studio[/bold]   {conn_badge}   |   {cls_badge}   |   {tail}"
        )

    def _render_table(self) -> None:
        table = self.query_one("#buckets-table", DataTable)
        if len(self._col_keys) < 5:
            return
        for spec in self._specs:
            diff = self._diffs.get(spec.key)
            if diff is None:
                continue
            remote_str = "—" if diff.remote_count is None else str(diff.remote_count)
            size_str = format_bytes(diff.local_bytes if diff.local_count > 0 else diff.remote_bytes)
            status = diff.status_label
            if status.startswith("●"):
                status_cell = Text(status, style="bold green")
            elif status.startswith(("↑", "↓", "⇅")):
                status_cell = Text(status, style="bold yellow")
            elif status.startswith(("✕", "⚠")):
                status_cell = Text(status, style="bold red")
            else:
                status_cell = Text(status, style="dim")

            table.update_cell(spec.key, self._col_keys[0], Text(spec.title))
            table.update_cell(spec.key, self._col_keys[1], Text(str(diff.local_count)))
            table.update_cell(spec.key, self._col_keys[2], Text(remote_str))
            table.update_cell(spec.key, self._col_keys[3], Text(size_str))
            table.update_cell(spec.key, self._col_keys[4], status_cell)

    def _render_dataset_card(self) -> None:
        spec = self._spec_by_key[self._selected_key]
        lines = [
            f"[bold cyan]{escape(spec.title)}[/bold cyan]  [dim](container: {escape(spec.container)})[/dim]",
            f"{escape(spec.subtitle)}",
            "",
        ]
        if spec.key == "raw-sessions":
            ds = self._dataset_summary
            if ds.has_manifest:
                mins = ds.total_duration_s / 60.0
                lines.append(
                    f"[bold]Corpus:[/bold] {ds.total_clips} clips · {ds.recorded_phrase_count}/{ds.phrase_count} recorded words ({mins:.1f} min)"
                )
                for sid, info in ds.by_session.items():
                    sr = info.get("sample_rate") or "?"
                    ch = "mono" if info.get("channels") == 1 else "stereo" if info.get("channels") == 2 else "?"
                    lines.append(f"  • {escape(sid)}: {info['count']} WAVs ({sr} Hz {ch})")
                lines.append("")
                lines.append('[dim]Python: pd.read_json("raw-sessions/manifest.jsonl", lines=True)[/dim]')
            else:
                lines.append("[yellow]No local manifest.jsonl yet. Press G (Pull) or S (Stage).[/yellow]")
        elif spec.key == "trained-heads":
            hs = self._heads_summary
            if hs.profiles:
                lines.append("[bold]Staged Speaker Profiles:[/bold]")
                for pname, info in hs.profiles.items():
                    lines.append(
                        f"  • [bold]{escape(pname)}[/bold]: {info['label_count']} classes | "
                        f"head.pt {format_bytes(info['head_size_bytes'])} ({format_short_time(info['head_mtime_iso'])})"
                    )
            else:
                lines.append("[yellow]No trained heads local yet. Press G (Pull) or S (Stage).[/yellow]")
            if hs.staging_notes:
                lines.append("")
                lines.append("[bold yellow]Upstream Changes Detected:[/bold yellow]")
                for note in hs.staging_notes:
                    lines.append(f"  • {escape(note)}")
        else:
            swa = self._swa_summary
            lines.append("[bold yellow]🔒 Pull-Only Bucket[/bold yellow] (protects SWA recordings)")
            lines.append(
                f"Local SWA sessions: [bold]{swa.session_count}[/bold] ({swa.recording_count} recordings)"
            )
            lines.append("[dim]Press O after pulling sessions to listen in browser.[/dim]")

        self.query_one("#dataset-card", Static).update("\n".join(lines))

    def _render_inspector(self) -> None:
        spec = self._spec_by_key[self._selected_key]
        shown_words, total_words = self._render_words_table()
        shown_files, total_files = self._render_files_table()
        pending_total = self._render_diff_view()

        title_parts = [f"3. Inspector · [white]{escape(spec.title)}[/white]"]
        if self._filter_query:
            title_parts.append(f"[dim](filter: {escape(self._filter_query)})[/dim]")
        self.query_one("#inspector-title", Static).update("  ".join(title_parts))

        tabs = self.query_one("#inspector-tabs", TabbedContent)
        words_label = f"Words ({shown_words}/{total_words})" if self._filter_query else f"Words ({shown_words})"
        files_label = f"Files ({shown_files}/{total_files})" if self._filter_query else f"Files ({shown_files})"
        diff_label = f"Sync Diff ({pending_total})" if pending_total > 0 else "Sync Diff ✓"
        try:
            tabs.get_tab("tab-words").label = words_label
            tabs.get_tab("tab-files").label = files_label
            tabs.get_tab("tab-diff").label = diff_label
        except Exception:
            pass

        self._update_detail_bar()

    def _render_words_table(self) -> tuple[int, int]:
        spec = self._spec_by_key[self._selected_key]
        diff = self._diffs.get(spec.key)
        table = self.query_one("#words-table", DataTable)
        q = self._filter_query
        self._word_detail_by_key = {}
        self._word_drill_filter = {}

        if spec.key == "raw-sessions":
            if self._words_col_mode != "raw-sessions":
                table.clear(columns=True)
                table.add_columns("Phrase", "Label", "Takes", "Duration", "Sessions", "Audio")
                self._words_col_mode = "raw-sessions"
            else:
                table.clear(columns=False)

            remote_only = False
            if self._dataset_summary.has_manifest:
                source_words = self._dataset_summary.words
            elif diff and diff.remote_files:
                source_words = derive_remote_raw_words(diff.remote_files)
                remote_only = True
            else:
                source_words = []

            total_words = len(source_words)
            filtered = [
                w
                for w in source_words
                if not q
                or q in w.label.lower()
                or q in w.display_text.lower()
                or any(q in s.lower() for s in w.sessions)
            ]
            if not filtered:
                msg = f"No words matching '{q}'" if q else "No manifest yet — pull or stage raw-sessions"
                table.add_row(Text(msg, style="dim"), Text("—"), Text("0"), Text("—"), Text("—"), Text("—"), key="__empty__")
                return 0, total_words

            for w in filtered:
                recorded = w.clip_count > 0
                row_key = f"word:{w.label}"
                dur_str = "—" if remote_only or not recorded else f"{w.total_duration_s:.1f}s"
                sess_str = ", ".join(w.sessions) if w.sessions else "—"
                sr_str = (
                    "Azure"
                    if remote_only
                    else ("/".join(f"{sr // 1000}k" for sr in w.sample_rates) if w.sample_rates else "—")
                )
                p_style = "bold white" if recorded else "dim"
                l_style = "cyan" if recorded else "dim"
                c_style = "bold green" if recorded else "dim"
                table.add_row(
                    Text(w.display_text, style=p_style),
                    Text(w.label, style=l_style),
                    Text(str(w.clip_count), style=c_style),
                    Text(dur_str, style="" if recorded else "dim"),
                    Text(sess_str, style="" if recorded else "dim"),
                    Text(sr_str, style="" if recorded else "dim"),
                    key=row_key,
                )
                if recorded and not remote_only:
                    self._word_detail_by_key[row_key] = (
                        f'{w.label} ("{w.display_text}") — {w.clip_count} WAV takes '
                        f'({w.total_duration_s:.1f}s) in {", ".join(w.sessions)} · Press Enter for WAV files'
                    )
                    self._word_drill_filter[row_key] = f"/{w.label}/"
                elif recorded and remote_only:
                    self._word_detail_by_key[row_key] = (
                        f'{w.label} ("{w.display_text}") — {w.clip_count} Azure WAV(s) in {", ".join(w.sessions)} '
                        f'· Press G to pull or Enter for files'
                    )
                    self._word_drill_filter[row_key] = f"/{w.label}/"
                else:
                    self._word_detail_by_key[row_key] = (
                        f'{w.label} ("{w.display_text}") — Lexicon target phrase (0 takes recorded yet)'
                    )
            return len(filtered), total_words

        if spec.key == "trained-heads":
            if self._words_col_mode != "trained-heads":
                table.clear(columns=True)
                table.add_columns("Phrase", "Class Label", "Profiles", "Takes", "Threshold")
                self._words_col_mode = "trained-heads"
            else:
                table.clear(columns=False)

            source_words = self._heads_summary.words
            total_words = len(source_words)
            filtered = [
                w
                for w in source_words
                if not q
                or q in w.label.lower()
                or q in w.display_text.lower()
                or any(q in p.lower() for p in w.profiles)
            ]
            if not filtered:
                msg = f"No head classes matching '{q}'" if q else "No staged head labels — pull or stage trained-heads"
                table.add_row(Text(msg, style="dim"), Text("—"), Text("—"), Text("0"), Text("—"), key="__empty__")
                return 0, total_words

            for w in filtered:
                row_key = f"word:{w.label}"
                is_core = "kirk_core" in w.profiles
                profiles_str = ", ".join(w.profiles)
                takes_str = str(w.take_count) if w.take_count > 0 else "—"
                thresh_str = f"{w.threshold:.2f}" if w.threshold is not None else "—"
                table.add_row(
                    Text(w.display_text, style="bold green" if is_core else "white"),
                    Text(w.label, style="cyan"),
                    Text(profiles_str, style="bold green" if is_core else "dim"),
                    Text(takes_str, style="" if w.take_count > 0 else "dim"),
                    Text(thresh_str),
                    key=row_key,
                )
                target_profile = "kirk_core/" if is_core else f"{w.profiles[0]}/"
                self._word_detail_by_key[row_key] = (
                    f'{w.label} ("{w.display_text}") — Profiles: {profiles_str} · '
                    f'{w.take_count} takes · threshold {thresh_str} · Press Enter for profile files'
                )
                self._word_drill_filter[row_key] = target_profile
            return len(filtered), total_words

        # sessions (SWA)
        if self._words_col_mode != "sessions":
            table.clear(columns=True)
            table.add_columns("Phrase", "Phrase ID", "Recordings", "Sessions", "Feedback")
            self._words_col_mode = "sessions"
        else:
            table.clear(columns=False)

        source_words = self._swa_summary.words
        total_words = len(source_words)
        filtered = [
            w
            for w in source_words
            if not q
            or q in w.phrase_id.lower()
            or q in w.display_text.lower()
            or q in w.sample_feedback.lower()
        ]
        if not filtered:
            msg = f"No SWA phrases matching '{q}'" if q else "0 SWA participant sessions yet"
            note = "—" if q else "17 phrase lists seeded in Azure"
            table.add_row(Text(msg, style="yellow" if not q else "dim"), Text("—"), Text("0"), Text("0"), Text(note, style="dim"), key="__empty__")
            return 0, total_words

        for w in filtered:
            row_key = f"word:{w.phrase_id}"
            table.add_row(
                Text(w.display_text, style="bold white"),
                Text(w.phrase_id, style="cyan"),
                Text(str(w.recording_count), style="bold green"),
                Text(str(w.session_count)),
                Text(w.sample_feedback, style="dim"),
                key=row_key,
            )
            self._word_detail_by_key[row_key] = (
                f'{w.phrase_id} ("{w.display_text}") — {w.recording_count} SWA recordings '
                f'across {w.session_count} sessions · Press Enter for session files'
            )
            self._word_drill_filter[row_key] = w.phrase_id
        return len(filtered), total_words

    def _render_files_table(self) -> tuple[int, int]:
        spec = self._spec_by_key[self._selected_key]
        diff = self._diffs.get(spec.key)
        local_scan = self._local_scans.get(spec.key, LocalScanResult(exists=False))
        files_table = self.query_one("#files-table", DataTable)
        files_table.clear(columns=False)
        self._file_detail_by_key = {}

        if diff is None:
            return 0, 0

        all_rows = build_bucket_file_rows(
            spec,
            local_scan,
            diff,
            self._dataset_summary,
            self._heads_summary,
            self._swa_summary,
        )
        total_files = len(all_rows)
        q = self._filter_query
        if q:
            filtered = [
                row
                for row in all_rows
                if q in row.path.lower()
                or q in row.category.lower()
                or q in row.details.lower()
                or q in row.sync_state.lower()
            ]
        else:
            filtered = all_rows

        shown_files = len(filtered)
        if not filtered:
            msg = f"No files matching '{q}'" if q else f"No local or remote files in {spec.title}"
            files_table.add_row(Text(msg, style="dim"), Text("—"), Text("—"), Text("—"), Text("—"), key="__empty__")
            return 0, total_files

        max_rows = 1000
        for row in filtered[:max_rows]:
            row_key = f"file:{row.path}"
            if row.sync_state.startswith("↑"):
                state_style = "bold yellow"
            elif row.sync_state.startswith("↓"):
                state_style = "bold cyan"
            elif row.sync_state.startswith("✕"):
                state_style = "bold red"
            elif row.sync_state.startswith("✓"):
                state_style = "green"
            else:
                state_style = "dim"
            files_table.add_row(
                Text(row.path),
                Text(row.category, style="cyan"),
                Text(row.details, style="dim"),
                Text(format_bytes(row.size_bytes)),
                Text(row.sync_state, style=state_style),
                key=row_key,
            )
            self._file_detail_by_key[row_key] = (
                f"{row.path}  ({format_bytes(row.size_bytes)}, {row.size_bytes:,} B) · "
                f"{row.category} · {row.details} · {row.sync_state}"
            )

        if shown_files > max_rows:
            files_table.add_row(
                Text(f"… {shown_files - max_rows} additional files omitted (type / to narrow)", style="dim"),
                Text(""),
                Text(""),
                Text(""),
                Text(""),
                key="__truncated__",
            )
        return shown_files, total_files

    def _render_diff_view(self) -> int:
        spec = self._spec_by_key[self._selected_key]
        diff = self._diffs.get(spec.key)
        if diff is None:
            self.query_one("#diff-view", Static).update("Scanning…")
            return 0

        if diff.error:
            self.query_one("#diff-view", Static).update(
                f"[red]Error querying {escape(spec.container)}: {escape(diff.error)}[/red]"
            )
            return 0
        if diff.offline:
            self.query_one("#diff-view", Static).update(
                f"[yellow]Offline preview for {escape(spec.title)}[/yellow]\n"
                f"Local files: {diff.local_count} ({format_bytes(diff.local_bytes)})\n"
                "Add config.json to compare against Azure Blob Storage."
            )
            return 0

        lines = [
            f"[bold]{escape(spec.title)}[/bold]  "
            f"(Local: {diff.local_count} | Azure: {diff.remote_count} | Latest remote: {format_short_time(diff.remote_latest_iso)})",
            "",
        ]
        pending_total = len(diff.to_upload) + len(diff.to_download) + len(diff.to_delete_remote)
        if pending_total == 0:
            lines.append("[bold green]✓ Local directory and Azure container are completely in sync.[/bold green]")
        else:
            for heading, items, style in (
                ("↑ Ready to Upload", diff.to_upload, "yellow"),
                ("↓ Ready to Download", diff.to_download, "cyan"),
                ("✕ Remote Orphans to Prune on Push", diff.to_delete_remote, "red"),
            ):
                if not items:
                    continue
                lines.append(f"[bold {style}]{heading} ({len(items)}):[/bold {style}]")
                for item in items[:20]:
                    lines.append(f"  [{style}]• {escape(item)}[/{style}]")
                if len(items) > 20:
                    lines.append(f"  [dim]… and {len(items) - 20} more (see Files tab)[/dim]")
                lines.append("")

        self.query_one("#diff-view", Static).update("\n".join(lines))
        return pending_total

    def _highlighted_row_key(self, table_id: str) -> str | None:
        table = self.query_one(f"#{table_id}", DataTable)
        if table.row_count == 0 or not table.is_valid_coordinate(table.cursor_coordinate):
            return None
        cell_key = table.coordinate_to_cell_key(table.cursor_coordinate)
        val = getattr(cell_key.row_key, "value", None)
        return val if isinstance(val, str) else None

    def _update_detail_bar(self, explicit_text: str | None = None) -> None:
        detail_widget = self.query_one("#inspector-detail", Static)
        if explicit_text is not None:
            detail_widget.update(escape(explicit_text))
            return

        active_tab = self.query_one("#inspector-tabs", TabbedContent).active
        if active_tab == "tab-words":
            key = self._highlighted_row_key("words-table")
            if key and key in self._word_detail_by_key:
                detail_widget.update(escape(self._word_detail_by_key[key]))
                return
        elif active_tab == "tab-files":
            key = self._highlighted_row_key("files-table")
            if key and key in self._file_detail_by_key:
                detail_widget.update(escape(self._file_detail_by_key[key]))
                return

        spec = self._spec_by_key[self._selected_key]
        detail_widget.update(f"[dim]{escape(spec.title)}: Use ↑/↓ in tables, / to filter, V to switch view.[/dim]")

    def _set_filter_value(self, new_value: str) -> None:
        self._filter_query = new_value.strip().lower()
        filter_input = self.query_one("#inspector-filter", Input)
        if filter_input.value != new_value:
            filter_input.value = new_value
        self._render_inspector()

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        key = getattr(event.row_key, "value", None)
        if not isinstance(key, str):
            return
        table_id = event.data_table.id
        if table_id == "buckets-table":
            if key in self._spec_by_key and key != self._selected_key:
                self._selected_key = key
                self._render_dataset_card()
                self._render_inspector()
                self.refresh_bindings()
        elif table_id == "words-table":
            detail = self._word_detail_by_key.get(key)
            if detail:
                self._update_detail_bar(detail)
        elif table_id == "files-table":
            detail = self._file_detail_by_key.get(key)
            if detail:
                self._update_detail_bar(detail)

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        key = getattr(event.row_key, "value", None)
        if not isinstance(key, str):
            return
        table_id = event.data_table.id
        if table_id == "buckets-table":
            if key in self._spec_by_key:
                self._selected_key = key
                self._render_dataset_card()
                self._render_inspector()
                self.refresh_bindings()
        elif table_id == "words-table":
            target = self._word_drill_filter.get(key)
            if target:
                self._set_filter_value(target)
                self.query_one("#inspector-tabs", TabbedContent).active = "tab-files"
                self.query_one("#files-table", DataTable).focus()
                self._update_detail_bar()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id != "inspector-filter":
            return
        normalized = event.value.strip().lower()
        if normalized == self._filter_query:
            return
        self._filter_query = normalized
        self._render_inspector()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "inspector-filter":
            return
        active_tab = self.query_one("#inspector-tabs", TabbedContent).active
        if active_tab == "tab-words":
            self.query_one("#words-table", DataTable).focus()
        elif active_tab == "tab-files":
            self.query_one("#files-table", DataTable).focus()
        else:
            self.query_one("#diff-scroll", VerticalScroll).focus()

    def on_tabbed_content_tab_activated(self, event: TabbedContent.TabActivated) -> None:
        if event.tabbed_content.id == "inspector-tabs":
            self._update_detail_bar()

    def on_key(self, event: events.Key) -> None:
        if event.key != "escape" or self.screen is not self.screen_stack[0]:
            return
        filter_input = self.query_one("#inspector-filter", Input)
        if self.focused is filter_input or bool(filter_input.value):
            if filter_input.value:
                self._set_filter_value("")
            self.query_one("#buckets-table", DataTable).focus()
            event.prevent_default()
            event.stop()

    def action_refresh_all(self) -> None:
        if not self._busy_transfer:
            self.post_log("Refreshing local and Azure state…", level="INFO")
            self.refresh_remote_worker()

    def action_focus_filter(self) -> None:
        self.query_one("#inspector-filter", Input).focus()

    def action_cycle_view(self) -> None:
        tabs = self.query_one("#inspector-tabs", TabbedContent)
        cur = tabs.active if tabs.active in self._TAB_ORDER else self._TAB_ORDER[0]
        next_idx = (self._TAB_ORDER.index(cur) + 1) % len(self._TAB_ORDER)
        tabs.active = self._TAB_ORDER[next_idx]

    def action_activate_tab(self, tab_id: str) -> None:
        if tab_id in self._TAB_ORDER:
            self.query_one("#inspector-tabs", TabbedContent).active = tab_id

    def action_focus_diff(self) -> None:
        self.action_activate_tab("tab-diff")

    def action_show_guide(self) -> None:
        self.push_screen(StudentGuideModal())

    def action_pull_selected(self) -> None:
        if self._busy_transfer:
            return
        if self._blob_service is None:
            self.post_log("Cannot pull while offline. Add config.json first.", level="WARNING")
            return
        spec = self._spec_by_key[self._selected_key]
        self._run_pull_worker(spec)

    def action_push_selected(self) -> None:
        if self._busy_transfer:
            return
        spec = self._spec_by_key[self._selected_key]
        if spec.pull_only:
            self.post_log(f"'{spec.title}' is Pull-Only to protect SWA recordings.", level="WARNING")
            return
        if self._blob_service is None:
            self.post_log("Cannot push while offline. Add config.json first.", level="WARNING")
            return

        diff = self._diffs.get(spec.key)
        up_count = len(diff.to_upload) if diff else 0
        del_count = len(diff.to_delete_remote) if (diff and spec.delete_orphaned_on_push) else 0
        if diff and up_count == 0 and del_count == 0:
            self.post_log(f"{spec.title} is already in sync with Azure (0 changes).", level="INFO")
            return

        preview_lines = [
            f"Container: [bold]{escape(spec.container)}[/bold]  ←  {escape(str(spec.local_dir.name))}/",
            f"Files to upload: [bold yellow]{up_count}[/bold yellow]   |   Remote orphans to delete: [bold red]{del_count}[/bold red]",
            "",
        ]
        sample = (diff.to_upload[:6] if diff else []) + [f"[DEL] {x}" for x in (diff.to_delete_remote[:4] if diff else [])]
        for line in sample:
            preview_lines.append(f"  • {escape(line)}")

        def _after_confirm(confirmed: bool | None) -> None:
            if confirmed:
                self._run_push_worker(spec)

        self.push_screen(
            ConfirmSyncModal(f"Confirm Push to '{spec.container}'", "\n".join(preview_lines)),
            _after_confirm,
        )

    def action_stage_classifier(self) -> None:
        if self._busy_transfer:
            return
        if not self.classifier_root.is_dir():
            self.post_log(f"Classifier repo not found at {self.classifier_root}.", level="WARNING")
            return
        self._run_stage_worker()

    def action_open_review(self) -> None:
        synced_dir = self.repo_root / "synced-files"
        sessions = discover_sessions(synced_dir=synced_dir)
        if not sessions:
            self.post_log(
                "No SWA sessions in synced-files/. Select 'sessions (SWA)' and press G to pull first.",
                level="WARNING",
            )
            return
        out_path = synced_dir / "review.html"
        out_path.write_text(generate_html(sessions), encoding="utf-8")
        self.post_log(f"Generated {out_path} ({len(sessions)} sessions).", level="INFO")
        webbrowser.open(out_path.as_uri())

    @work(thread=True, exclusive=True, group="transfer")
    def _run_pull_worker(self, spec: BucketSpec) -> None:
        self._set_busy(True)
        self.post_log(f"Pulling '{spec.container}' → {spec.local_dir.name}/ …", level="INFO")
        last_step = [0]

        def _on_prog(done: int, total: int, _name: str) -> None:
            if done == total or done - last_step[0] >= max(1, total // 5):
                last_step[0] = done
                self.post_log(f"Pull progress: {done}/{total} blobs…", level="INFO")

        try:
            cc = self._blob_service.get_container_client(spec.container)
            stats = sync_container(
                cc,
                spec.local_dir,
                verify_local_files=True,
                quiet=True,
                progress_callback=_on_prog,
            )
            self.post_log(
                f"Pull complete: downloaded={stats['downloaded']}, skipped={stats['skipped']}, errors={stats['errors']}.",
                level="ERROR" if stats["errors"] else "INFO",
            )
        except Exception as exc:
            self.post_log(f"Pull failed: {exc}", level="ERROR")
        finally:
            self._set_busy(False)
            self.refresh_remote_worker()

    @work(thread=True, exclusive=True, group="transfer")
    def _run_push_worker(self, spec: BucketSpec) -> None:
        self._set_busy(True)
        self.post_log(f"Pushing {spec.local_dir.name}/ → '{spec.container}' …", level="INFO")
        last_step = [0]

        def _on_prog(done: int, total: int, _name: str) -> None:
            if done == total or done - last_step[0] >= max(1, total // 5):
                last_step[0] = done
                self.post_log(f"Push progress: {done}/{total} files…", level="INFO")

        try:
            cc = self._blob_service.get_container_client(spec.container)
            stats = push_directory(
                cc,
                spec.local_dir,
                delete_orphaned=spec.delete_orphaned_on_push,
                quiet=True,
                progress_callback=_on_prog,
            )
            self.post_log(
                f"Push complete: uploaded={stats['uploaded']}, skipped={stats['skipped']}, "
                f"deleted={stats['deleted']}, errors={stats['errors']}.",
                level="ERROR" if stats["errors"] else "INFO",
            )
        except Exception as exc:
            self.post_log(f"Push failed: {exc}", level="ERROR")
        finally:
            self._set_busy(False)
            self.refresh_remote_worker()

    @work(thread=True, exclusive=True, group="transfer")
    def _run_stage_worker(self) -> None:
        self._set_busy(True)
        self.post_log(f"Staging from {self.classifier_root} …", level="INFO")
        try:
            raw_stats = stage_classifier_raw_sessions(
                self.classifier_root,
                self.repo_root / "raw-sessions",
            )
            head_stats = stage_trained_heads(
                self.classifier_root,
                self.repo_root / "trained-heads",
            )
            self.post_log(
                f"Stage complete: raw copied={raw_stats['copied']} (stale pruned={raw_stats['removed_stale']}), "
                f"heads copied={head_stats['copied']}.",
                level="INFO",
            )
        except Exception as exc:
            self.post_log(f"Staging failed: {exc}", level="ERROR")
        finally:
            self._set_busy(False)
            self.refresh_remote_worker()

    def _set_busy(self, busy: bool) -> None:
        self._busy_transfer = busy
        if self.is_running:
            try:
                self.call_from_thread(self.refresh_bindings)
            except RuntimeError:
                pass


def run_tui(
    config_path: Path = Path("config.json"),
    classifier_root: Path = Path("../audio-classifier-hear-me"),
) -> None:
    """Launch the Textual HearMe Sync Studio dashboard."""
    app = HearMeSyncApp(
        repo_root=Path(__file__).parent.resolve(),
        config_path=config_path,
        classifier_root=classifier_root,
    )
    app.run()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launch HearMe Data Sync Studio TUI")
    parser.add_argument("--config", "-c", type=Path, default=Path("config.json"))
    parser.add_argument("--classifier-root", type=Path, default=Path("../audio-classifier-hear-me"))
    return parser.parse_args()


if __name__ == "__main__":
    cli_args = _parse_args()
    run_tui(config_path=cli_args.config, classifier_root=cli_args.classifier_root)
