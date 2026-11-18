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
from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Button, DataTable, Footer, Header, RichLog, Static

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
    compute_bucket_diff,
    default_bucket_specs,
    fetch_all_remote_diffs,
    format_bytes,
    format_short_time,
    get_dataset_summary,
    get_heads_and_upstream_summary,
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
    width: 48%;
    height: 1fr;
    margin-right: 1;
}

#right-pane {
    width: 52%;
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
    height: 1fr;
    border: round $primary-lighten-2;
    background: $panel;
    padding: 0 1;
}

#diff-scroll {
    height: 1fr;
    border: round $primary-lighten-2;
    background: $panel;
    padding: 0 1;
    margin-bottom: 1;
}

#activity-log {
    height: 9;
    border: round $primary-lighten-2;
    background: $panel;
    padding: 0 1;
}

#action-bar {
    height: 3;
    margin: 1 1 0 1;
    align: left middle;
}

#action-bar Button {
    margin-right: 1;
    min-width: 12;
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
                "  2) Select [bold]raw-sessions[/bold] and press [bold]\\[G] Pull[/bold] to download training audio.",
                "  3) Select [bold]trained-heads[/bold] and press [bold]\\[G] Pull[/bold] to download head weights.",
                "",
                "[bold cyan]3. Load Raw Audio & Labels in Python / Pandas[/bold cyan]",
                '  [green]import pandas as pd[/green]',
                '  [green]df = pd.read_json("raw-sessions/manifest.jsonl", lines=True)[/green]',
                '  [green]print(df.groupby(["session_id", "sample_rate"]).size())[/green]',
                "",
                "[bold cyan]4. Updating From audio-classifier-hear-me[/bold cyan]",
                "  • Press [bold]\\[S] Stage[/bold] to copy new clips/heads locally in strict snake_case.",
                "  • Press [bold]\\[P] Push[/bold] to preview exact upload & prune counts before confirming.",
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
        Binding("d", "focus_diff", "Diff"),
        Binding("o", "open_review", "Review"),
        Binding("r", "refresh_all", "Refresh"),
        Binding("question_mark", "show_guide", "Guide", key_display="?"),
        Binding("q", "quit", "Quit"),
    ]

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

        self._local_scans: dict[str, LocalScanResult] = {}
        self._diffs: dict[str, BucketDiff] = {}
        self._dataset_summary = DatasetSummary()
        self._heads_summary = HeadsAndUpstreamSummary()

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
                yield Static("Dataset & Model Summary", classes="pane-title")
                with VerticalScroll(id="dataset-scroll"):
                    yield Static(id="dataset-card")
            with Vertical(id="right-pane"):
                yield Static("Live Sync Diff Preview", classes="pane-title")
                with VerticalScroll(id="diff-scroll"):
                    yield Static(id="diff-view")
                yield Static("Activity Log", classes="pane-title")
                yield RichLog(id="activity-log", highlight=True, markup=True)
        with Horizontal(id="action-bar"):
            yield Button("Pull \\[G]", id="btn-pull", variant="primary")
            yield Button("Push \\[P]", id="btn-push", variant="warning")
            yield Button("Stage \\[S]", id="btn-stage", variant="default")
            yield Button("Review HTML \\[O]", id="btn-review", variant="default")
            yield Button("Refresh \\[R]", id="btn-refresh", variant="default")
            yield Button("Guide \\[?]", id="btn-guide", variant="success")
        yield Footer()

    def on_mount(self) -> None:
        self._attach_log_capture()
        table = self.query_one("#buckets-table", DataTable)
        self._col_keys = list(table.add_columns("Bucket", "Local", "Azure", "Size", "Status"))
        for spec in self._specs:
            table.add_row(spec.title, "…", "…", "…", "Scanning…", key=spec.key)

        t0 = time.perf_counter()
        self._scan_local_state()
        self._render_all()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        self.post_log(f"Loaded local workspace in {elapsed_ms:.1f} ms.", level="INFO")

        if self._blob_service is not None or self._auto_connect:
            self.refresh_remote_worker()

    def on_unmount(self) -> None:
        self._detach_log_capture()

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

    @work(thread=True, exclusive=True, group="refresh")
    def refresh_remote_worker(self) -> None:
        local_scans = {spec.key: scan_local_dir(spec.local_dir) for spec in self._specs}
        dataset_summary = get_dataset_summary(self.repo_root / "raw-sessions")
        heads_summary = get_heads_and_upstream_summary(
            self.repo_root / "trained-heads",
            self.repo_root / "raw-sessions",
            self.classifier_root,
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
        self._render_diff_view()
        self._sync_button_states()

    def _render_banner(self) -> None:
        if self._blob_service is not None:
            conn_badge = f"[bold green]● Azure: {escape(self._account_label)}[/bold green]"
        else:
            conn_badge = f"[bold yellow]○ {escape(self._account_label)}[/bold yellow]"

        if not self._heads_summary.classifier_present:
            cls_badge = "[dim]Classifier: Not adjacent[/dim]"
        elif self._heads_summary.needs_staging:
            cls_badge = "[bold yellow]⚠ Classifier: Updates ready (\\[S] Stage)[/bold yellow]"
        else:
            cls_badge = "[green]✓ Classifier: Staged[/green]"

        workflow = (
            "[dim]Workflow: 1. Pick bucket (↑/↓)  →  2. \\[G] Pull or \\[S] Stage  →  3. Review Diff & \\[P] Push  |  \\[?] Guide[/dim]"
        )
        if self._connection_hint and self._blob_service is None:
            workflow = f"[yellow]{escape(self._connection_hint)}[/yellow]"

        self.query_one("#workflow-banner", Static).update(
            f"[bold]HearMe Data Sync Studio[/bold]   {conn_badge}   |   {cls_badge}\n{workflow}"
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

            table.update_cell(spec.key, self._col_keys[0], spec.title)
            table.update_cell(spec.key, self._col_keys[1], str(diff.local_count))
            table.update_cell(spec.key, self._col_keys[2], remote_str)
            table.update_cell(spec.key, self._col_keys[3], size_str)
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
                    f"[bold]Corpus:[/bold] {ds.total_clips} clips across {ds.phrase_count} classes ({mins:.1f} min)"
                )
                for sid, info in ds.by_session.items():
                    sr = info.get("sample_rate") or "?"
                    ch = "mono" if info.get("channels") == 1 else "stereo" if info.get("channels") == 2 else "?"
                    lines.append(f"  • {escape(sid)}: {info['count']} WAVs ({sr} Hz {ch})")
                lines.append("")
                lines.append('[dim]Python: pd.read_json("raw-sessions/manifest.jsonl", lines=True)[/dim]')
            else:
                lines.append("[yellow]No local manifest.jsonl yet. Press \\[G] Pull or \\[S] Stage.[/yellow]")
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
                lines.append("[yellow]No trained heads local yet. Press \\[G] Pull or \\[S] Stage.[/yellow]")
            if hs.staging_notes:
                lines.append("")
                lines.append("[bold yellow]Upstream Changes Detected:[/bold yellow]")
                for note in hs.staging_notes:
                    lines.append(f"  • {escape(note)}")
        else:
            diff = self._diffs.get("sessions")
            count = diff.local_count if diff else 0
            lines.append("[bold yellow]🔒 Pull-Only Bucket[/bold yellow] (protects live SWA participant recordings)")
            lines.append(f"Local SWA files in [bold]synced-files/[/bold]: {count}")
            lines.append("Press [bold]\\[O] Review HTML[/bold] after pulling sessions to listen in browser.")

        self.query_one("#dataset-card", Static).update("\n".join(lines))

    def _render_diff_view(self) -> None:
        spec = self._spec_by_key[self._selected_key]
        diff = self._diffs.get(spec.key)
        if diff is None:
            self.query_one("#diff-view", Static).update("Scanning…")
            return

        if diff.error:
            self.query_one("#diff-view", Static).update(f"[red]Error querying {escape(spec.container)}: {escape(diff.error)}[/red]")
            return
        if diff.offline:
            self.query_one("#diff-view", Static).update(
                f"[yellow]Offline preview for {escape(spec.title)}[/yellow]\n"
                f"Local files: {diff.local_count} ({format_bytes(diff.local_bytes)})\n"
                "Add config.json to compare against Azure Blob Storage."
            )
            return

        lines = [
            f"[bold]{escape(spec.title)}[/bold]  "
            f"(Local: {diff.local_count} | Azure: {diff.remote_count} | Latest remote: {format_short_time(diff.remote_latest_iso)})",
            "",
        ]
        if not diff.to_upload and not diff.to_download and not diff.to_delete_remote:
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
                for item in items[:10]:
                    lines.append(f"  [{style}]• {escape(item)}[/{style}]")
                if len(items) > 10:
                    lines.append(f"  [dim]… and {len(items) - 10} more[/dim]")
                lines.append("")

        self.query_one("#diff-view", Static).update("\n".join(lines))

    def _sync_button_states(self) -> None:
        spec = self._spec_by_key[self._selected_key]
        online = self._blob_service is not None
        self.query_one("#btn-pull", Button).disabled = (not online) or self._busy_transfer
        self.query_one("#btn-push", Button).disabled = (not online) or spec.pull_only or self._busy_transfer
        self.query_one("#btn-stage", Button).disabled = (not self._heads_summary.classifier_present) or self._busy_transfer
        self.query_one("#btn-refresh", Button).disabled = self._busy_transfer

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        key = getattr(event.row_key, "value", None)
        if isinstance(key, str) and key in self._spec_by_key and key != self._selected_key:
            self._selected_key = key
            self._render_dataset_card()
            self._render_diff_view()
            self._sync_button_states()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        key = getattr(event.row_key, "value", None)
        if isinstance(key, str) and key in self._spec_by_key:
            self._selected_key = key
            self._render_dataset_card()
            self._render_diff_view()
            self._sync_button_states()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        dispatch = {
            "btn-pull": self.action_pull_selected,
            "btn-push": self.action_push_selected,
            "btn-stage": self.action_stage_classifier,
            "btn-review": self.action_open_review,
            "btn-refresh": self.action_refresh_all,
            "btn-guide": self.action_show_guide,
        }
        handler = dispatch.get(event.button.id or "")
        if handler:
            handler()

    def action_refresh_all(self) -> None:
        if not self._busy_transfer:
            self.post_log("Refreshing local and Azure state…", level="INFO")
            self.refresh_remote_worker()

    def action_focus_diff(self) -> None:
        self.query_one("#diff-scroll", VerticalScroll).focus()

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
                "No SWA sessions in synced-files/. Select 'sessions (SWA)' and press [G] Pull first.",
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
                self.call_from_thread(self._sync_button_states)
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
