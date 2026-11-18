"""Tests for sync_inspector and Textual HearMeSyncApp TUI."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from textual.widgets import DataTable, Input, Static, TabbedContent

from sync import MANIFEST_FILE, PUSH_MANIFEST_FILE, save_manifest
from sync_inspector import (
    compute_bucket_diff,
    default_bucket_specs,
    derive_remote_raw_words,
    get_dataset_summary,
    get_heads_and_upstream_summary,
    scan_local_dir,
)
from tui import ConfirmSyncModal, HearMeSyncApp, QuickstartGuideModal


def _make_blob(name: str, etag: str = "0x1", size: int = 100):
    return SimpleNamespace(
        name=name,
        etag=etag,
        last_modified=datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc),
        size=size,
    )


def _make_mock_blob_service(container_blobs: dict[str, list]) -> MagicMock:
    service = MagicMock()

    def _get_cc(name: str):
        cc = MagicMock()
        cc.list_blobs.return_value = container_blobs.get(name, [])
        return cc

    service.get_container_client.side_effect = _get_cc
    return service


def _seed_workspace(root: Path, *, with_classifier: bool = True) -> Path:
    raw_dir = root / "raw-sessions"
    wav_path = raw_dir / "session_2026_08_25" / "all" / "all_01.wav"
    wav_path.parent.mkdir(parents=True)
    wav_path.write_bytes(b"wav-bytes")
    wav_stat = wav_path.stat()

    manifest_row = {
        "path": "session_2026_08_25/all/all_01.wav",
        "session_id": "session_2026_08_25",
        "label": "all",
        "display_text": "All",
        "take_id": "01",
        "sample_rate": 16000,
        "channels": 1,
        "duration_s": 1.5,
        "size": wav_stat.st_size,
    }
    (raw_dir / "manifest.jsonl").write_text(json.dumps(manifest_row) + "\n", encoding="utf-8")
    (raw_dir / "lexicon.json").write_text(
        json.dumps({"phrases": [{"id": "all", "display_text": "All"}]}),
        encoding="utf-8",
    )

    heads_dir = root / "trained-heads" / "kirk"
    heads_dir.mkdir(parents=True)
    head_file = heads_dir / "head.pt"
    head_file.write_bytes(b"head-pt")
    labels_file = heads_dir / "labels.json"
    labels_file.write_text(json.dumps(["all"]), encoding="utf-8")

    classifier_root = root / "audio-classifier-hear-me"
    if with_classifier:
        cls_profiles = classifier_root / "profiles" / "kirk"
        cls_profiles.mkdir(parents=True)
        (cls_profiles / "head.pt").write_bytes(b"head-pt-updated")
        (cls_profiles / "labels.json").write_text(json.dumps(["all"]), encoding="utf-8")
        edited = classifier_root / "Edited_Clips"
        edited.mkdir(parents=True)
        (edited / "All.wav").write_bytes(b"edited-wav")

    return classifier_root


def test_scan_local_dir_and_dataset_summary(tmp_path: Path):
    _seed_workspace(tmp_path, with_classifier=False)
    scan = scan_local_dir(tmp_path / "raw-sessions")
    assert scan.exists is True
    assert scan.file_count == 3
    assert "session_2026_08_25/all/all_01.wav" in scan.files
    assert "manifest.jsonl" in scan.files
    assert "lexicon.json" in scan.files

    summary = get_dataset_summary(tmp_path / "raw-sessions")
    assert summary.has_manifest is True
    assert summary.total_clips == 1
    assert summary.phrase_count == 1
    assert summary.by_session["session_2026_08_25"]["sample_rate"] == 16000


def test_get_heads_and_upstream_summary_detects_drift(tmp_path: Path):
    cls_root = _seed_workspace(tmp_path, with_classifier=True)
    summary = get_heads_and_upstream_summary(
        tmp_path / "trained-heads",
        tmp_path / "raw-sessions",
        cls_root,
    )
    assert summary.classifier_present is True
    assert summary.needs_staging is True
    assert summary.profiles["kirk"]["label_count"] == 1
    assert any("Edited_Clips" in note for note in summary.staging_notes)
    assert any("profile head" in note for note in summary.staging_notes)


def test_compute_bucket_diff_modes(tmp_path: Path):
    _seed_workspace(tmp_path, with_classifier=False)
    specs = {s.key: s for s in default_bucket_specs(tmp_path)}
    raw_dir = specs["raw-sessions"].local_dir
    scan = scan_local_dir(raw_dir)

    offline = compute_bucket_diff(specs["raw-sessions"], scan, remote_blobs=None)
    assert offline.status_label == "○ Offline"

    # Match push manifest for all 3 files + add one remote orphan
    push_manifest = {}
    remote_blobs = []
    for idx, (rel_path, (sz, mtime_ns)) in enumerate(sorted(scan.files.items())):
        etag = f"0xE{idx}"
        push_manifest[rel_path] = {"size": sz, "mtime_ns": mtime_ns, "etag": etag}
        remote_blobs.append(_make_blob(rel_path, etag=etag, size=sz))
    save_manifest(raw_dir, push_manifest, PUSH_MANIFEST_FILE)

    in_sync = compute_bucket_diff(specs["raw-sessions"], scan, remote_blobs=remote_blobs)
    assert in_sync.status_label == "● In Sync"
    assert in_sync.to_upload == []
    assert in_sync.to_download == []

    remote_with_orphan = remote_blobs + [_make_blob("legacy/old.wav", etag="0xOLD", size=50)]
    orphan_diff = compute_bucket_diff(specs["raw-sessions"], scan, remote_blobs=remote_with_orphan)
    assert orphan_diff.to_download == ["legacy/old.wav"]
    assert orphan_diff.to_delete_remote == ["legacy/old.wav"]

    # Cleanly pulled file tracked via .sync_manifest.json skips upload even if push manifest missing
    (raw_dir / PUSH_MANIFEST_FILE).unlink()
    sync_manifest = {
        rel_path: {"etag": f"0xE{idx}", "size": sz, "mtime_ns": mtime_ns}
        for idx, (rel_path, (sz, mtime_ns)) in enumerate(sorted(scan.files.items()))
    }
    save_manifest(raw_dir, sync_manifest, MANIFEST_FILE)
    pulled_diff = compute_bucket_diff(specs["raw-sessions"], scan, remote_blobs=remote_blobs)
    assert pulled_diff.to_upload == []
    assert pulled_diff.to_download == []


def test_scan_local_dir_skips_dashboard_exports(tmp_path: Path):
    synced_dir = tmp_path / "synced-files"
    synced_dir.mkdir(parents=True)
    (synced_dir / "hearme_dashboard.html").write_text("<html></html>", encoding="utf-8")
    (synced_dir / "hearme_powerbi.xlsx").write_bytes(b"xlsx-bytes")
    (synced_dir / "review.html").write_text("<html></html>", encoding="utf-8")

    scan = scan_local_dir(synced_dir)
    assert scan.exists is True
    assert scan.file_count == 0
    assert scan.total_bytes == 0


def test_derive_remote_raw_words():
    remote_files = {
        "session_2026_08_25/all/all_01.wav": (32000, "2026-11-18T12:00:00+00:00"),
        "session_2026_09_25/all/all_02.wav": (32000, "2026-11-18T12:00:00+00:00"),
        "session_2026_11_18_edited_clips/bless_you/bless_you_01.wav": (64000, "2026-11-18T12:00:00+00:00"),
        "manifest.jsonl": (1024, "2026-11-18T12:00:00+00:00"),
    }
    words = derive_remote_raw_words(remote_files)
    assert [w.label for w in words] == ["all", "bless_you"]
    assert words[0].clip_count == 2
    assert words[0].sessions == ("08_25", "09_25")
    assert words[1].display_text == "Bless you"
    assert words[1].sessions == ("11_18",)


def test_tui_offline_boot_and_guide_modal(tmp_path: Path):
    _seed_workspace(tmp_path, with_classifier=False)

    async def _run():
        app = HearMeSyncApp(
            repo_root=tmp_path,
            config_path=tmp_path / "missing_config.json",
            classifier_root=tmp_path / "missing-classifier",
            auto_connect=False,
        )
        async with app.run_test(size=(120, 36)) as pilot:
            await pilot.pause(0.05)
            assert len(app.query("#action-bar")) == 0
            table = app.query_one("#buckets-table", DataTable)
            assert table.row_count == 3
            assert app.check_action("pull_selected", ()) is None
            assert app.check_action("push_selected", ()) is None
            assert app.check_action("stage_classifier", ()) is None
            assert app.check_action("open_review", ()) is None

            banner_text = str(app.query_one("#workflow-banner", Static).render())
            assert "Offline" in banner_text
            assert app.query_one("#words-table", DataTable).row_count == 1
            assert app.query_one("#files-table", DataTable).row_count == 3

            await pilot.press("?")
            await pilot.pause(0.05)
            assert isinstance(app.screen, QuickstartGuideModal)

            await pilot.press("escape")
            await pilot.pause(0.05)
            assert not isinstance(app.screen, QuickstartGuideModal)

    asyncio.run(_run())


def test_tui_mock_azure_diff_navigation_and_confirm_modal(tmp_path: Path):
    cls_root = _seed_workspace(tmp_path, with_classifier=True)
    raw_scan = scan_local_dir(tmp_path / "raw-sessions")
    heads_scan = scan_local_dir(tmp_path / "trained-heads")

    # Mark trained-heads completely in sync; leave raw-sessions with 1 pending upload
    heads_manifest = {}
    head_blobs = []
    for idx, (rel, (sz, mtime_ns)) in enumerate(sorted(heads_scan.files.items())):
        etag = f"0xH{idx}"
        heads_manifest[rel] = {"size": sz, "mtime_ns": mtime_ns, "etag": etag}
        head_blobs.append(_make_blob(rel, etag=etag, size=sz))
    save_manifest(tmp_path / "trained-heads", heads_manifest, PUSH_MANIFEST_FILE)

    raw_push_manifest = {}
    raw_blobs = []
    raw_items = sorted(raw_scan.files.items())
    for idx, (rel, (sz, mtime_ns)) in enumerate(raw_items[:-1]):
        etag = f"0xR{idx}"
        raw_push_manifest[rel] = {"size": sz, "mtime_ns": mtime_ns, "etag": etag}
        raw_blobs.append(_make_blob(rel, etag=etag, size=sz))
    save_manifest(tmp_path / "raw-sessions", raw_push_manifest, PUSH_MANIFEST_FILE)

    mock_service = _make_mock_blob_service(
        {
            "raw-sessions": raw_blobs,
            "trained-heads": head_blobs,
            "sessions": [],
        }
    )

    async def _run():
        app = HearMeSyncApp(
            repo_root=tmp_path,
            config_path=tmp_path / "config.json",
            classifier_root=cls_root,
            blob_service=mock_service,
            auto_connect=False,
        )
        async with app.run_test(size=(120, 36)) as pilot:
            for _ in range(30):
                await pilot.pause(0.05)
                if not app._diffs["raw-sessions"].offline:
                    break

            assert app._diffs["raw-sessions"].status_label == "↑ 1 to push"
            assert app._diffs["trained-heads"].status_label == "● In Sync"
            assert app._diffs["sessions"].status_label == "● Empty (Ready)"
            assert app.check_action("push_selected", ()) is True
            assert app.check_action("pull_selected", ()) is True
            assert app.check_action("stage_classifier", ()) is True
            assert app.check_action("open_review", ()) is None

            # Pressing 'p' opens ConfirmSyncModal with upload preview; Esc cancels
            await pilot.press("p")
            await pilot.pause(0.05)
            assert isinstance(app.screen, ConfirmSyncModal)
            await pilot.press("escape")
            await pilot.pause(0.05)
            assert not isinstance(app.screen, ConfirmSyncModal)

            # Navigate down twice to 'sessions' (Pull-Only): push action dims in Footer
            table = app.query_one("#buckets-table", DataTable)
            table.focus()
            await pilot.press("down", "down")
            await pilot.pause(0.05)
            assert app._selected_key == "sessions"
            assert app.check_action("push_selected", ()) is None

    asyncio.run(_run())


def test_tui_words_files_filter_drilldown_and_remote_fallback(tmp_path: Path):
    _seed_workspace(tmp_path, with_classifier=False)

    # Expand lexicon with one unrecorded word + add kirk_core head profile
    lex_path = tmp_path / "raw-sessions" / "lexicon.json"
    lex_path.write_text(
        json.dumps(
            {
                "phrases": [
                    {"id": "all", "display_text": "All"},
                    {"id": "bless_you", "display_text": "Bless you"},
                ]
            }
        ),
        encoding="utf-8",
    )
    core_dir = tmp_path / "trained-heads" / "kirk_core"
    core_dir.mkdir(parents=True)
    (core_dir / "head.pt").write_bytes(b"core-pt")
    (core_dir / "labels.json").write_text(json.dumps(["bless_you"]), encoding="utf-8")
    (core_dir / "calibration.json").write_text(
        json.dumps({"default_threshold": 0.25, "per_class_thresholds": {"bless_you": 0.30}}),
        encoding="utf-8",
    )
    (tmp_path / "trained-heads" / "kirk" / "labels.json").write_text(
        json.dumps(["all", "bless_you"]),
        encoding="utf-8",
    )

    async def _run():
        app = HearMeSyncApp(
            repo_root=tmp_path,
            config_path=tmp_path / "missing.json",
            classifier_root=tmp_path / "missing-cls",
            auto_connect=False,
        )
        async with app.run_test(size=(120, 36)) as pilot:
            await pilot.pause(0.05)
            words_table = app.query_one("#words-table", DataTable)
            files_table = app.query_one("#files-table", DataTable)
            tabs = app.query_one("#inspector-tabs", TabbedContent)
            filter_input = app.query_one("#inspector-filter", Input)

            assert words_table.row_count == 2  # 'all' (1 take) + 'bless_you' (0 takes)
            assert files_table.row_count == 3

            # Press '/' to focus filter and narrow both tables to 'bless'
            await pilot.press("slash")
            await pilot.pause(0.05)
            filter_input.value = "bless"
            await pilot.pause(0.05)
            assert words_table.row_count == 1
            assert files_table.row_count == 1  # No WAV matches 'bless', empty notice row

            # Clear with Escape and return focus to buckets
            await pilot.press("escape")
            await pilot.pause(0.05)
            assert filter_input.value == ""
            assert words_table.row_count == 2

            # Drill down from 'all' word row into Files tab
            words_table.focus()
            await pilot.press("enter")
            await pilot.pause(0.05)
            assert tabs.active == "tab-files"
            assert filter_input.value == "/all/"
            assert files_table.row_count == 1  # session_2026_08_25/all/all_01.wav

            # Reset filter via Escape, switch to trained-heads, verify kirk_core comes first
            await pilot.press("escape")
            await pilot.pause(0.05)
            await pilot.press("v")  # tab-files -> tab-diff
            await pilot.pause(0.05)
            assert tabs.active == "tab-diff"
            await pilot.press("v")  # tab-diff -> tab-words
            await pilot.pause(0.05)
            assert tabs.active == "tab-words"

            buckets = app.query_one("#buckets-table", DataTable)
            buckets.focus()
            await pilot.press("down")
            await pilot.pause(0.05)
            assert app._selected_key == "trained-heads"
            assert words_table.row_count == 2
            first_word_cell = str(words_table.get_cell_at((0, 1)))
            assert first_word_cell == "bless_you"

    asyncio.run(_run())

    # Remote-only raw-sessions fallback when local manifest.jsonl is absent
    (tmp_path / "raw-sessions" / "manifest.jsonl").unlink()
    mock_service = _make_mock_blob_service(
        {
            "raw-sessions": [
                _make_blob("session_2026_08_25/goodnight/goodnight_01.wav", etag="0x1", size=12000)
            ],
            "trained-heads": [],
            "sessions": [],
        }
    )

    async def _run_remote_fallback():
        app = HearMeSyncApp(
            repo_root=tmp_path,
            config_path=tmp_path / "config.json",
            classifier_root=tmp_path / "missing-cls",
            blob_service=mock_service,
            auto_connect=False,
        )
        async with app.run_test(size=(120, 36)) as pilot:
            for _ in range(30):
                await pilot.pause(0.05)
                if not app._diffs["raw-sessions"].offline:
                    break
            words_table = app.query_one("#words-table", DataTable)
            assert words_table.row_count == 1
            assert str(words_table.get_cell_at((0, 1))) == "goodnight"

    asyncio.run(_run_remote_fallback())
