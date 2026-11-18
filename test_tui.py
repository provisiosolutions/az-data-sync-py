"""Tests for sync_inspector and Textual HearMeSyncApp TUI."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from textual.widgets import Button, DataTable, Static

from sync import MANIFEST_FILE, PUSH_MANIFEST_FILE, save_manifest
from sync_inspector import (
    compute_bucket_diff,
    default_bucket_specs,
    get_dataset_summary,
    get_heads_and_upstream_summary,
    scan_local_dir,
)
from tui import ConfirmSyncModal, HearMeSyncApp, StudentGuideModal


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
            table = app.query_one("#buckets-table", DataTable)
            assert table.row_count == 3
            assert app.query_one("#btn-push", Button).disabled is True
            assert app.query_one("#btn-stage", Button).disabled is True
            banner_text = str(app.query_one("#workflow-banner", Static).render())
            assert "Offline" in banner_text
            assert "[G] Pull" in banner_text
            assert str(app.query_one("#btn-pull", Button).label) == "Pull [G]"

            await pilot.press("?")
            await pilot.pause(0.05)
            assert isinstance(app.screen, StudentGuideModal)

            await pilot.press("escape")
            await pilot.pause(0.05)
            assert not isinstance(app.screen, StudentGuideModal)

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
            assert app.query_one("#btn-push", Button).disabled is False

            # Pressing 'p' opens ConfirmSyncModal with upload preview; Esc cancels
            await pilot.press("p")
            await pilot.pause(0.05)
            assert isinstance(app.screen, ConfirmSyncModal)
            await pilot.press("escape")
            await pilot.pause(0.05)
            assert not isinstance(app.screen, ConfirmSyncModal)

            # Navigate down twice to 'sessions' (Pull-Only): Push button must disable
            table = app.query_one("#buckets-table", DataTable)
            table.focus()
            await pilot.press("down", "down")
            await pilot.pause(0.05)
            assert app._selected_key == "sessions"
            assert app.query_one("#btn-push", Button).disabled is True

    asyncio.run(_run())
