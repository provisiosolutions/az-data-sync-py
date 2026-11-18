"""
Tests for Azure Blob Storage sync tool.

All Azure SDK calls are mocked — no real Azure connection required.
Run:  py -3 -m pytest test_sync.py -v
"""

import json
import logging
import re
import wave
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from azure.core.exceptions import ResourceExistsError
import pytest

from sync import (
    MANIFEST_FILE,
    PUSH_MANIFEST_FILE,
    build_swa_phrase_seeds,
    load_config,
    load_manifest,
    parse_raw_clip_stem,
    push_directory,
    save_manifest,
    should_download,
    should_upload,
    stage_classifier_raw_sessions,
    stage_trained_heads,
    sync_classifier_artifacts,
    sync_container,
    to_snake_case,
)
from review_sessions import (
    canonicalize_phrase_id,
    discover_sessions,
    export_sessions_for_classifier,
    generate_html,
)


# ---------------------------------------------------------------------------
# Helpers to build fake blob objects
# ---------------------------------------------------------------------------

def make_blob(
    name: str,
    etag: str = '"0x8D1234"',
    last_modified: datetime | None = None,
    size: int = 100,
):
    """Create a fake blob object matching the Azure SDK's BlobProperties shape."""
    return SimpleNamespace(
        name=name,
        etag=etag,
        last_modified=last_modified or datetime(2025, 6, 1, tzinfo=timezone.utc),
        size=size,
    )


def make_container_client(blobs: list, download_content: bytes = b"file-content"):
    """Build a mock ContainerClient that yields the given blobs."""
    client = MagicMock()
    client.list_blobs.return_value = blobs

    blob_client = MagicMock()
    stream = MagicMock()
    stream.readinto = lambda f: f.write(download_content)
    blob_client.download_blob.return_value = stream
    client.get_blob_client.return_value = blob_client

    return client


# ===========================================================================
# should_download()
# ===========================================================================

class TestShouldDownload:
    def test_new_blob_returns_true(self):
        blob = make_blob("file.txt")
        assert should_download(blob, None) is True

    def test_matching_etag_returns_false(self):
        blob = make_blob("file.txt", etag='"abc123"')
        entry = {"etag": '"abc123"'}
        assert should_download(blob, entry) is False

    def test_different_etag_returns_true(self):
        blob = make_blob("file.txt", etag='"new-etag"')
        entry = {"etag": '"old-etag"', "last_modified": "2025-01-01T00:00:00+00:00"}
        # Remote is newer than manifest, so should download
        assert should_download(blob, entry) is True

    def test_older_remote_skipped_when_etag_missing(self):
        blob = make_blob(
            "file.txt",
            etag=None,
            last_modified=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        entry = {
            "etag": None,
            "last_modified": "2025-06-01T00:00:00+00:00",
        }
        assert should_download(blob, entry) is False

    def test_newer_remote_downloaded_when_etag_missing(self):
        blob = make_blob(
            "file.txt",
            etag=None,
            last_modified=datetime(2025, 12, 1, tzinfo=timezone.utc),
        )
        entry = {
            "etag": None,
            "last_modified": "2025-06-01T00:00:00+00:00",
        }
        assert should_download(blob, entry) is True


# ===========================================================================
# Manifest I/O
# ===========================================================================

class TestManifest:
    def test_load_returns_empty_dict_when_missing(self, tmp_path):
        assert load_manifest(tmp_path) == {}

    def test_roundtrip(self, tmp_path):
        data = {"file.txt": {"etag": '"abc"', "size": 42}}
        save_manifest(tmp_path, data)
        assert load_manifest(tmp_path) == data

    def test_manifest_is_json_file(self, tmp_path):
        save_manifest(tmp_path, {"a": 1})
        raw = (tmp_path / MANIFEST_FILE).read_text(encoding="utf-8")
        assert json.loads(raw) == {"a": 1}


# ===========================================================================
# load_config()
# ===========================================================================

class TestLoadConfig:
    def test_valid_config(self, tmp_path):
        cfg_path = tmp_path / "cfg.json"
        cfg_path.write_text(json.dumps({
            "connection_string": "DefaultEndpointsProtocol=https;...",
            "container_name": "my-ctr",
        }))
        cfg = load_config(str(cfg_path))
        assert cfg["container_name"] == "my-ctr"

    def test_missing_connection_string_raises(self, tmp_path):
        cfg_path = tmp_path / "cfg.json"
        cfg_path.write_text(json.dumps({"container_name": "x"}))
        with pytest.raises(ValueError, match="connection_string"):
            load_config(str(cfg_path))

    def test_missing_container_raises(self, tmp_path):
        cfg_path = tmp_path / "cfg.json"
        cfg_path.write_text(json.dumps({"connection_string": "x"}))
        with pytest.raises(ValueError, match="container_name"):
            load_config(str(cfg_path))

    def test_empty_values_raise(self, tmp_path):
        cfg_path = tmp_path / "cfg.json"
        cfg_path.write_text(json.dumps({
            "connection_string": "",
            "container_name": "ctr",
        }))
        with pytest.raises(ValueError, match="connection_string"):
            load_config(str(cfg_path))


# ===========================================================================
# sync_container()  — the main sync logic
# ===========================================================================

class TestSyncContainer:
    def test_downloads_new_blobs(self, tmp_path):
        blobs = [make_blob("docs/readme.md"), make_blob("logo.png")]
        client = make_container_client(blobs, download_content=b"hello")

        stats = sync_container(client, tmp_path)

        assert stats["downloaded"] == 2
        assert stats["skipped"] == 0
        assert (tmp_path / "docs" / "readme.md").read_bytes() == b"hello"
        assert (tmp_path / "logo.png").read_bytes() == b"hello"

    def test_skips_unchanged_blobs(self, tmp_path):
        blob = make_blob("file.txt", etag='"same-etag"')
        # Pre-seed the manifest so the blob looks unchanged
        save_manifest(tmp_path, {
            "file.txt": {"etag": '"same-etag"', "last_modified": "2025-06-01T00:00:00+00:00"},
        })
        client = make_container_client([blob])

        stats = sync_container(client, tmp_path)

        assert stats["downloaded"] == 0
        assert stats["skipped"] == 1

    def test_skips_directory_markers(self, tmp_path):
        blobs = [make_blob("somedir/")]
        client = make_container_client(blobs)

        stats = sync_container(client, tmp_path)

        assert stats["downloaded"] == 0
        assert stats["skipped"] == 0

    def test_creates_nested_directories(self, tmp_path):
        blobs = [make_blob("a/b/c/deep.txt")]
        client = make_container_client(blobs, download_content=b"deep")

        sync_container(client, tmp_path)

        assert (tmp_path / "a" / "b" / "c" / "deep.txt").read_bytes() == b"deep"

    def test_prefix_passed_to_list_blobs(self, tmp_path):
        client = make_container_client([])

        sync_container(client, tmp_path, prefix="images/")

        client.list_blobs.assert_called_once_with(name_starts_with="images/")

    def test_empty_prefix_passes_none(self, tmp_path):
        client = make_container_client([])

        sync_container(client, tmp_path, prefix="")

        client.list_blobs.assert_called_once_with(name_starts_with=None)

    def test_manifest_is_saved_after_sync(self, tmp_path):
        blobs = [make_blob("a.txt", etag='"e1"')]
        client = make_container_client(blobs)

        sync_container(client, tmp_path)

        manifest = load_manifest(tmp_path)
        assert "a.txt" in manifest
        assert manifest["a.txt"]["etag"] == '"e1"'

    def test_delete_orphaned_removes_old_files(self, tmp_path):
        # Simulate a file that was synced previously but no longer in Azure
        (tmp_path / "gone.txt").write_text("old")
        save_manifest(tmp_path, {
            "gone.txt": {"etag": '"old"', "last_modified": "2025-01-01T00:00:00+00:00"},
        })
        client = make_container_client([])  # empty container now

        stats = sync_container(client, tmp_path, delete_orphaned=True)

        assert stats["deleted"] == 1
        assert not (tmp_path / "gone.txt").exists()

    def test_orphaned_files_kept_without_flag(self, tmp_path):
        (tmp_path / "gone.txt").write_text("old")
        save_manifest(tmp_path, {
            "gone.txt": {"etag": '"old"', "last_modified": "2025-01-01T00:00:00+00:00"},
        })
        client = make_container_client([])

        stats = sync_container(client, tmp_path, delete_orphaned=False)

        assert stats["deleted"] == 0
        assert (tmp_path / "gone.txt").exists()

    def test_download_error_counted_and_manifest_preserved(self, tmp_path):
        blob = make_blob("bad.txt", etag='"new"')
        old_entry = {"etag": '"old"', "last_modified": "2025-01-01T00:00:00+00:00"}
        save_manifest(tmp_path, {"bad.txt": old_entry})

        client = MagicMock()
        client.list_blobs.return_value = [blob]
        # Make download_blob raise
        blob_client = MagicMock()
        blob_client.download_blob.side_effect = Exception("network error")
        client.get_blob_client.return_value = blob_client

        stats = sync_container(client, tmp_path)

        assert stats["errors"] == 1
        assert stats["downloaded"] == 0
        # Old manifest entry preserved for retry on next run
        manifest = load_manifest(tmp_path)
        assert manifest["bad.txt"]["etag"] == '"old"'

    def test_second_run_skips_already_synced(self, tmp_path):
        """Simulates two sync runs — second run should skip the already-downloaded file."""
        blob = make_blob("readme.md", etag='"v1"')
        client = make_container_client([blob], download_content=b"# Hello")

        # First run
        stats1 = sync_container(client, tmp_path)
        assert stats1["downloaded"] == 1

        # Second run with same blob
        stats2 = sync_container(client, tmp_path)
        assert stats2["downloaded"] == 0
        assert stats2["skipped"] == 1

    def test_prefix_preserves_unrelated_manifest_and_files(self, tmp_path):
        """Syncing with --prefix must not wipe unrelated manifest keys or delete unrelated local files."""
        (tmp_path / "OTHER" / "keep.wav").parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / "OTHER" / "keep.wav").write_bytes(b"keep")
        (tmp_path / "AB12" / "stale.wav").parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / "AB12" / "stale.wav").write_bytes(b"stale")

        save_manifest(tmp_path, {
            "OTHER/keep.wav": {"etag": '"e-keep"', "last_modified": "2025-06-01T00:00:00+00:00"},
            "AB12/stale.wav": {"etag": '"e-stale"', "last_modified": "2025-06-01T00:00:00+00:00"},
        })

        blobs = [make_blob("AB12/fresh.wav", etag='"e-fresh"')]
        client = make_container_client(blobs, download_content=b"fresh")

        stats = sync_container(client, tmp_path, prefix="AB12/", delete_orphaned=True)

        assert stats["downloaded"] == 1
        assert stats["deleted"] == 1
        assert (tmp_path / "OTHER" / "keep.wav").exists()
        assert not (tmp_path / "AB12" / "stale.wav").exists()

        manifest = load_manifest(tmp_path)
        assert "OTHER/keep.wav" in manifest
        assert "AB12/fresh.wav" in manifest
        assert "AB12/stale.wav" not in manifest

    def test_verify_local_files_redownloads_missing_file(self, tmp_path):
        """When verify_local_files=True, a blob in manifest but missing on disk is re-downloaded."""
        blob = make_blob("missing.wav", etag='"same-etag"')
        save_manifest(tmp_path, {
            "missing.wav": {"etag": '"same-etag"', "last_modified": "2025-06-01T00:00:00+00:00"},
        })
        client = make_container_client([blob], download_content=b"restored")

        stats = sync_container(client, tmp_path, verify_local_files=True)

        assert stats["downloaded"] == 1
        assert stats["skipped"] == 0
        assert (tmp_path / "missing.wav").read_bytes() == b"restored"


# ===========================================================================
# Security
# ===========================================================================

class TestSecurity:
    def test_directory_traversal_blocked(self, tmp_path, caplog):
        # A blob with path traversal chars
        blob = make_blob("../outside.txt")
        client = make_container_client([blob])

        with caplog.at_level(logging.WARNING):
            stats = sync_container(client, tmp_path)

        assert stats["downloaded"] == 0
        assert stats["skipped"] == 1
        assert "SECURITY: Path escapes local directory" in caplog.text
        assert not (tmp_path.parent / "outside.txt").exists()

    def test_absolute_path_blocked(self, tmp_path, caplog):
        # A blob attempting to write to root/absolute path
        import os
        blob = make_blob("/etc/passwd" if os.name != "nt" else "C:\\Windows\\System32\\evil.dll")
        client = make_container_client([blob])

        with caplog.at_level(logging.WARNING):
            stats = sync_container(client, tmp_path)

        assert stats["downloaded"] == 0
        assert stats["skipped"] == 1
        assert "SECURITY: Path escapes local directory" in caplog.text

    def test_max_size_enforced(self, tmp_path, caplog):
        # A 100-byte blob
        blob = make_blob("huge.txt", size=100)
        client = make_container_client([blob])

        # Set max size to 50 bytes
        with caplog.at_level(logging.WARNING):
            stats = sync_container(client, tmp_path, max_size_bytes=50)

        assert stats["downloaded"] == 0
        assert stats["skipped"] == 1
        assert "SECURITY: Exceeds max size limit" in caplog.text


# ===========================================================================
# review_sessions.py — Recursive Session Discovery & Classifier Export
# ===========================================================================

class TestReviewSessions:
    def test_discover_sessions_missing_dir_returns_empty(self, tmp_path):
        missing = tmp_path / "does-not-exist"
        assert discover_sessions(synced_dir=missing) == []

    def test_discover_sessions_all_three_folder_formats(self, tmp_path):
        # 1. Legacy GUID folder
        guid_dir = tmp_path / "b40f28e2-8926-48d6-9e45-7e8cabf8e1ad"
        guid_dir.mkdir(parents=True)
        (guid_dir / "metadata.json").write_text(json.dumps({
            "sessionId": "b40f28e2-8926-48d6-9e45-7e8cabf8e1ad",
            "startTime": "2026-06-01T10:00:00Z",
            "recordings": [{"phraseId": "1", "phrase": "Water", "transcriptionFeedback": "good"}],
        }), encoding="utf-8")

        # 2. Standalone friendlyName folder
        friendly_dir = tmp_path / "2026-06-15-calm-otter"
        friendly_dir.mkdir(parents=True)
        (friendly_dir / "metadata.json").write_text(json.dumps({
            "sessionId": "c51a39f3-1111-2222-3333-444455556666",
            "startTime": "2026-06-15T12:00:00Z",
            "recordings": [{"phraseId": "1", "phrase": "Help me", "transcriptionFeedback": "good"}],
        }), encoding="utf-8")

        # 3. Nested {sessionCode}/{friendlyName}/ folder
        nested_dir = tmp_path / "K9P2" / "2026-06-30-bright-falcon"
        nested_dir.mkdir(parents=True)
        (nested_dir / "metadata.json").write_text(json.dumps({
            "sessionId": "d62b40a4-9999-8888-7777-666655554444",
            "startTime": "2026-06-30T15:30:00Z",
            "recordings": [
                {"phraseId": "1", "phrase": "I don't know", "transcriptionFeedback": "good"},
                {"phraseId": "2", "phrase": "Thank you", "transcriptionFeedback": "bad"},
            ],
        }), encoding="utf-8")

        save_manifest(tmp_path, {
            "b40f28e2-8926-48d6-9e45-7e8cabf8e1ad/1.wav": {"size": 1024},
            "2026-06-15-calm-otter/1.wav": {"size": 2048},
            "K9P2/2026-06-30-bright-falcon/1.wav": {"size": 4096},
            "K9P2/2026-06-30-bright-falcon/2.wav": {"size": 8192},
        })

        sessions = discover_sessions(synced_dir=tmp_path)
        assert len(sessions) == 3

        # Sorted newest first
        assert sessions[0].folder == "K9P2/2026-06-30-bright-falcon"
        assert sessions[0].session_code == "K9P2"
        assert [w.filename for w in sessions[0].wav_files] == ["1.wav", "2.wav"]

        assert sessions[1].folder == "2026-06-15-calm-otter"
        assert sessions[1].session_code == ""
        assert [w.filename for w in sessions[1].wav_files] == ["1.wav"]

        assert sessions[2].folder == "b40f28e2-8926-48d6-9e45-7e8cabf8e1ad"
        assert [w.filename for w in sessions[2].wav_files] == ["1.wav"]

    def test_discover_sessions_falls_back_to_disk_wavs(self, tmp_path):
        nested_dir = tmp_path / "ABCD" / "2026-06-30-swift-heron"
        nested_dir.mkdir(parents=True)
        (nested_dir / "metadata.json").write_text(json.dumps({
            "sessionId": "12345678-abcd-efgh",
            "startTime": "2026-06-30T16:00:00Z",
            "recordings": [{"phraseId": "1", "phrase": "Yes"}],
        }), encoding="utf-8")
        (nested_dir / "1.wav").write_bytes(b"RIFF....WAVEfmt ")

        sessions = discover_sessions(synced_dir=tmp_path)
        assert len(sessions) == 1
        assert len(sessions[0].wav_files) == 1
        assert sessions[0].wav_files[0].filename == "1.wav"
        assert sessions[0].wav_files[0].size == 16

    def test_generate_html_renders_nested_audio_paths_and_safe_dom(self, tmp_path):
        nested_dir = tmp_path / "K9P2" / "2026-06-30-bright-falcon"
        nested_dir.mkdir(parents=True)
        (nested_dir / "metadata.json").write_text(json.dumps({
            "sessionId": "d62b40a4-9999-8888-7777-666655554444",
            "startTime": "2026-06-30T15:30:00Z",
            "recordings": [{"phraseId": "1", "phrase": "Need bathroom", "transcriptionFeedback": "good"}],
        }), encoding="utf-8")
        (nested_dir / "1.wav").write_bytes(b"RIFF....WAVE")

        sessions = discover_sessions(synced_dir=tmp_path)
        html = generate_html(sessions)

        assert 'src="K9P2/2026-06-30-bright-falcon/1.wav"' in html
        assert "K9P2" in html
        assert "innerHTML" not in html

    def test_export_sessions_for_classifier(self, tmp_path):
        synced_dir = tmp_path / "synced"
        export_dir = tmp_path / "exported"
        nested_dir = synced_dir / "K9P2" / "2026-06-30-bright-falcon"
        nested_dir.mkdir(parents=True)
        (nested_dir / "metadata.json").write_text(json.dumps({
            "sessionId": "d62b40a4-9999-8888-7777-666655554444",
            "startTime": "2026-06-30T15:30:00Z",
            "recordings": [
                {"phraseId": "1", "phrase": "I don't know!", "transcriptionFeedback": "good"},
                {"phraseId": "2", "phrase": "Thank you", "transcriptionFeedback": "bad"},
            ],
        }), encoding="utf-8")
        (nested_dir / "1.wav").write_bytes(b"wav-good")
        (nested_dir / "2.wav").write_bytes(b"wav-bad")

        sessions = discover_sessions(synced_dir=synced_dir)
        stats = export_sessions_for_classifier(
            sessions=sessions,
            synced_dir=synced_dir,
            output_dir=export_dir,
            include_bad_feedback=False,
        )

        assert stats["exported_files"] == 1
        assert stats["skipped_bad"] == 1
        assert canonicalize_phrase_id("I don't know!") == "i_dont_know"
        exported_wavs = list((export_dir / "i_dont_know").glob("*.wav"))
        assert len(exported_wavs) == 1
        assert exported_wavs[0].read_bytes() == b"wav-good"

        manifest_lines = (export_dir / "export_manifest.jsonl").read_text(encoding="utf-8").strip().splitlines()
        assert len(manifest_lines) == 1
        row = json.loads(manifest_lines[0])
        assert row["canonical_label"] == "i_dont_know"
        assert row["session_code"] == "K9P2"


# ===========================================================================
# push_directory() & sync_classifier_artifacts()
# ===========================================================================

class TestPushAndClassifierSync:
    def test_push_directory_uploads_new_and_skips_unchanged(self, tmp_path):
        (tmp_path / "water").mkdir()
        wav = tmp_path / "water" / "water_01.wav"
        wav.write_bytes(b"RIFF....WAVE")

        client = make_container_client([])
        blob_client = client.get_blob_client.return_value
        blob_client.upload_blob.return_value = {"etag": '"etag-up-1"'}

        stats1 = push_directory(client, tmp_path, remote_prefix="classifier/dataset")
        assert stats1["uploaded"] == 1
        assert stats1["skipped"] == 0

        # Second run with unchanged file and matching manifest skips upload
        stats2 = push_directory(client, tmp_path, remote_prefix="classifier/dataset")
        assert stats2["uploaded"] == 0
        assert stats2["skipped"] == 1

    def test_push_directory_skips_symlinks(self, tmp_path):
        real_dir = tmp_path / "dataset" / "train" / "yes"
        real_dir.mkdir(parents=True)
        (real_dir / "yes_01.wav").write_bytes(b"wav-bytes")

        kirk_dir = tmp_path / "profiles" / "kirk"
        kirk_dir.mkdir(parents=True)
        (kirk_dir / "head.pt").write_bytes(b"head-weights")
        (kirk_dir / "audio_data").symlink_to(Path("../../dataset/train"))

        client = make_container_client([])
        blob_client = client.get_blob_client.return_value
        blob_client.upload_blob.return_value = {"etag": '"etag-1"'}

        stats = sync_classifier_artifacts(
            client,
            classifier_root=tmp_path,
            direction="push",
            include_exports=False,
        )
        # Only dataset/train/yes/yes_01.wav and profiles/kirk/head.pt are uploaded (not the symlinked audio_data twice!)
        assert stats["uploaded"] == 2
        uploaded_blob_names = [call.args[0] for call in client.get_blob_client.call_args_list]
        assert "classifier/dataset/train/yes/yes_01.wav" in uploaded_blob_names
        assert "classifier/profiles/kirk/head.pt" in uploaded_blob_names
        assert not any("profiles/kirk/audio_data" in name for name in uploaded_blob_names)

    def test_sync_classifier_artifacts_pull_strips_prefix_and_restores_symlink(self, tmp_path):
        blobs = [
            make_blob("classifier/dataset/train/water/water_01.wav", etag='"e1"'),
            make_blob("classifier/profiles/kirk/head.pt", etag='"e2"'),
            make_blob("classifier/results/best_model/model.safetensors", etag='"e3"'),
        ]
        client = make_container_client(blobs, download_content=b"payload")

        stats = sync_classifier_artifacts(
            client,
            classifier_root=tmp_path,
            direction="pull",
        )
        assert stats["downloaded"] == 3
        assert (tmp_path / "dataset" / "train" / "water" / "water_01.wav").read_bytes() == b"payload"
        assert (tmp_path / "profiles" / "kirk" / "head.pt").read_bytes() == b"payload"
        assert (tmp_path / "results" / "best_model" / "model.safetensors").read_bytes() == b"payload"
        assert (tmp_path / "profiles" / "kirk" / "audio_data").is_symlink()
        assert (tmp_path / "profiles" / "kirk" / "audio_data" / "water" / "water_01.wav").exists()


# ===========================================================================
# stage_classifier_raw_sessions(), stage_trained_heads(), and dual-head sync
# ===========================================================================

class TestStageRawSessionsAndHeads:
    def _build_classifier_fixture(self, root: Path) -> Path:
        cls_root = root / "classifier"
        (cls_root / "dataset" / "train" / "all").mkdir(parents=True)
        (cls_root / "dataset" / "train" / "ache").mkdir(parents=True)
        (cls_root / "dataset" / "train" / "bless_you").mkdir(parents=True)

        (cls_root / "dataset" / "train" / "all" / "all_01.wav").write_bytes(b"aug-wav")
        (cls_root / "dataset" / "train" / "ache" / "ache_01.wav").write_bytes(b"sep-wav")
        (cls_root / "dataset" / "train" / "bless_you" / "bless_you_01.wav").write_bytes(b"nov-resampled")

        manifest_rows = [
            {"label": "all", "path": "train/all/all_01.wav", "session_id": "session_2026-08-25"},
            {"label": "ache", "path": "train/ache/ache_01.wav", "session_id": "session_2026-09-25"},
            {"label": "bless_you", "path": "train/bless_you/bless_you_01.wav", "session_id": "session_2026-11-18"},
        ]
        (cls_root / "dataset" / "manifest.jsonl").write_text(
            "\n".join(json.dumps(r) for r in manifest_rows) + "\n",
            encoding="utf-8",
        )

        edited = cls_root / "Edited_Clips"
        edited.mkdir()
        (edited / "Bless you.wav").write_bytes(b"nov-raw-48k")
        (edited / "Video.mp4").write_bytes(b"mp4-bytes")
        return cls_root

    def test_stage_classifier_raw_sessions_separates_sessions_and_skips_mp4(self, tmp_path):
        cls_root = self._build_classifier_fixture(tmp_path)
        dest_dir = tmp_path / "raw-sessions"

        stats = stage_classifier_raw_sessions(cls_root, dest_dir)
        assert stats["copied"] == 3
        assert stats["unchanged"] == 0
        assert stats["skipped_mp4"] == 1
        assert stats["removed_stale"] == 0
        assert stats["sessions"] == {
            "session_2026_08_25": 1,
            "session_2026_09_25": 1,
            "session_2026_11_18_edited_clips": 1,
        }

        assert (dest_dir / "session_2026_08_25" / "all" / "all_01.wav").read_bytes() == b"aug-wav"
        assert (dest_dir / "session_2026_09_25" / "ache" / "ache_01.wav").read_bytes() == b"sep-wav"
        assert (
            dest_dir / "session_2026_11_18_edited_clips" / "bless_you" / "bless_you_01.wav"
        ).read_bytes() == b"nov-raw-48k"
        assert not (dest_dir / "session_2026_11_18").exists()
        assert not (dest_dir / "session_2026-11-18").exists()
        assert not (dest_dir / "session_2026_11_18_edited_clips" / "video" / "video_01.mp4").exists()

        for sname in (
            "session_2026_08_25",
            "session_2026_09_25",
            "session_2026_11_18_edited_clips",
        ):
            assert (dest_dir / sname / "session_manifest.jsonl").is_file()

        assert (dest_dir / "manifest.jsonl").is_file()
        assert (dest_dir / "lexicon.json").is_file()

        # Re-running is idempotent
        stats_again = stage_classifier_raw_sessions(cls_root, dest_dir)
        assert stats_again["copied"] == 0
        assert stats_again["unchanged"] == 3
        assert stats_again["skipped_mp4"] == 1
        assert stats_again["removed_stale"] == 0

    def test_stage_classifier_raw_sessions_includes_mp4_when_requested(self, tmp_path):
        cls_root = self._build_classifier_fixture(tmp_path)
        dest_dir = tmp_path / "raw-sessions"

        stats = stage_classifier_raw_sessions(cls_root, dest_dir, include_mp4=True)
        assert stats["copied"] == 4
        assert stats["skipped_mp4"] == 0
        assert stats["sessions"]["session_2026_11_18_edited_clips"] == 2
        assert (
            dest_dir / "session_2026_11_18_edited_clips" / "video" / "video_01.mp4"
        ).read_bytes() == b"mp4-bytes"

    def test_stage_trained_heads_copies_profiles_and_skips_symlinks(self, tmp_path):
        cls_root = self._build_classifier_fixture(tmp_path)
        kirk_dir = cls_root / "profiles" / "kirk"
        kirk_dir.mkdir(parents=True)
        (kirk_dir / "head.pt").write_bytes(b"kirk-head")
        (kirk_dir / "audio_data").symlink_to(Path("../../dataset/train"))

        kirk_core_dir = cls_root / "profiles" / "kirk_core"
        (kirk_core_dir / "checkpoints").mkdir(parents=True)
        (kirk_core_dir / "head.pt").write_bytes(b"core-head")
        (kirk_core_dir / "checkpoints" / "ckpt.pt").write_bytes(b"core-ckpt")
        (kirk_core_dir / "audio_data").mkdir()
        (kirk_core_dir / "audio_data" / "bless_you").symlink_to(Path("../../../dataset/train/bless_you"))

        dest_dir = tmp_path / "trained-heads"
        stats = stage_trained_heads(cls_root, dest_dir)

        assert stats["copied"] == 3
        assert stats["unchanged"] == 0
        assert stats["profiles"] == {"kirk": 1, "kirk_core": 2}
        assert (dest_dir / "kirk" / "head.pt").read_bytes() == b"kirk-head"
        assert (dest_dir / "kirk_core" / "head.pt").read_bytes() == b"core-head"
        assert (dest_dir / "kirk_core" / "checkpoints" / "ckpt.pt").read_bytes() == b"core-ckpt"
        assert not (dest_dir / "kirk" / "audio_data").exists()
        assert not (dest_dir / "kirk_core" / "audio_data").exists()

    def test_push_directory_creates_container_only_when_not_dry_run(self, tmp_path):
        (tmp_path / "sample.wav").write_bytes(b"wav")
        client = make_container_client([])
        client.get_blob_client.return_value.upload_blob.return_value = {"etag": '"e1"'}

        dry_stats = push_directory(client, tmp_path, dry_run=True)
        assert dry_stats["uploaded"] == 1
        client.create_container.assert_not_called()

        client.create_container.side_effect = ResourceExistsError("exists")
        live_stats = push_directory(client, tmp_path, dry_run=False)
        assert live_stats["uploaded"] == 1
        client.create_container.assert_called_once()

    def test_sync_classifier_artifacts_pull_restores_kirk_core_symlinks(self, tmp_path):
        blobs = [
            make_blob("classifier/dataset/train/water/water_01.wav", etag='"e1"'),
            make_blob("classifier/profiles/kirk_core/labels.json", etag='"e2"'),
        ]
        client = make_container_client(blobs)

        def get_blob_side_effect(blob_name: str):
            bc = MagicMock()
            stream = MagicMock()
            payload = b'["water"]' if blob_name.endswith("labels.json") else b"wav-bytes"
            stream.readinto = lambda f, p=payload: f.write(p)
            bc.download_blob.return_value = stream
            return bc

        client.get_blob_client.side_effect = get_blob_side_effect

        stats = sync_classifier_artifacts(
            client,
            classifier_root=tmp_path,
            direction="pull",
        )
        assert stats["downloaded"] == 2
        link = tmp_path / "profiles" / "kirk_core" / "audio_data" / "water"
        assert link.is_symlink()
        assert (link / "water_01.wav").read_bytes() == b"wav-bytes"

    def test_should_upload_matches_quoted_manifest_etag_against_unquoted_remote_etag(self, tmp_path):
        f = tmp_path / "head.pt"
        f.write_bytes(b"head-weights")
        stat = f.stat()
        push_entry = {
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "etag": '"0x8DF45B039D26E8B"',
        }
        remote_blob = make_blob(
            "kirk/head.pt",
            etag="0x8DF45B039D26E8B",
            size=stat.st_size,
        )
        assert should_upload(f, push_entry, remote_blob=remote_blob) is False

        remote_blob.etag = "0xDIFFERENT"
        assert should_upload(f, push_entry, remote_blob=remote_blob) is True

    def test_stage_classifier_raw_sessions_enforces_snake_case_wav_metadata_and_prunes_stale(self, tmp_path):
        cls_root = tmp_path / "classifier"
        (cls_root / "dataset").mkdir(parents=True)
        edited = cls_root / "Edited_Clips"
        edited.mkdir()

        wav_path = edited / "I'm not getting up today-2 (1).wav"
        with wave.open(str(wav_path), "wb") as wf:
            wf.setnchannels(2)
            wf.setsampwidth(2)
            wf.setframerate(48000)
            wf.writeframes(b"\x00" * (24000 * 2 * 2))

        assert to_snake_case("I'm not getting up today") == "im_not_getting_up_today"
        assert parse_raw_clip_stem("I'm not getting up today-2 (1)") == (
            "im_not_getting_up_today",
            "I'm not getting up today",
        )

        dest_dir = tmp_path / "raw-sessions"
        stale_dir = dest_dir / "session_2026-11-18-edited-clips"
        stale_dir.mkdir(parents=True)
        (stale_dir / "Old File.wav").write_bytes(b"stale")

        stats = stage_classifier_raw_sessions(cls_root, dest_dir)
        assert stats["copied"] == 1
        assert stats["removed_stale"] == 1
        assert not stale_dir.exists()

        snake_rel_re = re.compile(r"^[a-z0-9_]+(?:/[a-z0-9_]+)*\.[a-z0-9]+$")
        for fpath in dest_dir.rglob("*"):
            if fpath.is_file():
                rel = fpath.relative_to(dest_dir).as_posix()
                assert snake_rel_re.match(rel), f"Non-snake_case path found: {rel}"

        manifest_rows = [
            json.loads(line)
            for line in (dest_dir / "manifest.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert len(manifest_rows) == 1
        row = manifest_rows[0]
        assert row["label"] == "im_not_getting_up_today"
        assert row["display_text"] == "I'm not getting up today"
        assert row["original_filename"] == "I'm not getting up today-2 (1).wav"
        assert row["sample_rate"] == 48000
        assert row["channels"] == 2
        assert row["duration_s"] == 0.5
        assert row["path"] == (
            "session_2026_11_18_edited_clips/im_not_getting_up_today/im_not_getting_up_today_01.wav"
        )

        lexicon = json.loads((dest_dir / "lexicon.json").read_text(encoding="utf-8"))
        assert lexicon["phrases"] == [
            {"id": "im_not_getting_up_today", "display_text": "I'm not getting up today"}
        ]

    def test_push_directory_delete_orphaned_dry_run_and_live(self, tmp_path):
        local_wav = tmp_path / "session_2026_08_25" / "all" / "all_01.wav"
        local_wav.parent.mkdir(parents=True)
        local_wav.write_bytes(b"wav-bytes")

        client = make_container_client([make_blob("session_2026-08-25/old.wav")])
        client.get_blob_client.return_value.upload_blob.return_value = {"etag": '"e1"'}

        dry_stats = push_directory(client, tmp_path, delete_orphaned=True, dry_run=True)
        assert dry_stats["uploaded"] == 1
        assert dry_stats["deleted"] == 1
        client.delete_blob.assert_not_called()

        live_stats = push_directory(client, tmp_path, delete_orphaned=True, dry_run=False)
        assert live_stats["uploaded"] == 1
        assert live_stats["deleted"] == 1
        client.delete_blob.assert_called_once_with("session_2026-08-25/old.wav")

    def test_build_swa_phrase_seeds_excludes_silence_and_builds_deterministic_lists(self, tmp_path):
        lexicon_path = tmp_path / "lexicon.json"
        lexicon_path.write_text(
            json.dumps(
                {
                    "phrases": [
                        {"id": "bless_you", "display_text": "Bless you"},
                        {"id": "eat_now", "display_text": "Eat now"},
                        {"id": "silence", "display_text": "Silence"},
                    ]
                }
            ),
            encoding="utf-8",
        )
        core_labels_path = tmp_path / "labels.json"
        core_labels_path.write_text(json.dumps(["bless_you", "silence"]), encoding="utf-8")

        first = build_swa_phrase_seeds(
            lexicon_path,
            core_labels_path=core_labels_path,
            timestamp_iso="2026-11-18T16:00:00Z",
        )
        second = build_swa_phrase_seeds(
            lexicon_path,
            core_labels_path=core_labels_path,
            timestamp_iso="2026-11-18T16:00:00Z",
        )
        assert first == second

        assert len(first["phrase_lists"]) == 2
        lists_by_slug = {v["slug"]: v for v in first["phrase_lists"].values()}
        full_list = lists_by_slug["kirk-full-classifier-lexicon"]
        core_list = lists_by_slug["kirk-core-session-phrases"]

        assert full_list["isPublished"] is True
        assert full_list["title"] == "Kirk - Full Classifier Lexicon (2)"
        assert [p["text"] for p in full_list["phrases"]] == ["Bless you", "Eat now"]
        assert [p["orderIndex"] for p in full_list["phrases"]] == [0, 1]

        assert core_list["isPublished"] is True
        assert core_list["title"] == "Kirk - Core Session Phrases (1)"
        assert [p["text"] for p in core_list["phrases"]] == ["Bless you"]
        assert [p["orderIndex"] for p in core_list["phrases"]] == [0]

        assert first["phrase_sets"]["kirk-full-lexicon.json"]["phrases"] == ["Bless you", "Eat now"]
        assert first["phrase_sets"]["kirk-full-lexicon.json"]["phrasesPerSession"] == 20
        assert first["phrase_sets"]["kirk-core-phrases.json"]["phrases"] == ["Bless you"]
        assert first["phrase_sets"]["kirk-core-phrases.json"]["phrasesPerSession"] == 1

    def test_should_upload_skips_cleanly_pulled_file_via_sync_manifest(self, tmp_path):
        f = tmp_path / "all_01.wav"
        f.write_bytes(b"wav-data")
        stat = f.stat()
        sync_entry = {
            "etag": "0xABCD",
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        }
        stale_push_entry = {
            "etag": '"0xOLD"',
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns - 1000,
        }
        remote_blob = make_blob("all_01.wav", etag="0xABCD", size=stat.st_size)

        assert (
            should_upload(f, stale_push_entry, remote_blob=remote_blob, sync_entry=sync_entry)
            is False
        )
        remote_blob.etag = "0xCHANGED"
        assert (
            should_upload(f, stale_push_entry, remote_blob=remote_blob, sync_entry=sync_entry)
            is True
        )



