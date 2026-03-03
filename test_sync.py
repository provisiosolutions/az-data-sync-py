"""
Tests for Azure Blob Storage sync tool.

All Azure SDK calls are mocked — no real Azure connection required.
Run:  py -3 -m pytest test_sync.py -v
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from sync import (
    MANIFEST_FILE,
    load_config,
    load_manifest,
    save_manifest,
    should_download,
    sync_container,
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
