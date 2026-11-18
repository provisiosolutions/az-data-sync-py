# PROGRESS — Azure Blob Sync & HearMe Three-Bucket Bridge (`az-data-sync-py`)

> **Active Sprint Window**: Post-Colab Trained Heads Sync and Three-Bucket Parity  
> **Session Status**: Synced (`2026-11-18T17:32-05:00`)  
> **Branch**: `feat/three-bucket-snake-case-sync`  
> **Environment Health**: 58/58 Pytest unit tests passing (`test_sync.py`, `test_tui.py`)  
> **Connected Repository**: [`../audio-classifier-hear-me`](../audio-classifier-hear-me)

---

## 1. Completed in This Session (2026-11-18)

- [x] **Three-Bucket Isolation (`sync.py`, `.gitignore`, `.vscode/tasks.json`, `config.example.json`)**:
  - Added `stage_classifier_raw_sessions()`, `stage_trained_heads()`, `to_snake_case()`, `parse_raw_clip_stem()`, and `build_swa_phrase_seeds()`.
  - Enforced `snake_case` paths (`session_2026_08_25`, `session_2026_09_25`, `session_2026_11_18_edited_clips`) with `manifest.jsonl` (`578` rows) and `lexicon.json` (`137` phrases).
- [x] **Azure Sync, QA Cleanup & SWA Seed (`rghearmeappafc6`)**:
  - Synced `raw-sessions` (`583` `snake_case` blobs) and `trained-heads` (`18` blobs), archived `994` QA blobs into `qa-archive`, and seeded `Kirk - Core Session Phrases (29)` and `Kirk - Full Classifier Lexicon (136)`.
- [x] **Textual Sync Dashboard & Audio Preview (`tui.py`, `sync_inspector.py`, `audio_preview.py`, `sync.py --tui`)**:
  - Added `sync_inspector.py` and `audio_preview.py` (`Space` WAV playback and animated waveform preview), `Words` / `Files` / `Sync Diff` tabs, `/` filter, and pull-to-push manifest parity.
  - Skipped `.html` and `.xlsx` local report files during SWA scans (`sessions` stays `Local: 0`).
- [x] **Post-Colab `trained-heads` Re-Stage & Push (`sync.py`, `sync_inspector.py`, `test_sync.py`)**:
  - Updated `stage_trained_heads()` to prune stale checkpoints while keeping sync manifests (`copied=14, unchanged=4, removed_stale=2`).
  - Enabled `delete_orphaned_on_push=True` on `trained-heads` in `sync_inspector.py` and added Core export paths to `sync_classifier_artifacts()`.
  - Pushed Colab-trained `kirk` (`head_20261118_220133_70ffa9`, `137` classes) and `kirk_core` (`head_20261118_220209_0e0aaa`, `30` classes) to Azure (`18` local / `18` remote, `0` diffs, `0` errors).

---

## 2. Next Steps

1. Review local commits on `feat/three-bucket-snake-case-sync` in `az-data-sync-py` and push when ready.
