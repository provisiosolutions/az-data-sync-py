# PROGRESS — Azure Blob Sync & HearMe Classifier Bridge (`az-data-sync-py`)

> **Active Sprint Window**: Recursive Session Discovery, HearMe Classifier Export & Two-Way Artifact Sync  
> **Session Status**: ✅ **ACTIVE** (`2026-11-02T15:43-05:00`)  
> **Branch & Base SHA**: `main` (`603d273`)  
> **Environment Health**: 36/36 Pytest unit & integration tests passing in `0.46s` (`test_sync.py`)  
> **Connected Repository**: [`../audio-classifier-hear-me`](../audio-classifier-hear-me)

---

## 🏁 Completed in this Session (2026-11-02)

- [x] **Recursive Session Discovery & Disk Fallback (`review_sessions.py`)**:
  - Updated `discover_sessions()` to scan `rglob("metadata.json")` across legacy `{guid}/`, standalone `{friendlyName}/`, and nested `{sessionCode}/{friendlyName}/` directories without failing when `synced-files/` is absent.
  - Added `session_code` badge rendering in `_render_session_card()` and on-disk `.wav` fallback discovery when `.sync_manifest.json` is missing or partial.
  - Replaced `this.innerHTML` in `templates/review.html` (`<audio-player>`) with `document.createElement('audio')` and `this.replaceChildren(audio)`.

- [x] **HearMe Classifier Canonical Export Bridge (`review_sessions.py`)**:
  - Added `canonicalize_phrase_id()` and `export_sessions_for_classifier()` (`--export-classifier <dir>`, `--include-bad`) to export valid session recordings into `<canonical_label>/<session_slug>_<phrase_id>.wav` and `export_manifest.jsonl`.

- [x] **Prefix-Safe Sync, Missing File Recovery & Classifier Artifact Backup (`sync.py`)**:
  - Preserved out-of-prefix `.sync_manifest.json` entries during `--prefix` runs so partial syncs never wipe unrelated manifest keys or delete unrelated local files under `--delete-orphaned`.
  - Added `--verify-local` (`verify_local_files=True`) to re-download blobs that are missing on disk even when manifest ETags match.
  - Added `push_directory()` and `sync_classifier_artifacts()` (`--push`, `--classifier-root`, `--skip-exports`, `--dry-run`) to push and pull `audio-classifier-hear-me` datasets (`dataset/`), speaker heads (`profiles/`), and model weights (`results/best_model/`, Core ML, ONNX) under the `classifier/` prefix while skipping symlinks (`profiles/kirk/audio_data -> ../../dataset/train`).

- [x] **Expanded Test Suite (`test_sync.py`)**:
  - Added 10 new unit tests (`26` $\to$ `36` passing tests) covering recursive 3-format session discovery, disk WAV fallback, DOM safety, canonical classifier export, prefix-safe manifest retention, `--verify-local`, and `sync_classifier_artifacts()` push/pull round-trips.

---

## 🎯 Immediate Next Steps

1. **Commit & Push `az-data-sync-py`**: Stage and commit `sync.py`, `review_sessions.py`, `templates/review.html`, `test_sync.py`, `README.md`, `GEMINI.md`, and `PROGRESS.md` when ready to publish.
2. **Run Classifier Backup to Azure Blob Storage**: Execute `python sync.py -c config.json --push --classifier-root ../audio-classifier-hear-me` once `config.json` (or `AZURE_STORAGE_CONNECTION_STRING`) is configured.
