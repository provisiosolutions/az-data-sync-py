# PROGRESS — Azure Blob Sync & HearMe Three-Bucket Bridge (`az-data-sync-py`)

> **Active Sprint Window**: Separate SWA Sessions, Raw Kirk Sessions, and Trained Heads  
> **Session Status**: ✅ **READY FOR HANDOFF** (`2026-11-18T16:28-05:00`)  
> **Branch & Base SHA**: `feat/three-bucket-snake-case-sync` (`fe943d4`)  
> **Environment Health**: 55/55 Pytest unit tests passing (`test_sync.py`, `test_tui.py`, `test_export_powerbi.py`)  
> **Connected Repository**: [`../audio-classifier-hear-me`](../audio-classifier-hear-me)

---

## 1. Completed in This Session (2026-11-18)

- [x] **Three-Bucket Isolation (`sync.py`, `.gitignore`, `.vscode/tasks.json`, `config.example.json`)**:
  - Added `stage_classifier_raw_sessions()`, `stage_trained_heads()`, `to_snake_case()`, `parse_raw_clip_stem()`, and `build_swa_phrase_seeds()`.
  - Enforced strict `snake_case` paths (`session_2026_08_25`, `session_2026_09_25`, `session_2026_11_18_edited_clips`) with self-contained `manifest.jsonl` (`578` rows) and `lexicon.json` (`137` phrases).
  - Added live-only `create_container()`, `_normalize_etag()`, and `--delete-orphaned` support in `push_directory()`.
  - Defaulted `config.json` and `config.example.json` to `raw-sessions` (`./raw-sessions`).

- [x] **Azure Sync, QA Cleanup & SWA Seed (`rghearmeappafc6`)**:
  - Synced `raw-sessions` (`583` `snake_case` blobs) and `trained-heads` (`18` blobs).
  - Archived `994` QA blobs (`961` `sessions`, `30` `phrases`, `3` `QA Test List`) into `qa-archive` and deleted local `synced-files/`.
  - Seeded `Kirk - Core Session Phrases (29)` and `Kirk - Full Classifier Lexicon (136)` into `phrase-lists` (`17` total lists) and `phrases` (`7` total sets).

- [x] **Test Coverage (`test_sync.py`, `test_tui.py`)**:
  - Added 10 unit tests in `TestStageRawSessionsAndHeads` (`36` $\to$ `46` passing tests in `test_sync.py`) and 5 Textual/inspector tests in `test_tui.py`.

- [x] **Performant Student Sync Dashboard (`tui.py`, `sync_inspector.py`, `sync.py --tui`)**:
  - Added `sync_inspector.py` (`18.3 ms` local scan, `0.05 ms` cached summary, `428 ms` parallel 3-bucket Azure diff).
  - Added `tui.py` with offline mode, pull-only SWA lock, `ConfirmSyncModal`, `StudentGuideModal`, and quiet thread-safe progress logging.
  - Added `mtime_ns` pull-to-push parity (`should_upload` checks `.sync_manifest.json`) and `--tui` CLI flag.

- [x] **HearMe Power BI & Local HTML Dashboard (`export_powerbi.py`, `test_export_powerbi.py`)**:
  - Added `export_powerbi.py` (`4` unit tests) and `Dashboard: Export HearMe Power BI & HTML` task.
  - Built `synced-files/hearme_powerbi.xlsx` and `synced-files/hearme_dashboard.html` (`Lexicon=137`, `RawClips=578`, `SwaRecordings=0`, `HeadPriors=167`).
  - Pushed semantic model `HearMe Audio QA` (`c20900c5-ff00-4f20-be17-b78dc65d281e`) to Power BI `My Workspace` at $0.00 extra cost.

---

## 2. Next Steps

1. Re-stage and push `trained-heads/` (and back up `best_model` / `best_model_core`) after active Colab GPU training finishes.
2. Push `feat/three-bucket-snake-case-sync` and open a PR when ready.
