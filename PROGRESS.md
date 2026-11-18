# PROGRESS — Azure Blob Sync & HearMe Three-Bucket Bridge (`az-data-sync-py`)

> **Active Sprint Window**: Separate SWA Sessions, Raw Kirk Sessions, and Trained Heads  
> **Session Status**: ✅ **READY FOR HANDOFF** (`2026-11-18T16:05-05:00`)  
> **Branch & Base SHA**: `feat/three-bucket-snake-case-sync` (from `6afb708`)  
> **Environment Health**: 45/45 Pytest unit tests passing (`test_sync.py`)  
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

- [x] **Test Coverage (`test_sync.py`)**:
  - Added 9 unit tests in `TestStageRawSessionsAndHeads` (`36` $\to$ `45` passing tests).

---

## 2. Next Steps

1. Re-stage and push `trained-heads/` (and back up `best_model` / `best_model_core`) after active Colab GPU training finishes.
2. Push `feat/three-bucket-snake-case-sync` and open a PR when ready.
