# Three-Bucket Session and Trained-Head Sync

- **Added raw session and head staging (`sync.py`)**: Added `stage_classifier_raw_sessions()`, `stage_trained_heads()`, `to_snake_case()`, `parse_raw_clip_stem()`, `--stage-from-classifier`, `--raw-sessions-dir`, `--trained-heads-dir`, and `--include-mp4`.
- **Enforced `snake_case` and training manifests (`sync.py`)**: Renamed all `raw-sessions/` folders and `.wav` clips to strict `snake_case` and generated `manifest.jsonl`, `lexicon.json`, and per-session `session_manifest.jsonl`.
- **Isolated SWA vs. direct sessions**: Kept HearMe SWA sessions in `./synced-files/` (`sessions` container), direct Kirk raw WAV sessions in `./raw-sessions/` (`raw-sessions` container), and speaker heads in `./trained-heads/` (`trained-heads` container).
- **Updated dual-head classifier sync (`sync.py`)**: Added `results/best_model_core` sync targets and restored `profiles/kirk_core/audio_data/<label>` symlinks on pull.
- **Added live-push container creation and `--delete-orphaned` (`sync.py`)**: Created target containers during live `push_directory()` runs and supported pruning orphaned remote blobs while keeping `--dry-run` read-only.
- **Normalized HTTP/XML ETags (`sync.py`)**: Stripped surrounding double quotes in `_normalize_etag()` so incremental `--push` and pull checks match `upload_blob()` and `list_blobs()` ETags.
- **Added SWA phrase seeding (`sync.py`)**: Added `build_swa_phrase_seeds()` to generate deterministic `PhraseList` and `PhraseSet` JSON payloads from `lexicon.json` and `kirk_core/labels.json`.
- **Defaulted config to `raw-sessions` (`config.example.json`, `.vscode/tasks.json`)**: Updated default config and build task to sync `raw-sessions` (`./raw-sessions`) while keeping an explicit SWA `sessions` task.
- **Added Textual sync dashboard (`tui.py`, `sync_inspector.py`, `sync.py`)**: Added `--tui` dashboard with single clickable `Footer` actions via `check_action()`, right-pane `Words`, `Files`, and `Sync Diff` tabs, live `/` filtering, `Enter` word-to-files drill-down, remote-only word derivation, pull-only SWA lock, and push preview modal.
- **Excluded local dashboard exports (`sync.py`, `sync_inspector.py`)**: Added `hearme_dashboard.html` and `hearme_powerbi.xlsx` to `_IGNORED_FILENAMES` so local report exports in `synced-files/` stay out of SWA sync counts.
- **Added pull-to-push manifest parity (`sync.py`)**: Recorded `mtime_ns` in `.sync_manifest.json` and checked `sync_entry` in `should_upload()` so pulled files stay in sync on push checks.
- **Updated tests, tasks, and docs (`test_sync.py`, `test_tui.py`, `.vscode/tasks.json`, `.gitignore`, `GEMINI.md`, `README.md`, `PROGRESS.md`, `requirements.txt`)**: Added 10 sync tests, 8 TUI/inspector tests, `textual>=8.0.0`, and macOS task entries.
