# Project Instructions

## 🧠 Persistent Memory (MemPalace)
This project uses MemPalace for long-term memory.
- **Mining:** Run `mempalace mine .` before complex tasks to keep the palace current.
- **Context:** Use `mempalace` MCP tools to search `technical` or `palace` halls for past decisions.
- **Session Sync:** Search the `chats` wing for earlier session context.

## 📂 Three-Bucket Folder & Container Map
This repository separates HearMe SWA sessions, direct raw audio sessions, and trained speaker heads into three buckets:

| Local Directory | Azure Container | Direction | Contents |
| :--- | :--- | :--- | :--- |
| `synced-files/` | `sessions` | Pull only | HearMe SWA sessions (`{guid}/`, `{friendlyName}/`, `{sessionCode}/{friendlyName}/`) |
| `raw-sessions/` | `raw-sessions` | Stage & mirror (`--delete-orphaned`) | `snake_case` Kirk WAV sessions (`session_2026_08_25/`, `session_2026_09_25/`, `session_2026_11_18_edited_clips/`), `manifest.jsonl`, `lexicon.json` |
| `trained-heads/` | `trained-heads` | Stage & mirror (`--delete-orphaned`) | Trained speaker heads (`kirk/`, `kirk_core/`) without symlinked `audio_data/` |

## 📂 Naming, Sync & Tooling Rules
- **Default Config Bucket:** `config.json` defaults to `raw-sessions` (`./raw-sessions/`). Pass `--container sessions --local-dir ./synced-files` to pull SWA sessions.
- **Staging & Orphan Pruning:** `sync.py --stage-from-classifier ../audio-classifier-hear-me` stages strict `snake_case` WAVs, `manifest.jsonl`, and `lexicon.json` into `raw-sessions/` and active speaker heads into `trained-heads/`. The staging step prunes rotated checkpoints and stale files while keeping `.sync_manifest.json` and `.push_manifest.json`. Pass `--push --delete-orphaned` when pushing `raw-sessions` or `trained-heads`.
- **Azure SWA & Archive Containers:** Keep `sessions` Pull-Only (`{guid}/`, `{friendlyName}/`, `{sessionCode}/{friendlyName}/`). Store developer QA blobs in `qa-archive`, and store seeded clinical and Kirk phrase lists (`29` core, `136` full) in `phrase-lists` and `phrases`.
- **Interactive TUI & Audio Preview:** `tui.py` (`python sync.py --tui`), `sync_inspector.py`, and `audio_preview.py` provide live `Words`, `Files`, and `Sync Diff` inspection, `/` filtering, and `Space` local `.wav` waveform and audio playback.
- **Review Session Page:** `review_sessions.py` builds `synced-files/review.html` for SWA sessions (`.html` and `.xlsx` local reports are skipped during sync scans).
- **Classifier Weights & Tests:** `--classifier-root ../audio-classifier-hear-me` syncs `best_model`, `best_model_core`, `HearMeClassifier*`, and `HearMeCoreClassifier*` exports. Run `.venv/bin/pytest test_sync.py test_tui.py -q` to verify changes offline.
