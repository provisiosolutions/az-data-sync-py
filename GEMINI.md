# Project Instructions

## 🧠 Persistent Memory (MemPalace)
This project uses MemPalace for long-term memory.
- **Mining:** Run `mempalace mine .` before a complex task to keep the palace current.
- **Context:** Use `mempalace` MCP tools to search the `technical` or `palace` halls for past decisions.
- **Session Sync:** Search the `chats` wing when you need context from earlier sessions.

## 📂 Three-Bucket Folder & Container Map
This repository separates HearMe SWA sessions, direct raw audio sessions, and trained speaker heads into three buckets:

| Local Directory | Azure Container | Direction | Contents |
| :--- | :--- | :--- | :--- |
| `synced-files/` | `sessions` | Pull only | HearMe SWA sessions (`{guid}/`, `{friendlyName}/`, `{sessionCode}/{friendlyName}/`) |
| `raw-sessions/` | `raw-sessions` | Stage & push/pull | `snake_case` Kirk WAV sessions (`session_2026_08_25/`, `session_2026_09_25/`, `session_2026_11_18_edited_clips/`), `manifest.jsonl`, `lexicon.json` |
| `trained-heads/` | `trained-heads` | Stage & push/pull | Trained speaker heads (`kirk/`, `kirk_core/`) without symlinked `audio_data/` |

## 📂 Naming & Review Rules
- **Default Config Bucket:** `config.json` points to `raw-sessions` (`./raw-sessions/`). Pass `--container sessions --local-dir ./synced-files` to pull SWA sessions.
- **Azure SWA Paths:** Legacy SWA sessions use raw GUID folders. Modern SWA sessions use `{sessionCode}/{friendlyName}/` or `{friendlyName}/`.
- **Sync Tool Mapping:** `sync.py` mirrors each remote container into its local bucket directory.
- **Review Session Page:** `review_sessions.py` scans `synced-files/` recursively for `metadata.json` files and builds `review.html` for all three SWA folder shapes.
