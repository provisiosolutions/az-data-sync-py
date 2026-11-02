# Project Instructions

## 🧠 Persistent Memory (MemPalace)
This project uses MemPalace for long-term memory.
- **Mining:** Before starting a complex task, run `mempalace mine .` to ensure the palace is up to date.
- **Context:** If you need to know "why" a decision was made or find past snippets, use the `mempalace` MCP tools to search the 'technical' or 'palace' halls.
- **Session Sync:** I regularly mine my Antigravity session logs; if you lack context from our last chat, search the `chats` wing.

## 📂 Naming Conventions & Folder Structure
- **Azure Blob Storage Paths**:
  - Legacy sessions are stored under raw GUID folders (e.g. `b40f28e2-8926-48d6-9e45-7e8cabf8e1ad/`).
  - Modern sessions are stored under `{sessionCode}/{friendlyName}/` (if a 4-char session code is provided) or simply `{friendlyName}/` where `friendlyName` is deterministically generated in the format `{date}-{adjective}-{noun}` from the session GUID (see `SessionNameService.cs` in the backend).
- **Sync Tool Mapping**:
  - `sync.py` mirrors the remote container structure. Thus, files are downloaded into `synced-files/{sessionCode}/{friendlyName}/`, `synced-files/{friendlyName}/`, or `synced-files/{guid}/`.
- **Review Session Page**:
  - `review_sessions.py` generates `review.html`. It must scan recursively to find `metadata.json` files and correctly associate WAV files with their parent folder name, supporting legacy GUIDs, standalone friendly names, and nested `{sessionCode}/{friendlyName}/` folders.
