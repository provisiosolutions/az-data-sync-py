# Post-Colab Trained Heads Staging, Pruning, and Azure Sync

- **Prune stale staged checkpoints (`sync.py`, `tui.py`)**: Remove deleted files and empty folders in `trained-heads/` during `stage_trained_heads()` while preserving `.push_manifest.json` and `.sync_manifest.json`.
- **Sync Core classifier exports (`sync.py`)**: Add `HearMeCoreClassifier_flexible.mlpackage`, `HearMeCoreClassifier.onnx`, and `HearMeCoreClassifier_metadata.json` to `sync_classifier_artifacts()`.
- **Mirror remote `trained-heads` bucket (`sync_inspector.py`)**: Set `delete_orphaned_on_push=True` for `trained-heads` so pushed mirrors remove rotated checkpoints cleanly.
- **Add regression test (`test_sync.py`)**: Verify `stage_trained_heads()` prunes stale checkpoints and verify `sync_classifier_artifacts()` counts all Core exports.
