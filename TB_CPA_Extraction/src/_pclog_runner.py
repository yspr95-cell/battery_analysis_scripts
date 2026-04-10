"""
_pclog_runner.py  —  Subprocess shim: rebuild PC log from existing JSON status files.
Reads a JSON config file passed as argv[1] with {"base_path": "..."}.
Scans backend_base/ for extraction status JSON files, picks the latest per ZIP archive,
and writes/updates the PC trace log Excel.
DO NOT run this file directly.
"""

import sys
import json
import socket
import time
from pathlib import Path

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.paths import PATHS_OBJ
from src.trace_log import ExtractionTraceLog, _COLUMNS

import pandas as pd

# Extraction status files are named YYYYMMDD_HHMMSS_status.json
# Harmonize status files are named hm_YYYYMMDD_HHMMSS_status.json — must be excluded.
def _is_extraction_json(path: Path) -> bool:
    """Return True only for extraction status JSON files (not harmonize hm_ files)."""
    name = path.name
    return name.endswith("_status.json") and not name.startswith("hm_")


def _entry_is_archive(entry) -> bool:
    """
    Sanity-check that an entry looks like an extraction archive dict.
    Extraction entries always have 'to_copy' and/or 'copied_files_meta'.
    Harmonize entries (keyed by file path, not archive path) do not.
    """
    if not isinstance(entry, dict):
        return False
    return "to_copy" in entry or "copied_files_meta" in entry


def _parse_run_ts(stem: str) -> str:
    """
    Extract a sortable ISO timestamp from a filename stem like 20240315_143022_status.
    Falls back to the raw stem part if parsing fails.
    """
    # stem = "20240315_143022_status"  →  parts = "20240315_143022"
    parts = stem.replace("_status", "")
    try:
        t = time.strptime(parts, "%Y%m%d_%H%M%S")
        return time.strftime("%Y-%m-%d %H:%M:%S", t)
    except ValueError:
        return parts


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("[ERROR] Usage: _pclog_runner.py <config_json_path>", flush=True)
        sys.exit(1)

    config_path = Path(sys.argv[1])
    if not config_path.exists():
        print(f"[ERROR] Config file not found: {config_path}", flush=True)
        sys.exit(1)

    kwargs = json.loads(config_path.read_text(encoding="utf-8"))
    base_path = kwargs.get("base_path", "")
    if not base_path:
        print("[ERROR] base_path is empty.", flush=True)
        sys.exit(1)

    try:
        paths = PATHS_OBJ(base_path=base_path)
        backend_dir = paths.backend_path

        # Only extraction status JSON files (exclude harmonize hm_ files)
        json_files = sorted(f for f in backend_dir.glob("*_status.json") if _is_extraction_json(f))
        if not json_files:
            print(f"[PCLog] No extraction status JSON files found in {backend_dir.name}", flush=True)
            sys.exit(0)

        print(f"[PCLog] Found {len(json_files)} extraction JSON file(s) in {backend_dir.name}", flush=True)

        # Merge all JSONs: for each ZIP archive path keep only the latest run's entry.
        # Later filename timestamp = later run, so it takes precedence.
        merged_status: dict = {}
        merged_timestamps: dict = {}  # zip_path_str -> run_timestamp string
        skipped = 0

        for jf in json_files:
            run_ts_str = _parse_run_ts(jf.stem)

            try:
                data = json.loads(jf.read_text(encoding="utf-8"))
            except Exception as e:
                print(f"[PCLog] Warning: could not read {jf.name}: {e}", flush=True)
                continue

            if not isinstance(data, dict):
                print(f"[PCLog] Warning: unexpected format in {jf.name}, skipping.", flush=True)
                continue

            for zip_path_str, entry in data.items():
                if not _entry_is_archive(entry):
                    skipped += 1
                    continue  # skip harmonize or other non-archive entries
                existing_ts = merged_timestamps.get(zip_path_str)
                if existing_ts is None or run_ts_str > existing_ts:
                    merged_status[zip_path_str] = entry
                    merged_timestamps[zip_path_str] = run_ts_str

        if skipped:
            print(f"[PCLog] Skipped {skipped} non-archive entries (e.g. harmonize entries).", flush=True)

        if not merged_status:
            print("[PCLog] No valid extraction archive entries found in JSON files.", flush=True)
            sys.exit(0)

        print(f"[PCLog] Processing {len(merged_status)} unique ZIP archive(s).", flush=True)

        # Diagnostic: show counts for the first entry to verify structure
        first_key = next(iter(merged_status))
        first_entry = merged_status[first_key]
        print(
            f"[PCLog] Sample entry ({Path(first_key).name}): "
            f"to_copy={len(first_entry.get('to_copy', {}).get('meta', {}))}, "
            f"copied={len(first_entry.get('copied_files_meta', {}))}, "
            f"corrupt={len(first_entry.get('corrupted', {}).get('names', []))}, "
            f"ignored={len(first_entry.get('to_ignore', {}).get('names', []))}, "
            f"unknown={len(first_entry.get('unknown', {}).get('names', []))}",
            flush=True,
        )

        # Build a fresh trace log from all merged entries
        hostname = socket.gethostname()
        pc_logs_dir = paths.logs_path / "pc_logs"
        pc_logs_dir.mkdir(exist_ok=True)
        log_path = pc_logs_dir / f"extraction_trace_log_{hostname}.xlsx"

        trace = ExtractionTraceLog.__new__(ExtractionTraceLog)
        trace.log_path  = log_path
        trace._hostname = hostname
        trace._df       = pd.DataFrame(columns=_COLUMNS)

        for zip_path_str, entry in merged_status.items():
            trace._upsert_row(zip_path_str, entry, merged_timestamps[zip_path_str])

        trace.save()
        print(f"[PCLog] Rebuilt PC log → {log_path.name}  ({len(trace._df)} entries)", flush=True)
        sys.exit(0)

    except Exception as e:
        print(f"[PCLog] ERROR: {e}", flush=True)
        import traceback; traceback.print_exc()
        sys.exit(1)
