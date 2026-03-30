"""
_pclog_runner.py  —  Subprocess shim: rebuild PC log from existing JSON status files.
Reads a JSON config file passed as argv[1] with {"base_path": "..."}.
Scans backend_base/ for *_status.json files, picks the latest per ZIP archive,
and writes/updates the PC trace log Excel.
DO NOT run this file directly.
"""

import sys
import json
import socket
from pathlib import Path
from datetime import datetime

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.paths import PATHS_OBJ
from src.trace_log import ExtractionTraceLog

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

        # Collect all status JSON files — filename format: YYYYMMDD_HHMMSS_status.json
        json_files = sorted(backend_dir.glob("*_status.json"))
        if not json_files:
            print(f"[PCLog] No status JSON files found in {backend_dir.name}", flush=True)
            sys.exit(0)

        print(f"[PCLog] Found {len(json_files)} status JSON file(s) in {backend_dir.name}", flush=True)

        # Group by ZIP path — keep only the latest JSON per ZIP (latest filename = latest run)
        # Each JSON is a status_dict keyed by zip_path_str
        # We merge them: later files overwrite earlier for the same zip_path
        merged_status: dict = {}
        merged_timestamps: dict = {}  # zip_path -> run_timestamp string

        for jf in json_files:
            # Extract timestamp from filename: YYYYMMDD_HHMMSS_status.json
            stem = jf.stem  # e.g. "20240315_143022_status"
            parts = stem.split("_status")[0]  # "20240315_143022"
            try:
                ts = datetime.strptime(parts, "%Y%m%d_%H%M%S")
                run_ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                run_ts_str = parts  # fallback

            try:
                data = json.loads(jf.read_text(encoding="utf-8"))
            except Exception as e:
                print(f"[PCLog] Warning: could not read {jf.name}: {e}", flush=True)
                continue

            for zip_path_str, entry in data.items():
                existing_ts = merged_timestamps.get(zip_path_str)
                if existing_ts is None or run_ts_str > existing_ts:
                    merged_status[zip_path_str] = entry
                    merged_timestamps[zip_path_str] = run_ts_str

        if not merged_status:
            print("[PCLog] No valid ZIP entries found in JSON files.", flush=True)
            sys.exit(0)

        # Write PC trace log
        hostname = socket.gethostname()
        pc_logs_dir = paths.logs_path / "pc_logs"
        pc_logs_dir.mkdir(exist_ok=True)
        log_path = pc_logs_dir / f"extraction_trace_log_{hostname}.xlsx"

        # Build a fresh trace log (overwrite existing) from all merged entries
        trace = ExtractionTraceLog.__new__(ExtractionTraceLog)
        import pandas as pd
        from src.trace_log import _COLUMNS
        trace.log_path  = log_path
        trace._hostname = hostname
        trace._df       = pd.DataFrame(columns=_COLUMNS)

        # Record each ZIP using its latest timestamp
        for zip_path_str, entry in merged_status.items():
            trace._upsert_row(zip_path_str, entry, merged_timestamps[zip_path_str])

        trace.save()
        print(f"[PCLog] Rebuilt PC log → {log_path.name}  ({len(trace._df)} entries)", flush=True)
        sys.exit(0)

    except Exception as e:
        print(f"[PCLog] ERROR: {e}", flush=True)
        import traceback; traceback.print_exc()
        sys.exit(1)
