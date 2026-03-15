"""
run_config.py  —  TB_CPA_Evaluate
====================================
SINGLE ENTRY POINT — edit only the USER CONFIGURATION section below.

Run with:
    python battery_analysis_scripts/TB_CPA_Evaluate/run_config.py

Output location:
    {OUTPUT_ROOT}/step_evals/{cell_id}/{filename}_step_eval.csv

    If OUTPUT_ROOT is None, the output folder is created as a sibling of
    HARMONIZED_PATH:
        .../03_Harmonized_Data/   →   .../04_Evaluated_Data/step_evals/
"""

# ============================================================
# USER CONFIGURATION — edit this section only
# ============================================================

# Path to the 03_Harmonized_Data folder (or any folder with harmonized CSVs)
HARMONIZED_PATH = r"C:\path\to\03_Harmonized_Data"

# Root for evaluated output.
# None → auto: sibling '04_Evaluated_Data/' next to HARMONIZED_PATH
# Path → write to this folder (step_evals/ subfolder is created inside)
OUTPUT_ROOT = None

# ── Skip / rerun control ──────────────────────────────────────────────────────
# True  → skip files whose _step_eval.csv already exists
# False → re-evaluate every file
SKIP_RERUN = True

# Cell IDs listed here will be force-re-evaluated even when SKIP_RERUN = True.
# Leave empty [] to apply SKIP_RERUN to all cells.
# Example: ["LFP44X_001", "LFP44X_002"]
SKIP_RERUN_EXCEPT_IDs = []

# ── Cell filter ───────────────────────────────────────────────────────────────
# List specific cell folder names to process. Leave empty [] to process ALL cells.
# Example: ["LFP44X_001", "096_DQ_P_002"]
RUN_CELL_IDs = []

# ── Logging ───────────────────────────────────────────────────────────────────
# Path to a folder where the debug log will be written.
# None → log to console only (no file written)
# Example: r"C:\path\to\06_Logs"
LOG_PATH = None

# ============================================================
# DO NOT EDIT BELOW THIS LINE
# ============================================================
import sys
import os

# Ensure the package root (TB_CPA_Evaluate/) is on sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from evaluate_run import run_evaluate

result = run_evaluate(
    harmonized_path=HARMONIZED_PATH,
    output_root=OUTPUT_ROOT,
    skip_rerun=SKIP_RERUN,
    skip_rerun_except_ids=SKIP_RERUN_EXCEPT_IDs,
    run_cell_ids=RUN_CELL_IDs,
    log_path=LOG_PATH,
)

print(
    f"\n[Done]  processed={result['processed']}  "
    f"skipped={result['skipped']}  "
    f"failed={result['failed']}\n"
)
