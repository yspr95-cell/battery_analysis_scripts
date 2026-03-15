"""
evaluate_run.py  —  TB_CPA_Evaluate
======================================
Batch evaluation entry point.  Discovers harmonized CSVs under a given root,
runs StepEvaluator on each, and saves per-step summary CSVs to:

    {output_root}/step_evals/{cell_id}/{stem}_step_eval.csv

Call from run_config.py or directly:
    python evaluate_run.py   (with constants edited at top of run_config.py)
"""

import logging
import socket
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from evaluate.evaluator import StepEvaluator

logger = logging.getLogger(__name__)

_OUTPUT_SUFFIX = '_step_eval'
_STEP_EVALS_SUBDIR = 'step_evals'


# ── Public API ────────────────────────────────────────────────────────────────

def run_evaluate(
    harmonized_path,
    output_root=None,
    skip_rerun: bool = True,
    skip_rerun_except_ids: list = None,
    run_cell_ids: list = None,
    log_path=None,
) -> dict:
    """
    Batch evaluation pipeline.

    Parameters
    ----------
    harmonized_path      : Path to folder containing harmonized CSVs
                           (e.g. .../03_Harmonized_Data/)
    output_root          : Root for output files.
                           None → sibling folder '04_Evaluated_Data/' next to
                           harmonized_path.
    skip_rerun           : True → skip files whose step_eval CSV already exists
    skip_rerun_except_ids: cell IDs to force-rerun even when skip_rerun=True
    run_cell_ids         : restrict to these cell folder names; [] = all cells
    log_path             : folder for debug log; None = log to console only

    Returns
    -------
    dict with keys: processed, skipped, failed, total
    """
    harmonized_path  = Path(harmonized_path)
    skip_rerun_except_ids = skip_rerun_except_ids or []
    run_cell_ids = run_cell_ids or []

    # ── Output root ───────────────────────────────────────────────────────────
    if output_root is None:
        output_root = harmonized_path.parent / '04_Evaluated_Data'
    else:
        output_root = Path(output_root)

    step_evals_root = output_root / _STEP_EVALS_SUBDIR

    # ── Logging ───────────────────────────────────────────────────────────────
    _setup_logging(log_path)
    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"\n>>>>>>>>>> TB_CPA_Evaluate  —  run started at {run_ts} <<<<<<<<<<\n")
    logger.info(f"Harmonized path : {harmonized_path}")
    logger.info(f"Output root     : {step_evals_root}")
    logger.info(f"skip_rerun      : {skip_rerun}  |  except: {skip_rerun_except_ids}")

    # ── Discover harmonized CSVs ──────────────────────────────────────────────
    csv_paths = _discover_files(harmonized_path, run_cell_ids)
    logger.info(f"Found {len(csv_paths)} harmonized CSV(s) to consider.")

    # ── Main loop ─────────────────────────────────────────────────────────────
    evaluator = StepEvaluator()
    counts = dict(processed=0, skipped=0, failed=0, total=len(csv_paths))

    for csv_path in csv_paths:
        cell_id = csv_path.parent.stem
        out_path = _resolve_output_path(csv_path, step_evals_root)

        # Skip logic
        if out_path.exists() and skip_rerun and cell_id not in skip_rerun_except_ids:
            logger.info(f"  SKIP  {csv_path.name}  (already evaluated)")
            counts['skipped'] += 1
            continue

        t0 = time.perf_counter()
        result = evaluator.run(csv_path)
        elapsed = time.perf_counter() - t0

        if result.is_valid:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            result.summary.to_csv(out_path, index=False)
            logger.info(
                f"  OK    {csv_path.name}  →  {result.n_steps} steps  "
                f"[{elapsed:.1f}s]  →  {out_path.name}"
            )
            if result.warnings:
                for w in result.warnings:
                    logger.warning(f"         {w}")
            counts['processed'] += 1
        else:
            logger.error(
                f"  FAIL  {csv_path.name}  —  {'; '.join(result.errors)}"
            )
            counts['failed'] += 1

    logger.info(
        f"\n[Done]  processed={counts['processed']}  "
        f"skipped={counts['skipped']}  "
        f"failed={counts['failed']}  "
        f"total={counts['total']}\n"
    )
    logging.shutdown()
    return counts


# ── Helpers ───────────────────────────────────────────────────────────────────

def _discover_files(harmonized_path: Path, run_cell_ids: list) -> list[Path]:
    """
    Find all CSV files under harmonized_path.
    Excludes files that already end with _step_eval.csv (evaluated outputs).
    Optionally restricts to specific cell-ID parent folders.
    """
    all_csvs = sorted(harmonized_path.rglob('*.csv'))
    # Exclude evaluated outputs
    all_csvs = [p for p in all_csvs if not p.stem.endswith(_OUTPUT_SUFFIX)]
    # Cell filter
    if run_cell_ids:
        all_csvs = [p for p in all_csvs if p.parent.stem in run_cell_ids]
    return all_csvs


def _resolve_output_path(csv_path: Path, step_evals_root: Path) -> Path:
    """
    Build the output path:
        step_evals_root / {cell_id} / {stem}_step_eval.csv
    """
    cell_id = csv_path.parent.stem
    return step_evals_root / cell_id / f"{csv_path.stem}{_OUTPUT_SUFFIX}.csv"


def _setup_logging(log_path):
    """Configure logging: always console INFO; optionally file DEBUG."""
    handlers = [logging.StreamHandler()]
    if log_path is not None:
        log_path = Path(log_path)
        log_path.mkdir(parents=True, exist_ok=True)
        hostname = socket.gethostname()
        fh = logging.FileHandler(log_path / f"evaluate_debug_{hostname}.log", encoding='utf-8')
        fh.setLevel(logging.DEBUG)
        handlers.append(fh)

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s  %(levelname)-8s  %(message)s',
        handlers=handlers,
    )
