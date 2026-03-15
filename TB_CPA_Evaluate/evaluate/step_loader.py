"""
step_loader.py  —  TB_CPA_Evaluate
====================================
Load a harmonized CSV, validate mandatory columns, infer the Step column when
absent, and compute elapsed_in_step for offset-based feature extraction.
"""

import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Column definitions ────────────────────────────────────────────────────────
MANDATORY_COLS = ['Total_time_s', 'Date_time', 'Voltage_V', 'Current_A', 'Capacity_step_Ah']

TEMP_COLS = ['T_Cell_degC', 'T_Anode_degC', 'T_Cathode_degC', 'T_Chamber_degC', 'T_cold_degC']

TEMP_SUFFIX_MAP = {
    'T_Cell_degC':    'Cell',
    'T_Anode_degC':   'Anode',
    'T_Cathode_degC': 'Cathode',
    'T_Chamber_degC': 'Chamber',
    'T_cold_degC':    'cold',
}

# Capacity reset threshold: a drop larger than this (Ah) marks a new step
_CAPACITY_RESET_THRESHOLD_AH = 0.005


# ── Public API ────────────────────────────────────────────────────────────────

def load_harmonized_csv(filepath: Path) -> tuple[pd.DataFrame, list[str]]:
    """
    Load a harmonized CSV produced by TB_CPA_Harmonize_v1.2.

    Returns
    -------
    df       : DataFrame with all present columns coerced to appropriate dtypes
    warnings : list of non-fatal warning strings
    """
    warnings: list[str] = []
    filepath = Path(filepath)

    # ── Read CSV ──────────────────────────────────────────────────────────────
    read_kwargs = dict(comment='#', low_memory=False)
    try:
        df = pd.read_csv(filepath, encoding='utf-8', **read_kwargs)
    except UnicodeDecodeError:
        df = pd.read_csv(filepath, encoding='iso-8859-1', **read_kwargs)
        warnings.append('Encoding fallback: iso-8859-1 used.')

    # ── Validate mandatory columns ────────────────────────────────────────────
    missing = [c for c in MANDATORY_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing mandatory columns: {missing} in {filepath.name}")

    # ── Coerce numeric columns ────────────────────────────────────────────────
    numeric_cols = [
        'Total_time_s', 'Unix_time', 'Voltage_V', 'Current_A', 'Power_W',
        'Capacity_step_Ah', 'Energy_step_Wh', 'Cycle',
    ] + TEMP_COLS
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Warn on all-NaN mandatory cols
    for col in MANDATORY_COLS:
        if df[col].isna().all():
            warnings.append(f"Column '{col}' is present but all-NaN.")

    # ── Step column dtype ─────────────────────────────────────────────────────
    if 'Step' in df.columns:
        df['Step'] = pd.to_numeric(df['Step'], errors='coerce')
        df['Step'] = df['Step'].astype('Int64')

    return df, warnings


def infer_step_column(df: pd.DataFrame) -> pd.Series:
    """
    Detect step boundaries from Capacity_step_Ah resets and return an integer
    step-ID Series (0-based, same index as df).

    A new step is detected whenever Capacity_step_Ah drops by more than
    _CAPACITY_RESET_THRESHOLD_AH between consecutive rows.
    """
    cap = df['Capacity_step_Ah']

    if cap.isna().all():
        # Cannot infer — treat entire file as one step
        return pd.Series(0, index=df.index, dtype='Int64')

    delta = cap.diff()
    # Mark boundary at any row where capacity drops significantly
    is_boundary = (delta < -_CAPACITY_RESET_THRESHOLD_AH).fillna(False)
    step_ids = is_boundary.cumsum().astype('Int64')
    return step_ids


def add_elapsed_in_step(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add 'elapsed_in_step' column: seconds elapsed since the first row of each
    step group.  Vectorised via groupby transform — efficient on large DataFrames.
    """
    df['elapsed_in_step'] = (
        df['Total_time_s']
        - df.groupby('Step', sort=False)['Total_time_s'].transform('first')
    )
    return df


def load_and_prepare(filepath: Path) -> tuple[pd.DataFrame, list[str], list[str]]:
    """
    Top-level entry point.

    1. load_harmonized_csv()
    2. Infer Step column if absent or all-NaN
    3. add_elapsed_in_step()

    Returns
    -------
    df        : prepared DataFrame (with 'elapsed_in_step' column)
    temp_cols : temperature columns actually present in df
    warnings  : accumulated warning strings
    """
    df, warnings = load_harmonized_csv(filepath)

    # Infer Step if missing
    if 'Step' not in df.columns or df['Step'].isna().all():
        warnings.append("'Step' column absent or all-NaN — inferring from Capacity_step_Ah resets.")
        df['Step'] = infer_step_column(df)
    else:
        # Forward-fill any isolated NaN step values mid-step
        df['Step'] = df['Step'].ffill().astype('Int64')

    df = add_elapsed_in_step(df)

    temp_cols = [c for c in TEMP_COLS if c in df.columns and not df[c].isna().all()]

    logger.info(
        f"Loaded {filepath.name}: {len(df):,} rows, "
        f"{df['Step'].nunique()} steps, "
        f"temp cols: {[TEMP_SUFFIX_MAP[c] for c in temp_cols]}"
    )

    return df, temp_cols, warnings
