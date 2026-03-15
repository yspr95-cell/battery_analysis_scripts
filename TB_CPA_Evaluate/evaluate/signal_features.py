"""
signal_features.py  —  TB_CPA_Evaluate
========================================
Stateless helper functions for per-step feature extraction:
  - value_at_offset()   : value of a column at a given elapsed-time offset
  - offset_features()   : dict of {prefix+suffix: value} for a set of offsets
  - stat_features()     : mean / median per column
  - time_features()     : unix_start, unix_end, duration_s, delta stats
"""

import numpy as np
import pandas as pd

# ── Offset definitions ────────────────────────────────────────────────────────

# Voltage gets extra long offsets (1800 s, 3600 s)
VOLTAGE_OFFSETS     = (0.0, 1.0, 10.0, 18.0, 180.0, 1800.0, 3600.0)
VOLTAGE_SUFFIXES    = ('t0', 't1s', 't10s', 't18s', 't180s', 't1800s', 't3600s')

# All other signals use the standard 5-offset set
STANDARD_OFFSETS    = (0.0, 1.0, 10.0, 18.0, 180.0)
STANDARD_SUFFIXES   = ('t0', 't1s', 't10s', 't18s', 't180s')


# ── Core helpers ──────────────────────────────────────────────────────────────

def value_at_offset(
    grp: pd.DataFrame,
    offset_s: float,
    col: str,
    elapsed_col: str = 'elapsed_in_step',
    min_fraction: float = 0.5,
) -> float:
    """
    Return the value of `col` at the row whose elapsed_in_step is closest to
    offset_s.

    Returns NaN if:
    - `col` is absent from grp
    - all values in `col` are NaN
    - step duration < offset_s * min_fraction  (step too short to be meaningful)
    """
    try:
        if col not in grp.columns:
            return float('nan')
        if elapsed_col not in grp.columns:
            return float('nan')

        elapsed = grp[elapsed_col]
        elapsed_valid = elapsed.dropna()
        if elapsed_valid.empty:
            return float('nan')

        step_duration = elapsed_valid.max()

        if offset_s > 0 and step_duration < offset_s * min_fraction:
            return float('nan')

        idx = int((elapsed - offset_s).abs().argmin())
        val = grp[col].iloc[idx]
        return float(val) if pd.notna(val) else float('nan')

    except Exception:
        return float('nan')


def offset_features(
    grp: pd.DataFrame,
    col: str,
    prefix: str,
    offsets: tuple,
    suffix_names: tuple,
    include_final: bool = True,
) -> dict:
    """
    Build a dict of {f'{prefix}{suffix}': value_at_offset(offset)} for each
    (offset, suffix) pair.  Optionally appends `{prefix}final`.

    Parameters
    ----------
    col          : source column name in grp (e.g. 'Voltage_V')
    prefix       : output key prefix (e.g. 'V_')
    offsets      : tuple of float offsets in seconds
    suffix_names : tuple of strings matching offsets (same length)
    include_final: if True, also add {prefix}final = last non-NaN value
    """
    result = {}
    for offset, suffix in zip(offsets, suffix_names):
        result[f'{prefix}{suffix}'] = value_at_offset(grp, offset, col)

    if include_final:
        try:
            col_data = grp[col].dropna() if col in grp.columns else pd.Series([], dtype=float)
            result[f'{prefix}final'] = float(col_data.iloc[-1]) if not col_data.empty else float('nan')
        except Exception:
            result[f'{prefix}final'] = float('nan')

    return result


def stat_features(
    grp: pd.DataFrame,
    col: str,
    prefix: str,
    stats: tuple = ('mean', 'median'),
) -> dict:
    """
    Compute scalar statistics on `col` for the group.
    Returns dict: {f'{prefix}mean': …, f'{prefix}median': …}
    """
    result = {}
    try:
        if col not in grp.columns or grp[col].isna().all():
            for s in stats:
                result[f'{prefix}{s}'] = float('nan')
            return result

        series = grp[col].dropna()
        for s in stats:
            if s == 'mean':
                result[f'{prefix}mean'] = float(series.mean())
            elif s == 'median':
                result[f'{prefix}median'] = float(series.median())
            else:
                result[f'{prefix}{s}'] = float('nan')
    except Exception:
        for s in stats:
            result[f'{prefix}{s}'] = float('nan')

    return result


def time_features(grp: pd.DataFrame) -> dict:
    """
    Derive timing statistics.

    Preferred source: Unix_time column.
    Fallback for duration_s: Total_time_s range.

    Returns
    -------
    unix_start   : first Unix_time in step
    unix_end     : last Unix_time in step
    duration_s   : unix_end - unix_start (or Total_time_s range)
    dt_median_s  : median row-to-row time delta
    dt_min_s     : minimum row-to-row time delta (excluding zeros)
    dt_max_s     : maximum row-to-row time delta
    """
    result = {
        'unix_start':  float('nan'),
        'unix_end':    float('nan'),
        'duration_s':  float('nan'),
        'dt_median_s': float('nan'),
        'dt_min_s':    float('nan'),
        'dt_max_s':    float('nan'),
    }

    try:
        # Unix_time-based stats
        if 'Unix_time' in grp.columns and not grp['Unix_time'].isna().all():
            ut = grp['Unix_time'].dropna()
            result['unix_start'] = float(ut.iloc[0])
            result['unix_end']   = float(ut.iloc[-1])
            result['duration_s'] = float(ut.iloc[-1] - ut.iloc[0])

            deltas = ut.diff().dropna()
            deltas = deltas[deltas > 0]   # exclude zeros (duplicate timestamps)
            if not deltas.empty:
                result['dt_median_s'] = float(deltas.median())
                result['dt_min_s']    = float(deltas.min())
                result['dt_max_s']    = float(deltas.max())

        # Fallback: duration from Total_time_s
        elif 'Total_time_s' in grp.columns and not grp['Total_time_s'].isna().all():
            ts = grp['Total_time_s'].dropna()
            result['duration_s'] = float(ts.iloc[-1] - ts.iloc[0])

            deltas = ts.diff().dropna()
            deltas = deltas[deltas > 0]
            if not deltas.empty:
                result['dt_median_s'] = float(deltas.median())
                result['dt_min_s']    = float(deltas.min())
                result['dt_max_s']    = float(deltas.max())

    except Exception:
        pass

    return result
