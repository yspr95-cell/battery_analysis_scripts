"""
step_classifier.py  —  TB_CPA_Evaluate
========================================
Classify each step group into one of nine Step_type strings:

    CC_Charge | CV_Charge | CC-CV_Charge | CP_Charge
    CC_Discharge | CV_Discharge | CC-CV_Discharge | CP_Discharge
    Rest

Algorithm overview
------------------
1. |I_mean| <= CURRENT_THRESHOLD_A  →  Rest
2. direction = Charge (I_mean > 0) or Discharge (I_mean < 0)
3. Multi-phase check: _detect_cc_cv()  →  CC-CV_{direction}
4. Constant-power check: power CoV < CP_COV_THRESHOLD  →  CP_{direction}
5. Constant-voltage check: voltage CoV < CV_COV_THRESHOLD +
   current trending away from initial  →  CV_{direction}
6. Default  →  CC_{direction}
"""

import numpy as np
import pandas as pd

# ── Thresholds ────────────────────────────────────────────────────────────────
CURRENT_THRESHOLD_A  = 0.05    # |I_mean| below this → Rest
CC_COV_THRESHOLD     = 0.05    # current CoV < this → CC phase
CV_COV_THRESHOLD     = 0.002   # voltage CoV < this → CV candidate
CP_COV_THRESHOLD     = 0.05    # power CoV < this → CP
CC_CV_MIN_ROWS       = 20      # minimum rows to attempt CC-CV detection
CC_CV_TRENDING_FRAC  = 0.70    # fraction of late rows where current must trend
CC_CV_SPIKE_RATIO    = 2.0     # |dI/dt| at inflection must be this × rest of step


# ── Public function ───────────────────────────────────────────────────────────

def classify_step(grp: pd.DataFrame) -> str:
    """
    Classify a step group and return a Step_type string.

    Parameters
    ----------
    grp : DataFrame for a single step (reset_index applied, so iloc[0] = first row)
    """
    try:
        if 'Current_A' not in grp.columns or grp['Current_A'].isna().all():
            return 'Unknown'

        i_series = grp['Current_A'].dropna()
        if i_series.empty:
            return 'Unknown'

        i_mean = float(i_series.mean())

        # ── Step 1: Rest ──────────────────────────────────────────────────────
        if abs(i_mean) <= CURRENT_THRESHOLD_A:
            return 'Rest'

        # ── Step 2: Direction ─────────────────────────────────────────────────
        direction = 'Charge' if i_mean > 0 else 'Discharge'

        # ── Step 3: CC-CV (multi-phase, checked first) ────────────────────────
        if _detect_cc_cv(grp, i_series, direction):
            return f'CC-CV_{direction}'

        # ── Step 4: CP (constant power) ───────────────────────────────────────
        if _is_cp(grp):
            return f'CP_{direction}'

        # ── Step 5: CV (constant voltage) ────────────────────────────────────
        if _is_cv(grp, i_series, direction):
            return f'CV_{direction}'

        # ── Step 6: Default CC ────────────────────────────────────────────────
        return f'CC_{direction}'

    except Exception:
        return 'Unknown'


# ── Private helpers ───────────────────────────────────────────────────────────

def _cov(series: pd.Series) -> float:
    """Coefficient of variation (std / |mean|).  Returns inf if mean ≈ 0."""
    s = series.dropna()
    if s.empty:
        return float('inf')
    mean_abs = abs(s.mean())
    if mean_abs < 1e-12:
        return float('inf')
    return float(s.std() / mean_abs)


def _is_cp(grp: pd.DataFrame) -> bool:
    """True if Power_W is present and its CoV is below CP_COV_THRESHOLD."""
    if 'Power_W' not in grp.columns:
        return False
    pw = grp['Power_W'].dropna()
    if pw.empty or pw.abs().mean() < 1e-6:
        return False
    return _cov(pw.abs()) < CP_COV_THRESHOLD


def _is_cv(grp: pd.DataFrame, i_series: pd.Series, direction: str) -> bool:
    """
    True if voltage is nearly constant AND current is trending away from
    its initial value (declining for Charge, rising in absolute terms for
    Discharge).
    """
    if 'Voltage_V' not in grp.columns:
        return False
    v = grp['Voltage_V'].dropna()
    if v.empty:
        return False

    if _cov(v) >= CV_COV_THRESHOLD:
        return False

    # Check that current is trending (not flat) — distinguishes CV from Rest-with-small-I
    if len(i_series) < 4:
        return True   # short step, trust voltage CoV alone

    diffs = i_series.diff().dropna()
    if direction == 'Charge':
        # Current should be declining: dI/dt < 0
        trending = (diffs < 0).sum() / len(diffs)
    else:
        # For discharge (I < 0), absolute value should be declining → I rising toward 0
        trending = (diffs > 0).sum() / len(diffs)

    return trending >= 0.55   # majority of rows declining


def _detect_cc_cv(grp: pd.DataFrame, i_series: pd.Series, direction: str) -> bool:
    """
    Detect a CC-CV transition within the step.

    Returns True if all three conditions hold:
      1. Early half of step has low current CoV (CC phase)
      2. Late half has low voltage CoV AND current is trending (CV phase)
      3. An inflection (|dI/dt| spike) is visible near the midpoint

    Guard: returns False immediately if len(grp) < CC_CV_MIN_ROWS.
    """
    if len(grp) < CC_CV_MIN_ROWS:
        return False

    if 'Voltage_V' not in grp.columns:
        return False

    v_series = grp['Voltage_V']
    n = len(grp)
    split = n // 2

    # ── Condition 1: early half is CC ─────────────────────────────────────────
    i_early = i_series.iloc[:split]
    early_cov = _cov(i_early.abs())
    if early_cov >= CC_COV_THRESHOLD:
        return False

    # ── Condition 2: late half is CV ──────────────────────────────────────────
    v_late = v_series.iloc[split:].dropna()
    if v_late.empty:
        return False
    late_v_cov = _cov(v_late)
    if late_v_cov >= CV_COV_THRESHOLD:
        return False

    i_late = i_series.iloc[split:]
    late_diffs = i_late.diff().dropna()
    if len(late_diffs) == 0:
        return False

    if direction == 'Charge':
        trending = (late_diffs < 0).sum() / len(late_diffs)
    else:
        trending = (late_diffs > 0).sum() / len(late_diffs)

    if trending < CC_CV_TRENDING_FRAC:
        return False

    # ── Condition 3: inflection guard (|dI/dt| spike near midpoint) ──────────
    dI_abs = i_series.diff().abs().fillna(0)

    window_half = max(3, n // 20)
    lo = max(0, split - window_half)
    hi = min(n, split + window_half)

    spike_region = dI_abs.iloc[lo:hi]
    rest_parts   = pd.concat([dI_abs.iloc[:lo], dI_abs.iloc[hi:]])

    rest_median = rest_parts.median() if not rest_parts.empty else 0.0
    if rest_median < 1e-12:
        # No clear baseline — accept if spike region has any significant dI/dt
        # (avoids divide-by-zero false negative)
        return spike_region.median() > 1e-6

    spike_ratio = spike_region.median() / rest_median
    return spike_ratio >= CC_CV_SPIKE_RATIO
