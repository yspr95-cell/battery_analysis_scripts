"""
interpolate_table.py

Interpolate NaN values in a 2D lookup table where:
  - rows    = SOC points  (index)
  - columns = temperature points

Two interpolation axes are supported:
  1. Temperature axis  – Arrhenius or linear fitting per SOC row
  2. SOC axis          – linear interpolation / extrapolation after temperature axis is done

Usage example
-------------
    from interpolate_table import interpolate_table

    filled_df = interpolate_table(
        df,
        method="arrhenius",          # "arrhenius" | "linear"
        exclude_temps=[],            # temperature columns to exclude from fitting
        n_neighbors=None,            # int or None (all points) for Arrhenius
        extrapolate=False,           # True = extrapolate outside temp range, False = clamp
        soc_extrapolate=False,       # True = extrapolate outside SOC range, False = clamp
        plot=True,                   # show per-SOC fitting plots
    )
"""

from __future__ import annotations

import warnings
from typing import Sequence

import numpy as np
import pandas as pd
from scipy.optimize import curve_fit
from scipy.stats import pearsonr

# ─────────────────────────────────────────────────────────────────────────────
# Arrhenius model helpers
# ─────────────────────────────────────────────────────────────────────────────

_R = 8.314  # J mol⁻¹ K⁻¹


def _to_kelvin(t: float | np.ndarray) -> float | np.ndarray:
    """Assume temperatures ≤ 200 are in °C; convert to K."""
    arr = np.asarray(t, dtype=float)
    mask = arr < 200.0
    arr = np.where(mask, arr + 273.15, arr)
    return arr


def _arrhenius_model(T_K: np.ndarray, A: float, Ea: float) -> np.ndarray:
    """y = A * exp(-Ea / (R * T))"""
    return A * np.exp(-Ea / (_R * T_K))


def _fit_arrhenius(
    temps: np.ndarray,
    values: np.ndarray,
    n_neighbors: int | None,
    query_temps: np.ndarray,
    extrapolate: bool,
) -> np.ndarray:
    """
    Fit an Arrhenius curve to (temps, values) and evaluate at query_temps.

    Parameters
    ----------
    temps        : 1-D array of known temperature points (°C or K)
    values       : 1-D array of known values (same length as temps)
    n_neighbors  : use only the n nearest known points around each query point
                   (None → use all points for a single global fit)
    query_temps  : temperatures at which to evaluate the fitted model
    extrapolate  : if False, clamp outside [min(temps), max(temps)]

    Returns
    -------
    result : 1-D array of interpolated values at query_temps
    """
    T_K = _to_kelvin(temps)
    q_K = _to_kelvin(query_temps)

    result = np.full(len(query_temps), np.nan)

    # ── global fit (n_neighbors is None) ────────────────────────────────────
    if n_neighbors is None:
        try:
            popt, _ = curve_fit(
                _arrhenius_model,
                T_K,
                values,
                p0=[values.mean() * np.exp(5000 / T_K.mean()), 5000.0],
                maxfev=10_000,
            )
            fitted = _arrhenius_model(q_K, *popt)
        except RuntimeError:
            # fall back to linear in log-space
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                coeffs = np.polyfit(1.0 / T_K, np.log(np.abs(values) + 1e-30), 1)
            fitted = np.exp(np.polyval(coeffs, 1.0 / q_K))

        if not extrapolate:
            lo, hi = temps.min(), temps.max()
            fitted = np.where(
                (query_temps < lo) | (query_temps > hi),
                np.interp(query_temps, temps, values),  # clamp = boundary value
                fitted,
            )
        result[:] = fitted
        return result

    # ── local fit using n nearest neighbours ────────────────────────────────
    for i, (qt, qK) in enumerate(zip(query_temps, q_K)):
        distances = np.abs(temps - qt)
        idx = np.argsort(distances)[:n_neighbors]
        t_local = T_K[idx]
        v_local = values[idx]

        if len(t_local) < 2:
            result[i] = v_local[0] if len(v_local) == 1 else np.nan
            continue

        # check if extrapolating
        in_range = temps.min() <= qt <= temps.max()

        if not extrapolate and not in_range:
            # clamp to boundary
            result[i] = values[np.argmin(np.abs(temps - qt))]
            continue

        try:
            popt, _ = curve_fit(
                _arrhenius_model,
                t_local,
                v_local,
                p0=[v_local.mean() * np.exp(5000 / t_local.mean()), 5000.0],
                maxfev=10_000,
            )
            result[i] = _arrhenius_model(qK, *popt)
        except RuntimeError:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                coeffs = np.polyfit(1.0 / t_local, np.log(np.abs(v_local) + 1e-30), 1)
            result[i] = np.exp(np.polyval(coeffs, 1.0 / qK))

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Linear temperature interpolation helper
# ─────────────────────────────────────────────────────────────────────────────

def _interp_linear_temp(
    temps: np.ndarray,
    values: np.ndarray,
    query_temps: np.ndarray,
    extrapolate: bool,
) -> np.ndarray:
    """Linear interpolation along temperature axis."""
    if extrapolate:
        result = np.interp(query_temps, temps, values, left=np.nan, right=np.nan)
        # manual extrapolation for out-of-range points
        # left
        lo_mask = query_temps < temps[0]
        if lo_mask.any() and len(temps) >= 2:
            slope = (values[1] - values[0]) / (temps[1] - temps[0])
            result[lo_mask] = values[0] + slope * (query_temps[lo_mask] - temps[0])
        # right
        hi_mask = query_temps > temps[-1]
        if hi_mask.any() and len(temps) >= 2:
            slope = (values[-1] - values[-2]) / (temps[-1] - temps[-2])
            result[hi_mask] = values[-1] + slope * (query_temps[hi_mask] - temps[-1])
    else:
        # np.interp already clamps by default
        result = np.interp(query_temps, temps, values)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# SOC axis interpolation
# ─────────────────────────────────────────────────────────────────────────────

def _interp_along_soc(
    soc_values: np.ndarray,
    column_data: np.ndarray,
    soc_extrapolate: bool,
) -> np.ndarray:
    """
    Fill NaNs along a single temperature column using linear SOC interpolation.

    Parameters
    ----------
    soc_values   : full SOC axis
    column_data  : 1-D array of values (may contain NaN) at each SOC
    soc_extrapolate : whether to extrapolate beyond known SOC range

    Returns
    -------
    filled column_data
    """
    known_mask = ~np.isnan(column_data)
    if known_mask.sum() < 2:
        return column_data  # not enough to interpolate

    known_soc = soc_values[known_mask]
    known_val = column_data[known_mask]

    result = column_data.copy()
    nan_mask = np.isnan(column_data)

    if not nan_mask.any():
        return result

    q_soc = soc_values[nan_mask]

    if soc_extrapolate:
        interped = np.interp(q_soc, known_soc, known_val, left=np.nan, right=np.nan)
        # left extrapolation
        lo = q_soc < known_soc[0]
        if lo.any() and len(known_soc) >= 2:
            slope = (known_val[1] - known_val[0]) / (known_soc[1] - known_soc[0])
            interped[lo] = known_val[0] + slope * (q_soc[lo] - known_soc[0])
        # right extrapolation
        hi = q_soc > known_soc[-1]
        if hi.any() and len(known_soc) >= 2:
            slope = (known_val[-1] - known_val[-2]) / (known_soc[-1] - known_soc[-2])
            interped[hi] = known_val[-1] + slope * (q_soc[hi] - known_soc[-1])
    else:
        interped = np.interp(q_soc, known_soc, known_val)

    result[nan_mask] = interped
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Plotting helpers
# ─────────────────────────────────────────────────────────────────────────────

def _plot_soc_row(
    soc: float,
    all_temps: np.ndarray,
    original_row: np.ndarray,
    filled_row: np.ndarray,
    method: str,
    exclude_temps: Sequence[float],
    n_neighbors: int | None,
    extrapolate: bool,
) -> None:
    """Plot fitting curve, original, and interpolated points for one SOC row."""
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        warnings.warn("matplotlib is not installed – skipping plots.")
        return

    known_mask = ~np.isnan(original_row)
    interp_mask = np.isnan(original_row) & ~np.isnan(filled_row)

    # build smooth curve for display
    if known_mask.sum() >= 2:
        t_smooth = np.linspace(all_temps.min(), all_temps.max(), 300)
        known_t = all_temps[known_mask]
        known_v = original_row[known_mask]

        if method == "arrhenius":
            smooth_v = _fit_arrhenius(
                known_t, known_v, n_neighbors, t_smooth, extrapolate=True
            )
        else:
            smooth_v = _interp_linear_temp(known_t, known_v, t_smooth, extrapolate=True)
    else:
        t_smooth = np.array([])
        smooth_v = np.array([])

    fig, ax = plt.subplots(figsize=(8, 4))

    if len(t_smooth):
        ax.plot(t_smooth, smooth_v, "b-", lw=1.5, label="fit / interpolant")

    ax.scatter(
        all_temps[known_mask],
        original_row[known_mask],
        color="green",
        zorder=5,
        s=60,
        label="original known",
    )
    if interp_mask.any():
        ax.scatter(
            all_temps[interp_mask],
            filled_row[interp_mask],
            color="red",
            marker="x",
            zorder=6,
            s=80,
            lw=2,
            label="interpolated / extrapolated",
        )
    if exclude_temps:
        excl_arr = np.array(exclude_temps, dtype=float)
        excl_in_range = excl_arr[
            (excl_arr >= all_temps.min()) & (excl_arr <= all_temps.max())
        ]
        for et in excl_in_range:
            ax.axvline(et, color="gray", ls="--", lw=0.8, alpha=0.6)

    ax.set_xlabel("Temperature (°C or K)")
    ax.set_ylabel("Value")
    ax.set_title(
        f"SOC = {soc:.4g}  |  method = {method}"
        + (f"  |  n_neighbors = {n_neighbors}" if n_neighbors else "")
    )
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()


# ─────────────────────────────────────────────────────────────────────────────
# Main public function
# ─────────────────────────────────────────────────────────────────────────────

def interpolate_table(
    df: pd.DataFrame,
    method: str = "arrhenius",
    exclude_temps: Sequence[float] | None = None,
    n_neighbors: int | None = None,
    extrapolate: bool = False,
    soc_extrapolate: bool = False,
    plot: bool = False,
) -> pd.DataFrame:
    """
    Fill NaN values in a SOC × Temperature lookup table.

    The function works in two passes:
      1. **Temperature axis** – for each SOC row, use available temperature
         columns to fit and fill missing temperature columns.
      2. **SOC axis** – for each temperature column still containing NaNs,
         interpolate linearly across SOC.

    Parameters
    ----------
    df : pd.DataFrame
        Table with SOC as the index and temperature values as column names.
        Values may be numeric (float) and may contain NaN.

    method : {"arrhenius", "linear"}
        Interpolation method along the temperature axis.
        - "arrhenius": fits  y = A·exp(−Ea / (R·T))  to known points.
        - "linear"   : standard linear interpolation between adjacent points.
        Default: "arrhenius".

    exclude_temps : list of float, optional
        Temperature column(s) to exclude from the *fitting* step (they are
        treated as targets that need to be filled, not as known calibration
        points).  Default: None (all non-NaN columns used).

    n_neighbors : int or None
        Arrhenius only.  Number of nearest temperature points to use for a
        local fit around each query point.  None means a single global fit
        over all available points.  Default: None.

    extrapolate : bool
        Temperature axis.  If True, extrapolate beyond the range of known
        temperature points.  If False (default), clamp to boundary values.

    soc_extrapolate : bool
        SOC axis.  If True, extrapolate beyond the range of known SOC points.
        If False (default), clamp to boundary values.

    plot : bool
        If True, display a matplotlib figure for each SOC row showing the
        fitted curve, original points, and interpolated points.
        Default: False.

    Returns
    -------
    pd.DataFrame
        Copy of *df* with NaN values filled where possible.  Cells that
        cannot be filled (e.g. too few known points) remain NaN.

    Raises
    ------
    ValueError
        If *method* is not "arrhenius" or "linear".
    """
    if method not in ("arrhenius", "linear"):
        raise ValueError(f"method must be 'arrhenius' or 'linear', got '{method}'")

    exclude_temps = list(exclude_temps) if exclude_temps else []

    # make a numeric working copy
    result = df.copy().astype(float)
    all_temps = np.array(result.columns, dtype=float)
    soc_index = np.array(result.index, dtype=float)

    # ── Pass 1: interpolate along temperature axis row by row ────────────────
    for soc, row in result.iterrows():
        original_row = row.values.copy()

        # columns available for fitting = non-NaN and not excluded
        fit_mask = ~np.isnan(original_row) & ~np.isin(all_temps, exclude_temps)

        if fit_mask.sum() < 2:
            # not enough points to fit; skip temperature-axis pass for this row
            continue

        # columns that need to be filled
        fill_mask = np.isnan(original_row)
        # also fill excluded columns if they are NaN
        fill_mask_with_excl = fill_mask  # same: excluded & NaN should be filled

        if not fill_mask_with_excl.any():
            continue  # nothing to fill

        fit_temps = all_temps[fit_mask]
        fit_vals = original_row[fit_mask]
        query_temps = all_temps[fill_mask_with_excl]

        if method == "arrhenius":
            interped = _fit_arrhenius(
                fit_temps, fit_vals, n_neighbors, query_temps, extrapolate
            )
        else:
            interped = _interp_linear_temp(
                fit_temps, fit_vals, query_temps, extrapolate
            )

        filled_row = original_row.copy()
        filled_row[fill_mask_with_excl] = interped
        result.loc[soc] = filled_row

        if plot:
            _plot_soc_row(
                soc=soc,
                all_temps=all_temps,
                original_row=original_row,
                filled_row=filled_row,
                method=method,
                exclude_temps=exclude_temps,
                n_neighbors=n_neighbors,
                extrapolate=extrapolate,
            )

    # ── Pass 2: interpolate along SOC axis column by column ──────────────────
    for col_temp in all_temps:
        col = result[col_temp].values.copy()
        if not np.isnan(col).any():
            continue
        filled_col = _interp_along_soc(soc_index, col, soc_extrapolate)
        result[col_temp] = filled_col

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Query function: evaluate at new SOC × Temperature points
# ─────────────────────────────────────────────────────────────────────────────

def query_table(
    df: pd.DataFrame,
    new_soc: Sequence[float],
    new_temps: Sequence[float],
    method: str = "arrhenius",
    exclude_temps: Sequence[float] | None = None,
    n_neighbors: int | None = None,
    extrapolate: bool = False,
    soc_extrapolate: bool = False,
    plot: bool = False,
) -> pd.DataFrame:
    """
    Evaluate a SOC × Temperature lookup table at arbitrary new SOC and
    temperature coordinates.

    Internally this function:
      1. Fills any NaNs in *df* via :func:`interpolate_table` (same settings).
      2. For every requested SOC point, builds a virtual row by linearly
         interpolating (or extrapolating) across the original SOC axis.
      3. For every requested temperature point, applies the chosen temperature
         interpolation method (Arrhenius or linear) across the virtual row.

    Parameters
    ----------
    df : pd.DataFrame
        Source table with SOC as the index and temperature values as column
        names.  May contain NaN.

    new_soc : sequence of float
        SOC values at which to evaluate the table.

    new_temps : sequence of float
        Temperature values at which to evaluate the table.

    method : {"arrhenius", "linear"}
        Interpolation method along the temperature axis.  Default: "arrhenius".

    exclude_temps : list of float, optional
        Temperature columns to exclude from the fitting basis (passed through
        to :func:`interpolate_table`).  Default: None.

    n_neighbors : int or None
        Arrhenius only.  Local neighbourhood size.  None = global fit.
        Default: None.

    extrapolate : bool
        Temperature axis – extrapolate beyond the original temperature range.
        Default: False (clamp).

    soc_extrapolate : bool
        SOC axis – extrapolate beyond the original SOC range.
        Default: False (clamp).

    plot : bool
        If True, display a matplotlib figure for each new SOC row showing the
        fitted curve along the temperature axis, original grid points (at that
        SOC slice), and the newly queried points.  Default: False.

    Returns
    -------
    pd.DataFrame
        New table with *new_soc* as the index (name preserved from *df*) and
        *new_temps* as the columns.  All values are float.
    """
    new_soc_arr = np.asarray(new_soc, dtype=float)
    new_temps_arr = np.asarray(new_temps, dtype=float)

    # ── Step 1: fill the source table ────────────────────────────────────────
    filled = interpolate_table(
        df,
        method=method,
        exclude_temps=exclude_temps,
        n_neighbors=n_neighbors,
        extrapolate=extrapolate,
        soc_extrapolate=soc_extrapolate,
        plot=False,
    )

    orig_soc = np.array(filled.index, dtype=float)
    orig_temps = np.array(filled.columns, dtype=float)
    grid = filled.values.astype(float)  # shape (n_soc, n_temp)

    # ── Step 2: interpolate along SOC axis → virtual rows at new_soc_arr ─────
    virtual_grid = np.empty((len(new_soc_arr), len(orig_temps)))
    for j in range(len(orig_temps)):
        col = grid[:, j]
        known_mask = ~np.isnan(col)
        if known_mask.sum() < 1:
            virtual_grid[:, j] = np.nan
            continue
        known_soc = orig_soc[known_mask]
        known_val = col[known_mask]
        if soc_extrapolate:
            interped = np.interp(new_soc_arr, known_soc, known_val,
                                 left=np.nan, right=np.nan)
            lo = new_soc_arr < known_soc[0]
            if lo.any() and len(known_soc) >= 2:
                slope = (known_val[1] - known_val[0]) / (known_soc[1] - known_soc[0])
                interped[lo] = known_val[0] + slope * (new_soc_arr[lo] - known_soc[0])
            hi = new_soc_arr > known_soc[-1]
            if hi.any() and len(known_soc) >= 2:
                slope = (known_val[-1] - known_val[-2]) / (known_soc[-1] - known_soc[-2])
                interped[hi] = known_val[-1] + slope * (new_soc_arr[hi] - known_soc[-1])
        else:
            interped = np.interp(new_soc_arr, known_soc, known_val)
        virtual_grid[:, j] = interped

    # ── Step 3: interpolate along temperature axis → final values ────────────
    result_grid = np.empty((len(new_soc_arr), len(new_temps_arr)))

    for i, _ in enumerate(new_soc_arr):
        row = virtual_grid[i]
        fit_mask = ~np.isnan(row)

        if fit_mask.sum() < 2:
            result_grid[i, :] = np.nan
            continue

        fit_t = orig_temps[fit_mask]
        fit_v = row[fit_mask]

        if method == "arrhenius":
            result_grid[i, :] = _fit_arrhenius(
                fit_t, fit_v, n_neighbors, new_temps_arr, extrapolate
            )
        else:
            result_grid[i, :] = _interp_linear_temp(
                fit_t, fit_v, new_temps_arr, extrapolate
            )

    index_name = df.index.name or "SOC"
    out = pd.DataFrame(result_grid, index=new_soc_arr, columns=new_temps_arr)
    out.index.name = index_name
    out.columns.name = df.columns.name

    # ── Optional plots: one per new SOC row ──────────────────────────────────
    if plot:
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            warnings.warn("matplotlib is not installed – skipping plots.")
            return out

        exclude_set = set(exclude_temps) if exclude_temps else set()

        for i, soc_val in enumerate(new_soc_arr):
            row = virtual_grid[i]
            fit_mask = ~np.isnan(row)
            if fit_mask.sum() < 2:
                continue

            fit_t = orig_temps[fit_mask]
            fit_v = row[fit_mask]

            # smooth curve across full original temp range
            t_smooth = np.linspace(orig_temps.min(), orig_temps.max(), 300)
            if method == "arrhenius":
                v_smooth = _fit_arrhenius(fit_t, fit_v, n_neighbors, t_smooth,
                                          extrapolate=True)
            else:
                v_smooth = _interp_linear_temp(fit_t, fit_v, t_smooth,
                                               extrapolate=True)

            _, ax = plt.subplots(figsize=(8, 4))
            ax.plot(t_smooth, v_smooth, "b-", lw=1.5, label="fit / interpolant")
            ax.scatter(orig_temps[fit_mask], row[fit_mask], color="green",
                       zorder=5, s=60, label="original grid points (at this SOC)")
            ax.scatter(new_temps_arr, result_grid[i], color="red", marker="x",
                       zorder=6, s=80, lw=2, label="queried points")
            if exclude_set:
                for et in exclude_set:
                    if orig_temps.min() <= et <= orig_temps.max():
                        ax.axvline(et, color="gray", ls="--", lw=0.8, alpha=0.6)
            ax.set_xlabel("Temperature (°C or K)")
            ax.set_ylabel("Value")
            ax.set_title(
                f"query_table  |  SOC = {soc_val:.4g}  |  method = {method}"
                + (f"  |  n_neighbors = {n_neighbors}" if n_neighbors else "")
            )
            ax.legend(fontsize=8)
            ax.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.show()

    return out


# ─────────────────────────────────────────────────────────────────────────────
# Quick smoke-test / demo
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import pandas as pd
    import numpy as np

    # Build a toy SOC × Temperature table with some NaNs
    soc_pts = [0.0, 0.1, 0.2, 0.5, 0.8, 1.0]
    temp_pts = [-20.0, 0.0, 25.0, 40.0, 60.0]

    # Synthetic "true" values:  value = (1 + SOC) * exp(-500/(R*(T+273.15)))
    R = 8.314
    true_vals = {
        t: [(1 + s) * np.exp(-500 / (R * (t + 273.15))) for s in soc_pts]
        for t in temp_pts
    }

    df_full = pd.DataFrame(true_vals, index=soc_pts)
    df_full.index.name = "SOC"

    # Introduce NaNs
    df_nan = df_full.copy()
    df_nan.loc[0.2, 0.0] = np.nan
    df_nan.loc[0.5, -20.0] = np.nan
    df_nan.loc[0.5, 60.0] = np.nan
    df_nan.loc[0.8, 25.0] = np.nan
    df_nan.loc[1.0, 40.0] = np.nan

    print("=== Input table (with NaNs) ===")
    print(df_nan.to_string())

    filled = interpolate_table(
        df_nan,
        method="arrhenius",
        exclude_temps=[],
        n_neighbors=None,
        extrapolate=False,
        soc_extrapolate=False,
        plot=True,
    )

    print("\n=== Filled table ===")
    print(filled.to_string())

    print("\n=== Residuals vs. ground truth ===")
    print((filled - df_full).abs().to_string())

    # ── query_table demo ─────────────────────────────────────────────────────
    new_soc_pts  = [0.05, 0.25, 0.6, 0.9]
    new_temp_pts = [-10.0, 10.0, 30.0, 50.0]

    queried = query_table(
        df_nan,
        new_soc=new_soc_pts,
        new_temps=new_temp_pts,
        method="arrhenius",
        extrapolate=False,
        soc_extrapolate=False,
    )

    print("\n=== query_table output (new SOC × new temps) ===")
    print(queried.to_string())

    # compare against true values at the same coordinates
    true_query = pd.DataFrame(
        {t: [(1 + s) * np.exp(-500 / (R * (t + 273.15))) for s in new_soc_pts]
         for t in new_temp_pts},
        index=new_soc_pts,
    )
    true_query.index.name = "SOC"
    print("\n=== Residuals vs. ground truth (query points) ===")
    print((queried - true_query).abs().to_string())
