import pandas as pd
import numpy as np
from scipy.interpolate import interp1d
from scipy.optimize import curve_fit


def get_decimal_places(series):
    """Estimate the number of decimal places in a numeric pandas Series."""
    decimals = series.dropna().astype(str).str.split('.').str[1]
    decimals = decimals[decimals.notnull()]
    if not decimals.empty:
        return max(decimals.map(len).mode()[0], 2)
    return 2


def interpolate_dataframe_with_rounding(df, reference_col, new_values):
    """Interpolate all columns based on a reference column, preserving decimal precision.

    Parameters:
    - df: pandas DataFrame
    - reference_col: str, column to interpolate on
    - new_values: array-like, new values for interpolation

    Returns:
    - interpolated_df: pandas DataFrame with interpolated and rounded values
    """
    interpolated_data = {reference_col: new_values}

    for col in df.columns:
        if col != reference_col:
            f = interp1d(df[reference_col], df[col], kind='linear', bounds_error=False)
            interpolated_values = f(new_values)
            decimals = get_decimal_places(df[col])
            interpolated_data[col] = np.round(interpolated_values, decimals)

    return pd.DataFrame(interpolated_data)


def arrhenius(T, A, Ea):
    """Arrhenius equation: k(T) = A * exp(-Ea / (R * T))

    Parameters:
    - T: temperature in Celsius (converted to Kelvin internally)
    - A: pre-exponential factor
    - Ea: activation energy (J/mol)
    """
    R = 8.314  # J/(mol*K)
    return A * np.exp(-Ea / (R * (T + 273.15)))


def fit_arrhenius(temperatures, values):
    """Fit Arrhenius model to all valid (non-NaN) data points.

    Returns: (fitted_values, popt) where popt = [A, Ea]
    """
    temperatures = np.array(temperatures, dtype=float)
    values = np.array(values, dtype=float)

    mask = ~np.isnan(values)
    T_fit = temperatures[mask]
    V_fit = values[mask]

    if len(T_fit) < 3:
        raise ValueError("Not enough data points to fit Arrhenius model.")

    popt, _ = curve_fit(arrhenius, T_fit, V_fit, maxfev=10000)
    fitted_values = arrhenius(temperatures, *popt)

    return fitted_values, popt


def fit_arrhenius_first_three(temperatures, values):
    """Fit Arrhenius on first 3 non-NaN points (low temperature extrapolation).

    Returns: (fitted_values, popt) where popt = [A, Ea]
    """
    temperatures = np.array(temperatures, dtype=float)
    values = np.array(values, dtype=float)

    mask = ~np.isnan(values)
    T_fit = temperatures[mask][:3]
    V_fit = values[mask][:3]

    if len(T_fit) < 3:
        raise ValueError("Not enough data points to fit Arrhenius model.")

    popt, _ = curve_fit(arrhenius, T_fit, V_fit, maxfev=10000)
    fitted_values = arrhenius(temperatures, *popt)

    return fitted_values, popt


def fit_arrhenius_last_three(temperatures, values):
    """Fit Arrhenius on last 3 non-NaN points (high temperature extrapolation).

    Returns: (fitted_values, popt) where popt = [A, Ea]
    """
    temperatures = np.array(temperatures, dtype=float)
    values = np.array(values, dtype=float)

    mask = ~np.isnan(values)
    T_fit = temperatures[mask][-3:]
    V_fit = values[mask][-3:]

    if len(T_fit) < 3:
        raise ValueError("Not enough data points to fit Arrhenius model.")

    popt, _ = curve_fit(arrhenius, T_fit, V_fit, maxfev=10000)
    fitted_values = arrhenius(temperatures, *popt)

    return fitted_values, popt
