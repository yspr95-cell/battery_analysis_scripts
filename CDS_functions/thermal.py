import numpy as np

from helpers import get_non_outlier_indices
from plotting import plot_T_estimate_for_Ceff


def get_closest_indices(original_times, resampled_times):
    """Returns indices of original_times closest to each resampled_time."""
    original_times = np.array(original_times)
    resampled_times = np.array(resampled_times)
    indices = [np.argmin(np.abs(original_times - t)) for t in resampled_times]
    return np.unique(indices)


def simulate_T_from_Ceff_Qgen(time, init_temperature, Ta, c_eff, Qgen=None):
    """Simulate temperature using Euler's method.

    Equation: dT/dt = Qgen - c_eff * (T - Ta)
    Temperatures in degC.
    """
    time = np.array(time)

    if Qgen is None:
        Qgen = np.zeros(len(time))

    estimated_temperature = [init_temperature]
    for i in range(len(time) - 1):
        dt = time[i + 1] - time[i]
        dT = Qgen[i] * dt - c_eff * (estimated_temperature[-1] - Ta) * dt
        estimated_temperature.append(estimated_temperature[-1] + dT)

    return np.array(estimated_temperature)


def get_fit_err_for_Ceff(time, temperature, Ta, c_eff):
    """Calculate RMS error between measured and simulated temperature."""
    time = np.array(time)
    temperature = np.array(temperature)
    initial_temperature = temperature[0]
    estimate = simulate_T_from_Ceff_Qgen(time, initial_temperature, Ta, c_eff)
    return np.sqrt(np.mean(np.square(estimate - temperature)))


def estimate_Ceff(time_raw, temperature_raw, T_ambient, time_cut=600, resample_time=1):
    """Grid search over Ceff values to find optimal thermal capacitance.

    Tests Ceff values: [0, 0.005, 0.006, ..., 0.014]

    Returns: (ceff_optimal, fig)
    """
    time = np.array(time_raw) - np.min(time_raw)
    temperature = np.array(temperature_raw)

    resampled_indices = get_closest_indices(
        original_times=time,
        resampled_times=np.arange(0, time_cut, resample_time)
    )
    time = time[resampled_indices]
    temperature = temperature[resampled_indices]

    time_filter = time < time_cut
    temperature = temperature[time_filter]
    time = time[time_filter]

    ceffs = np.array([0, 0.005, 0.006, 0.007, 0.008, 0.009, 0.01, 0.011, 0.012, 0.013, 0.014])
    rms_err = np.array([])
    for i in ceffs:
        temp = get_fit_err_for_Ceff(time=time, temperature=temperature, Ta=T_ambient, c_eff=i)
        rms_err = np.append(rms_err, temp)

    ceff_optimal = ceffs[np.argmin(rms_err)]
    fig = plot_T_estimate_for_Ceff(
        time, temperature, T_ambient, ceff_optimal,
        title_text=f"Ceff: {ceff_optimal} @{round(T_ambient)}degC"
    )

    return ceff_optimal, fig


def estimate_Q_gen(time_raw, temperature_raw, T_ambient, C_eff, resample_time=1):
    """Estimate heat generation rate from temperature dynamics.

    Returns: (Qgen_array, Qgen_mean, fig)
    """
    time = np.array(time_raw) - np.min(time_raw)
    temperature = np.array(temperature_raw)

    Qgen = np.diff(temperature, prepend=0) / np.diff(time, prepend=1) + C_eff * (temperature - T_ambient)
    Qgen[Qgen == np.inf] = np.nan

    Qgen_mean = np.mean(Qgen[get_non_outlier_indices(Qgen)])

    fig = plot_T_estimate_for_Ceff(
        time=time, temperature=temperature, Ta=T_ambient, c_eff=C_eff,
        title_text="", Qgen=np.ones(len(time)) * Qgen_mean
    )

    return Qgen, Qgen_mean, fig
