import pandas as pd
import numpy as np

from helpers import is_within_range, closest_lower_number, filter_by_proximity, non_averaging_median


def calculate_SOC_draft(df_in, NOMINAL_CAPACITY):
    """Draft SOC calculation using C3 discharge cycles (scaling adjustment).

    Identifies C3 discharge/charge steps by capacity and current criteria,
    then calculates SOC = Capacity_Ah * 100 / Q_std.

    Returns: (df, c3_dch_steps, c3_cha_steps)
    """
    temp_df = df_in.copy()

    c3_dch_steps = [i for i in temp_df['Step_id'].unique() if
                    is_within_range(temp_df.loc[temp_df['Step_id'] == i, 'Capacity_step_Ah'].iloc[-1],
                                   [-NOMINAL_CAPACITY * 0.9, -NOMINAL_CAPACITY * 1.1]) and
                    is_within_range(temp_df.loc[temp_df['Step_id'] == i, 'Current_A'].mean() * 3,
                                   [-NOMINAL_CAPACITY * 0.9, -NOMINAL_CAPACITY * 1.1])]

    c3_cha_steps = [i for i in temp_df['Step_id'].unique() if
                    is_within_range(temp_df.loc[temp_df['Step_id'] == i, 'Capacity_step_Ah'].iloc[-1],
                                   [NOMINAL_CAPACITY * 0.9, NOMINAL_CAPACITY * 1.1]) and
                    is_within_range(temp_df.loc[temp_df['Step_id'] == i, 'Current_A'].mean() * 3,
                                   [NOMINAL_CAPACITY * 0.9, NOMINAL_CAPACITY * 1.1])]

    temp_df['SOC'] = None
    temp_df['Q_std'] = None
    for i in temp_df['Step_id'].unique():
        prev_std_dch_step = closest_lower_number(c3_dch_steps, i)
        if prev_std_dch_step != None:
            Q_std = -temp_df.loc[temp_df['Step_id'] == prev_std_dch_step, 'Capacity_step_Ah'].iloc[-1]
            temp_df.loc[temp_df['Step_id'] == i, 'Q_std'] = Q_std
            temp_df.loc[temp_df['Step_id'] == i, 'SOC'] = temp_df.loc[temp_df['Step_id'] == i, 'Capacity_Ah'] * 100 / Q_std
        else:
            Q_std = -temp_df.loc[temp_df['Step_id'] == c3_dch_steps[0], 'Capacity_step_Ah'].iloc[-1]
            temp_df.loc[temp_df['Step_id'] == i, 'Q_std'] = Q_std
            temp_df.loc[temp_df['Step_id'] == i, 'SOC'] = temp_df.loc[temp_df['Step_id'] == i, 'Capacity_Ah'] * 100 / Q_std
    return temp_df, c3_dch_steps, c3_cha_steps


def calculate_SOC(df_input, nominal_cap=215, max_cell_volt=3.8):
    """Corrected SOC with offset adjustment at full charge points.

    Calls calculate_SOC_draft first, then adjusts SOC to 100% at each
    full charge point (Voltage >= max_cell_volt AND Current <= nominal_cap/19).

    Returns: (df, full_charge_steps, c3_dch_steps, c3_cha_steps)
    """
    df_in = df_input.copy()
    df_in, c3_dch_steps, c3_cha_steps = calculate_SOC_draft(df_in, nominal_cap)

    full_charge_steps = df_in.loc[
        (df_in['Voltage_V'] >= max_cell_volt) & (df_in['Current_A'] <= nominal_cap / 19),
        'Step_id'
    ].unique()
    df_in['SOC_corrected'] = None

    for charge_step in full_charge_steps:
        if charge_step == full_charge_steps[0]:
            charge_step_last_SOC = df_in.loc[df_in['Step_id'] == charge_step, 'SOC'].iloc[-1]
            df_in.loc[:, 'SOC_corrected'] = df_in.loc[:, 'SOC'] + 100 - charge_step_last_SOC
        else:
            charge_step_last_SOC = df_in.loc[df_in['Step_id'] == charge_step, 'SOC'].iloc[-1]
            df_in.loc[df_in['Step_id'] >= charge_step, 'SOC_corrected'] = (
                df_in.loc[df_in['Step_id'] >= charge_step, 'SOC'] + 100 - charge_step_last_SOC
            )
    df_in['SOC_corrected'] = df_in['SOC_corrected'].astype(np.float64)

    return df_in, full_charge_steps, c3_dch_steps, c3_cha_steps


def calculate_SOC_draft_QC_capability(df_in, NOMINAL_CAPACITY):
    """QC variant of draft SOC with RPT cycle detection.

    Filters C3 discharge steps to RPT-only cycles where discharge step i
    is followed by a charge step at i+2.

    Returns: (df, c3_dch_steps, c3_cha_steps, all_c3_dch_steps)
    """
    temp_df = df_in.copy()

    all_c3_dch_steps = [i for i in temp_df['Step_id'].unique() if
                        is_within_range(temp_df.loc[temp_df['Step_id'] == i, 'Capacity_step_Ah'].iloc[-1],
                                       [-NOMINAL_CAPACITY * 0.9, -NOMINAL_CAPACITY * 1.1]) and
                        is_within_range(temp_df.loc[temp_df['Step_id'] == i, 'Current_A'].mean() * 3,
                                       [-NOMINAL_CAPACITY * 0.9, -NOMINAL_CAPACITY * 1.1])]

    c3_cha_steps = [i for i in temp_df['Step_id'].unique() if
                    is_within_range(temp_df.loc[temp_df['Step_id'] == i, 'Capacity_step_Ah'].iloc[-1],
                                   [NOMINAL_CAPACITY * 0.9, NOMINAL_CAPACITY * 1.1]) and
                    is_within_range(temp_df.loc[temp_df['Step_id'] == i, 'Current_A'].mean() * 3,
                                   [NOMINAL_CAPACITY * 0.9, NOMINAL_CAPACITY * 1.1])]

    # RPT-only C3 discharge steps: discharge at step i followed by charge at i+2
    c3_dch_steps = [i for i in all_c3_dch_steps if i + 2 in c3_cha_steps]
    c3_cha_steps = [i + 2 for i in c3_dch_steps]

    temp_df['SOC'] = None
    temp_df['Q_std'] = None
    for i in temp_df['Step_id'].unique():
        prev_std_dch_step = closest_lower_number(c3_dch_steps, i)
        if prev_std_dch_step != None:
            Q_std = -temp_df.loc[temp_df['Step_id'] == prev_std_dch_step, 'Capacity_step_Ah'].iloc[-1]
            assert(is_within_range(Q_std, [NOMINAL_CAPACITY * 0.8, NOMINAL_CAPACITY * 1.1]))
            temp_df.loc[temp_df['Step_id'] == i, 'Q_std'] = Q_std
            temp_df.loc[temp_df['Step_id'] == i, 'SOC'] = temp_df.loc[temp_df['Step_id'] == i, 'Capacity_Ah'] * 100 / Q_std
        else:
            Q_std = -temp_df.loc[temp_df['Step_id'] == c3_dch_steps[0], 'Capacity_step_Ah'].iloc[-1]
            assert(is_within_range(Q_std, [NOMINAL_CAPACITY * 0.8, NOMINAL_CAPACITY * 1.1]))
            temp_df.loc[temp_df['Step_id'] == i, 'Q_std'] = Q_std
            temp_df.loc[temp_df['Step_id'] == i, 'SOC'] = temp_df.loc[temp_df['Step_id'] == i, 'Capacity_Ah'] * 100 / Q_std
    return temp_df, c3_dch_steps, c3_cha_steps, all_c3_dch_steps


def calculate_SOC_QC_capability(df_input, nominal_cap=215, max_cell_volt=3.8, min_cell_volt=2.5):
    """QC variant of corrected SOC with RPT cycle detection.

    Returns: (df, full_charge_steps, c3_dch_steps, c3_cha_steps, all_c3_dch_steps)
    """
    df_in = df_input.copy()
    df_in, c3_dch_steps, c3_cha_steps, all_c3_dch_steps = calculate_SOC_draft_QC_capability(df_in, nominal_cap)

    full_charge_steps = df_in.loc[
        (df_in['Voltage_V'] >= max_cell_volt) & (df_in['Current_A'] <= nominal_cap / 19),
        'Step_id'
    ].unique()
    df_in['SOC_corrected'] = None

    for charge_step in full_charge_steps:
        if charge_step == full_charge_steps[0]:
            charge_step_last_SOC = df_in.loc[df_in['Step_id'] == charge_step, 'SOC'].iloc[-1]
            df_in.loc[:, 'SOC_corrected'] = df_in.loc[:, 'SOC'] + 100 - charge_step_last_SOC
        else:
            charge_step_last_SOC = df_in.loc[df_in['Step_id'] == charge_step, 'SOC'].iloc[-1]
            df_in.loc[df_in['Step_id'] >= charge_step, 'SOC_corrected'] = (
                df_in.loc[df_in['Step_id'] >= charge_step, 'SOC'] + 100 - charge_step_last_SOC
            )
    df_in['SOC_corrected'] = df_in['SOC_corrected'].astype(np.float64)

    return df_in, full_charge_steps, c3_dch_steps, c3_cha_steps, all_c3_dch_steps


def find_OCV_index_from_split(split_df_input, resample_time=60, ocv_stable_duration=600, temperature_stable_duration=3600):
    """Identify first stable OCV point from a time-split segment.

    Stability criteria:
    - Voltage change < 5 mV over ocv_stable_duration
    - Temperature change < 2 degC over temperature_stable_duration

    Requires columns: Unix_datetime, Voltage_filt_V, T_Cell_degC, index_copy
    """
    tempdf = split_df_input[['Unix_datetime', 'Voltage_filt_V', 'T_Cell_degC', 'index_copy']].set_index(
        'Unix_datetime', inplace=False, drop=True
    ).resample(f'{resample_time}s').agg(non_averaging_median)

    tempdf['OCV_flag'] = (
        tempdf['Voltage_filt_V'] - tempdf['Voltage_filt_V'].shift(int(ocv_stable_duration / resample_time))
    ) * 1000 < 5
    tempdf['T_STABLE_flag'] = (
        tempdf['T_Cell_degC'] - tempdf['T_Cell_degC'].shift(int(temperature_stable_duration / resample_time))
    ) < 2

    return tempdf.loc[tempdf['T_STABLE_flag'] & tempdf['OCV_flag'], 'index_copy'].iloc[0]
