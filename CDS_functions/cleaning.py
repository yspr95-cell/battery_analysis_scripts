import pandas as pd


def fix_step_series(in_series):
    """Correct step counter wraparound by detecting backward jumps and adding offset."""
    step_series = in_series.copy()
    for i in step_series[step_series.diff() < -1].index:
        offset = step_series.loc[i - 1]
        step_series.loc[i:] += offset
    return step_series


def fix_step_series_new(in_series):
    """Alternative step fixing using cumulative sum of absolute diffs."""
    step_series = in_series.copy()
    return step_series.diff().abs().clip(upper=1).cumsum().fillna(0)


def fix_capacity_counting(df_in):
    """Accumulate Capacity_step_Ah across steps into Capacity_Ah.

    NOTE: fix step id before capacity counting.
    Requires Step_id and Capacity_step_Ah columns.
    """
    df = df_in.copy()
    df['Capacity_Ah'] = None
    last_step_cap = 0
    for id in df['Step_id'].unique():
        df.loc[df['Step_id'] == id, 'Capacity_Ah'] = df.loc[df['Step_id'] == id, 'Capacity_step_Ah'] + last_step_cap
        last_step_cap = df.loc[df['Step_id'] == id, 'Capacity_Ah'].iloc[-1]
    df['Capacity_Ah'] = df['Capacity_Ah'] - df['Capacity_Ah'].min()
    return df


def check_time_gap(cell_df, threshold=3600):
    """Detect time gaps exceeding threshold (seconds) in Unix_datetime column.

    Returns: (has_gap, gap_indices, gap_values)
    """
    time_gaps = cell_df['Unix_datetime'].diff().dt.seconds
    if any(time_gaps > threshold):
        return True, time_gaps[time_gaps > threshold].index, time_gaps[time_gaps > threshold].values
    else:
        return False, [], []


def split_on_time_gaps(df, time_col, threshold):
    """Split DataFrame into segments at time discontinuities.

    Returns: list of DataFrame segments
    """
    df = df.sort_values(by=time_col).reset_index(drop=True)
    time_diffs = df[time_col].diff()
    split_indices = time_diffs[time_diffs > threshold].index

    segments = []
    start_idx = 0
    for idx in split_indices:
        segments.append(df.iloc[start_idx:idx])
        start_idx = idx
    segments.append(df.iloc[start_idx:])

    return segments
