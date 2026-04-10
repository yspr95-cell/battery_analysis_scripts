# TB_CPA_Evaluate — Architecture

> Last updated: 2026-04-10 (performance pass — see decisions.md)
> Update this file when modules are added/removed, data flow changes, or the output schema changes.

---

## Purpose

Cell-level preprocessing pipeline for harmonized battery test data.
Based on `basic_evaluation_cop/03_Run_scripts/Experiments/Run_Base_evaluation.py`.
Consumes per-cell CSV files from `03_Harmonized_Data/`, applies step fixing,
cumulative capacity reconstruction, and SOC calculation, then exports a
`_processed_data.csv` and an interactive overview HTML plot per cell.

---

## Directory Structure

```
TB_CPA_Evaluate/
├── run_config.py              # User-facing entry point — edit parameters here
├── run_evaluate.py            # Pipeline runner: mirrors Run_Base_evaluation.py
├── evaluate_gui.py            # GUI entry point — run with: python evaluate_gui.py
└── src/                       # Supporting library (verbatim from basic_evaluation_cop)
    ├── __init__.py
    ├── paths.py               # PATHS_OBJ (03_/04_/06_ folder convention), long_path()
    ├── data_io.py             # read_harm_cell_data, export_to_excel, extract_2D_table_from_excel, long_path
    ├── cleaning.py            # fix_step_series, fix_capacity_counting, check_time_gap, split_on_time_gaps
    ├── helpers.py             # Pure math/search utilities
    ├── soc_calculations.py    # calculate_SOC_reset_zero_full_dch
    ├── interpolation.py       # dynamic_resampling, interpolate_dataframe_with_rounding, fit_arrhenius*
    ├── plotting.py            # plot_cell_data + general-purpose plotting helpers
    ├── table_interpolation.py # interpolate_table, query_table (SOC × Temperature lookup)
    ├── meta.py                # write_meta, read_meta, sources_changed, params_changed, build_gaps_info
    ├── eval_steps/            # Step-level feature extraction
    │   ├── __init__.py        # re-exports extract_step_features
    │   └── step_features.py   # extract_step_features() — per-step summary table
    └── gui/                   # PySide6 GUI package
        ├── __init__.py        # re-exports main()
        ├── app.py             # MainWindow, ConfigEditorWidget, ConsoleWidget, RunWorker
        └── _gui_runner.py     # Subprocess shim: reads JSON config → calls run_evaluate()
```

---

## Data Flow

```
BASE_PATH/03_Harmonized_Data/{CELLID}/
  └─► src.data_io.read_harm_cell_data()
        • Glob all *{CELLID}*.csv files recursively
        • Concatenate, sort by Unix_time
        • Add Unix_datetime, Unix_total_time columns
        • Deduplicate on (Unix_time, Current_A)
        └─► cell_df

  └─► src.cleaning.check_time_gap()          [warning only]

  └─► src.cleaning.fix_step_series()
        • diff().abs().clip(1).cumsum() on raw Step column
        └─► cell_df['Step_id']  (monotone incrementing integer per step)

  └─► src.cleaning.fix_capacity_counting()
        • Accumulate Capacity_step_Ah across step boundaries
        • Handles cycler resets at step transitions
        └─► cell_df['Capacity_Ah']  (continuous cumulative Ah, zero-based)

  └─► src.soc_calculations.calculate_SOC_reset_zero_full_dch()
        • Identify C/3 discharge steps → Q_std per period
        • cell_df['SOC']           = Capacity_Ah * 100 / Q_std
        • cell_df['SOC_corrected'] = SOC reset to 0 after each full discharge
        └─► (cell_df, full_charge_steps, c3_dch_steps, c3_cha_steps,
              all_c3_dch_steps, full_dch_steps)

  └─► cell_df.to_csv(…/{CELLID}_processed_data.csv)

  └─► src.meta.build_gaps_info(cell_df, gap_indx, gap_time)
        • Converts raw check_time_gap output to serialisable gap dicts
        • Each gap: start/resume datetime + unix ts, duration in s and h

  └─► src.eval_steps.extract_step_features(cell_df)
        • groupby('Step_id') → one row per step
        • identity: step_id, cycle_number, n_rows, step_name
        • time: start, end, duration, median_interval (all Unix seconds)
        • Voltage_V, Current_A, Capacity_step_Ah, Energy_step_Wh:
            start, end, @1s/@10s/@18s/@180s (+ @1800s/@3600s for Voltage)
        • T_Cell/Anode/Cathode/cold _degC: mean, start, end
        • T_Chamber_degC: start, end, mean, unique temps (rounded to 5°, ≥5% freq)
        └─► {YYYYMMDD}_{CELLID}_step_features.csv

  └─► src.meta.write_meta(...)
        └─► {CELLID}_meta.json

  └─► src.interpolation.dynamic_resampling()
        • Retain rows on significant Voltage / Current / Step_id changes
        • Force-keep at least 1 row per 60 s
        └─► dyn_cell_df  (compressed ~10–100× for plotting)

  └─► src.plotting.plot_cell_data(dyn_cell_df)
        └─► …/{CELLID}_Full_Test_overview_resampled_plot.html
```

---

## Module Responsibilities

### `run_config.py`
User-facing entry point. Editable parameters:
- Cell identity: `BASE_PATH`, `NOMINAL_CAPACITY`, `MAX_CELL_VOLT`, `MIN_CELL_VOLT`
- Run control: `SKIP_RERUN`, `SKIP_RERUN_EXCEPT_IDs`, `RUN_CELL_IDs`
- Source change detection: `SOURCE_SIZE_CHANGE_THRESHOLD_KB`
- Overview plot settings: `PLOT_VOLTAGE_THRESHOLD_V`, `PLOT_CURRENT_THRESHOLD_A`, `PLOT_MIN_INTERVAL_S`

Log path is auto-derived as `BASE_PATH/06_Logs/debug_logs/` — not user-configurable.
Calls `run_evaluate.run_evaluate()`.

### `run_evaluate.py`
- Builds `PATHS_OBJ` → resolves `03_Harmonized_Data/`, `04_Evaluated_Data/`, `06_Logs/debug_logs/`
- Discovers cell subfolders; filters by `RUN_CELL_IDs` if provided
- Loops over cells, runs full pipeline per cell (mirrors `Run_Base_evaluation.py`)
- Writes `_processed_data.csv`, `_step_features.csv`, `_meta.json`, and `_Full_Test_overview_resampled_plot.html`
- Returns `dict(processed, skipped, failed, total)`

### `evaluate_gui.py` + `src/gui/`
- `evaluate_gui.py` — thin root-level entry point; adds package root to `sys.path` and calls `src.gui.main()`
- `src/gui/app.py` — PySide6 GUI: `MainWindow`, `ConfigEditorWidget` (all Evaluate parameters), `ConsoleWidget` (live stdout/stderr), `RunWorker(QThread)` (subprocess-based)
- `src/gui/_gui_runner.py` — subprocess shim: reads JSON config written by GUI → calls `run_evaluate(**kwargs)`
- Configs are persisted to `gui_configs.json` in the package root
- Styled with Catppuccin Mocha dark theme (QSS)

### `src/paths.py`
- `PATHS_OBJ(base_path)` — derives `harmonized_path`, `evaluated_path`, `logs_path`
- `long_path(path)` — prepends `\\?\` for Windows paths > 260 characters

### `src/data_io.py`
- `read_harm_cell_data(harm_path, cellid, suffixes)` — loads, merges, sorts, deduplicates CSVs
- `export_to_excel(data_dict, output_path)` — writes dict of DataFrames to multi-sheet Excel
- `extract_2D_table_from_excel(...)` — extracts a 2D lookup table from an Excel sheet by heading search

### `src/cleaning.py`
- `fix_step_series(series)` — converts raw cycler step column to monotone `Step_id`
- `fix_capacity_counting(df)` — reconstructs continuous `Capacity_Ah` across step boundaries
- `check_time_gap(df, threshold)` — detects timestamp jumps > threshold (warning-only)
- `split_on_time_gaps(df, time_col, threshold)` — splits df at time discontinuities

### `src/helpers.py`
Pure utilities: `is_within_range`, `closest_lower_number`, `closest_nth_higher_number`,
`find_closest_indx_series`, `find_closest_argindx_series`, `find_range`,
`filter_by_proximity`, `non_averaging_median`, `get_non_outlier_indices`,
`find_matching_column_number`

### `src/soc_calculations.py`
- `calculate_SOC_reset_zero_full_dch(df, nominal_cap, max_cell_volt, min_cell_volt)`
- Internal `_calculate_SOC_draft_reset_dch_zero()` — step detection and Q_std assignment

### `src/interpolation.py`
- `dynamic_resampling(df, time_col, change_thresholds, min_interval_seconds)` — plot compression
- `interpolate_dataframe_with_rounding(df, reference_col, new_values)` — linear interpolation with decimal preservation
- `fit_arrhenius(temperatures, values)` — global Arrhenius fit
- `fit_arrhenius_first_three(temperatures, values)` — fit on first 3 non-NaN points
- `fit_arrhenius_last_three(temperatures, values)` — fit on last 3 non-NaN points

### `src/plotting.py`
- `plot_cell_data(cell_df, ...)` — 3-panel overview (Temperature / Voltage / Current)
- `plot_ocv_vs_soc(ocv_table, cell_id)` — OCV vs SOC colored by temperature
- `plot_dual_axis(df, ...)` — dual Y-axis scatter
- `plot_QC_subplots(temp_df, ...)` — 2-panel QC plot with SOC reference lines
- `general_dual_axis_plot(df, ...)` — configurable dual-axis with styling options
- `get_color_for_range(value, ...)` — colormap lookup for a scalar value
- `plot_surface_from_table(interpolated_df, ...)` — 3D surface from a 2D lookup table

### `src/table_interpolation.py`
- `interpolate_table(df, method, ...)` — fills NaNs in a SOC × Temperature table (Arrhenius or linear)
- `query_table(df, new_socs, new_temps, ...)` — evaluates the table at arbitrary SOC/temperature points

### `src/meta.py`
- `write_meta(out_path, cell_id, harm_cell_path, params, stats, time_gaps, output_files)` — writes `{CELLID}_meta.json`
- `read_meta(out_path, cell_id)` — loads meta JSON; returns `None` if absent or unreadable
- `sources_changed(meta, harm_cell_path)` — compares current source CSVs against recorded (name, size, mtime); returns `(bool, reason_str)`
- `params_changed(meta, params)` — compares pipeline params; returns `(bool, reason_str)`
- `build_gaps_info(cell_df, gap_indx, gap_time)` — converts `check_time_gap` output to serialisable list of gap dicts

### `src/eval_steps/step_features.py`
- `extract_step_features(cell_df, time_col)` — main entry point; returns one-row-per-step DataFrame
- `_val_at_elapsed(grp_time, grp_val, elapsed_s)` — signal value at t_start + elapsed_s; NaN if step too short
- `_unique_chamber_temps(series)` — rounded chamber temps with ≥5% frequency
- All columns are optional; missing ones produce NaN columns, never raise

---

## Output Files (per cell, under `04_Evaluated_Data/{CELLID}/`)

| File | Contents |
|------|----------|
| `{CELLID}_processed_data.csv` | Full time-series with `Step_id`, `Capacity_Ah`, `SOC`, `SOC_corrected`, `Q_std` added |
| `{CELLID}_Full_Test_overview_resampled_plot.html` | Interactive Plotly overview, dynamically resampled |
| `{YYYYMMDD}_{CELLID}_step_features.csv` | Per-step summary: identity, time, signal snapshots, temperatures |
| `{CELLID}_meta.json` | Run metadata: source files (name/size/mtime), pipeline params, time gaps, processing stats |

---

## Folder Convention

```
BASE_PATH/
├── 03_Harmonized_Data/
│   └── {CELLID}/
│       └── *{CELLID}*.csv            ← input files
├── 04_Evaluated_Data/
│   └── {CELLID}/
│       ├── {CELLID}_processed_data.csv
│       ├── {YYYYMMDD}_{CELLID}_step_features.csv
│       ├── {CELLID}_meta.json
│       └── {CELLID}_Full_Test_overview_resampled_plot.html
└── 06_Logs/
    └── debug_logs/
        └── evaluate_debug_{hostname}.log
```

Folder names are fixed in `src/paths.py` via `PATHS_OBJ`. Only `BASE_PATH` is user-facing.

---

## Dependencies

| Library  | Usage |
|----------|-------|
| pandas   | DataFrame I/O, merge, sort, dedup |
| numpy    | Capacity accumulation, SOC scaling, resampling masks |
| plotly   | Interactive HTML overview and analysis plots |
| scipy    | Arrhenius curve fitting, linear interpolation |
| matplotlib | `get_color_for_range`, `plot_T_estimate_for_Ceff`, table interpolation plots |
| openpyxl | Excel I/O via `export_to_excel`, `extract_2D_table_from_excel` |
| stdlib   | logging, pathlib, socket, time |

---

## Error Handling

| Level    | Behaviour |
|----------|-----------|
| Cell     | Exception caught in `run_evaluate` loop; logged as FAIL, pipeline continues |
| SOC calc | Exception caught; `SOC_corrected` and `Q_std` set to NaN, warning logged, cell still exported |
| Time gap | Detected and logged as warning; does not abort processing |
| Empty data | Empty DataFrame → `ValueError` → FAIL |
| Long paths | `long_path()` adds `\\?\` prefix transparently before all file I/O |
