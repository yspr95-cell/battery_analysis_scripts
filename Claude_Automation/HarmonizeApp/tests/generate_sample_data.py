"""Generate mock battery test data files for testing HarmonizeApp."""

import pandas as pd
import numpy as np
from pathlib import Path

OUTPUT_DIR = Path(__file__).parent / "sample_data"
OUTPUT_DIR.mkdir(exist_ok=True)

np.random.seed(42)
N = 1_000_000  # rows per sheet (Excel max ~1,048,576 incl. header)


def generate_mcm_style():
    """MCM supplier: Excel with 'Record' and 'Info' sheets, header on row 2."""
    # Row 0-1 are metadata, row 2 is the actual header
    meta_rows = pd.DataFrame({
        0: ["Test Report", "Station: MCM-01"],
        1: ["Cell ID: LFP44X_001", "Date: 2025-01-15"],
        2: ["", ""],
    })

    time_s = np.cumsum(np.random.uniform(0.5, 2.0, N))
    voltage = 3.2 + 0.5 * np.sin(np.linspace(0, 6 * np.pi, N)) + np.random.normal(0, 0.01, N)
    current = np.where(np.arange(N) % 100 < 50, 2.0, -2.0)
    state = np.where(current > 0, "C", "D")
    step_idx = np.repeat(np.arange(1, N // 20 + 1), 20)[:N]
    cycle_idx = np.repeat(np.arange(1, N // 100 + 1), 100)[:N]

    data = pd.DataFrame({
        "Test Time": [f"0.{int(t//3600):02d}:{int((t%3600)//60):02d}:{t%60:06.3f}" for t in time_s],
        "Step Time": [f"0.00:{int((t%60)//1):02d}:{t%1:.3f}" for t in time_s % 60],
        "DPT Time": pd.date_range("2025-01-15 10:00:00", periods=N, freq="2s"),
        "Step Index": step_idx,
        "State": state,
        "Cycle Index": cycle_idx,
        "U(V)": np.round(voltage, 4),
        "I(A)": np.round(np.abs(current) + np.random.normal(0, 0.01, N), 4),
        "Capacity(Ah)": np.round(np.cumsum(np.abs(current) * 2 / 3600), 4),
        "Energy(Wh)": np.round(np.cumsum(np.abs(current) * voltage * 2 / 3600), 4),
        "Temperature 1": np.round(25.0 + np.random.normal(0, 0.5, N), 2),
        "Temperature 2": np.round(25.5 + np.random.normal(0, 0.5, N), 2),
        "Temperature 3": np.round(24.8 + np.random.normal(0, 0.5, N), 2),
    })

    info = pd.DataFrame({
        "Property": ["Cell ID", "Manufacturer", "Chemistry", "Capacity (Ah)", "Test Station"],
        "Value": ["LFP44X_001", "MCM", "LFP", "50", "Station-3"],
    })

    filepath = OUTPUT_DIR / "MCM_LFP44X_001_test.xlsx"
    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
        # Write metadata + blank row + data for Record sheet
        meta_df = pd.DataFrame([
            ["Test Report - MCM Standard Format"] + [""] * (len(data.columns) - 1),
            ["Cell ID: LFP44X_001", "Date: 2025-01-15"] + [""] * (len(data.columns) - 2),
        ], columns=data.columns)
        combined = pd.concat([meta_df, data], ignore_index=True)
        combined.to_excel(writer, sheet_name="Record", index=False)

        info.to_excel(writer, sheet_name="Info", index=False)

    print(f"  Created: {filepath.name} (sheets: Record, Info)")


def generate_got_style():
    """GOTION supplier: Excel with 'Data' and 'Summary' sheets."""
    time_s = np.cumsum(np.random.uniform(0.5, 1.5, N))
    voltage = 3.6 + 0.3 * np.sin(np.linspace(0, 4 * np.pi, N))
    current = np.where(np.arange(N) % 80 < 40, 1.5, -1.5)
    step_names = np.where(current > 0, "CCCVCharge", "CCDisCharge")
    step_names = np.where(np.arange(N) % 80 == 0, "Rest", step_names)

    data = pd.DataFrame({
        "Absolute Time": pd.date_range("2025-02-10 08:00:00", periods=N, freq="1500ms"),
        "Total Time(s)": np.round(time_s, 3),
        "Step": np.repeat(np.arange(1, N // 20 + 1), 20)[:N],
        "Step Name": step_names,
        "Cycle": np.repeat(np.arange(1, N // 80 + 2), 80)[:N],
        "Voltage(V)": np.round(voltage, 4),
        "Current(A)": np.round(current + np.random.normal(0, 0.005, N), 4),
        "Capacity(Ah)": np.round(np.cumsum(np.abs(current) * 1.5 / 3600), 4),
        "Energy(Wh)": np.round(np.cumsum(np.abs(current) * voltage * 1.5 / 3600), 4),
        "CellTemp(degC)": np.round(26.0 + np.random.normal(0, 0.3, N), 2),
        "ChamberTemp(degC)": np.round(25.0 + np.random.normal(0, 0.1, N), 2),
    })

    summary = pd.DataFrame({
        "Cycle": [1, 2, 3],
        "Charge Capacity(Ah)": [1.2, 1.19, 1.18],
        "Discharge Capacity(Ah)": [1.15, 1.14, 1.13],
    })

    filepath = OUTPUT_DIR / "DQ_GOT0042_cycle_test.xlsx"
    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
        data.to_excel(writer, sheet_name="Data", index=False)
        summary.to_excel(writer, sheet_name="Summary", index=False)

    print(f"  Created: {filepath.name} (sheets: Data, Summary)")


def generate_csv_style():
    """SZ supplier: semicolon-separated CSV with metadata row."""
    time_s = np.cumsum(np.random.uniform(1.0, 3.0, N))
    voltage = 3.4 + 0.4 * np.sin(np.linspace(0, 3 * np.pi, N))
    current = np.where(np.arange(N) % 60 < 30, 1.0, -1.0)

    data = pd.DataFrame({
        "Time_s": np.round(time_s, 2),
        "DateTime": pd.date_range("2025-03-01 14:00:00", periods=N, freq="2500ms"),
        "Step_Nr": np.repeat(np.arange(1, N // 15 + 2), 15)[:N],
        "Cycle_Nr": np.repeat(np.arange(1, N // 60 + 2), 60)[:N],
        "Voltage_V": np.round(voltage, 4),
        "Current_A": np.round(current + np.random.normal(0, 0.002, N), 4),
        "Total_Capacity_Ah": np.round(np.cumsum(np.abs(current) * 2.5 / 3600), 4),
        "Step_Capacity_Ah": np.round(np.cumsum(np.abs(current) * 2.5 / 3600) % 0.05, 4),
        "Total_Energy_Wh": np.round(np.cumsum(np.abs(current) * voltage * 2.5 / 3600), 4),
        "Step_Energy_Wh": np.round(np.cumsum(np.abs(current) * voltage * 2.5 / 3600) % 0.2, 4),
        "T_Cell": np.round(27.0 + np.random.normal(0, 0.4, N), 2),
        "T_Chamber": np.round(25.0 + np.random.normal(0, 0.1, N), 2),
    })

    filepath = OUTPUT_DIR / "096_006_P_019_test.csv"
    # Write metadata line then data with semicolon separator
    with open(filepath, 'w', encoding='iso-8859-1') as f:
        f.write("# SZ Test Data Export; Cell: 096_006_P_019; Date: 2025-03-01\n")
        data.to_csv(f, sep=';', index=False)

    print(f"  Created: {filepath.name} (CSV, semicolon-separated)")


if __name__ == "__main__":
    print("Generating sample data files...")
    generate_mcm_style()
    #generate_got_style()
    #generate_csv_style()
    print(f"\nAll files saved to: {OUTPUT_DIR}")
