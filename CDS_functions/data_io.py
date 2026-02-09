import pandas as pd
from pathlib import Path


def read_harm_cell_data(harm_path, cellid, suffixes=None):
    cell_path = harm_path / cellid
    cell_df = pd.DataFrame([])
    cell_files_paths = []

    if cell_path.exists():
        if suffixes:
            for suffix in suffixes:
                temp = list(cell_path.rglob(fr"*{cellid}*{suffix}*.csv"))
                cell_files_paths.extend(temp)
        else:
            cell_files_paths = list(cell_path.rglob(fr"*{cellid}*.csv"))
        for file in cell_files_paths:
            temp = pd.read_csv(file)
            temp['file_name'] = file.name
            cell_df = pd.concat([cell_df, temp], axis=0, ignore_index=True)
        cell_df = cell_df.reset_index()
        cell_df = cell_df.sort_values(by=['Unix_time', 'index'], ascending=[True, True])
        cell_df = cell_df.drop(columns=['index'])

        cell_df['Unix_datetime'] = pd.to_datetime(cell_df['Unix_time'], unit='s')
        cell_df['Unix_total_time'] = (cell_df['Unix_datetime'] - cell_df['Unix_datetime'].min()).dt.total_seconds()
        cell_df = cell_df.drop_duplicates(subset=['Unix_time', 'Current_A'], keep='last', inplace=False, ignore_index=True)
        cell_df = cell_df.reset_index(inplace=False).drop(columns=['index'])
    else:
        print("Cell does not exist")
    return cell_df


def export_to_excel(data_dict, output_path):
    """Export dictionary of DataFrames to multi-sheet Excel file.

    Parameters:
    - data_dict: dict of {sheet_name: DataFrame}
    - output_path: Path or str to output .xlsx file
    """
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, df in data_dict.items():
            df.to_excel(writer, sheet_name=sheet_name, index=True)
