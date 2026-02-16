"""Harmonization engine: applies column mapping to source data."""

from pathlib import Path
import pandas as pd
import logging

from core.schema import FOCUS_COLS_ETL
from core.file_loader import load_sheet

logger = logging.getLogger(__name__)


def harmonize_df(mapping: dict[str, str | None], source_df: pd.DataFrame,
                 full_mapping: dict[str, list[str]] | None = None) -> pd.DataFrame:
    """Apply column mapping to a source DataFrame.

    Supports direct mapping and OR-fallback. When full_mapping is provided,
    it tries each source column in order until one is found in the DataFrame.

    Args:
        mapping: {target_col: source_col} simple mapping (used if full_mapping is None).
        source_df: The source DataFrame with original column names.
        full_mapping: {target_col: [source1, source2, ...]} with fallback order.

    Returns:
        DataFrame with only the mapped target columns, in FOCUS_COLS_ETL order.
    """
    result = pd.DataFrame(index=source_df.index)

    for target_col in FOCUS_COLS_ETL:
        if full_mapping is not None:
            sources = full_mapping.get(target_col, [])
            for src in sources:
                if src in source_df.columns:
                    result[target_col] = source_df[src]
                    break
        else:
            source_col = mapping.get(target_col)
            if source_col and source_col != "(unmapped)" and source_col in source_df.columns:
                result[target_col] = source_df[source_col]

    return result


def export_harmonized(mapping: dict[str, str | None],
                      filepath: Path,
                      output_path: Path,
                      sheet_name: str | None = None,
                      header_row: int = 0,
                      output_format: str = 'csv',
                      full_mapping: dict[str, list[str]] | None = None) -> int:
    """Load full file, apply mapping, export to disk.

    Args:
        mapping: Column mapping dict.
        filepath: Source file path.
        output_path: Where to save the output.
        sheet_name: Sheet name for Excel files.
        header_row: Header row index.
        output_format: One of 'csv', 'excel', 'parquet'.
        full_mapping: Optional full mapping with fallbacks.

    Returns:
        Number of rows exported.
    """
    logger.info(f"Loading full data from {filepath.name}...")
    full_df = load_sheet(filepath, sheet_name=sheet_name, header_row=header_row)

    logger.info(f"Harmonizing {len(full_df)} rows...")
    harmonized = harmonize_df(mapping, full_df, full_mapping=full_mapping)

    logger.info(f"Exporting to {output_path.name} ({output_format})...")
    _write_output(harmonized, output_path, output_format)

    logger.info(f"Exported {len(harmonized)} rows to {output_path}")
    return len(harmonized)


def _write_output(df: pd.DataFrame, output_path: Path, output_format: str):
    """Write DataFrame to file in the specified format."""
    if output_format == 'csv':
        df.to_csv(output_path, index=False)
    elif output_format == 'excel':
        df.to_excel(output_path, index=False, engine='openpyxl')
    elif output_format == 'parquet':
        df.to_parquet(output_path, index=False)
    else:
        raise ValueError(f"Unsupported format: {output_format}")
