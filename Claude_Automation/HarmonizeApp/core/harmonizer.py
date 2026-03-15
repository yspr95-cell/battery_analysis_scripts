"""Harmonization engine: applies column mapping to source data."""

from pathlib import Path
import pandas as pd
import logging

from core.schema import FOCUS_COLS_ETL
from core.file_loader import load_sheet, detect_data_start_row
from core.formula_engine import evaluate_formula, get_col_map

logger = logging.getLogger(__name__)


def harmonize_df(mapping: dict[str, str | None], source_df: pd.DataFrame,
                 full_mapping: dict[str, list[str]] | None = None,
                 formula_mapping: dict[str, dict] | None = None) -> pd.DataFrame:
    """Apply column mapping to a source DataFrame.

    Supports direct mapping, OR-fallback, and formula expressions.

    Args:
        mapping: {target_col: source_col} simple mapping (used if full_mapping is None).
        source_df: The source DataFrame with original column names.
        full_mapping: {target_col: [source1, source2, ...]} with fallback order.
        formula_mapping: {target_col: {'expression': str, 'level': int (1 or 2)}}
            Level 1 formulas reference source columns; level 2 reference target
            columns already computed in this pass.

    Returns:
        DataFrame with only the mapped target columns, in FOCUS_COLS_ETL order.
    """
    result = pd.DataFrame(index=source_df.index)
    src_col_map = get_col_map(list(source_df.columns))

    # --- Phase 1: direct / or_fallback / level-1 formulas ---
    for target_col in FOCUS_COLS_ETL:
        # Formula (level 1) – operates on source columns
        if formula_mapping and target_col in formula_mapping:
            fm = formula_mapping[target_col]
            if fm.get('level', 1) == 1:
                try:
                    result[target_col] = evaluate_formula(
                        fm['expression'], source_df, src_col_map
                    )
                except Exception as e:
                    logger.warning(f"Formula failed for {target_col}: {e}")
                continue

        # Direct / or_fallback
        if full_mapping is not None:
            sources = full_mapping.get(target_col, [])
            for src in sources:
                if src in source_df.columns:
                    result[target_col] = source_df[src].values
                    break
        else:
            source_col = mapping.get(target_col)
            if source_col and source_col != "(unmapped)" and source_col in source_df.columns:
                result[target_col] = source_df[source_col].values

    # --- Phase 2: level-2 formulas – operate on the (partial) result ---
    if formula_mapping:
        res_col_map = get_col_map(list(result.columns))
        for target_col in FOCUS_COLS_ETL:
            if target_col in formula_mapping:
                fm = formula_mapping[target_col]
                if fm.get('level', 1) == 2:
                    try:
                        result[target_col] = evaluate_formula(
                            fm['expression'], result, res_col_map
                        )
                    except Exception as e:
                        logger.warning(f"Level-2 formula failed for {target_col}: {e}")

    return result


def export_harmonized(mapping: dict[str, str | None],
                      filepath: Path,
                      output_path: Path,
                      sheet_name: str | None = None,
                      header_row: int = 0,
                      output_format: str = 'csv',
                      full_mapping: dict[str, list[str]] | None = None,
                      data_start_row: int = -1,
                      formula_mapping: dict[str, dict] | None = None) -> int:
    """Load full file, apply mapping, export to disk.

    Args:
        mapping: Column mapping dict.
        filepath: Source file path.
        output_path: Where to save the output.
        sheet_name: Sheet name for Excel files.
        header_row: Header row index.
        output_format: One of 'csv', 'excel', 'parquet'.
        full_mapping: Optional full mapping with fallbacks.
        data_start_row: Number of non-data leading rows to skip after header.
                        -1 (default) = auto-detect. 0 = no skip.

    Returns:
        Number of rows exported.
    """
    logger.info(f"Loading full data from {filepath.name}...")
    full_df = load_sheet(filepath, sheet_name=sheet_name, header_row=header_row)

    # Drop non-data leading rows (e.g. metadata rows after the header)
    if data_start_row == -1:
        data_start_row = detect_data_start_row(full_df)
    if data_start_row > 0:
        logger.info(f"Skipping {data_start_row} non-data row(s) at start of data")
        full_df = full_df.iloc[data_start_row:].reset_index(drop=True)

    logger.info(f"Harmonizing {len(full_df)} rows...")
    harmonized = harmonize_df(mapping, full_df, full_mapping=full_mapping,
                              formula_mapping=formula_mapping)

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
