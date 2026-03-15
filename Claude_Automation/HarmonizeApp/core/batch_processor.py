"""Batch processing engine: processes multiple files with a saved config."""

from pathlib import Path
from dataclasses import dataclass, field
import logging

from core.file_loader import (
    get_file_type, get_sheet_names, load_sheet, detect_header_row,
    detect_data_start_row, SUPPORTED_EXTENSIONS,
)
from core.harmonizer import harmonize_df, _write_output
from core.config_manager import MappingConfig, config_to_mapping

logger = logging.getLogger(__name__)


@dataclass
class BatchFileResult:
    """Result for a single file in a batch run."""
    filepath: Path
    status: str = "pending"  # pending, processing, success, failed, skipped
    output_path: Path | None = None
    row_count: int = 0
    error: str = ""


@dataclass
class BatchResult:
    """Overall result of a batch run."""
    files: list[BatchFileResult] = field(default_factory=list)

    @property
    def success_count(self) -> int:
        return sum(1 for f in self.files if f.status == "success")

    @property
    def failed_count(self) -> int:
        return sum(1 for f in self.files if f.status == "failed")

    @property
    def skipped_count(self) -> int:
        return sum(1 for f in self.files if f.status == "skipped")

    @property
    def total_rows(self) -> int:
        return sum(f.row_count for f in self.files)


def discover_files(input_folder: Path, file_filter: str = "") -> list[Path]:
    """Find all supported data files in a folder.

    Args:
        input_folder: Folder to scan.
        file_filter: Comma-separated glob patterns (e.g. "*.xlsx, *.csv").
                     Empty string means all supported extensions.

    Returns:
        Sorted list of matching file paths.
    """
    if file_filter.strip():
        patterns = [p.strip() for p in file_filter.split(",")]
    else:
        patterns = [f"*{ext}" for ext in SUPPORTED_EXTENSIONS]

    files = []
    for pattern in patterns:
        files.extend(input_folder.glob(pattern))

    # Deduplicate and filter to supported extensions
    seen = set()
    result = []
    for f in sorted(files):
        if f.suffix.lower() in SUPPORTED_EXTENSIONS and f not in seen:
            seen.add(f)
            result.append(f)
    return result


def process_single_file(filepath: Path,
                        config: MappingConfig,
                        output_folder: Path,
                        output_format: str = "csv",
                        skip_existing: bool = False) -> BatchFileResult:
    """Process a single file using a mapping config.

    Args:
        filepath: Input file path.
        config: Mapping configuration.
        output_folder: Where to write output.
        output_format: csv, excel, or parquet.
        skip_existing: Skip if output file already exists.

    Returns:
        BatchFileResult with status and details.
    """
    ext_map = {"csv": ".csv", "excel": ".xlsx", "parquet": ".parquet"}
    out_ext = ext_map.get(output_format, ".csv")
    out_name = filepath.stem + "_harmonized" + out_ext
    output_path = output_folder / out_name

    result = BatchFileResult(filepath=filepath, output_path=output_path)

    if skip_existing and output_path.exists():
        result.status = "skipped"
        return result

    result.status = "processing"

    try:
        # Determine sheet name
        file_type = get_file_type(filepath)
        sheet_name = None
        if file_type == "excel":
            sheets = get_sheet_names(filepath)
            sheet_pattern = config.file_settings.sheet_pattern
            if sheet_pattern:
                # Find first sheet matching pattern
                import fnmatch
                for s in sheets:
                    if fnmatch.fnmatch(s, sheet_pattern):
                        sheet_name = s
                        break
                if sheet_name is None:
                    sheet_name = sheets[0] if sheets else None
            else:
                sheet_name = sheets[0] if sheets else None

        # Determine header row
        header_row = config.file_settings.header_row
        if header_row == 0:
            # Auto-detect
            header_row = detect_header_row(filepath, sheet_name=sheet_name)

        # Load data
        source_df = load_sheet(filepath, sheet_name=sheet_name, header_row=header_row)

        # Drop non-data leading rows (metadata rows after the header)
        data_start = detect_data_start_row(source_df)
        if data_start > 0:
            logger.info(f"Skipping {data_start} non-data row(s) in {filepath.name}")
            source_df = source_df.iloc[data_start:].reset_index(drop=True)

        # Build mapping - use full mapping with fallbacks, plus any formulas
        full_mapping = {}
        simple_mapping = {}
        formula_mapping = {}
        for target, cm in config.column_mappings.items():
            if cm.mapping_type == "formula" and cm.formula_expression:
                formula_mapping[target] = {
                    'expression': cm.formula_expression,
                    'level': cm.formula_level,
                }
                full_mapping[target] = []
                simple_mapping[target] = None
            elif cm.mapping_type != "unmapped" and cm.source_columns:
                full_mapping[target] = cm.source_columns
                simple_mapping[target] = cm.source_columns[0]
            else:
                full_mapping[target] = []
                simple_mapping[target] = None

        # Harmonize
        harmonized = harmonize_df(simple_mapping, source_df,
                                  full_mapping=full_mapping,
                                  formula_mapping=formula_mapping or None)

        if len(harmonized.columns) == 0:
            result.status = "failed"
            result.error = "No columns could be mapped"
            return result

        # Write output
        _write_output(harmonized, output_path, output_format)
        result.status = "success"
        result.row_count = len(harmonized)

    except Exception as e:
        result.status = "failed"
        result.error = str(e)
        logger.error(f"Failed to process {filepath.name}: {e}")

    return result
