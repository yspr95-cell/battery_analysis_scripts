"""Validation for column mapping completeness."""

from dataclasses import dataclass, field
from core.schema import FOCUS_COLS_ETL, MANDATORY_COLS_ETL


@dataclass
class ValidationResult:
    """Result of validating a column mapping."""
    column_status: dict[str, str] = field(default_factory=dict)
    # status values: 'mapped', 'unmapped', 'missing_mandatory'

    @property
    def is_valid(self) -> bool:
        """True if all mandatory columns are mapped."""
        return all(
            self.column_status.get(col) == 'mapped'
            for col in MANDATORY_COLS_ETL
        )

    @property
    def mapped_count(self) -> int:
        return sum(1 for s in self.column_status.values() if s == 'mapped')

    @property
    def mandatory_mapped_count(self) -> int:
        return sum(
            1 for col in MANDATORY_COLS_ETL
            if self.column_status.get(col) == 'mapped'
        )

    @property
    def total_count(self) -> int:
        return len(FOCUS_COLS_ETL)

    @property
    def mandatory_total(self) -> int:
        return len(MANDATORY_COLS_ETL)

    def summary(self) -> str:
        parts = [f"Mapped {self.mapped_count}/{self.total_count}"]
        if self.is_valid:
            parts.append(f"Mandatory {self.mandatory_mapped_count}/{self.mandatory_total} ✓")
        else:
            parts.append(f"Mandatory {self.mandatory_mapped_count}/{self.mandatory_total} ✗")
        return " | ".join(parts)


def validate_mapping(mapping: dict[str, str | None]) -> ValidationResult:
    """Validate a column mapping dict.

    Args:
        mapping: {target_col: source_col_or_None} for each target in FOCUS_COLS_ETL.

    Returns:
        ValidationResult with per-column status.
    """
    result = ValidationResult()
    for col in FOCUS_COLS_ETL:
        source = mapping.get(col)
        if source and source != "(unmapped)":
            result.column_status[col] = 'mapped'
        elif col in MANDATORY_COLS_ETL:
            result.column_status[col] = 'missing_mandatory'
        else:
            result.column_status[col] = 'unmapped'
    return result
