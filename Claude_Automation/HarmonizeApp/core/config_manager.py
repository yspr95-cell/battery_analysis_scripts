"""Configuration save/load for column mapping."""

import json
from pathlib import Path
from dataclasses import dataclass, field, asdict
from datetime import datetime


CONFIG_SCHEMA_VERSION = "harmonize_config_v1"


@dataclass
class FileSettings:
    """File-level settings stored in a config."""
    header_row: int = 0
    sheet_pattern: str = ""
    encoding: str = "utf-8"


@dataclass
class ColumnMapping:
    """Mapping for a single target column."""
    mapping_type: str = "direct"  # "direct", "or_fallback", "unmapped"
    source_columns: list[str] = field(default_factory=list)


@dataclass
class MappingConfig:
    """Full mapping configuration, serializable to JSON."""
    name: str = "Untitled"
    created: str = ""
    file_settings: FileSettings = field(default_factory=FileSettings)
    column_mappings: dict[str, ColumnMapping] = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = {
            "$schema": CONFIG_SCHEMA_VERSION,
            "name": self.name,
            "created": self.created or datetime.now().isoformat(),
            "file_settings": asdict(self.file_settings),
            "column_mappings": {},
        }
        for target, cm in self.column_mappings.items():
            d["column_mappings"][target] = {
                "mapping_type": cm.mapping_type,
                "source_columns": cm.source_columns,
            }
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "MappingConfig":
        cfg = cls()
        cfg.name = d.get("name", "Untitled")
        cfg.created = d.get("created", "")

        fs = d.get("file_settings", {})
        cfg.file_settings = FileSettings(
            header_row=fs.get("header_row", 0),
            sheet_pattern=fs.get("sheet_pattern", ""),
            encoding=fs.get("encoding", "utf-8"),
        )

        cfg.column_mappings = {}
        for target, cm_data in d.get("column_mappings", {}).items():
            cfg.column_mappings[target] = ColumnMapping(
                mapping_type=cm_data.get("mapping_type", "direct"),
                source_columns=cm_data.get("source_columns", []),
            )
        return cfg


def save_config(config: MappingConfig, filepath: Path) -> None:
    """Save config to JSON file."""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(config.to_dict(), f, indent=2, ensure_ascii=False)


def load_config(filepath: Path) -> MappingConfig:
    """Load config from JSON file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        d = json.load(f)
    schema = d.get("$schema", "")
    if schema and schema != CONFIG_SCHEMA_VERSION:
        raise ValueError(f"Unsupported config version: {schema}")
    return MappingConfig.from_dict(d)


def mapping_to_config(mapping: dict[str, str | None],
                      name: str = "Untitled",
                      header_row: int = 0,
                      sheet_pattern: str = "") -> MappingConfig:
    """Convert a simple mapping dict to a MappingConfig."""
    cfg = MappingConfig(
        name=name,
        created=datetime.now().isoformat(),
        file_settings=FileSettings(header_row=header_row, sheet_pattern=sheet_pattern),
    )
    for target, source in mapping.items():
        if source and source != "(unmapped)":
            cfg.column_mappings[target] = ColumnMapping(
                mapping_type="direct",
                source_columns=[source],
            )
        else:
            cfg.column_mappings[target] = ColumnMapping(
                mapping_type="unmapped",
                source_columns=[],
            )
    return cfg


def config_to_mapping(config: MappingConfig) -> dict[str, str | None]:
    """Convert a MappingConfig to a simple mapping dict.

    For or_fallback mappings, returns only the first source column
    (the UI will handle the full fallback list).
    """
    mapping = {}
    for target, cm in config.column_mappings.items():
        if cm.mapping_type == "unmapped" or not cm.source_columns:
            mapping[target] = None
        else:
            mapping[target] = cm.source_columns[0]
    return mapping
