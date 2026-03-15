"""Main application window for HarmonizeApp."""

from pathlib import Path

from PySide6.QtCore import Qt, QThread, Signal, QObject
from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QComboBox, QSpinBox, QFileDialog,
    QLineEdit, QStatusBar, QMessageBox, QApplication,
    QSplitter, QGroupBox, QMenu, QTabWidget, QInputDialog,
)

from core.file_loader import (
    get_file_type, get_sheet_names, load_sheet, load_preview,
    detect_header_row, detect_data_start_row, get_file_size_mb, SUPPORTED_EXTENSIONS,
)
from core.auto_mapper import suggest_mapping
from core.harmonizer import harmonize_df, export_harmonized
from core.validation import validate_mapping
from core.config_manager import (
    MappingConfig, save_config, load_config,
    mapping_to_config, config_to_mapping, ColumnMapping,
)
from ui.preview_table import PreviewTableView
from ui.mapping_panel import MappingPanel
from ui.batch_tab import BatchTab
from ui.file_browser_tab import FileBrowserTab

import pandas as pd


class _LoadWorker(QObject):
    """Background worker for loading files without freezing the UI."""
    finished = Signal(pd.DataFrame, int)
    error = Signal(str)

    def __init__(self, filepath, sheet_name, header_row, max_preview_rows):
        super().__init__()
        self.filepath = filepath
        self.sheet_name = sheet_name
        self.header_row = header_row
        self.max_preview_rows = max_preview_rows

    def run(self):
        try:
            preview_df, total_rows = load_preview(
                self.filepath,
                sheet_name=self.sheet_name,
                header_row=self.header_row,
                max_rows=self.max_preview_rows,
            )
            self.finished.emit(preview_df, total_rows)
        except Exception as e:
            self.error.emit(str(e))


class _ExportWorker(QObject):
    """Background worker for exporting harmonized data."""
    finished = Signal(int, str)
    error = Signal(str)

    def __init__(self, mapping, filepath, output_path, sheet_name,
                 header_row, output_format, full_mapping=None,
                 data_start_row=0, formula_mapping=None):
        super().__init__()
        self.mapping = mapping
        self.filepath = filepath
        self.output_path = output_path
        self.sheet_name = sheet_name
        self.header_row = header_row
        self.output_format = output_format
        self.full_mapping = full_mapping
        self.data_start_row = data_start_row
        self.formula_mapping = formula_mapping

    def run(self):
        try:
            row_count = export_harmonized(
                self.mapping, self.filepath, self.output_path,
                sheet_name=self.sheet_name,
                header_row=self.header_row,
                output_format=self.output_format,
                full_mapping=self.full_mapping,
                data_start_row=self.data_start_row,
                formula_mapping=self.formula_mapping,
            )
            self.finished.emit(row_count, str(self.output_path))
        except Exception as e:
            self.error.emit(str(e))


class SingleFileTab(QWidget):
    """Single file processing tab: load, map, preview, export."""

    def __init__(self, status_bar: QStatusBar, parent=None):
        super().__init__(parent)
        self._status_bar = status_bar
        self._current_filepath: Path | None = None
        self._current_df: pd.DataFrame | None = None
        self._total_rows: int = 0
        self._data_start_row: int = 0   # non-data rows detected at top of loaded df
        self._load_thread: QThread | None = None
        self._load_worker: _LoadWorker | None = None
        self._export_thread: QThread | None = None
        self._export_worker: _ExportWorker | None = None
        self.MAX_PREVIEW_ROWS = 1000
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)

        # --- Row 1: File selection ---
        file_row = QHBoxLayout()
        file_row.addWidget(QLabel("File:"))

        self._file_path_edit = QLineEdit()
        self._file_path_edit.setReadOnly(True)
        self._file_path_edit.setPlaceholderText("No file loaded")
        file_row.addWidget(self._file_path_edit, stretch=1)

        self._browse_btn = QPushButton("Browse...")
        self._browse_btn.clicked.connect(self._on_browse)
        file_row.addWidget(self._browse_btn)

        self._load_btn = QPushButton("Load")
        self._load_btn.clicked.connect(self._on_load)
        self._load_btn.setEnabled(False)
        file_row.addWidget(self._load_btn)

        layout.addLayout(file_row)

        # --- Row 2: Sheet + Header row + Config buttons ---
        options_row = QHBoxLayout()

        options_row.addWidget(QLabel("Sheet:"))
        self._sheet_combo = QComboBox()
        self._sheet_combo.setMinimumWidth(180)
        self._sheet_combo.currentTextChanged.connect(self._on_sheet_changed)
        options_row.addWidget(self._sheet_combo)

        options_row.addSpacing(20)
        options_row.addWidget(QLabel("Header row:"))
        self._header_spin = QSpinBox()
        self._header_spin.setMinimum(0)
        self._header_spin.setMaximum(100)
        self._header_spin.setValue(0)
        self._header_spin.setToolTip("0-indexed row number to use as column headers")
        self._header_spin.valueChanged.connect(self._on_header_changed)
        options_row.addWidget(self._header_spin)

        self._auto_detect_btn = QPushButton("Auto-detect")
        self._auto_detect_btn.setToolTip("Auto-detect header row")
        self._auto_detect_btn.clicked.connect(self._on_auto_detect_header)
        self._auto_detect_btn.setEnabled(False)
        options_row.addWidget(self._auto_detect_btn)

        options_row.addSpacing(20)

        # Config save/load buttons
        self._save_config_btn = QPushButton("Save Config")
        self._save_config_btn.setToolTip("Save current mapping to JSON config file")
        self._save_config_btn.clicked.connect(self._on_save_config)
        options_row.addWidget(self._save_config_btn)

        self._load_config_btn = QPushButton("Load Config")
        self._load_config_btn.setToolTip("Load mapping from JSON config file")
        self._load_config_btn.clicked.connect(self._on_load_config)
        options_row.addWidget(self._load_config_btn)

        options_row.addStretch()

        self._size_label = QLabel("")
        options_row.addWidget(self._size_label)

        layout.addLayout(options_row)

        # --- Middle: QSplitter with mapping panel (left) + source preview (right) ---
        splitter = QSplitter(Qt.Orientation.Horizontal)

        self._mapping_panel = MappingPanel()
        self._mapping_panel.mapping_changed.connect(self._on_mapping_changed)
        self._mapping_panel.setMinimumWidth(320)
        splitter.addWidget(self._mapping_panel)

        source_group = QGroupBox("Source Data Preview")
        source_layout = QVBoxLayout(source_group)
        source_layout.setContentsMargins(4, 12, 4, 4)
        self._preview_table = PreviewTableView()
        source_layout.addWidget(self._preview_table)
        splitter.addWidget(source_group)

        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([340, 700])

        layout.addWidget(splitter, stretch=2)

        # --- Bottom: Harmonized preview + buttons ---
        bottom_group = QGroupBox("Harmonized Preview")
        bottom_layout = QVBoxLayout(bottom_group)
        bottom_layout.setContentsMargins(4, 12, 4, 4)

        btn_row = QHBoxLayout()
        btn_row.addStretch()

        self._apply_btn = QPushButton("Apply Mapping")
        self._apply_btn.setToolTip("Apply current mapping to preview data")
        self._apply_btn.clicked.connect(self._on_apply)
        self._apply_btn.setEnabled(False)
        btn_row.addWidget(self._apply_btn)

        self._export_btn = QPushButton("Export...")
        self._export_btn.setToolTip("Export harmonized data to file")
        self._export_btn.setEnabled(False)
        export_menu = QMenu(self._export_btn)
        export_menu.addAction("Export as CSV", lambda: self._on_export('csv'))
        export_menu.addAction("Export as Excel (.xlsx)", lambda: self._on_export('excel'))
        export_menu.addAction("Export as Parquet", lambda: self._on_export('parquet'))
        self._export_btn.setMenu(export_menu)
        btn_row.addWidget(self._export_btn)

        bottom_layout.addLayout(btn_row)

        self._harmonized_table = PreviewTableView()
        bottom_layout.addWidget(self._harmonized_table)

        layout.addWidget(bottom_group, stretch=1)

    # --- File loading ---

    def load_file(self, filepath_str: str):
        """Load a file by path (called externally, e.g. from file browser)."""
        self._file_path_edit.setText(filepath_str)
        self._load_btn.setEnabled(True)
        self._on_load()

    def _on_browse(self):
        ext_filter = "Data files (*.xlsx *.xls *.csv *.tsv);;All files (*)"
        filepath, _ = QFileDialog.getOpenFileName(
            self, "Select Data File", "", ext_filter
        )
        if filepath:
            self.load_file(filepath)

    def _on_load(self):
        filepath_str = self._file_path_edit.text().strip()
        if not filepath_str:
            return

        filepath = Path(filepath_str)
        if not filepath.exists():
            QMessageBox.warning(self, "File not found", f"File does not exist:\n{filepath}")
            return

        if filepath.suffix.lower() not in SUPPORTED_EXTENSIONS:
            QMessageBox.warning(
                self, "Unsupported format",
                f"Unsupported file extension: {filepath.suffix}\n"
                f"Supported: {', '.join(sorted(SUPPORTED_EXTENSIONS))}"
            )
            return

        self._current_filepath = filepath

        size_mb = get_file_size_mb(filepath)
        if size_mb >= 1.0:
            self._size_label.setText(f"{size_mb:.1f} MB")
        else:
            self._size_label.setText(f"{size_mb * 1024:.0f} KB")

        self._sheet_combo.blockSignals(True)
        self._sheet_combo.clear()
        try:
            sheets = get_sheet_names(filepath)
            self._sheet_combo.addItems(sheets)
        except Exception as e:
            QMessageBox.critical(self, "Error reading file", str(e))
            self._sheet_combo.blockSignals(False)
            return
        self._sheet_combo.blockSignals(False)

        self._auto_detect_btn.setEnabled(True)
        self._harmonized_table.set_dataframe(pd.DataFrame())
        self._mapping_panel.clear_mapping()
        self._on_auto_detect_header()

    def _on_auto_detect_header(self):
        if self._current_filepath is None:
            return
        sheet = self._sheet_combo.currentText() or None
        try:
            row = detect_header_row(self._current_filepath, sheet_name=sheet)
            self._header_spin.blockSignals(True)
            self._header_spin.setValue(row)
            self._header_spin.blockSignals(False)
        except Exception as e:
            self._status_bar.showMessage(f"Header detection failed: {e}")
        self._reload_preview()

    def _on_sheet_changed(self, sheet_name: str):
        if self._current_filepath is None:
            return
        self._on_auto_detect_header()

    def _on_header_changed(self, value: int):
        self._reload_preview()

    def _reload_preview(self):
        if self._current_filepath is None:
            return
        self._cancel_load()

        sheet = self._sheet_combo.currentText() or None
        header_row = self._header_spin.value()
        size_mb = get_file_size_mb(self._current_filepath)

        if size_mb < 50:
            self._status_bar.showMessage("Loading...")
            QApplication.processEvents()
            try:
                preview_df, total_rows = load_preview(
                    self._current_filepath,
                    sheet_name=sheet,
                    header_row=header_row,
                    max_rows=self.MAX_PREVIEW_ROWS,
                )
                self._on_preview_loaded(preview_df, total_rows)
            except Exception as e:
                self._status_bar.showMessage(f"Error: {e}")
                QMessageBox.critical(self, "Error loading data", str(e))
        else:
            self._status_bar.showMessage("Loading large file...")
            self._set_ui_loading(True)
            self._load_thread = QThread()
            self._load_worker = _LoadWorker(
                self._current_filepath, sheet, header_row, self.MAX_PREVIEW_ROWS
            )
            self._load_worker.moveToThread(self._load_thread)
            self._load_thread.started.connect(self._load_worker.run)
            self._load_worker.finished.connect(self._on_preview_loaded)
            self._load_worker.finished.connect(self._load_thread.quit)
            self._load_worker.error.connect(self._on_load_error)
            self._load_worker.error.connect(self._load_thread.quit)
            self._load_thread.start()

    def _on_preview_loaded(self, preview_df: pd.DataFrame, total_rows: int):
        # --- Auto-detect and drop non-data leading rows ---
        data_start = detect_data_start_row(preview_df)
        if data_start > 0:
            preview_df = preview_df.iloc[data_start:].reset_index(drop=True)
            total_rows = max(total_rows - data_start, 0)
        self._data_start_row = data_start

        self._current_df = preview_df
        self._total_rows = total_rows
        self._preview_table.set_dataframe(preview_df)
        self._set_ui_loading(False)

        # Update mapping panel source columns (preserves existing selections)
        self._mapping_panel.set_source_columns(list(preview_df.columns))
        self._mapping_panel.set_preview_df(preview_df)

        # --- Auto-suggest column mapping for unmapped rows ---
        suggestions = suggest_mapping(list(preview_df.columns))
        self._mapping_panel.apply_suggestions(suggestions)
        n_suggested = sum(1 for v in suggestions.values() if v is not None)

        self._apply_btn.setEnabled(True)
        self._update_status_bar()

        # Show what was done automatically
        msgs = []
        if data_start > 0:
            msgs.append(f"Skipped {data_start} non-data row{'s' if data_start > 1 else ''}")
        if n_suggested > 0:
            msgs.append(f"Auto-mapped {n_suggested} column{'s' if n_suggested > 1 else ''}")
        if msgs:
            self._status_bar.showMessage(" | ".join(msgs))

    def _on_load_error(self, error_msg: str):
        self._set_ui_loading(False)
        self._status_bar.showMessage(f"Error: {error_msg}")
        QMessageBox.critical(self, "Error loading data", error_msg)

    def _set_ui_loading(self, loading: bool):
        self._browse_btn.setEnabled(not loading)
        self._load_btn.setEnabled(not loading)
        self._sheet_combo.setEnabled(not loading)
        self._header_spin.setEnabled(not loading)
        self._auto_detect_btn.setEnabled(not loading)
        self._apply_btn.setEnabled(not loading)
        self._export_btn.setEnabled(not loading)

    def _cancel_load(self):
        if self._load_thread is not None and self._load_thread.isRunning():
            self._load_thread.quit()
            self._load_thread.wait(2000)
            self._load_thread = None
            self._load_worker = None

    # --- Mapping & Harmonization ---

    def _on_mapping_changed(self):
        self._update_status_bar()

    def _on_apply(self):
        if self._current_df is None:
            return

        mapping = self._mapping_panel.get_mapping()
        full_mapping = self._mapping_panel.get_full_mapping()
        formula_mapping = self._mapping_panel.get_formula_mapping()
        result = validate_mapping(mapping)

        if result.mapped_count == 0 and not formula_mapping:
            QMessageBox.information(
                self, "No columns mapped",
                "Please map at least one source column to a target column."
            )
            return

        harmonized = harmonize_df(mapping, self._current_df,
                                  full_mapping=full_mapping,
                                  formula_mapping=formula_mapping)
        self._harmonized_table.set_dataframe(harmonized)
        self._export_btn.setEnabled(True)
        self._status_bar.showMessage(
            f"Applied mapping: {len(harmonized)} rows x {len(harmonized.columns)} columns | "
            + result.summary()
        )

    def _on_export(self, output_format: str):
        if self._current_filepath is None:
            return

        mapping = self._mapping_panel.get_mapping()
        full_mapping = self._mapping_panel.get_full_mapping()
        formula_mapping = self._mapping_panel.get_formula_mapping()
        result = validate_mapping(mapping)

        if result.mapped_count == 0 and not formula_mapping:
            QMessageBox.information(
                self, "No columns mapped",
                "Please map at least one source column to a target column."
            )
            return

        if not result.is_valid:
            reply = QMessageBox.warning(
                self, "Missing mandatory columns",
                f"Not all mandatory columns are mapped.\n\n"
                f"{result.summary()}\n\n"
                f"Export anyway?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if reply != QMessageBox.StandardButton.Yes:
                return

        ext_map = {'csv': '.csv', 'excel': '.xlsx', 'parquet': '.parquet'}
        filter_map = {
            'csv': 'CSV files (*.csv)',
            'excel': 'Excel files (*.xlsx)',
            'parquet': 'Parquet files (*.parquet)',
        }
        default_name = self._current_filepath.stem + "_harmonized" + ext_map[output_format]
        default_dir = str(self._current_filepath.parent / default_name)

        output_path, _ = QFileDialog.getSaveFileName(
            self, "Export Harmonized Data", default_dir,
            filter_map[output_format]
        )
        if not output_path:
            return

        output_path = Path(output_path)
        sheet = self._sheet_combo.currentText() or None
        header_row = self._header_spin.value()

        self._status_bar.showMessage(f"Exporting to {output_path.name}...")
        self._set_ui_loading(True)

        self._export_thread = QThread()
        self._export_worker = _ExportWorker(
            mapping, self._current_filepath, output_path,
            sheet, header_row, output_format, full_mapping,
            data_start_row=self._data_start_row,
            formula_mapping=formula_mapping,
        )
        self._export_worker.moveToThread(self._export_thread)
        self._export_thread.started.connect(self._export_worker.run)
        self._export_worker.finished.connect(self._on_export_done)
        self._export_worker.finished.connect(self._export_thread.quit)
        self._export_worker.error.connect(self._on_export_error)
        self._export_worker.error.connect(self._export_thread.quit)
        self._export_thread.start()

    def _on_export_done(self, row_count: int, output_path: str):
        self._set_ui_loading(False)
        self._status_bar.showMessage(f"Exported {row_count:,} rows to {Path(output_path).name}")
        QMessageBox.information(
            self, "Export complete",
            f"Successfully exported {row_count:,} rows to:\n{output_path}"
        )

    def _on_export_error(self, error_msg: str):
        self._set_ui_loading(False)
        self._status_bar.showMessage(f"Export failed: {error_msg}")
        QMessageBox.critical(self, "Export failed", error_msg)

    # --- Config Save/Load ---

    def _on_save_config(self):
        mapping = self._mapping_panel.get_mapping()
        full_mapping = self._mapping_panel.get_full_mapping()
        formula_mapping = self._mapping_panel.get_formula_mapping()

        # Ask for config name
        name, ok = QInputDialog.getText(
            self, "Save Config", "Config name:",
            text=self._current_filepath.stem if self._current_filepath else "MyConfig"
        )
        if not ok or not name:
            return

        # Build config with full fallback info
        config = mapping_to_config(
            mapping, name=name,
            header_row=self._header_spin.value(),
            sheet_pattern=self._sheet_combo.currentText() or "",
        )
        # Upgrade to full mapping with fallbacks
        for target, sources in full_mapping.items():
            if len(sources) > 1:
                config.column_mappings[target] = ColumnMapping(
                    mapping_type="or_fallback",
                    source_columns=sources,
                )
            elif len(sources) == 1:
                config.column_mappings[target] = ColumnMapping(
                    mapping_type="direct",
                    source_columns=sources,
                )
        # Add formula mappings
        for target, fm in formula_mapping.items():
            config.column_mappings[target] = ColumnMapping(
                mapping_type="formula",
                formula_expression=fm.get('expression', ''),
                formula_level=fm.get('level', 1),
            )

        filepath, _ = QFileDialog.getSaveFileName(
            self, "Save Config", f"{name}.json",
            "JSON files (*.json);;All files (*)"
        )
        if not filepath:
            return

        try:
            save_config(config, Path(filepath))
            self._status_bar.showMessage(f"Config saved to {Path(filepath).name}")
        except Exception as e:
            QMessageBox.critical(self, "Save failed", str(e))

    def _on_load_config(self):
        filepath, _ = QFileDialog.getOpenFileName(
            self, "Load Config", "",
            "JSON files (*.json);;All files (*)"
        )
        if not filepath:
            return

        try:
            config = load_config(Path(filepath))
        except Exception as e:
            QMessageBox.critical(self, "Load failed", str(e))
            return

        # Apply file settings
        self._header_spin.blockSignals(True)
        self._header_spin.setValue(config.file_settings.header_row)
        self._header_spin.blockSignals(False)

        # Build full mapping from config
        full_mapping = {}
        formula_mapping = {}
        for target, cm in config.column_mappings.items():
            if cm.mapping_type == "formula":
                if cm.formula_expression:
                    formula_mapping[target] = {
                        'expression': cm.formula_expression,
                        'level': cm.formula_level,
                    }
            else:
                full_mapping[target] = cm.source_columns if cm.source_columns else []

        self._mapping_panel.set_full_mapping(full_mapping)
        self._mapping_panel.set_formula_mapping(formula_mapping)
        self._status_bar.showMessage(f"Config loaded: {config.name}")

    def get_current_config(self) -> MappingConfig | None:
        """Build a MappingConfig from current state (for sending to batch tab)."""
        mapping = self._mapping_panel.get_mapping()
        full_mapping = self._mapping_panel.get_full_mapping()
        formula_mapping = self._mapping_panel.get_formula_mapping()

        config = mapping_to_config(
            mapping,
            name=self._current_filepath.stem if self._current_filepath else "Untitled",
            header_row=self._header_spin.value(),
            sheet_pattern=self._sheet_combo.currentText() or "",
        )
        for target, sources in full_mapping.items():
            if len(sources) > 1:
                config.column_mappings[target] = ColumnMapping(
                    mapping_type="or_fallback",
                    source_columns=sources,
                )
            elif len(sources) == 1:
                config.column_mappings[target] = ColumnMapping(
                    mapping_type="direct",
                    source_columns=sources,
                )
        for target, fm in formula_mapping.items():
            config.column_mappings[target] = ColumnMapping(
                mapping_type="formula",
                formula_expression=fm.get('expression', ''),
                formula_level=fm.get('level', 1),
            )
        return config

    def _update_status_bar(self):
        parts = []
        if self._current_filepath:
            parts.append(self._current_filepath.name)
            sheet = self._sheet_combo.currentText()
            if sheet:
                parts.append(f"Sheet: {sheet}")
            if self._total_rows:
                parts.append(f"{self._total_rows:,} rows")
                if self._current_df is not None and len(self._current_df) < self._total_rows:
                    parts.append(f"(previewing {len(self._current_df):,})")

        mapping = self._mapping_panel.get_mapping()
        result = validate_mapping(mapping)
        if result.mapped_count > 0:
            parts.append(result.summary())

        self._status_bar.showMessage(" | ".join(parts) if parts else "Ready")

    @property
    def current_dataframe(self) -> pd.DataFrame | None:
        return self._current_df

    @property
    def current_filepath(self) -> Path | None:
        return self._current_filepath

    @property
    def total_rows(self) -> int:
        return self._total_rows


class MainWindow(QMainWindow):
    """Main window with three tabs: File Browser, Single File, Batch."""

    def __init__(self):
        super().__init__()
        self.setWindowTitle("HarmonizeApp - Battery Test Data Harmonization")
        self.setMinimumSize(1000, 700)
        self.resize(1200, 800)
        self._build_ui()

    def _build_ui(self):
        self._status_bar = QStatusBar()
        self.setStatusBar(self._status_bar)
        self._status_bar.showMessage("Ready")

        self._tabs = QTabWidget()
        self.setCentralWidget(self._tabs)

        # Tab 1: File Browser
        self._browser_tab = FileBrowserTab()
        self._tabs.addTab(self._browser_tab, "File Browser")

        # Tab 2: Single File
        self._single_tab = SingleFileTab(self._status_bar)
        self._tabs.addTab(self._single_tab, "Single File")

        # Tab 3: Batch
        self._batch_tab = BatchTab()
        self._tabs.addTab(self._batch_tab, "Batch")

        # Connect file browser signals
        self._browser_tab.open_in_single.connect(self._open_file_in_single)
        self._browser_tab.set_batch_input.connect(self._set_batch_folder)

    def _open_file_in_single(self, filepath: str):
        self._tabs.setCurrentWidget(self._single_tab)
        self._single_tab.load_file(filepath)

    def _set_batch_folder(self, folder: str):
        self._tabs.setCurrentWidget(self._batch_tab)
        self._batch_tab._input_edit.setText(folder)
        out = str(Path(folder) / "harmonized")
        self._batch_tab._output_edit.setText(out)
        # If single file tab has a config, send it to batch
        config = self._single_tab.get_current_config()
        if config:
            self._batch_tab.set_config(config, display_name="From Single File tab")
