"""Batch processing tab: process multiple files with a saved config."""

from pathlib import Path

from PySide6.QtCore import Qt, QThread, Signal, QObject
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QLineEdit, QFileDialog, QComboBox, QCheckBox,
    QGroupBox, QProgressBar, QMessageBox,
    QTableWidget, QTableWidgetItem, QHeaderView, QAbstractItemView,
)

from core.config_manager import load_config, MappingConfig
from core.batch_processor import discover_files, process_single_file, BatchFileResult


class _BatchWorker(QObject):
    """Background worker for batch processing."""
    file_started = Signal(int, str)   # (index, filename)
    file_done = Signal(int, object)   # (index, BatchFileResult)
    all_done = Signal()
    error = Signal(str)

    def __init__(self, files, config, output_folder, output_format, skip_existing):
        super().__init__()
        self.files = files
        self.config = config
        self.output_folder = output_folder
        self.output_format = output_format
        self.skip_existing = skip_existing
        self._cancelled = False

    def cancel(self):
        self._cancelled = True

    def run(self):
        try:
            for i, filepath in enumerate(self.files):
                if self._cancelled:
                    break
                self.file_started.emit(i, filepath.name)
                result = process_single_file(
                    filepath, self.config, self.output_folder,
                    self.output_format, self.skip_existing,
                )
                self.file_done.emit(i, result)
        except Exception as e:
            self.error.emit(str(e))
        finally:
            self.all_done.emit()


class BatchTab(QWidget):
    """Batch processing UI tab."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._config: MappingConfig | None = None
        self._files: list[Path] = []
        self._batch_thread: QThread | None = None
        self._batch_worker: _BatchWorker | None = None
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)

        # --- Settings group ---
        settings = QGroupBox("Batch Settings")
        settings_layout = QVBoxLayout(settings)

        # Input folder
        row1 = QHBoxLayout()
        row1.addWidget(QLabel("Input Folder:"))
        self._input_edit = QLineEdit()
        self._input_edit.setPlaceholderText("Select folder containing data files...")
        row1.addWidget(self._input_edit, stretch=1)
        browse_in = QPushButton("Browse...")
        browse_in.clicked.connect(self._browse_input)
        row1.addWidget(browse_in)
        settings_layout.addLayout(row1)

        # Output folder
        row2 = QHBoxLayout()
        row2.addWidget(QLabel("Output Folder:"))
        self._output_edit = QLineEdit()
        self._output_edit.setPlaceholderText("Select output folder...")
        row2.addWidget(self._output_edit, stretch=1)
        browse_out = QPushButton("Browse...")
        browse_out.clicked.connect(self._browse_output)
        row2.addWidget(browse_out)
        settings_layout.addLayout(row2)

        # Config file
        row3 = QHBoxLayout()
        row3.addWidget(QLabel("Config:"))
        self._config_edit = QLineEdit()
        self._config_edit.setPlaceholderText("Select mapping config JSON...")
        self._config_edit.setReadOnly(True)
        row3.addWidget(self._config_edit, stretch=1)
        browse_cfg = QPushButton("Browse...")
        browse_cfg.clicked.connect(self._browse_config)
        row3.addWidget(browse_cfg)
        settings_layout.addLayout(row3)

        # Options row
        row4 = QHBoxLayout()
        row4.addWidget(QLabel("File Filter:"))
        self._filter_edit = QLineEdit()
        self._filter_edit.setPlaceholderText("*.xlsx, *.csv (empty = all supported)")
        self._filter_edit.setMaximumWidth(250)
        row4.addWidget(self._filter_edit)

        row4.addSpacing(20)
        row4.addWidget(QLabel("Export as:"))
        self._format_combo = QComboBox()
        self._format_combo.addItems(["CSV", "Excel (.xlsx)", "Parquet"])
        row4.addWidget(self._format_combo)

        row4.addSpacing(20)
        self._skip_check = QCheckBox("Skip already processed")
        self._skip_check.setChecked(True)
        row4.addWidget(self._skip_check)

        row4.addStretch()
        settings_layout.addLayout(row4)

        layout.addWidget(settings)

        # --- Scan + action buttons ---
        btn_row = QHBoxLayout()
        self._scan_btn = QPushButton("Scan Files")
        self._scan_btn.clicked.connect(self._on_scan)
        btn_row.addWidget(self._scan_btn)

        btn_row.addStretch()

        self._start_btn = QPushButton("Start Batch")
        self._start_btn.setEnabled(False)
        self._start_btn.clicked.connect(self._on_start)
        btn_row.addWidget(self._start_btn)

        self._cancel_btn = QPushButton("Cancel")
        self._cancel_btn.setEnabled(False)
        self._cancel_btn.clicked.connect(self._on_cancel)
        btn_row.addWidget(self._cancel_btn)

        layout.addLayout(btn_row)

        # --- Progress ---
        self._progress = QProgressBar()
        self._progress.setVisible(False)
        layout.addWidget(self._progress)

        self._progress_label = QLabel("")
        layout.addWidget(self._progress_label)

        # --- File table ---
        self._table = QTableWidget()
        self._table.setColumnCount(4)
        self._table.setHorizontalHeaderLabels(["File", "Status", "Rows", "Details"])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Fixed)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Fixed)
        self._table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        self._table.setColumnWidth(1, 100)
        self._table.setColumnWidth(2, 80)
        self._table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        layout.addWidget(self._table, stretch=1)

    def _browse_input(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Input Folder")
        if folder:
            self._input_edit.setText(folder)
            # Auto-set output folder to input/harmonized/
            out = Path(folder) / "harmonized"
            self._output_edit.setText(str(out))

    def _browse_output(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Output Folder")
        if folder:
            self._output_edit.setText(folder)

    def _browse_config(self):
        filepath, _ = QFileDialog.getOpenFileName(
            self, "Select Config File", "", "JSON files (*.json);;All files (*)"
        )
        if filepath:
            try:
                self._config = load_config(Path(filepath))
                self._config_edit.setText(filepath)
            except Exception as e:
                QMessageBox.critical(self, "Invalid config", str(e))

    def set_config(self, config: MappingConfig, display_name: str = ""):
        """Set config programmatically (e.g. from single-file tab)."""
        self._config = config
        self._config_edit.setText(display_name or config.name)

    def _on_scan(self):
        input_folder = self._input_edit.text().strip()
        if not input_folder or not Path(input_folder).is_dir():
            QMessageBox.warning(self, "Invalid folder", "Please select a valid input folder.")
            return

        file_filter = self._filter_edit.text().strip()
        self._files = discover_files(Path(input_folder), file_filter)

        self._table.setRowCount(len(self._files))
        for i, f in enumerate(self._files):
            self._table.setItem(i, 0, QTableWidgetItem(f.name))
            self._table.setItem(i, 1, QTableWidgetItem("Pending"))
            self._table.setItem(i, 2, QTableWidgetItem(""))
            self._table.setItem(i, 3, QTableWidgetItem(""))

        self._start_btn.setEnabled(len(self._files) > 0 and self._config is not None)
        self._progress_label.setText(f"Found {len(self._files)} files")

    def _get_output_format(self) -> str:
        idx = self._format_combo.currentIndex()
        return ["csv", "excel", "parquet"][idx]

    def _on_start(self):
        if not self._files or self._config is None:
            return

        output_folder = Path(self._output_edit.text().strip())
        if not output_folder.parent.exists():
            QMessageBox.warning(self, "Invalid folder", "Output folder parent does not exist.")
            return

        output_folder.mkdir(parents=True, exist_ok=True)

        # Setup progress
        self._progress.setMaximum(len(self._files))
        self._progress.setValue(0)
        self._progress.setVisible(True)
        self._start_btn.setEnabled(False)
        self._cancel_btn.setEnabled(True)
        self._scan_btn.setEnabled(False)

        # Launch worker
        self._batch_thread = QThread()
        self._batch_worker = _BatchWorker(
            self._files, self._config, output_folder,
            self._get_output_format(), self._skip_check.isChecked(),
        )
        self._batch_worker.moveToThread(self._batch_thread)
        self._batch_thread.started.connect(self._batch_worker.run)
        self._batch_worker.file_started.connect(self._on_file_started)
        self._batch_worker.file_done.connect(self._on_file_done)
        self._batch_worker.all_done.connect(self._on_batch_done)
        self._batch_worker.error.connect(self._on_batch_error)
        self._batch_worker.all_done.connect(self._batch_thread.quit)
        self._batch_thread.start()

    def _on_cancel(self):
        if self._batch_worker:
            self._batch_worker.cancel()
        self._cancel_btn.setEnabled(False)

    def _on_file_started(self, index: int, filename: str):
        self._table.setItem(index, 1, QTableWidgetItem("Processing..."))
        self._progress_label.setText(f"Processing {index + 1}/{len(self._files)}: {filename}")

    def _on_file_done(self, index: int, result: BatchFileResult):
        status_text = result.status.capitalize()
        self._table.setItem(index, 1, QTableWidgetItem(status_text))

        # Color the status cell
        status_item = self._table.item(index, 1)
        if result.status == "success":
            status_item.setForeground(Qt.GlobalColor.darkGreen)
            self._table.setItem(index, 2, QTableWidgetItem(f"{result.row_count:,}"))
        elif result.status == "failed":
            status_item.setForeground(Qt.GlobalColor.red)
            self._table.setItem(index, 3, QTableWidgetItem(result.error))
        elif result.status == "skipped":
            status_item.setForeground(Qt.GlobalColor.gray)
            self._table.setItem(index, 3, QTableWidgetItem("Already exists"))

        self._progress.setValue(index + 1)

    def _on_batch_done(self):
        self._start_btn.setEnabled(True)
        self._cancel_btn.setEnabled(False)
        self._scan_btn.setEnabled(True)

        # Count results
        success = sum(1 for i in range(self._table.rowCount())
                      if self._table.item(i, 1) and "Success" in self._table.item(i, 1).text())
        failed = sum(1 for i in range(self._table.rowCount())
                     if self._table.item(i, 1) and "Failed" in self._table.item(i, 1).text())
        skipped = sum(1 for i in range(self._table.rowCount())
                      if self._table.item(i, 1) and "Skipped" in self._table.item(i, 1).text())

        self._progress_label.setText(
            f"Done: {success} success, {failed} failed, {skipped} skipped"
        )

    def _on_batch_error(self, error_msg: str):
        QMessageBox.critical(self, "Batch error", error_msg)
