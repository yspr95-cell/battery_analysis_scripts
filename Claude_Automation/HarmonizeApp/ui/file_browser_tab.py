"""File browser tab: explore folders, preview files, send to other tabs."""

from pathlib import Path

from PySide6.QtCore import Qt, Signal, QDir, QModelIndex
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QSplitter, QTreeView, QListWidget, QListWidgetItem,
    QFileSystemModel, QGroupBox, QFileDialog, QLineEdit,
)

from core.file_loader import (
    get_file_type, get_sheet_names, get_file_size_mb,
    load_preview, detect_header_row, SUPPORTED_EXTENSIONS,
)
from ui.preview_table import PreviewTableView

import pandas as pd


class FileBrowserTab(QWidget):
    """File browser with folder tree, file list, and quick preview."""

    open_in_single = Signal(str)   # filepath string -> open in Single File tab
    set_batch_input = Signal(str)  # folder path string -> set as batch input folder

    def __init__(self, parent=None):
        super().__init__(parent)
        self._current_folder: Path | None = None
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)

        # Folder bar
        folder_row = QHBoxLayout()
        folder_row.addWidget(QLabel("Folder:"))
        self._folder_edit = QLineEdit()
        self._folder_edit.setReadOnly(True)
        self._folder_edit.setPlaceholderText("Select a folder to browse...")
        folder_row.addWidget(self._folder_edit, stretch=1)
        browse_btn = QPushButton("Browse...")
        browse_btn.clicked.connect(self._on_browse_folder)
        folder_row.addWidget(browse_btn)
        layout.addLayout(folder_row)

        # Main splitter: folder tree | file list | preview
        splitter = QSplitter(Qt.Orientation.Horizontal)

        # Left: folder tree
        self._fs_model = QFileSystemModel()
        self._fs_model.setFilter(QDir.Filter.Dirs | QDir.Filter.NoDotAndDotDot)
        self._fs_model.setRootPath("")

        self._tree = QTreeView()
        self._tree.setModel(self._fs_model)
        self._tree.setHeaderHidden(True)
        # Hide Size, Type, Date columns
        for col in range(1, self._fs_model.columnCount()):
            self._tree.hideColumn(col)
        self._tree.clicked.connect(self._on_folder_selected)
        self._tree.setMinimumWidth(200)
        splitter.addWidget(self._tree)

        # Center: file list
        center = QWidget()
        center_layout = QVBoxLayout(center)
        center_layout.setContentsMargins(0, 0, 0, 0)

        self._file_list = QListWidget()
        self._file_list.currentItemChanged.connect(self._on_file_selected)
        self._file_list.itemDoubleClicked.connect(self._on_file_double_clicked)
        center_layout.addWidget(self._file_list)

        # Action buttons
        btn_row = QHBoxLayout()
        self._open_btn = QPushButton("Open in Single File")
        self._open_btn.setEnabled(False)
        self._open_btn.clicked.connect(self._on_open_single)
        btn_row.addWidget(self._open_btn)

        self._batch_btn = QPushButton("Set as Batch Input")
        self._batch_btn.setEnabled(False)
        self._batch_btn.clicked.connect(self._on_set_batch)
        btn_row.addWidget(self._batch_btn)
        center_layout.addLayout(btn_row)

        center.setMinimumWidth(200)
        splitter.addWidget(center)

        # Right: quick preview
        preview_group = QGroupBox("Quick Preview")
        preview_layout = QVBoxLayout(preview_group)
        preview_layout.setContentsMargins(4, 12, 4, 4)

        self._info_label = QLabel("Select a file to preview")
        self._info_label.setWordWrap(True)
        preview_layout.addWidget(self._info_label)

        self._preview_table = PreviewTableView()
        preview_layout.addWidget(self._preview_table)

        splitter.addWidget(preview_group)

        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 1)
        splitter.setStretchFactor(2, 2)
        splitter.setSizes([200, 200, 400])

        layout.addWidget(splitter, stretch=1)

    def _on_browse_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Folder")
        if folder:
            self._set_folder(Path(folder))

    def _set_folder(self, folder: Path):
        self._current_folder = folder
        self._folder_edit.setText(str(folder))

        # Navigate tree to this folder
        idx = self._fs_model.index(str(folder))
        if idx.isValid():
            self._tree.setCurrentIndex(idx)
            self._tree.scrollTo(idx)

        self._refresh_file_list(folder)
        self._batch_btn.setEnabled(True)

    def _on_folder_selected(self, index: QModelIndex):
        folder_path = Path(self._fs_model.filePath(index))
        if folder_path.is_dir():
            self._current_folder = folder_path
            self._folder_edit.setText(str(folder_path))
            self._refresh_file_list(folder_path)
            self._batch_btn.setEnabled(True)

    def _refresh_file_list(self, folder: Path):
        self._file_list.clear()
        self._preview_table.set_dataframe(pd.DataFrame())
        self._info_label.setText("Select a file to preview")
        self._open_btn.setEnabled(False)

        if not folder.is_dir():
            return

        files = sorted(
            f for f in folder.iterdir()
            if f.is_file() and f.suffix.lower() in SUPPORTED_EXTENSIONS
        )

        for f in files:
            size_mb = get_file_size_mb(f)
            if size_mb >= 1.0:
                size_str = f"{size_mb:.1f} MB"
            else:
                size_str = f"{size_mb * 1024:.0f} KB"
            item = QListWidgetItem(f"{f.name}  ({size_str})")
            item.setData(Qt.ItemDataRole.UserRole, str(f))
            self._file_list.addItem(item)

    def _on_file_selected(self, current: QListWidgetItem | None, previous):
        if current is None:
            self._open_btn.setEnabled(False)
            return

        self._open_btn.setEnabled(True)
        filepath = Path(current.data(Qt.ItemDataRole.UserRole))

        try:
            size_mb = get_file_size_mb(filepath)
            file_type = get_file_type(filepath)
            sheets = get_sheet_names(filepath)

            info_parts = [f"File: {filepath.name}"]
            info_parts.append(f"Type: {file_type}")
            if size_mb >= 1.0:
                info_parts.append(f"Size: {size_mb:.1f} MB")
            else:
                info_parts.append(f"Size: {size_mb * 1024:.0f} KB")
            if len(sheets) > 1:
                info_parts.append(f"Sheets: {', '.join(sheets)}")

            self._info_label.setText(" | ".join(info_parts))

            # Quick preview (first 100 rows of first sheet)
            sheet = sheets[0] if sheets else None
            header = detect_header_row(filepath, sheet_name=sheet)
            preview_df, _ = load_preview(filepath, sheet_name=sheet,
                                         header_row=header, max_rows=100)
            self._preview_table.set_dataframe(preview_df)

        except Exception as e:
            self._info_label.setText(f"Error: {e}")
            self._preview_table.set_dataframe(pd.DataFrame())

    def _on_file_double_clicked(self, item: QListWidgetItem):
        filepath = item.data(Qt.ItemDataRole.UserRole)
        if filepath:
            self.open_in_single.emit(filepath)

    def _on_open_single(self):
        current = self._file_list.currentItem()
        if current:
            filepath = current.data(Qt.ItemDataRole.UserRole)
            self.open_in_single.emit(filepath)

    def _on_set_batch(self):
        if self._current_folder:
            self.set_batch_input.emit(str(self._current_folder))
