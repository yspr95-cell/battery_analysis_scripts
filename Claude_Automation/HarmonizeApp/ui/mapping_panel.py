"""Column mapping panel: 16 target columns with source column dropdowns.

Supports direct mapping and OR-fallback (try first source, fall back to second, etc.).
"""

from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QComboBox,
    QScrollArea, QGroupBox, QFrame, QPushButton,
)

from core.schema import FOCUS_COLS_ETL, MANDATORY_COLS_ETL, COLUMN_METADATA
from core.validation import validate_mapping, ValidationResult

UNMAPPED = "(unmapped)"


class _FallbackRow(QWidget):
    """A single OR-fallback combo row with a remove button."""
    changed = Signal()
    remove_requested = Signal(object)  # emits self

    def __init__(self, source_columns: list[str], parent=None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(2)

        lbl = QLabel("OR")
        lbl.setFixedWidth(24)
        lbl.setStyleSheet("color: #7f8c8d; font-style: italic;")
        layout.addWidget(lbl)

        self.combo = QComboBox()
        self.combo.addItem(UNMAPPED)
        self.combo.addItems(source_columns)
        self.combo.currentTextChanged.connect(lambda _: self.changed.emit())
        layout.addWidget(self.combo, stretch=1)

        self.remove_btn = QPushButton("-")
        self.remove_btn.setFixedSize(22, 22)
        self.remove_btn.setToolTip("Remove fallback")
        self.remove_btn.clicked.connect(lambda: self.remove_requested.emit(self))
        layout.addWidget(self.remove_btn)

    def set_source_columns(self, columns: list[str]):
        current = self.combo.currentText()
        self.combo.blockSignals(True)
        self.combo.clear()
        self.combo.addItem(UNMAPPED)
        self.combo.addItems(columns)
        idx = self.combo.findText(current)
        if idx >= 0:
            self.combo.setCurrentIndex(idx)
        self.combo.blockSignals(False)


class MappingPanel(QWidget):
    """Panel with 16 rows for mapping source columns to target schema.

    Each row supports direct mapping or OR-fallback with multiple source columns.
    """

    mapping_changed = Signal()  # emitted when any dropdown changes

    def __init__(self, parent=None):
        super().__init__(parent)
        self._combos: dict[str, QComboBox] = {}
        self._status_labels: dict[str, QLabel] = {}
        self._fallback_containers: dict[str, QVBoxLayout] = {}
        self._fallback_rows: dict[str, list[_FallbackRow]] = {}
        self._add_buttons: dict[str, QPushButton] = {}
        self._source_columns: list[str] = []
        self._build_ui()

    def _build_ui(self):
        outer_layout = QVBoxLayout(self)
        outer_layout.setContentsMargins(0, 0, 0, 0)

        group = QGroupBox("Column Mapping")
        group_layout = QVBoxLayout(group)
        group_layout.setContentsMargins(6, 12, 6, 6)
        group_layout.setSpacing(2)

        # Header row
        header = QHBoxLayout()
        h_target = QLabel("Target")
        h_target.setFixedWidth(140)
        h_target.setStyleSheet("font-weight: bold;")
        header.addWidget(h_target)
        h_source = QLabel("Map From")
        h_source.setStyleSheet("font-weight: bold;")
        header.addWidget(h_source, stretch=1)
        h_status = QLabel("St")
        h_status.setFixedWidth(24)
        h_status.setStyleSheet("font-weight: bold;")
        header.addWidget(h_status)
        group_layout.addLayout(header)

        # Separator
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        group_layout.addWidget(sep)

        # Scrollable area for the 16 mapping rows
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll_content = QWidget()
        scroll_layout = QVBoxLayout(scroll_content)
        scroll_layout.setContentsMargins(0, 0, 0, 0)
        scroll_layout.setSpacing(2)

        for col_name in FOCUS_COLS_ETL:
            row_widget = self._create_mapping_row(col_name)
            scroll_layout.addWidget(row_widget)

        scroll_layout.addStretch()
        scroll.setWidget(scroll_content)
        group_layout.addWidget(scroll)

        outer_layout.addWidget(group)

    def _create_mapping_row(self, target_col: str) -> QWidget:
        container = QWidget()
        container_layout = QVBoxLayout(container)
        container_layout.setContentsMargins(0, 1, 0, 1)
        container_layout.setSpacing(1)

        # Primary row
        row = QHBoxLayout()

        # Target label
        is_mandatory = target_col in MANDATORY_COLS_ETL
        label = QLabel(target_col)
        label.setFixedWidth(140)
        if is_mandatory:
            font = label.font()
            font.setBold(True)
            label.setFont(font)
            label.setToolTip(f"{COLUMN_METADATA[target_col]['description']} [MANDATORY]")
        else:
            label.setToolTip(COLUMN_METADATA[target_col]['description'])
        row.addWidget(label)

        # Mandatory indicator
        if is_mandatory:
            star = QLabel("*")
            star.setFixedWidth(10)
            star.setStyleSheet("color: #e74c3c; font-weight: bold;")
            row.addWidget(star)
        else:
            spacer_lbl = QLabel("")
            spacer_lbl.setFixedWidth(10)
            row.addWidget(spacer_lbl)

        # Source column dropdown
        combo = QComboBox()
        combo.addItem(UNMAPPED)
        combo.setMinimumWidth(150)
        combo.currentTextChanged.connect(lambda _: self._on_combo_changed())
        row.addWidget(combo, stretch=1)

        # Add fallback button
        add_btn = QPushButton("+")
        add_btn.setFixedSize(22, 22)
        add_btn.setToolTip("Add OR-fallback source column")
        add_btn.clicked.connect(lambda _, tc=target_col: self._add_fallback(tc))
        row.addWidget(add_btn)
        self._add_buttons[target_col] = add_btn

        self._combos[target_col] = combo

        # Status indicator
        status = QLabel("—")
        status.setFixedWidth(24)
        status.setAlignment(label.alignment())
        self._status_labels[target_col] = status
        row.addWidget(status)

        container_layout.addLayout(row)

        # Fallback container (initially empty)
        fallback_layout = QVBoxLayout()
        fallback_layout.setContentsMargins(155, 0, 46, 0)
        fallback_layout.setSpacing(1)
        container_layout.addLayout(fallback_layout)
        self._fallback_containers[target_col] = fallback_layout
        self._fallback_rows[target_col] = []

        return container

    def _add_fallback(self, target_col: str, source_value: str | None = None):
        """Add an OR-fallback row for a target column."""
        fb = _FallbackRow(self._source_columns)
        if source_value:
            idx = fb.combo.findText(source_value)
            if idx >= 0:
                fb.combo.setCurrentIndex(idx)
        fb.changed.connect(self._on_combo_changed)
        fb.remove_requested.connect(lambda row, tc=target_col: self._remove_fallback(tc, row))
        self._fallback_containers[target_col].addWidget(fb)
        self._fallback_rows[target_col].append(fb)
        self._on_combo_changed()

    def _remove_fallback(self, target_col: str, row: _FallbackRow):
        """Remove an OR-fallback row."""
        if row in self._fallback_rows[target_col]:
            self._fallback_rows[target_col].remove(row)
            self._fallback_containers[target_col].removeWidget(row)
            row.deleteLater()
            self._on_combo_changed()

    def set_source_columns(self, columns: list[str]):
        """Update all dropdowns with new source column names."""
        self._source_columns = list(columns)
        for combo in self._combos.values():
            combo.blockSignals(True)
            current = combo.currentText()
            combo.clear()
            combo.addItem(UNMAPPED)
            combo.addItems(self._source_columns)
            idx = combo.findText(current)
            if idx >= 0:
                combo.setCurrentIndex(idx)
            combo.blockSignals(False)
        # Update fallback rows too
        for rows in self._fallback_rows.values():
            for fb in rows:
                fb.set_source_columns(self._source_columns)
        self._update_status()

    def get_mapping(self) -> dict[str, str | None]:
        """Return current mapping: {target_col: source_col_or_None}.

        For direct mappings, returns the single source column.
        For OR-fallback, returns the first non-unmapped source.
        Use get_full_mapping() to get all fallback sources.
        """
        mapping = {}
        for target_col, combo in self._combos.items():
            val = combo.currentText()
            if val != UNMAPPED:
                mapping[target_col] = val
            else:
                found = None
                for fb in self._fallback_rows.get(target_col, []):
                    fb_val = fb.combo.currentText()
                    if fb_val != UNMAPPED:
                        found = fb_val
                        break
                mapping[target_col] = found
        return mapping

    def get_full_mapping(self) -> dict[str, list[str]]:
        """Return full mapping with all fallback sources.

        Returns: {target_col: [primary, fallback1, fallback2, ...]}
        Empty list means unmapped.
        """
        mapping = {}
        for target_col, combo in self._combos.items():
            sources = []
            val = combo.currentText()
            if val != UNMAPPED:
                sources.append(val)
            for fb in self._fallback_rows.get(target_col, []):
                fb_val = fb.combo.currentText()
                if fb_val != UNMAPPED:
                    sources.append(fb_val)
            mapping[target_col] = sources
        return mapping

    def set_mapping(self, mapping: dict[str, str | None]):
        """Set dropdown selections from a simple mapping dict."""
        for target_col, combo in self._combos.items():
            combo.blockSignals(True)
            source = mapping.get(target_col)
            if source:
                idx = combo.findText(source)
                if idx >= 0:
                    combo.setCurrentIndex(idx)
                else:
                    combo.setCurrentIndex(0)
            else:
                combo.setCurrentIndex(0)
            combo.blockSignals(False)
        self._update_status()

    def set_full_mapping(self, full_mapping: dict[str, list[str]]):
        """Set mapping from a full mapping dict with fallbacks."""
        for target_col, combo in self._combos.items():
            sources = full_mapping.get(target_col, [])

            # Clear existing fallbacks
            for fb in list(self._fallback_rows.get(target_col, [])):
                self._remove_fallback(target_col, fb)

            combo.blockSignals(True)
            if sources:
                idx = combo.findText(sources[0])
                combo.setCurrentIndex(idx if idx >= 0 else 0)
                for src in sources[1:]:
                    self._add_fallback(target_col, source_value=src)
            else:
                combo.setCurrentIndex(0)
            combo.blockSignals(False)
        self._update_status()

    def clear_mapping(self):
        """Reset all dropdowns to unmapped and remove fallbacks."""
        for target_col, combo in self._combos.items():
            combo.blockSignals(True)
            combo.setCurrentIndex(0)
            combo.blockSignals(False)
            for fb in list(self._fallback_rows.get(target_col, [])):
                self._remove_fallback(target_col, fb)
        self._update_status()

    def validate(self) -> ValidationResult:
        """Validate current mapping state."""
        return validate_mapping(self.get_mapping())

    def _on_combo_changed(self):
        self._update_status()
        self.mapping_changed.emit()

    def _update_status(self):
        result = self.validate()
        for col_name, status_label in self._status_labels.items():
            status = result.column_status.get(col_name, 'unmapped')
            if status == 'mapped':
                status_label.setText("✓")
                status_label.setStyleSheet("color: #27ae60; font-weight: bold;")
            elif status == 'missing_mandatory':
                status_label.setText("✗")
                status_label.setStyleSheet("color: #e74c3c; font-weight: bold;")
            else:
                status_label.setText("—")
                status_label.setStyleSheet("color: #95a5a6;")
