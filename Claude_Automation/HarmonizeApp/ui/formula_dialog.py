"""Formula editor dialog: enter, validate, and preview a column formula."""

import pandas as pd
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QTextEdit, QTableWidget, QTableWidgetItem, QGroupBox,
    QDialogButtonBox, QSplitter, QWidget, QRadioButton,
    QButtonGroup, QFrame,
)

from core.formula_engine import validate_formula, evaluate_formula, get_col_map, sanitize_name


class FormulaDialog(QDialog):
    """Dialog to compose and validate a column formula.

    The user writes expressions using the safe column identifiers shown
    in the column reference table. A 'Validate' button checks syntax and
    safety; 'Preview' shows the first few evaluated values.
    """

    def __init__(self, target_col: str,
                 source_columns: list[str],
                 harmonized_columns: list[str] | None = None,
                 preview_df: pd.DataFrame | None = None,
                 initial_expr: str = "",
                 initial_level: int = 1,
                 parent=None):
        super().__init__(parent)
        self._target_col = target_col
        self._source_columns = list(source_columns)
        self._harmonized_columns = list(harmonized_columns or [])
        self._preview_df = preview_df
        self._result_expr: str = ""
        self._result_level: int = 1

        self.setWindowTitle(f"Formula Editor — {target_col}")
        self.setMinimumSize(640, 500)
        self.resize(720, 560)
        self._build_ui()

        # Populate with initial values
        if initial_expr:
            self._formula_edit.setPlainText(initial_expr)
        self._set_level(initial_level)

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self):
        layout = QVBoxLayout(self)

        # --- Level selector ---
        level_row = QHBoxLayout()
        level_row.addWidget(QLabel("Formula applies to:"))
        self._level_grp = QButtonGroup(self)
        self._rb_level1 = QRadioButton("Source columns (Level 1)")
        self._rb_level2 = QRadioButton("Harmonized columns (Level 2)")
        self._rb_level1.setToolTip(
            "Formula uses raw source data columns.\n"
            "Evaluated before any column mapping."
        )
        self._rb_level2.setToolTip(
            "Formula uses already-mapped target columns.\n"
            "Useful for derived values like Power = Voltage_V * Current_A."
        )
        self._level_grp.addButton(self._rb_level1, 1)
        self._level_grp.addButton(self._rb_level2, 2)
        self._rb_level1.setChecked(True)
        self._rb_level1.toggled.connect(self._on_level_changed)
        level_row.addWidget(self._rb_level1)
        level_row.addWidget(self._rb_level2)
        level_row.addStretch()
        layout.addLayout(level_row)

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        layout.addWidget(sep)

        # --- Main splitter: col reference (left) | formula input (right) ---
        splitter = QSplitter(Qt.Orientation.Horizontal)

        # Column reference table
        ref_group = QGroupBox("Available columns (click to insert)")
        ref_layout = QVBoxLayout(ref_group)
        ref_layout.setContentsMargins(4, 12, 4, 4)
        self._col_table = QTableWidget()
        self._col_table.setColumnCount(2)
        self._col_table.setHorizontalHeaderLabels(["Safe name", "Original"])
        self._col_table.horizontalHeader().setStretchLastSection(True)
        self._col_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._col_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._col_table.cellDoubleClicked.connect(self._on_col_insert)
        self._col_table.setToolTip("Double-click a row to insert the safe name at cursor")
        ref_layout.addWidget(self._col_table)
        splitter.addWidget(ref_group)

        # Formula input + controls
        formula_widget = QWidget()
        formula_layout = QVBoxLayout(formula_widget)
        formula_layout.setContentsMargins(0, 0, 0, 0)

        formula_group = QGroupBox("Formula expression")
        fg_layout = QVBoxLayout(formula_group)
        fg_layout.setContentsMargins(4, 12, 4, 4)

        self._formula_edit = QTextEdit()
        self._formula_edit.setFixedHeight(80)
        mono = QFont("Consolas", 10)
        mono.setStyleHint(QFont.StyleHint.Monospace)
        self._formula_edit.setFont(mono)
        self._formula_edit.setPlaceholderText(
            "e.g.  Voltage_V * Current_A\n"
            "      np.cumsum(I_A_) / 3600"
        )
        fg_layout.addWidget(self._formula_edit)

        btn_row = QHBoxLayout()
        self._validate_btn = QPushButton("Validate")
        self._validate_btn.clicked.connect(self._on_validate)
        btn_row.addWidget(self._validate_btn)

        self._preview_btn = QPushButton("Preview (5 rows)")
        self._preview_btn.clicked.connect(self._on_preview)
        self._preview_btn.setEnabled(self._preview_df is not None)
        btn_row.addWidget(self._preview_btn)
        btn_row.addStretch()
        fg_layout.addLayout(btn_row)

        self._status_label = QLabel("")
        self._status_label.setWordWrap(True)
        fg_layout.addWidget(self._status_label)

        formula_layout.addWidget(formula_group)

        # Preview table
        prev_group = QGroupBox("Preview")
        prev_layout = QVBoxLayout(prev_group)
        prev_layout.setContentsMargins(4, 12, 4, 4)
        self._preview_table = QTableWidget()
        self._preview_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        prev_layout.addWidget(self._preview_table)
        formula_layout.addWidget(prev_group, stretch=1)

        splitter.addWidget(formula_widget)
        splitter.setSizes([220, 480])
        layout.addWidget(splitter, stretch=1)

        # --- Dialog buttons ---
        btn_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        clear_btn = btn_box.addButton("Clear formula", QDialogButtonBox.ButtonRole.ResetRole)
        clear_btn.clicked.connect(self._on_clear)
        btn_box.accepted.connect(self._on_accept)
        btn_box.rejected.connect(self.reject)
        layout.addWidget(btn_box)

        # Populate column table with level-1 columns initially
        self._populate_col_table()

    # ------------------------------------------------------------------
    # Column reference table
    # ------------------------------------------------------------------

    def _populate_col_table(self):
        level = 2 if self._rb_level2.isChecked() else 1
        if level == 1:
            columns = self._source_columns
        else:
            columns = self._harmonized_columns

        col_map = get_col_map(columns)
        self._col_table.setRowCount(len(col_map))
        for row, (safe, orig) in enumerate(col_map.items()):
            self._col_table.setItem(row, 0, QTableWidgetItem(safe))
            self._col_table.setItem(row, 1, QTableWidgetItem(orig))
        self._col_table.resizeColumnsToContents()

    def _on_col_insert(self, row: int, _col: int):
        safe_name = self._col_table.item(row, 0)
        if safe_name:
            cursor = self._formula_edit.textCursor()
            cursor.insertText(safe_name.text())
            self._formula_edit.setFocus()

    # ------------------------------------------------------------------
    # Level selector
    # ------------------------------------------------------------------

    def _set_level(self, level: int):
        if level == 2:
            self._rb_level2.setChecked(True)
        else:
            self._rb_level1.setChecked(True)
        self._populate_col_table()

    def _on_level_changed(self):
        self._populate_col_table()

    # ------------------------------------------------------------------
    # Validate / Preview
    # ------------------------------------------------------------------

    def _on_validate(self):
        expr = self._formula_edit.toPlainText().strip()
        valid, err = validate_formula(expr)
        if valid:
            self._status_label.setText("✓ Valid formula")
            self._status_label.setStyleSheet("color: #27ae60; font-weight: bold;")
        else:
            self._status_label.setText(f"✗ {err}")
            self._status_label.setStyleSheet("color: #e74c3c;")

    def _on_preview(self):
        if self._preview_df is None:
            return
        expr = self._formula_edit.toPlainText().strip()
        level = 2 if self._rb_level2.isChecked() else 1

        if level == 1:
            df = self._preview_df
        else:
            # Use harmonized column names if available
            df = self._preview_df

        col_map = get_col_map(list(df.columns))
        try:
            series = evaluate_formula(expr, df, col_map)
            self._status_label.setText("✓ Preview OK")
            self._status_label.setStyleSheet("color: #27ae60; font-weight: bold;")
            self._show_preview(df, series)
        except Exception as e:
            self._status_label.setText(f"✗ {e}")
            self._status_label.setStyleSheet("color: #e74c3c;")
            self._preview_table.setRowCount(0)

    def _show_preview(self, df: pd.DataFrame, result: pd.Series):
        n = min(5, len(result))
        self._preview_table.setRowCount(n)
        self._preview_table.setColumnCount(2)
        self._preview_table.setHorizontalHeaderLabels(["Row", "Result"])
        for i in range(n):
            self._preview_table.setItem(i, 0, QTableWidgetItem(str(i)))
            val = result.iloc[i]
            self._preview_table.setItem(i, 1, QTableWidgetItem(
                f"{val:.6g}" if isinstance(val, float) else str(val)
            ))
        self._preview_table.resizeColumnsToContents()

    # ------------------------------------------------------------------
    # Dialog accept / clear
    # ------------------------------------------------------------------

    def _on_clear(self):
        self._formula_edit.clear()
        self._status_label.setText("")
        self._preview_table.setRowCount(0)

    def _on_accept(self):
        expr = self._formula_edit.toPlainText().strip()
        if expr:
            valid, err = validate_formula(expr)
            if not valid:
                self._status_label.setText(f"✗ Cannot save: {err}")
                self._status_label.setStyleSheet("color: #e74c3c;")
                return
        self._result_expr = expr
        self._result_level = 2 if self._rb_level2.isChecked() else 1
        self.accept()

    # ------------------------------------------------------------------
    # Result accessors
    # ------------------------------------------------------------------

    @property
    def formula_expression(self) -> str:
        """The validated formula string (empty if cleared)."""
        return self._result_expr

    @property
    def formula_level(self) -> int:
        """1 = source columns, 2 = harmonized columns."""
        return self._result_level
