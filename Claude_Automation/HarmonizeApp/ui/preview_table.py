"""QTableView model for displaying pandas DataFrames."""

from PySide6.QtCore import Qt, QAbstractTableModel, QModelIndex
from PySide6.QtWidgets import QTableView, QHeaderView
import pandas as pd


class DataFrameModel(QAbstractTableModel):
    """Qt table model backed by a pandas DataFrame.

    The DataFrame passed in should already be trimmed to preview size.
    This model does NOT hold the full dataset - only what should be displayed.
    """

    def __init__(self, df: pd.DataFrame | None = None):
        super().__init__()
        self._df = pd.DataFrame() if df is None else df
        # Pre-convert visible data to strings for fast rendering
        self._str_cache: list[list[str]] | None = None

    def set_dataframe(self, df: pd.DataFrame):
        self.beginResetModel()
        self._df = df
        self._str_cache = None  # Invalidate cache
        self.endResetModel()

    def dataframe(self) -> pd.DataFrame:
        return self._df

    def rowCount(self, parent=QModelIndex()):
        return len(self._df)

    def columnCount(self, parent=QModelIndex()):
        return len(self._df.columns)

    def _ensure_str_cache(self):
        """Build a string cache of all visible cells for fast rendering."""
        if self._str_cache is not None:
            return
        df = self._df
        cache = []
        for i in range(len(df)):
            row_strs = []
            for j in range(len(df.columns)):
                val = df.iat[i, j]
                if pd.isna(val):
                    row_strs.append("")
                else:
                    row_strs.append(str(val))
            cache.append(row_strs)
        self._str_cache = cache

    def data(self, index: QModelIndex, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
        if role == Qt.ItemDataRole.DisplayRole:
            self._ensure_str_cache()
            return self._str_cache[index.row()][index.column()]
        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        if orientation == Qt.Orientation.Horizontal:
            return str(self._df.columns[section])
        return str(section + 1)  # 1-indexed row numbers


class PreviewTableView(QTableView):
    """Configured QTableView for data preview."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._model = DataFrameModel()
        self.setModel(self._model)

        # Appearance
        self.setAlternatingRowColors(True)
        self.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self.horizontalHeader().setStretchLastSection(True)
        self.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.verticalHeader().setDefaultSectionSize(24)

    def set_dataframe(self, df: pd.DataFrame):
        self._model.set_dataframe(df)
        # Only auto-resize for manageable column counts
        if len(df.columns) <= 30:
            self.resizeColumnsToContents()
            for col in range(self._model.columnCount()):
                if self.columnWidth(col) > 200:
                    self.setColumnWidth(col, 200)

    def dataframe(self) -> pd.DataFrame:
        return self._model.dataframe()

    def row_count(self) -> int:
        return len(self._model.dataframe())

    def col_count(self) -> int:
        return len(self._model.dataframe().columns)
