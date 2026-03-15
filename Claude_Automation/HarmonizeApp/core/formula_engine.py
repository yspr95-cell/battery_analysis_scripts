"""AST-based sandboxed formula evaluator for column expressions.

Formulas reference source columns via sanitized Python identifiers.
Example: 'U_V_ * I_A_'  or  'Voltage_V * Current_A'

Security model:
- Whitelist of allowed AST node types
- No builtins except an explicit safe set
- No attribute access starting with '__'
- Blocked dangerous names (exec, eval, open, ...)
"""

import ast
import re
from types import SimpleNamespace

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Safe numpy subset exposed to formulas
# ---------------------------------------------------------------------------
_NP = SimpleNamespace(
    abs=np.abs, round=np.round, sqrt=np.sqrt,
    log=np.log, log10=np.log10, exp=np.exp,
    cumsum=np.cumsum, diff=np.diff,
    where=np.where, nan=np.nan, inf=np.inf,
    isnan=np.isnan, isinf=np.isinf,
    clip=np.clip, sign=np.sign,
    zeros_like=np.zeros_like, ones_like=np.ones_like,
)

_SAFE_BUILTINS: dict = {
    'abs': abs, 'round': round, 'min': min, 'max': max, 'sum': sum,
    'len': len, 'int': int, 'float': float, 'bool': bool,
    'True': True, 'False': False, 'None': None,
    'np': _NP,
}

# ---------------------------------------------------------------------------
# AST whitelist
# ---------------------------------------------------------------------------
_ALLOWED_NODES = frozenset({
    ast.Expression, ast.Module, ast.Expr,
    # Arithmetic
    ast.BinOp, ast.UnaryOp,
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod, ast.Pow,
    ast.USub, ast.UAdd,
    # Comparisons / boolean
    ast.Compare, ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE,
    ast.BoolOp, ast.And, ast.Or, ast.Not,
    # Literals
    ast.Constant,
    # Names / attribute access
    ast.Name, ast.Load, ast.Attribute,
    # Calls
    ast.Call,
    # Subscript / slice
    ast.Subscript, ast.Index, ast.Slice,
    # Conditional expression (a if cond else b)
    ast.IfExp,
    # Tuple / List (for multi-arg functions)
    ast.Tuple, ast.List,
    # Keyword args
    ast.keyword,
})

_BLOCKED_NAMES = frozenset({
    'exec', 'eval', 'open', 'import', '__import__', 'compile',
    'getattr', 'setattr', 'delattr', 'vars', 'dir', 'globals',
    'locals', 'type', 'input', 'print', 'breakpoint', 'help',
    'exit', 'quit',
})


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sanitize_name(name: str) -> str:
    """Convert a column name to a valid Python identifier for formula use.

    Examples:
        'U(V)'        -> 'U_V_'
        'Test Time'   -> 'Test_Time'
        'Temperature 1' -> 'Temperature_1'
    """
    safe = re.sub(r'[^a-zA-Z0-9_]', '_', str(name))
    safe = re.sub(r'_+', '_', safe).strip('_')
    if not safe:
        safe = 'col'
    if safe[0].isdigit():
        safe = 'c' + safe
    return safe


def get_col_map(columns: list[str]) -> dict[str, str]:
    """Build {safe_identifier: original_column_name} mapping.

    Handles collisions by appending _2, _3, etc. to the safe name.
    """
    result: dict[str, str] = {}
    seen: dict[str, int] = {}
    for col in columns:
        safe = sanitize_name(col)
        if safe not in seen:
            seen[safe] = 1
            result[safe] = col
        else:
            seen[safe] += 1
            result[f"{safe}_{seen[safe]}"] = col
    return result


def validate_formula(expr: str) -> tuple[bool, str]:
    """Check if a formula string is syntactically valid and safe to evaluate.

    Returns:
        (is_valid, error_message)  — error_message is '' when valid.
    """
    if not expr.strip():
        return False, "Formula is empty"

    try:
        tree = ast.parse(expr.strip(), mode='eval')
    except SyntaxError as e:
        return False, f"Syntax error: {e}"

    for node in ast.walk(tree):
        if type(node) not in _ALLOWED_NODES:
            return False, f"Disallowed operation: {type(node).__name__}"
        if isinstance(node, ast.Attribute) and node.attr.startswith('__'):
            return False, f"Disallowed attribute: {node.attr}"
        if isinstance(node, ast.Name) and node.id in _BLOCKED_NAMES:
            return False, f"Disallowed name: {node.id}"
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id in _BLOCKED_NAMES:
                return False, f"Disallowed function: {node.func.id}"

    return True, ""


def evaluate_formula(expr: str, df: pd.DataFrame,
                     col_map: dict[str, str] | None = None) -> pd.Series:
    """Evaluate a formula expression against a DataFrame.

    Args:
        expr: Formula string using safe column identifiers.
        df: DataFrame whose columns are available in the formula namespace.
        col_map: Optional pre-built {safe_name: original_col} mapping.
                 If None, auto-built from df.columns.

    Returns:
        pd.Series with the formula result (same length as df).

    Raises:
        ValueError: If the formula is invalid or evaluation fails.
    """
    if col_map is None:
        col_map = get_col_map(list(df.columns))

    valid, err = validate_formula(expr)
    if not valid:
        raise ValueError(f"Formula validation failed: {err}")

    # Build restricted namespace
    namespace = dict(_SAFE_BUILTINS)
    for safe_name, orig_col in col_map.items():
        if orig_col in df.columns:
            namespace[safe_name] = df[orig_col]
        # Also allow direct use of original names if they're valid identifiers
    for col in df.columns:
        if col.isidentifier() and col not in namespace:
            namespace[col] = df[col]

    try:
        result = eval(
            compile(ast.parse(expr.strip(), mode='eval'), '<formula>', 'eval'),
            {'__builtins__': {}},
            namespace,
        )
    except NameError as e:
        raise ValueError(
            f"Unknown column name in formula: {e}\n"
            f"Use the safe names shown in the column list."
        )
    except Exception as e:
        raise ValueError(f"Formula evaluation error: {e}")

    if isinstance(result, pd.Series):
        return result.reset_index(drop=True)
    else:
        return pd.Series([result] * len(df), index=df.index, dtype=float)
