"""Auto-suggest column mapping based on keyword matching against source column names.

Each target column has an ordered keyword list (most specific first).
Prefix "=" means exact match; otherwise substring match (case-insensitive).
A source column is only assigned if exactly ONE candidate matches a keyword
(ambiguous multi-matches are skipped). Each source column can only be used once.
"""

from core.schema import FOCUS_COLS_ETL

# Keywords per target column, tried in priority order.
# "=" prefix → exact match;  no prefix → source must CONTAIN the keyword.
_TARGET_KEYWORDS: dict[str, list[str]] = {
    'Total_time_s':     ['total time', 'test time', 'total_time', 'test_time',
                         'elapsed time', 'time_s', 'time(s)', 'run time'],
    'Date_time':        ['dpt time', 'dpt_time', 'absolute time', 'abs_time',
                         'date_time', 'datetime', 'timestamp', 'abs time'],
    'Unix_time':        ['unix time', 'unix_time', 'epoch'],
    'Step':             ['step index', 'step_index', 'step_nr', 'step_idx',
                         'step idx', '=step', '=step_nr'],
    'Step_name':        ['step name', 'step_name', 'step type', 'step_type',
                         '=state', '=mode', '=process'],
    'Cycle':            ['cycle index', 'cycle_index', 'cycle_nr', 'cycle_idx',
                         '=cycle', '=cycle_nr'],
    'Voltage_V':        ['voltage', 'u(v)', 'spannung', 'cell_voltage',
                         'cell voltage', '=u_v', 'v(v)'],
    'Current_A':        ['current', 'i(a)', 'strom', 'cell_current',
                         'cell current', '=i_a'],
    'Power_W':          ['power', 'w(w)', 'leistung'],
    'Capacity_step_Ah': ['capacity', 'cap(ah)', 'capacity(ah)', 'cap_ah',
                         'kapazität', 'kapazitat'],
    'Energy_step_Wh':   ['energy', 'energie', 'energy(wh)'],
    'T_Cell_degC':      ['t_cell', 'tcell', 'celltemp', 'cell_temp',
                         'cell temp', 'temperature 1', 'temperature1',
                         'temp 1', 'temp1', 't_zelle'],
    'T_Anode_degC':     ['anode', 't_anode', 'temp_anode',
                         'temperature 2', 'temperature2', 'temp 2', 'temp2'],
    'T_Cathode_degC':   ['cathode', 't_cathode', 'temp_cathode',
                         'temperature 3', 'temperature3', 'temp 3', 'temp3'],
    'T_Chamber_degC':   ['chambertemp', 'chamber_temp', 'chamber temp',
                         't_chamber', 'chamber'],
    'T_cold_degC':      ['cold', 't_cold', 'cold_side', 'cold side'],
}


def suggest_mapping(source_columns: list[str]) -> dict[str, str | None]:
    """Suggest target → source mapping based on keyword matching.

    Only returns high-confidence (unambiguous) matches.
    Each source column is assigned to at most one target.

    Returns:
        {target_col: source_col}  — value is None when no match found.
    """
    # Build lowercase lookup: lower_name → original_name
    src_lower: dict[str, str] = {col.lower().strip(): col for col in source_columns}
    result: dict[str, str | None] = {col: None for col in FOCUS_COLS_ETL}
    used: set[str] = set()

    for target_col in FOCUS_COLS_ETL:
        keywords = _TARGET_KEYWORDS.get(target_col, [])
        match = _find_match(keywords, src_lower, used)
        if match is not None:
            result[target_col] = match
            used.add(match)

    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _find_match(
    keywords: list[str],
    src_lower: dict[str, str],
    used: set[str],
) -> str | None:
    """Try each keyword in order; return source col if exactly one candidate."""
    for kw in keywords:
        candidates = _match_keyword(kw, src_lower, used)
        if len(candidates) == 1:
            return candidates[0]
        # 0 matches → try next keyword; 2+ → ambiguous, try next keyword
    return None


def _match_keyword(
    keyword: str,
    src_lower: dict[str, str],
    used: set[str],
) -> list[str]:
    """Return original source column names matching the keyword (excluding used)."""
    if keyword.startswith('='):
        target_kw = keyword[1:].lower()
        return [orig for low, orig in src_lower.items()
                if low == target_kw and orig not in used]
    else:
        kw_l = keyword.lower()
        return [orig for low, orig in src_lower.items()
                if kw_l in low and orig not in used]
