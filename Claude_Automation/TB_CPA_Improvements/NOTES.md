# TB_CPA Improvements — Running Notes

## Overview
Improvements to the TB_CPA_TestDataAutomation pipeline, separated into two independent repos.
Original repo (`TB_CPA_TestDataAutomation/`) stays untouched.

## New Repos
- `TB_CPA_Extraction/` — extraction pipeline (main.py + archive/file handling)
- `TB_CPA_Harmonize/` — harmonization pipeline (harmonize_run.py + supplier transforms)

## Key Decisions

### Repo separation (Issue 3)
- User chose: **duplicate paths.py** in each repo (user sets same base_path in both)
- Both repos run standalone from their own directory: `python main.py` / `python harmonize_run.py`
- Import style in new repos: `from src.dependencies import *` (local, not `from TB_CPA_TestDataAutomation.src...`)

### Auto-detect data sheet (Issue 1)
- Strategy: pure row-count (sheet with most rows wins)
- Applied as **fallback only** — config Datasheet pattern is tried first
- Affected methods: all `get_raw_data()` in `hm_import_data.py` except `cfg_mcm_xls_01` and `cfg_sz_std_01`
- New function: `detect_data_sheet(xl_dict: dict) -> str` in `hm_general_support.py`

### Auto-detect header row (Issue 2)
- Strategy: scan first 15 rows, score each by (string cell count)^2 / total cell count
- Applied as **third fallback** after: config value → "step" substring search → auto-detect
- New function: `detect_header_row_auto(df, max_scan_rows=15) -> int` in `hm_general_support.py`
- Returns 1-based row index

### No changes to transform functions (Issue 4)
- User's harmonization is working — transform logic not touched
- Existing TODO noted: numeric string conversion in gen_clean_datasheet() is partial

## Import fixes applied in new repos
| Old import | New import |
|-----------|-----------|
| `from TB_CPA_TestDataAutomation.src.dependencies import *` | `from src.dependencies import *` |
| `from TB_CPA_TestDataAutomation.harmonize.hm_supplier_config import ...` | `from harmonize.hm_supplier_config import ...` |

## Dependencies split
- **Extraction only**: patool, shutil, hashlib, magic, winsound (all in src/dependencies.py)
- **Harmonize only**: fnmatch, gc (lighter set — no patool needed)
- Both keep full requirements.txt for simplicity (user can trim later)
