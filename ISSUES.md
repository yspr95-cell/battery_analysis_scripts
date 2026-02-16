# TB_CPA_TestDataAutomation - Codebase Issues & Improvements

Generated: 2026-02-14

All file paths are relative to `battery_analysis_scripts/TB_CPA_TestDataAutomation/`.

---

## P0: Bugs

### BUG-01: Dead code - impossible condition
**File:** `src/file_handling.py:531`
```python
if len(_files) < 0:  # len() never returns negative; should be == 0
    return None
```
**Impact:** Function never early-returns on empty list, may cause downstream errors.

### BUG-02: Duplicate class definition overwrites first
**File:** `harmonize/hm_import_data.py:145` and `harmonize/hm_import_data.py:178`
```python
class cfg_srf_std_01:  # defined at line 145
    ...
class cfg_srf_std_01:  # defined AGAIN at line 178, silently overwrites the first
    ...
```
**Impact:** First definition is unreachable. If they differ, one supplier config is lost.

### BUG-03: Undefined variable in logging statement
**File:** `harmonize/hm_import_data.py:349` (approx)
```python
logging.warning(f"Sheet check failed in gen_clean_datasheet(): {i} in file {self.filepath.name}")
# `i` is not defined in this scope
```
**Impact:** Will raise `NameError` if this code path executes.

---

## P1: Error Handling

### ERR-01: Bare `except:` clauses (26 occurrences)
Bare excepts catch everything including `KeyboardInterrupt` and `SystemExit`, making it impossible to interrupt the program or diagnose failures.

| File | Lines |
|------|-------|
| `src/clear_backlog.py` | 59, 96, 133 |
| `src/file_handling.py` | 509 |
| `src/extract_archive.py` | 33 (also uses `print('exception')` instead of logging) |
| `harmonize_run.py` | 58 |
| `harmonize_SZ_run.py` | 65 |
| `harmonize/supplier_support_func/hm_general_support.py` | 103 |
| `harmonize/supplier_support_func/hm_gen_trans_func.py` | 22 |
| `harmonize/supplier_support_func/hm_mcm_trans_func.py` | 70, 78, 103, 146, 157 |
| `harmonize/supplier_support_func/hm_got_trans_func.py` | 23, 80 |
| `harmonize/supplier_support_func/hm_srf_trans_func.py` | 50, 58, 84, 129 |
| `harmonize/supplier_support_func/hm_sz_trans_func.py` | 9, 18, 28 |
| `harmonize/supplier_support_func/hm_tru_trans_func.py` | 36, 44, 101 |

**Fix:** Replace `except:` with `except Exception as e:` at minimum, and log the error.

### ERR-02: Silent failures in transform functions
All supplier transform functions return `None` on error without any logging. When a transformation fails, there is no trace of what went wrong.

**Files:** All `hm_*_trans_func.py` files in `harmonize/supplier_support_func/`.

**Fix:** Add `logging.error(f"Transform failed: {e}")` in each except block.

---

## P2: Code Duplication

### DUP-01: Near-identical config classes in hm_import_data.py
**File:** `harmonize/hm_import_data.py`

8 classes follow the exact same pattern (`__init__`, `get_sheet_names()`, `get_raw_data()`):
- `cfg_mcm_std_01` (line 9)
- `cfg_mcm_std_02` (line 42)
- `cfg_mcm_exp_02` (line 75)
- `cfg_mcm_xls_01` (line 109)
- `cfg_srf_std_01` (line 145 & 178 - duplicate!)
- `cfg_got_std_01` (line 216)
- `cfg_tru_std_01` (line 254)
- `cfg_got_c32_01` (line 292)

**Fix:** Create a `BaseSupplierConfig` base class. Each supplier config overrides only what differs (sheet name logic, read parameters). Eliminates hundreds of duplicated lines.

---

## P3: Robustness

### ROB-01: `globals()` for dynamic function dispatch
**Files:**
- `harmonize/hm_import_data.py:380-381, 418`
- `harmonize/supplier_support_func/hm_gen_trans_func.py:57-59, 68-69`

```python
cfg_cls = globals()[match_configs_df.loc[indx, 'Config_id']](file_path)
```

**Risk:** If the ETL config Excel contains an unexpected string, this could call arbitrary functions. Fragile because it depends on all needed functions being imported into the module's global namespace.

**Fix:** Use an explicit registry dict: `CONFIGS = {"cfg_mcm_std_01": cfg_mcm_std_01, ...}`.

### ROB-02: Wildcard imports everywhere (40+ occurrences)
Nearly every module uses `from ... import *`, making it impossible to trace where functions originate and risking namespace collisions.

**Key examples:**
- `src/file_handling.py:1-3` imports `*` from 3 modules
- `src/clear_backlog.py:3-5` imports `*` from 3 modules
- `harmonize/supplier_support_func/hm_gen_trans_func.py:3-9` imports `*` from 7 modules
- `main.py:6-11` imports `*` from 5 modules

**Fix:** Replace with explicit imports (`from .dependencies import Path, pd, json, ...`).

### ROB-03: Hardcoded base path requires source code edits
**File:** `src/paths.py:17`
```python
base_path = Path(r"C:\Users\WYBT00P\OneDrive - Volkswagen AG\...")
```
**Fix:** Support environment variable or `.env` file: `base_path = Path(os.environ.get("TB_CPA_BASE_PATH", r"C:\Users\..."))`.

### ROB-04: Hardcoded network paths and cell IDs
**File:** `harmonize_SZ_run.py:27-28`
```python
sz_raw_dir = r"//vw.vwg/vwdfs/PowerCo/Salzgitter/..."
sz_cells = ["096_006_P_019", "096_006_P_020", "096_006_P_021"]
```
**Fix:** Move to config file or accept as command-line arguments.

### ROB-05: Magic number without explanation
**File:** `src/file_handling.py:363`
```python
if abs(dest_file.stat().st_size - src_file.stat().st_size) <= 10000:
```
**Fix:** Extract to named constant: `SIZE_TOLERANCE_BYTES = 10_000`.

---

## P4: Quality of Life

### QOL-01: Debug print statements left in production code
| File | Line | Content |
|------|------|---------|
| `src/extract_archive.py` | 33 | `print('exception')` |
| `src/file_handling.py` | 68 | `print([p.name for p in all_files if ...])` |
| `harmonize/supplier_support_func/hm_general_support.py` | 131 | `print(unify_col, ' :: ', file_col)` |

**Fix:** Replace with `logging.debug()`.

### QOL-02: No log rotation
**File:** `main.py:36`
```python
logging.basicConfig(filename=debug_path / "debug_logfile.log", level=logging.DEBUG, ...)
```
**Fix:** Use `RotatingFileHandler` to prevent unbounded log growth.

### QOL-03: No unit tests
The project has no test directory or test files. Functions that are pure and testable:
- All transform functions in `harmonize/supplier_support_func/`
- File comparison functions in `src/file_handling.py` (`compare_files_shallow`, `compare_files_hash_if_same`)
- Config matching in `harmonize/hm_import_data.py` (`find_matching_config`)
- Data cleaning in `harmonize/supplier_support_func/hm_general_support.py` (`gen_clean_datasheet`)

### QOL-04: No config validation on load
Config files (`format_config.yaml`, `supplier_data_ETL_config.xlsx`) are loaded without validating required keys/columns exist. A typo in the config would cause cryptic errors deep in the pipeline.

---

## Summary

| Priority | Count | Description |
|----------|-------|-------------|
| P0 - Bugs | 3 | Logic error, duplicate class, undefined variable |
| P1 - Error Handling | 26+ locations | Bare excepts, silent failures |
| P2 - Code Duplication | 8 classes | Near-identical config classes |
| P3 - Robustness | 5 | globals(), wildcard imports, hardcoded paths |
| P4 - Quality of Life | 4 | Print stmts, no log rotation, no tests, no config validation |
