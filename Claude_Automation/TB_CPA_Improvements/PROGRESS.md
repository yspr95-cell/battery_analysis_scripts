# TB_CPA Improvements — Progress Tracker

## Phase 1 — TB_CPA_Harmonize improvements (COMPLETE)

| # | Issue | Status | Location |
|---|-------|--------|----------|
| 1 | Auto-detect data sheet (largest row count fallback) | DONE | `TB_CPA_Harmonize/harmonize/hm_import_data.py` + `hm_general_support.py` |
| 2 | Auto-detect header row (string-label density heuristic) | DONE | `TB_CPA_Harmonize/harmonize/supplier_support_func/hm_general_support.py` |
| 3 | Separate into independent repos (Extraction + Harmonize) | DONE | `TB_CPA_Extraction/` and `TB_CPA_Harmonize/` created |
| 4 | Better harmonization approach suggestion | DONE → Phase 2 | Resulted in full redesign as TB_CPA_Harmonize_v2 |

---

## Phase 2 — TB_CPA_Harmonize_v2 (COMPLETE)

Supplier-agnostic redesign. No supplier detection. Data-driven pipeline.

### What was built

| File | Status | Notes |
|------|--------|-------|
| `TB_CPA_Harmonize_v2/src/paths.py` | DONE | Duplicate of v1; user sets `base_path` |
| `TB_CPA_Harmonize_v2/src/dependencies.py` | DONE | Harmonize-only imports (no patool/winsound) |
| `TB_CPA_Harmonize_v2/harmonize/column_registry.py` | DONE | 16-column schema + keyword tiers |
| `TB_CPA_Harmonize_v2/harmonize/file_inspector.py` | DONE | Format/encoding/sheet/header detection |
| `TB_CPA_Harmonize_v2/harmonize/column_mapper.py` | DONE | Scoring + greedy assignment → MappingResult |
| `TB_CPA_Harmonize_v2/harmonize/transform_engine.py` | DONE (stubs) | 6 transform classes; 2 stubs remain |
| `TB_CPA_Harmonize_v2/harmonize/harmonizer.py` | DONE | Full orchestrator → HarmonizeResult |
| `TB_CPA_Harmonize_v2/harmonize/overrides/override_loader.py` | DONE | YAML load + merge into MappingResult |
| `TB_CPA_Harmonize_v2/harmonize/overrides/examples/mcm_example.yaml` | DONE | Reference override for MCM files |
| `TB_CPA_Harmonize_v2/harmonize_run.py` | DONE | Batch entry point, status JSON |
| `TB_CPA_Harmonize_v2/requirements.txt` | DONE | |
| `TB_CPA_Harmonize_v2/README.md` | DONE | User guide |
| `TB_CPA_Harmonize_v2/ARCHITECTURE.md` | DONE | Full architecture map |

### Test results (verified working)

| Supplier | Format | Result | Notes |
|----------|--------|--------|-------|
| MCM | xlsx (1M rows, `d_hms_ms` time) | `is_valid=True`, 10 cols mapped at conf 1.0 | Header row auto-detected correctly |
| GOTION | xlsx | `is_valid=True`, 13 cols incl. Date_time + Unix_time | Mixed microsecond datetime handled |
| SZ | CSV (semicolon, `#` comment header) | `is_valid=True`, Step_name inferred from current | `comment='#'` fix applied |

### Bugs found and fixed during build

| Bug | Fix |
|-----|-----|
| Unit-stripping regex removed `(V)` from `U(V)`, leaving `u` with no keyword match | Removed unit stripping from `_normalise()` — keywords already include unit forms |
| `pd.to_datetime` failed on mixed microsecond timestamps (`2025-01-01.500000`) | Added `format='mixed'` to both detector and transformer |
| SZ CSV `#` comment line caused column count mismatch (3 fields vs 12) | Added `comment='#'` to `pd.read_csv()` in `_load_csv()` |
| `Step_name` marked mandatory caused SZ to fail (SZ has no step name column) | Changed `Step_name` mandatory → False; Harmonizer infers from current sign |
| `Step_Capacity_Ah` scored same as `Total_Capacity_Ah` for SZ | Added `step_capacity_ah`, `step_capacity` etc. to `keywords_exact/high` |
| `Total_time_s` didn't match `Time_s` (SZ column name) | Added `time_s`, `total_time_s` etc. to `keywords_exact` |

---

## What Remains To Do

### High priority — stubs in transform_engine.py

These two methods raise `NotImplementedError`. Needed for MCM/SRF files if time needs to be reconstructed from step + test columns.

**1. `TimeTransformer.build_total_time_from_step_plus_test()`**
- Location: `TB_CPA_Harmonize_v2/harmonize/transform_engine.py` ~line 210
- Logic: cumsum of step_time deltas (clipped ≥0 on resets), corrected at each step boundary using test_time
- Reference: `hm_srf_trans_func.py → srf_transform_reltime()` in TB_CPA_Harmonize/

**2. `TimeTransformer.build_datetime_with_gap_correction()`**
- Location: `TB_CPA_Harmonize_v2/harmonize/transform_engine.py` ~line 230
- Logic: base_datetime + rel_seconds; where abs_time_delta - rel_time_delta > 60s, add abs_time_delta to rel_seconds from that index onward
- Reference: `hm_tru_trans_func.py → tru_transform_unixtime()` in TB_CPA_Harmonize/

### Medium priority — wire stubs into Harmonizer

After implementing the stubs, the Harmonizer's `_build_unified()` needs to call them when two time columns are present (step time + test time). Currently only single-column time is handled.

### Low priority — additional override YAMLs

Create override YAMLs for known file patterns (SRF, TRURON) once real files are tested.

---

## Key Design Decisions (for reference)

| Decision | Choice | Reason |
|----------|--------|--------|
| Supplier detection | Removed entirely | Source of brittleness; all branching is data-driven |
| ETL Excel config | Removed | Replaced by column_registry.py + YAML overrides |
| Override system | Additive only (never replaces) | Auto-detection should always run first |
| Step_name mandatory | No | Can always be inferred from current sign |
| Unit suffix stripping | Removed from normalise() | Keywords include unit forms (e.g. `u(v)`) |
| Scoring thresholds | 0.3 mandatory / 0.4 optional | Low enough to catch unusual names, high enough to avoid false positives |
| Original repos | Untouched | TB_CPA_Harmonize/ and TB_CPA_Extraction/ work independently |

---

## File Locations Summary

```
battery_analysis_scripts/
├── TB_CPA_TestDataAutomation/     original — DO NOT MODIFY
├── TB_CPA_Extraction/             Phase 1 — extraction only, independent repo
├── TB_CPA_Harmonize/              Phase 1 — harmonize with auto-detect improvements
├── TB_CPA_Harmonize_v2/           Phase 2 — supplier-agnostic redesign
└── Claude_Automation/
    ├── ISSUES.md
    └── TB_CPA_Improvements/
        ├── NOTES.md               design decisions log
        └── PROGRESS.md            this file
```
