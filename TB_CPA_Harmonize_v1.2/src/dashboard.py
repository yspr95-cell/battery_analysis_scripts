"""
dashboard.py  —  TB_CPA_Harmonize v1.2
Generates a self-contained HTML dashboard from the TraceLog DataFrame.

Multi-PC: merges all harmonize_trace_log_*.xlsx from 06_Logs/pc_logs/ before
aggregating, so the dashboard reflects all PCs that processed files.
"""

import json
from pathlib import Path
from datetime import datetime
import pandas as pd


class DashboardGenerator:
    """
    Merges per-PC trace logs from logs_path/pc_logs/ and writes
    harmonize_dashboard.html to the Logs folder.

    Usage:
        gen = DashboardGenerator(trace_log, logs_path)
        gen.generate(logs_path / "harmonize_dashboard.html")
    """

    def __init__(self, trace_log, logs_path: Path = None):
        # Merge all per-PC log files if logs_path provided (multi-PC mode)
        if logs_path is not None:
            pc_logs_dir = logs_path / "pc_logs"
            frames = []
            if pc_logs_dir.exists():
                for p in sorted(pc_logs_dir.glob("harmonize_trace_log_*.xlsx")):
                    try:
                        frames.append(pd.read_excel(p, dtype=str))
                    except Exception as e:
                        print(f"[Dashboard] Warning: could not read {p.name}: {e}")
            if frames:
                self._df = pd.concat(frames, ignore_index=True)
            else:
                self._df = trace_log.df.copy()
        else:
            self._df = trace_log.df.copy()

    def generate(self, output_path: Path):
        df = self._df

        # ── Aggregate per-cell summary ─────────────────────────────────────
        if df.empty:
            cell_summary = []
            total_stats = {"total": 0, "harmonized": 0, "skipped": 0, "failed": 0,
                           "ok": 0, "modified": 0, "deleted": 0}
        else:
            groups = df.groupby("Cell_ID")
            cell_summary = []
            for cell_id, grp in groups:
                cell_summary.append({
                    "cell_id":    cell_id,
                    "total":      len(grp),
                    "harmonized": int((grp["Status"] == "Harmonized").sum()),
                    "skipped":    int((grp["Status"] == "Skipped").sum()),
                    "failed":     int(grp["Status"].isin(["Failed", "No_config"]).sum()),
                    "ok":         int((grp["Current_status"] == "OK").sum()),
                    "modified":   int((grp["Current_status"] == "Modified").sum()),
                    "deleted":    int((grp["Current_status"] == "Deleted").sum()),
                    "files":      _build_file_rows(grp),
                })
            total_stats = {
                "total":      len(df),
                "harmonized": int((df["Status"] == "Harmonized").sum()),
                "skipped":    int((df["Status"] == "Skipped").sum()),
                "failed":     int(df["Status"].isin(["Failed", "No_config"]).sum()),
                "ok":         int((df["Current_status"] == "OK").sum()),
                "modified":   int((df["Current_status"] == "Modified").sum()),
                "deleted":    int((df["Current_status"] == "Deleted").sum()),
            }

        run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        html = _build_html(cell_summary, total_stats, run_ts)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(html, encoding="utf-8")
        print(f"[Dashboard] Generated → {output_path.name}")


# ── HTML builder ──────────────────────────────────────────────────────────────

def _build_file_rows(grp: pd.DataFrame) -> list[dict]:
    rows = []
    for _, r in grp.iterrows():
        rows.append({
            "file_name":      str(r.get("File_name", "—")),
            "supplier":       str(r.get("Supplier", "—")),
            "config":         str(r.get("Config_used", "—")),
            "status":         str(r.get("Status", "—")),
            "skip_reason":    str(r.get("Skip_reason", "—")),
            "row_count":      str(r.get("Row_count", "—")),
            "file_size_kb":   str(r.get("File_size_KB", "—")),
            "date_harm":      str(r.get("Date_harmonized", "—")),
            "current_status": str(r.get("Current_status", "—")),
            "error":          str(r.get("Error_message", "—")),
            "run_ts":         str(r.get("Run_timestamp", "—")),
        })
    return rows


def _build_html(cell_summary: list, total_stats: dict, run_ts: str) -> str:
    data_json = json.dumps({"cells": cell_summary, "totals": total_stats}, indent=2)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Harmonize Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
<style>
  :root {{
    --blue-dark:  #1F4E79;
    --blue-mid:   #2E75B6;
    --blue-light: #D6E4F0;
    --green:      #70AD47;
    --orange:     #ED7D31;
    --red:        #FF4C4C;
    --grey-bg:    #F4F7FB;
    --grey-line:  #DEE2E6;
    --text:       #212529;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Segoe UI', Arial, sans-serif; background: var(--grey-bg); color: var(--text); }}

  /* ── Header ── */
  .header {{
    background: linear-gradient(135deg, var(--blue-dark) 0%, var(--blue-mid) 100%);
    color: white; padding: 20px 32px; display: flex; align-items: center; gap: 24px;
  }}
  .header h1 {{ font-size: 1.5rem; font-weight: 700; letter-spacing: .5px; }}
  .header .subtitle {{ font-size: 0.85rem; opacity: .8; margin-top: 4px; }}

  /* ── Summary cards ── */
  .cards {{ display: flex; gap: 16px; padding: 20px 32px; flex-wrap: wrap; }}
  .card {{
    background: white; border-radius: 10px; padding: 16px 24px;
    box-shadow: 0 2px 8px rgba(0,0,0,.07); min-width: 140px; flex: 1;
    border-top: 4px solid var(--blue-mid);
  }}
  .card.green  {{ border-top-color: var(--green); }}
  .card.orange {{ border-top-color: var(--orange); }}
  .card.red    {{ border-top-color: var(--red); }}
  .card .val {{ font-size: 2rem; font-weight: 700; }}
  .card .lbl {{ font-size: 0.78rem; color: #6c757d; margin-top: 4px; text-transform: uppercase; letter-spacing: .5px; }}

  /* ── Chart ── */
  .chart-wrap {{
    background: white; border-radius: 10px; margin: 0 32px 24px;
    padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,.07);
  }}
  .chart-wrap h2 {{ font-size: 1rem; color: var(--blue-dark); margin-bottom: 14px; }}
  .chart-wrap canvas {{ max-height: 280px; }}

  /* ── Cell table ── */
  .section {{ margin: 0 32px 32px; }}
  .section h2 {{ font-size: 1rem; color: var(--blue-dark); margin-bottom: 10px; }}
  table {{ width: 100%; border-collapse: collapse; background: white;
           border-radius: 10px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,.07); }}
  th {{
    background: var(--blue-dark); color: white; text-align: left;
    padding: 10px 14px; font-size: .82rem; text-transform: uppercase; letter-spacing: .4px;
    cursor: pointer; user-select: none;
  }}
  th:hover {{ background: var(--blue-mid); }}
  td {{ padding: 10px 14px; font-size: .88rem; border-bottom: 1px solid var(--grey-line); }}
  tr.cell-row {{ cursor: pointer; transition: background .15s; }}
  tr.cell-row:hover {{ background: var(--blue-light); }}
  tr.cell-row td:first-child::before {{ content: '▶  '; font-size: .7rem; color: var(--blue-mid); }}
  tr.cell-row.open td:first-child::before {{ content: '▼  '; }}

  /* ── Detail rows ── */
  tr.detail-row td {{ padding: 0; background: #f9fbfd; }}
  .detail-inner {{ padding: 12px 24px 16px 40px; overflow-x: auto; }}
  .detail-inner table {{ box-shadow: none; border-radius: 6px; }}
  .detail-inner th {{ background: #4472C4; font-size: .76rem; }}
  .detail-inner td {{ font-size: .80rem; }}

  /* status badges */
  .badge {{
    display: inline-block; padding: 2px 8px; border-radius: 4px;
    font-size: .76rem; font-weight: 600;
  }}
  .badge-Harmonized   {{ background: #C6EFCE; color: #276221; }}
  .badge-Skipped      {{ background: #FFEB9C; color: #7D5A00; }}
  .badge-Failed,
  .badge-No_config    {{ background: #FFC7CE; color: #9C0006; }}
  .badge-OK           {{ background: #C6EFCE; color: #276221; }}
  .badge-Modified     {{ background: #FFEB9C; color: #7D5A00; }}
  .badge-Deleted      {{ background: #FFC7CE; color: #9C0006; }}
  .badge-Not_applicable {{ background: #F2F2F2; color: #666; }}
</style>
</head>
<body>

<div class="header">
  <div>
    <h1>TB_CPA Harmonization Dashboard</h1>
    <div class="subtitle">Generated: {run_ts} &nbsp;|&nbsp; TB_CPA_Harmonize v1.2</div>
  </div>
</div>

<div class="cards" id="cards"></div>

<div class="chart-wrap">
  <h2>Files per Cell — Harmonized / Skipped / Failed</h2>
  <canvas id="barChart"></canvas>
</div>

<div class="section">
  <h2>Cell Summary &nbsp;<small style="font-weight:normal;color:#6c757d">(click a row to expand file details)</small></h2>
  <table id="cellTable">
    <thead>
      <tr>
        <th onclick="sortTable(0)">Cell ID ⇅</th>
        <th onclick="sortTable(1)">Total ⇅</th>
        <th onclick="sortTable(2)">Harmonized ⇅</th>
        <th onclick="sortTable(3)">Skipped ⇅</th>
        <th onclick="sortTable(4)">Failed ⇅</th>
        <th onclick="sortTable(5)">OK ⇅</th>
        <th onclick="sortTable(6)">Modified ⇅</th>
        <th onclick="sortTable(7)">Deleted ⇅</th>
      </tr>
    </thead>
    <tbody id="cellTbody"></tbody>
  </table>
</div>

<script>
const DATA = {data_json};

// ── Cards ──────────────────────────────────────────────────────────────────
(function renderCards() {{
  const t = DATA.totals;
  const defs = [
    {{ val: t.total,      lbl: 'Total Files',  cls: '' }},
    {{ val: t.harmonized, lbl: 'Harmonized',   cls: 'green' }},
    {{ val: t.skipped,    lbl: 'Skipped',      cls: 'orange' }},
    {{ val: t.failed,     lbl: 'Failed',       cls: 'red' }},
    {{ val: t.ok,         lbl: 'Current: OK',  cls: 'green' }},
    {{ val: t.modified,   lbl: 'Modified',     cls: 'orange' }},
    {{ val: t.deleted,    lbl: 'Deleted',      cls: 'red' }},
  ];
  const wrap = document.getElementById('cards');
  defs.forEach(d => {{
    wrap.innerHTML += `<div class="card ${{d.cls}}"><div class="val">${{d.val}}</div><div class="lbl">${{d.lbl}}</div></div>`;
  }});
}})();

// ── Bar Chart ──────────────────────────────────────────────────────────────
(function renderChart() {{
  const cells = DATA.cells;
  const labels = cells.map(c => c.cell_id);
  new Chart(document.getElementById('barChart'), {{
    type: 'bar',
    data: {{
      labels,
      datasets: [
        {{ label: 'Harmonized', data: cells.map(c => c.harmonized), backgroundColor: '#70AD47' }},
        {{ label: 'Skipped',    data: cells.map(c => c.skipped),    backgroundColor: '#ED7D31' }},
        {{ label: 'Failed',     data: cells.map(c => c.failed),     backgroundColor: '#FF4C4C' }},
      ]
    }},
    options: {{
      responsive: true,
      plugins: {{ legend: {{ position: 'top' }} }},
      scales: {{
        x: {{ stacked: true }},
        y: {{ stacked: true, beginAtZero: true, ticks: {{ stepSize: 1 }} }}
      }}
    }}
  }});
}})();

// ── Cell table + expand ────────────────────────────────────────────────────
const tbody = document.getElementById('cellTbody');

function badge(val) {{
  return `<span class="badge badge-${{val.replace(/[^a-zA-Z_]/g,'')}}">${{val}}</span>`;
}}

function renderFileDetail(files) {{
  const cols = ['file_name','supplier','config','status','skip_reason','row_count','file_size_kb','date_harm','current_status','error','run_ts'];
  const heads = ['File','Supplier','Config','Status','Skip Reason','Rows','Size KB','Date Harmonized','Current Status','Error','Run Timestamp'];
  let html = '<div class="detail-inner"><table><thead><tr>';
  heads.forEach(h => html += `<th>${{h}}</th>`);
  html += '</tr></thead><tbody>';
  files.forEach(f => {{
    html += '<tr>';
    cols.forEach(c => {{
      const v = f[c] ?? '—';
      if (c === 'status' || c === 'current_status') html += `<td>${{badge(v)}}</td>`;
      else html += `<td>${{v}}</td>`;
    }});
    html += '</tr>';
  }});
  html += '</tbody></table></div>';
  return html;
}}

DATA.cells.forEach((cell, i) => {{
  // Summary row
  const tr = document.createElement('tr');
  tr.className = 'cell-row';
  tr.innerHTML = `
    <td>${{cell.cell_id}}</td>
    <td>${{cell.total}}</td>
    <td>${{cell.harmonized}}</td>
    <td>${{cell.skipped}}</td>
    <td>${{cell.failed}}</td>
    <td>${{cell.ok}}</td>
    <td>${{cell.modified}}</td>
    <td>${{cell.deleted}}</td>
  `;
  tbody.appendChild(tr);

  // Detail row (hidden by default)
  const detailTr = document.createElement('tr');
  detailTr.className = 'detail-row';
  detailTr.style.display = 'none';
  const detailTd = document.createElement('td');
  detailTd.colSpan = 8;
  detailTd.innerHTML = renderFileDetail(cell.files);
  detailTr.appendChild(detailTd);
  tbody.appendChild(detailTr);

  tr.addEventListener('click', () => {{
    const open = detailTr.style.display !== 'none';
    detailTr.style.display = open ? 'none' : 'table-row';
    tr.classList.toggle('open', !open);
  }});
}});

// ── Sort ───────────────────────────────────────────────────────────────────
let _sortDir = {{}};
function sortTable(colIdx) {{
  const rows = Array.from(tbody.querySelectorAll('tr.cell-row'));
  const dir = (_sortDir[colIdx] = !_sortDir[colIdx]);
  rows.sort((a, b) => {{
    const va = a.cells[colIdx].textContent.trim();
    const vb = b.cells[colIdx].textContent.trim();
    const na = parseFloat(va), nb = parseFloat(vb);
    const cmp = isNaN(na) ? va.localeCompare(vb) : na - nb;
    return dir ? cmp : -cmp;
  }});
  // Re-insert rows with their paired detail row
  rows.forEach(r => {{
    const next = r.nextElementSibling;
    tbody.appendChild(r);
    if (next && next.classList.contains('detail-row')) tbody.appendChild(next);
  }});
}}
</script>
</body>
</html>"""
