#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import os
import re
import sqlite3
import tempfile
from pathlib import Path
from typing import Optional

import pandas as pd


# -----------------------------
# Paths
# -----------------------------
BASE = Path(__file__).resolve().parent.parent

# Keep database path aligned with ui/app.py so refresh and UI use the same SQLite file
DEFAULT_DATA_DIR = Path(tempfile.gettempdir()) / "planner_app"
DATA_DIR = Path(os.getenv("PLANNER_DATA_DIR", DEFAULT_DATA_DIR))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "planner_v2.db"

# Excel source files live in the repo under incoming/
DEFAULT_INCOMING_DIR = BASE / "incoming"
INCOMING_DIR = Path(os.getenv("PLANNER_INCOMING_DIR", DEFAULT_INCOMING_DIR))

BU_XLSX = INCOMING_DIR / "BU Shortages Export - Details.xlsx"
HRS_XLSX = INCOMING_DIR / "Scheduled Hours - Sheet1.xlsx"
LBR_XLSX = INCOMING_DIR / "DeptLbr2026.xlsx"
SDD_XLSX = INCOMING_DIR / "Scheduled Deliveries Details Export.xlsx"


# -----------------------------
# Helpers
# -----------------------------
def _norm_str(x: object) -> str:
    return "" if x is None else str(x).strip()


def stable_int_id(*parts: object) -> int:
    s = "|".join("" if p is None else str(p) for p in parts)
    h = hashlib.md5(s.encode("utf-8")).digest()
    u64 = int.from_bytes(h[:8], byteorder="big", signed=False)
    return u64 & 0x7FFFFFFFFFFFFFFF  # signed 63-bit


def wo_norm(x: object) -> str:
    s = _norm_str(x)
    if not s:
        return ""
    s = re.sub(r"\(.*?\)", "", s).strip()
    return s


def digits_only(x: object) -> Optional[int]:
    s = "" if x is None else str(x)
    d = re.sub(r"\D", "", s)
    return int(d) if d else None


def parse_date_iso(x: object) -> Optional[str]:
    ts = pd.to_datetime(x, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.date().isoformat()


def to_float(x: object) -> float:
    """Robust numeric parser for exports that may include $, commas, blanks, or parentheses."""
    try:
        if x is None:
            return 0.0
        s = str(x).strip()
        if not s or s.lower() in {"nan", "none"}:
            return 0.0
        # Handle accounting negatives like (1,234.56)
        neg = False
        if s.startswith("(") and s.endswith(")"):
            neg = True
            s = s[1:-1]
        # Remove common formatting
        s = s.replace("$", "").replace(",", "").strip()
        if not s:
            return 0.0
        v = float(s)
        return -v if neg else v
    except Exception:
        return 0.0


def first_sheet_name(path: Path) -> str:
    xl = pd.ExcelFile(path)
    return xl.sheet_names[0]


def find_title_row(df_raw: pd.DataFrame, required_cols: list[str]) -> int:
    req = [c.strip().lower() for c in required_cols]
    max_scan = min(len(df_raw), 80)
    for i in range(max_scan):
        row = df_raw.iloc[i].astype(str).str.strip().str.lower().tolist()
        if all(any(r == cell for cell in row) for r in req):
            return i
    raise ValueError(f"Could not find header row with required columns: {required_cols}")


def read_export_with_title_row(path: Path, sheet_name: str, required_cols: list[str]) -> pd.DataFrame:
    df_raw = pd.read_excel(path, sheet_name=sheet_name, header=None, dtype=str)
    header_i = find_title_row(df_raw, required_cols)
    headers = df_raw.iloc[header_i].astype(str).str.strip().tolist()
    df = df_raw.iloc[header_i + 1 :].copy()
    df.columns = headers
    df = df.dropna(how="all")
    return df


def validate_input_files() -> None:
    required = [BU_XLSX, HRS_XLSX, LBR_XLSX, SDD_XLSX]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing required refresh input files:\n" + "\n".join(missing)
        )


# -----------------------------
# Work Centers
# -----------------------------
ASSY_CTRS = {"ASM", "ASSY"}  # ignore FASY
TEST_CTRS = {
    "TEST",
    "SKYLF",
    "RED257",
    "RED282",
    "SKYHF",
    "SKYATF",
    "SKYATR",
    "SKYATS",
    "REDBRK",
    "RED606",
    "SKYAPV",
    "SKYTRM",
    "SKYRES",
}
IGNORE_CTRS = {"FASY"}


# -----------------------------
# Schema (create base tables)
# -----------------------------
SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS main_line (
  main_id        INTEGER PRIMARY KEY,
  delivery       TEXT,
  customer       TEXT,
  bu             TEXT,
  type           TEXT,
  parent_part    TEXT,
  parent_wo      TEXT,
  parent_wo_norm TEXT,
  due_date       TEXT,
  dock_date      TEXT,
  wo_qty         REAL,
  line_value     REAL,
  short_free    INTEGER DEFAULT 0,
  is_active      INTEGER DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_main_active ON main_line(is_active);
CREATE INDEX IF NOT EXISTS idx_main_delivery ON main_line(delivery);
CREATE INDEX IF NOT EXISTS idx_main_wo_norm ON main_line(parent_wo_norm);

CREATE TABLE IF NOT EXISTS main_targets (
  main_id       INTEGER PRIMARY KEY,
  assy_date     TEXT,
  test_date     TEXT,
  stock_date    TEXT,
  updated_at    TEXT,
  FOREIGN KEY(main_id) REFERENCES main_line(main_id)
);

CREATE TABLE IF NOT EXISTS sub_object (
  sub_key       TEXT PRIMARY KEY,
  sub_part      TEXT,
  sub_source    TEXT,
  source_digits INTEGER,
  sub_resp      TEXT,
  sub_eta_date  TEXT,
  sub_desc      TEXT,
  comp_remaining TEXT,
  is_active     INTEGER DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_sub_active ON sub_object(is_active);
CREATE INDEX IF NOT EXISTS idx_sub_digits ON sub_object(source_digits);

CREATE TABLE IF NOT EXISTS peg_sub (
  main_id   INTEGER,
  sub_key   TEXT,
  is_active INTEGER DEFAULT 1,
  PRIMARY KEY(main_id, sub_key),
  FOREIGN KEY(main_id) REFERENCES main_line(main_id),
  FOREIGN KEY(sub_key) REFERENCES sub_object(sub_key)
);

CREATE INDEX IF NOT EXISTS idx_peg_active ON peg_sub(is_active);

CREATE TABLE IF NOT EXISTS sub_plan (
  sub_key       TEXT PRIMARY KEY,
  clear_date    TEXT,
  assy_date     TEXT,
  test_date     TEXT,
  stock_date    TEXT,
  status        TEXT DEFAULT 'open',
  updated_at    TEXT,
  FOREIGN KEY(sub_key) REFERENCES sub_object(sub_key)
);

CREATE TABLE IF NOT EXISTS op_hours (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  wo_pl         TEXT,
  wo_pl_norm    TEXT,
  wo_pl_digits  INTEGER,
  part_number   TEXT,
  wk_ctr        TEXT,
  op_seq        TEXT,
  qty           REAL,
  rem_hrs       REAL,
  start_date    TEXT,
  complete_date TEXT,
  rem_setup_hrs REAL
);

CREATE INDEX IF NOT EXISTS idx_hours_norm ON op_hours(wo_pl_norm);
CREATE INDEX IF NOT EXISTS idx_hours_digits ON op_hours(wo_pl_digits);
CREATE INDEX IF NOT EXISTS idx_hours_wkctr ON op_hours(wk_ctr);

CREATE TABLE IF NOT EXISTS part_recent_employee (
  part_number TEXT,
  emp_name    TEXT,
  emp_num     TEXT,
  rank        INTEGER,
  PRIMARY KEY(part_number, rank)
);

CREATE TABLE IF NOT EXISTS part_recent_employee_dept (
  part_number TEXT,
  dept        TEXT,          -- 'assy' or 'test'
  emp_name    TEXT,
  emp_num     TEXT,
  rank        INTEGER,
  PRIMARY KEY(part_number, dept, rank)
);

CREATE INDEX IF NOT EXISTS idx_pred_part ON part_recent_employee_dept(part_number);
CREATE INDEX IF NOT EXISTS idx_pred_dept ON part_recent_employee_dept(dept);
"""


def _table_cols(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table});").fetchall()}


def migrate_schema(conn: sqlite3.Connection):
    """
    Add new columns safely if running against an older DB.
    """
    # main_line: add short_free if missing
    cols = _table_cols(conn, "main_line")
    if "short_free" not in cols:
        conn.execute("ALTER TABLE main_line ADD COLUMN short_free INTEGER DEFAULT 0;")

    # main_targets: add assy_emp/test_emp if missing
    cols = _table_cols(conn, "main_targets")
    if "assy_emp" not in cols:
        conn.execute("ALTER TABLE main_targets ADD COLUMN assy_emp TEXT;")
    if "test_emp" not in cols:
        conn.execute("ALTER TABLE main_targets ADD COLUMN test_emp TEXT;")

    # sub_plan: add assy_emp/test_emp if missing
    cols = _table_cols(conn, "sub_plan")
    if "assy_emp" not in cols:
        conn.execute("ALTER TABLE sub_plan ADD COLUMN assy_emp TEXT;")
    if "test_emp" not in cols:
        conn.execute("ALTER TABLE sub_plan ADD COLUMN test_emp TEXT;")

    # sub_object: add comp_remaining if missing (BU "Comp. Remaining")
    cols = _table_cols(conn, "sub_object")
    if "comp_remaining" not in cols:
        conn.execute("ALTER TABLE sub_object ADD COLUMN comp_remaining TEXT;")

    # op_hours: ensure rem_setup_hrs exists
    cols = _table_cols(conn, "op_hours")
    if "rem_setup_hrs" not in cols:
        conn.execute("ALTER TABLE op_hours ADD COLUMN rem_setup_hrs REAL;")

    # dept-specific recent employees (new table)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS part_recent_employee_dept (
          part_number TEXT,
          dept        TEXT,
          emp_name    TEXT,
          emp_num     TEXT,
          rank        INTEGER,
          PRIMARY KEY(part_number, dept, rank)
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pred_part ON part_recent_employee_dept(part_number);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pred_dept ON part_recent_employee_dept(dept);")


# -----------------------------
# BU refresh
# -----------------------------
def refresh_bu_shortages(conn: sqlite3.Connection, bu_path: Path, sdd_path: Optional[Path] = None) -> tuple[int, int, int]:
    bu_sheet = first_sheet_name(bu_path)
    print(f"BU file: {bu_path.name} | sheet: {bu_sheet}")

    required = ["Delivery", "Customer", "BU", "Type", "Part #", "Work Order"]
    df = read_export_with_title_row(bu_path, bu_sheet, required_cols=required)

    df["Delivery"] = df["Delivery"].astype(str).str.strip()
    df["Customer"] = df.get("Customer", "").astype(str).str.strip()
    df["BU"] = df.get("BU", "").astype(str).str.strip()
    df["Type"] = df.get("Type", "").astype(str).str.strip()
    df["Part #"] = df.get("Part #", "").astype(str).str.strip()
    df["Work Order"] = df.get("Work Order", "").astype(str).str.strip()
    df["wo_norm"] = df["Work Order"].map(wo_norm)

    df["Due Date"] = df.get("Due Date", None)
    df["Dock Date"] = df.get("Dock Date", None)
    df["WO Qty"] = df.get("WO Qty", None)
    df["Value"] = df.get("Value", None)

    df["Comp. Part #"] = df.get("Comp. Part #", "").astype(str).str.strip()
    df["Comp. Source"] = df.get("Comp. Source", "").astype(str).str.strip()
    df["Comp. Resp."] = df.get("Comp. Resp.", "").astype(str).str.strip()
    df["Comp. ETA"] = df.get("Comp. ETA", None)
    df["Comp. Description"] = df.get("Comp. Description", "").astype(str).str.strip()
    df["Comp. Remaining"] = df.get("Comp. Remaining", "").astype(str).str.strip()

    # MAIN grain: Delivery + WO(norm)  (allows WO to show on multiple Deliveries)
    mains = df[
        ["Delivery", "Customer", "BU", "Type", "Part #", "Work Order", "wo_norm", "Due Date", "Dock Date", "WO Qty", "Value"]
    ].drop_duplicates(subset=["Delivery", "wo_norm"]).copy()

    # BU export contains only shortage mains -> not short-free
    mains["short_free"] = 0

    # Optionally add short-free mains from Scheduled Deliveries export
    if sdd_path is not None and Path(sdd_path).exists():
        sdd_sheet = first_sheet_name(Path(sdd_path))
        print(f"SDD file: {Path(sdd_path).name} | sheet: {sdd_sheet}")

        sdd_required = ["Delivery", "Customer", "BU", "Type", "Part #", "Work Order"]
        sdd = read_export_with_title_row(Path(sdd_path), sdd_sheet, required_cols=sdd_required)

        sdd["Delivery"] = sdd["Delivery"].astype(str).str.strip()
        sdd["Customer"] = sdd.get("Customer", "").astype(str).str.strip()
        sdd["BU"] = sdd.get("BU", "").astype(str).str.strip()
        sdd["Type"] = sdd.get("Type", "").astype(str).str.strip()
        sdd["Part #"] = sdd.get("Part #", "").astype(str).str.strip()
        sdd["Work Order"] = sdd.get("Work Order", "").astype(str).str.strip()
        sdd["wo_norm"] = sdd["Work Order"].map(wo_norm)

        # Date/qty/value columns (best effort)
        sdd["Due Date"] = sdd.get("Due Date", None)
        sdd["Dock Date"] = sdd.get("Dock Date", None)
        sdd["WO Qty"] = sdd.get("WO Qty", None)

        # Scheduled Deliveries often does NOT have a single Value column.
        # Your business rule: line_value = sum of bucket columns:
        # In Stock, Final Insp, Final Assy, Test, Assy, Pin Plug, Bond, Kitting, Kit/Short
        def _ci_key(c: object) -> str:
            return re.sub(r"\s+", " ", str(c).strip().lower())

        col_map = {_ci_key(c): c for c in sdd.columns}

        bucket_names = [
            "in stock",
            "final insp",
            "final assy",
            "test",
            "assy",
            "pin plug",
            "bond",
            "kitting",
            "kit/short",
        ]

        present_buckets = [col_map[n] for n in bucket_names if n in col_map]

        if len(present_buckets) > 0:
            # Compute bucket sum as Value
            v = 0.0
            for c in present_buckets:
                v = v + sdd[c].map(to_float)
            sdd["Value"] = v
        else:
            # Fallbacks if buckets are missing
            if "Value" in sdd.columns:
                sdd["Value"] = sdd.get("Value", None)
            elif "Bal Due" in sdd.columns:
                sdd["Value"] = sdd.get("Bal Due", None)
            else:
                sdd["Value"] = None

        sdd_mains = sdd[
            [
                "Delivery",
                "Customer",
                "BU",
                "Type",
                "Part #",
                "Work Order",
                "wo_norm",
                "Due Date",
                "Dock Date",
                "WO Qty",
                "Value",
            ]
        ].drop_duplicates(subset=["Delivery", "wo_norm"]).copy()

        # Anything present in SDD but not present in BU shortages is considered short-free
        bu_keys = set(zip(mains["Delivery"].astype(str), mains["wo_norm"].astype(str)))
        sdd_keys = list(zip(sdd_mains["Delivery"].astype(str), sdd_mains["wo_norm"].astype(str)))
        keep_mask = [k not in bu_keys for k in sdd_keys]
        sdd_mains = sdd_mains.loc[keep_mask].copy()
        sdd_mains["short_free"] = 1

        # Append
        if len(sdd_mains) > 0:
            mains = pd.concat([mains, sdd_mains], ignore_index=True)

    mains["main_id"] = [stable_int_id(r["Delivery"], r["wo_norm"]) for _, r in mains.iterrows()]
    mains["due_date"] = mains["Due Date"].map(parse_date_iso)
    mains["dock_date"] = mains["Dock Date"].map(parse_date_iso)
    mains["wo_qty"] = mains["WO Qty"].map(to_float)
    mains["line_value"] = mains["Value"].map(to_float)

    # SUB keys
    df["source_digits"] = df["Comp. Source"].map(digits_only)

    def mk_sub_key(r) -> str:
        base = f"{r['Comp. Part #']}|{r['source_digits'] or ''}|{r['Comp. Description']}"
        return hashlib.md5(base.encode("utf-8")).hexdigest()[:24]

    df["sub_key"] = df.apply(mk_sub_key, axis=1)

    subs = df[
        [
            "sub_key",
            "Comp. Part #",
            "Comp. Source",
            "source_digits",
            "Comp. Resp.",
            "Comp. ETA",
            "Comp. Description",
            "Comp. Remaining",
        ]
    ].drop_duplicates(subset=["sub_key"]).copy()

    subs["sub_part"] = subs["Comp. Part #"]
    subs["sub_source"] = subs["Comp. Source"]
    subs["sub_resp"] = subs["Comp. Resp."]
    subs["sub_eta_date"] = subs["Comp. ETA"].map(parse_date_iso)
    subs["sub_desc"] = subs["Comp. Description"]
    subs["comp_remaining"] = subs["Comp. Remaining"].astype(str).str.strip()

    pegs = df[["Delivery", "wo_norm", "sub_key"]].dropna().drop_duplicates().copy()
    pegs["main_id"] = [stable_int_id(r["Delivery"], r["wo_norm"]) for _, r in pegs.iterrows()]

    # Soft refresh
    conn.execute("UPDATE main_line SET is_active=0;")
    conn.execute("UPDATE sub_object SET is_active=0;")
    conn.execute("UPDATE peg_sub SET is_active=0;")

    cur = conn.cursor()

    for _, r in mains.iterrows():
        cur.execute(
            """
            INSERT INTO main_line(
              main_id, delivery, customer, bu, type, parent_part, parent_wo, parent_wo_norm,
              due_date, dock_date, wo_qty, line_value, short_free, is_active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(main_id) DO UPDATE SET
              delivery=excluded.delivery,
              customer=excluded.customer,
              bu=excluded.bu,
              type=excluded.type,
              parent_part=excluded.parent_part,
              parent_wo=excluded.parent_wo,
              parent_wo_norm=excluded.parent_wo_norm,
              due_date=excluded.due_date,
              dock_date=excluded.dock_date,
              wo_qty=excluded.wo_qty,
              line_value=excluded.line_value,
              short_free=excluded.short_free,
              is_active=1;
            """,
            (
                int(r["main_id"]),
                _norm_str(r["Delivery"]),
                _norm_str(r["Customer"]),
                _norm_str(r["BU"]),
                _norm_str(r["Type"]),
                _norm_str(r["Part #"]),
                _norm_str(r["Work Order"]),
                _norm_str(r["wo_norm"]),
                r["due_date"],
                r["dock_date"],
                float(r["wo_qty"]),
                float(r["line_value"]),
                int(r.get("short_free", 0)),
            ),
        )

    for _, r in subs.iterrows():
        cur.execute(
            """
            INSERT INTO sub_object(
              sub_key, sub_part, sub_source, source_digits, sub_resp, sub_eta_date, sub_desc, comp_remaining, is_active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(sub_key) DO UPDATE SET
              sub_part=excluded.sub_part,
              sub_source=excluded.sub_source,
              source_digits=excluded.source_digits,
              sub_resp=excluded.sub_resp,
              sub_eta_date=excluded.sub_eta_date,
              sub_desc=excluded.sub_desc,
              comp_remaining=excluded.comp_remaining,
              is_active=1;
            """,
            (
                _norm_str(r["sub_key"]),
                _norm_str(r["sub_part"]),
                _norm_str(r["sub_source"]),
                int(r["source_digits"]) if pd.notna(r["source_digits"]) else None,
                _norm_str(r["sub_resp"]),
                r["sub_eta_date"],
                _norm_str(r["sub_desc"]),
                _norm_str(r.get("comp_remaining", "")),
            ),
        )

    for _, r in pegs.iterrows():
        cur.execute(
            """
            INSERT INTO peg_sub(main_id, sub_key, is_active)
            VALUES (?, ?, 1)
            ON CONFLICT(main_id, sub_key) DO UPDATE SET is_active=1;
            """,
            (int(r["main_id"]), _norm_str(r["sub_key"])),
        )

    return len(mains), len(subs), len(pegs)


# -----------------------------
# Hours refresh
# -----------------------------
def refresh_hours(conn: sqlite3.Connection, hrs_path: Path) -> int:
    hrs_sheet = first_sheet_name(hrs_path)
    print(f"HRS file: {hrs_path.name} | sheet: {hrs_sheet}")

    required = ["WO/PL", "Part Number", "Wk Ctr", "Op Seq", "Qty", "Rem hrs"]
    df = read_export_with_title_row(hrs_path, hrs_sheet, required_cols=required)

    def colfind(name: str) -> str:
        t = name.strip().lower()
        for c in df.columns:
            if str(c).strip().lower() == t:
                return c
        raise KeyError(f"Missing column: {name}")

    wo_col = colfind("WO/PL")
    part_col = colfind("Part Number")
    wk_col = colfind("Wk Ctr")
    op_col = colfind("Op Seq")
    qty_col = colfind("Qty")
    rem_col = colfind("Rem hrs")

    start_col = next((c for c in df.columns if str(c).strip().lower() == "start"), None)
    complete_col = next((c for c in df.columns if str(c).strip().lower() == "complete"), None)
    setup_col = next((c for c in df.columns if str(c).strip().lower() == "rem setup hrs"), None)

    keep = [wo_col, part_col, wk_col, op_col, qty_col, rem_col]
    if start_col:
        keep.append(start_col)
    if complete_col:
        keep.append(complete_col)
    if setup_col:
        keep.append(setup_col)

    df2 = df[keep].copy()
    df2.rename(
        columns={
            wo_col: "wo_pl",
            part_col: "part_number",
            wk_col: "wk_ctr",
            op_col: "op_seq",
            qty_col: "qty",
            rem_col: "rem_hrs",
            start_col: "start" if start_col else start_col,
            complete_col: "complete" if complete_col else complete_col,
            setup_col: "rem_setup_hrs" if setup_col else setup_col,
        },
        inplace=True,
    )

    df2["wo_pl"] = df2["wo_pl"].astype(str).str.strip()
    df2["wo_pl_norm"] = df2["wo_pl"].map(wo_norm)
    df2["wo_pl_digits"] = df2["wo_pl"].map(digits_only)
    df2["part_number"] = df2["part_number"].astype(str).str.strip()
    df2["wk_ctr"] = df2["wk_ctr"].astype(str).str.strip().str.upper()
    df2["op_seq"] = df2["op_seq"].astype(str).str.strip()

    df2["qty"] = df2["qty"].map(to_float)
    df2["rem_hrs"] = df2["rem_hrs"].map(to_float)

    if "rem_setup_hrs" not in df2.columns:
        df2["rem_setup_hrs"] = 0.0
    else:
        df2["rem_setup_hrs"] = df2["rem_setup_hrs"].map(to_float)

    df2["start_date"] = df2["start"].map(parse_date_iso) if "start" in df2.columns else None
    df2["complete_date"] = df2["complete"].map(parse_date_iso) if "complete" in df2.columns else None

    conn.execute("DELETE FROM op_hours;")
    cur = conn.cursor()

    for _, r in df2.iterrows():
        cur.execute(
            """
            INSERT INTO op_hours(
              wo_pl, wo_pl_norm, wo_pl_digits, part_number, wk_ctr, op_seq,
              qty, rem_hrs, start_date, complete_date, rem_setup_hrs
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                _norm_str(r["wo_pl"]),
                _norm_str(r["wo_pl_norm"]),
                int(r["wo_pl_digits"]) if pd.notna(r["wo_pl_digits"]) else None,
                _norm_str(r["part_number"]),
                _norm_str(r["wk_ctr"]),
                _norm_str(r["op_seq"]),
                float(r["qty"]),
                float(r["rem_hrs"]),
                r["start_date"],
                r["complete_date"],
                float(r["rem_setup_hrs"]),
            ),
        )

    return len(df2)


# -----------------------------
# Labor refresh (dept-aware)
# -----------------------------
def refresh_labor(conn: sqlite3.Connection, lbr_path: Path) -> int:
    xl = pd.ExcelFile(lbr_path)
    sheet = "Labor" if "Labor" in xl.sheet_names else xl.sheet_names[0]
    print(f"LBR file: {lbr_path.name} | sheet: {sheet}")

    df = pd.read_excel(lbr_path, sheet_name=sheet)

    col_norm = {c: re.sub(r"\s+", " ", str(c).strip().lower()) for c in df.columns}

    def pick_any(keys):
        for key in keys:
            k = key.lower()
            for col, n in col_norm.items():
                if n == k or k in n:
                    return col
        return None

    part_c = pick_any(["part number", "part", "item number", "item", "p/n", "pn", "part nbr", "part nbro"])  # tolerant
    empn_c = pick_any(["emp nam", "employee name", "employee", "emp name", "name", "resource", "emp nam"])  # tolerant
    empid_c = pick_any(["emp #", "emp#", "employee number", "employee id", "badge", "badge#", "emp num"])  # tolerant
    dept_c = pick_any(["department", "dept", "operation", "labor type", "work type"])  # user: Department has Assembly/Test
    date_c = pick_any(["work date", "date", "transaction date", "trx date", "labor date"])

    if part_c is None:
        conn.execute("DELETE FROM part_recent_employee;")
        conn.execute("DELETE FROM part_recent_employee_dept;")
        return 0

    df[part_c] = df[part_c].astype(str).str.strip()

    if empn_c is None:
        df["__empname"] = ""
        empn_c = "__empname"
    else:
        df[empn_c] = df[empn_c].astype(str).str.strip()

    if empid_c is None:
        df["__empnum"] = ""
        empid_c = "__empnum"
    else:
        df[empid_c] = df[empid_c].astype(str).str.strip()

    if dept_c is None:
        df["__dept"] = ""
        dept_c = "__dept"
    else:
        df[dept_c] = df[dept_c].astype(str).str.strip()

    # Date ordering (best-effort)
    if date_c is not None:
        df["__dt"] = pd.to_datetime(df[date_c], errors="coerce")
        df = df.sort_values("__dt", ascending=True)

    def norm_dept(x: object) -> str:
        s = _norm_str(x).lower()
        if not s:
            return ""
        # Normalize to just two buckets
        if "test" in s:
            return "test"
        if "assy" in s or "asm" in s or "assembly" in s:
            return "assy"
        return ""

    df["__dept_norm"] = df[dept_c].map(norm_dept)

    # Build dept-specific recency map: last 2 unique employees per (part, dept)
    out_dept: list[tuple[str, str, str, str, int]] = []
    for (part, dept), g in df[df["__dept_norm"] != ""].groupby([part_c, "__dept_norm"], dropna=True):
        seen = set()
        recent: list[tuple[str, str]] = []
        for _, r in g.iloc[::-1].iterrows():
            nm = _norm_str(r.get(empn_c, ""))
            en = _norm_str(r.get(empid_c, ""))
            key = (nm, en)
            if key in seen:
                continue
            seen.add(key)
            if nm or en:
                recent.append(key)
            if len(recent) >= 2:
                break
        for rank, (nm, en) in enumerate(recent, start=1):
            out_dept.append((str(part).strip(), dept, nm, en, rank))

    # Backward-compatible table (dept-agnostic): last 3 employees overall
    out_flat: list[tuple[str, str, str, int]] = []
    for part, g in df.groupby(part_c, dropna=True):
        seen = set()
        recent: list[tuple[str, str]] = []
        for _, r in g.iloc[::-1].iterrows():
            nm = _norm_str(r.get(empn_c, ""))
            en = _norm_str(r.get(empid_c, ""))
            key = (nm, en)
            if key in seen:
                continue
            seen.add(key)
            if nm or en:
                recent.append(key)
            if len(recent) >= 3:
                break
        for rank, (nm, en) in enumerate(recent, start=1):
            out_flat.append((str(part).strip(), nm, en, rank))

    # Persist
    conn.execute("DELETE FROM part_recent_employee;")
    conn.execute("DELETE FROM part_recent_employee_dept;")

    cur = conn.cursor()
    for part, nm, en, rank in out_flat:
        cur.execute(
            """
            INSERT INTO part_recent_employee(part_number, emp_name, emp_num, rank)
            VALUES (?, ?, ?, ?);
            """,
            (part, nm, en, int(rank)),
        )

    for part, dept, nm, en, rank in out_dept:
        cur.execute(
            """
            INSERT INTO part_recent_employee_dept(part_number, dept, emp_name, emp_num, rank)
            VALUES (?, ?, ?, ?, ?);
            """,
            (part, dept, nm, en, int(rank)),
        )

    # Return count of unique employees referenced
    emp_set = set((nm, en) for _, _, nm, en, _ in out_dept if nm or en)
    return len(emp_set)


# -----------------------------
# Main
# -----------------------------
def main():
    print("Using incoming files:")
    print(f"  BU : {BU_XLSX}")
    print(f"  HRS: {HRS_XLSX}")
    print(f"  LBR: {LBR_XLSX}")
    print(f"  SDD: {SDD_XLSX}")
    print(f"  DB : {DB_PATH}")
    validate_input_files()

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.executescript(SCHEMA_SQL)
    migrate_schema(conn)

    try:
        conn.execute("BEGIN;")

        mains_n, subs_n, pegs_n = refresh_bu_shortages(conn, BU_XLSX, SDD_XLSX)
        print(f"Loaded mains={mains_n} subs={subs_n} pegs={pegs_n}")

        hrs_n = refresh_hours(conn, HRS_XLSX)
        print(f"Loaded op_hours rows={hrs_n}")

        emp_n = refresh_labor(conn, LBR_XLSX)
        print(f"Loaded employees={emp_n}")

        conn.commit()
        print(f"✅ Refresh committed: {DB_PATH}")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
