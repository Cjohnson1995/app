from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
import os
import io
import tempfile
from typing import Optional, Sequence

import pandas as pd
import altair as alt
import streamlit as st
import streamlit.components.v1 as components


# -----------------------------
# Paths
# -----------------------------
BASE = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = Path(tempfile.gettempdir()) / "planner_app"
DATA_DIR = Path(os.getenv("PLANNER_DATA_DIR", DEFAULT_DATA_DIR))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "planner_v2.db"

def _table_cols(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table});").fetchall()}


# Helper to check if a table has a given column
def has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    try:
        return col in _table_cols(conn, table)
    except Exception:
        return False


# Ensure schema is up-to-date before anything else
def ensure_schema():
    """Make UI resilient if refresh_v2.py hasn't been run since new columns were introduced."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    st.caption(f"Database path: {DB_PATH}")
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys=ON;")

        # main_targets: create if missing, otherwise add columns if missing
        if not _has_table(conn, "main_targets"):
            conn.execute(
                """
                CREATE TABLE main_targets (
                    main_id INTEGER PRIMARY KEY,
                    assy_date TEXT,
                    test_date TEXT,
                    stock_date TEXT,
                    assy_emp TEXT,
                    test_emp TEXT,
                    updated_at TEXT
                );
                """
            )
        else:
            cols = _table_cols(conn, "main_targets")
            if "assy_date" not in cols:
                conn.execute("ALTER TABLE main_targets ADD COLUMN assy_date TEXT;")
            if "test_date" not in cols:
                conn.execute("ALTER TABLE main_targets ADD COLUMN test_date TEXT;")
            if "stock_date" not in cols:
                conn.execute("ALTER TABLE main_targets ADD COLUMN stock_date TEXT;")
            if "assy_emp" not in cols:
                conn.execute("ALTER TABLE main_targets ADD COLUMN assy_emp TEXT;")
            if "test_emp" not in cols:
                conn.execute("ALTER TABLE main_targets ADD COLUMN test_emp TEXT;")
            if "updated_at" not in cols:
                conn.execute("ALTER TABLE main_targets ADD COLUMN updated_at TEXT;")

        # sub_plan: create if missing, otherwise add planning columns if missing
        if not _has_table(conn, "sub_plan"):
            conn.execute(
                """
                CREATE TABLE sub_plan (
                    sub_key TEXT PRIMARY KEY,
                    clear_date TEXT,
                    assy_date TEXT,
                    test_date TEXT,
                    stock_date TEXT,
                    assy_emp TEXT,
                    test_emp TEXT,
                    status TEXT,
                    updated_at TEXT
                );
                """
            )
        else:
            cols = _table_cols(conn, "sub_plan")
            if "clear_date" not in cols:
                conn.execute("ALTER TABLE sub_plan ADD COLUMN clear_date TEXT;")
            if "assy_date" not in cols:
                conn.execute("ALTER TABLE sub_plan ADD COLUMN assy_date TEXT;")
            if "test_date" not in cols:
                conn.execute("ALTER TABLE sub_plan ADD COLUMN test_date TEXT;")
            if "stock_date" not in cols:
                conn.execute("ALTER TABLE sub_plan ADD COLUMN stock_date TEXT;")
            if "assy_emp" not in cols:
                conn.execute("ALTER TABLE sub_plan ADD COLUMN assy_emp TEXT;")
            if "test_emp" not in cols:
                conn.execute("ALTER TABLE sub_plan ADD COLUMN test_emp TEXT;")
            if "status" not in cols:
                conn.execute("ALTER TABLE sub_plan ADD COLUMN status TEXT;")
            if "updated_at" not in cols:
                conn.execute("ALTER TABLE sub_plan ADD COLUMN updated_at TEXT;")

        # main_line: add short_free if missing (only if table exists)
        if _has_table(conn, "main_line"):
            cols = _table_cols(conn, "main_line")
            if "short_free" not in cols:
                conn.execute("ALTER TABLE main_line ADD COLUMN short_free INTEGER DEFAULT 0;")

        # sub_object: add comp_remaining (BU "Comp. Remaining") if missing (only if table exists)
        if _has_table(conn, "sub_object"):
            cols = _table_cols(conn, "sub_object")
            if "comp_remaining" not in cols:
                conn.execute("ALTER TABLE sub_object ADD COLUMN comp_remaining TEXT;")
        missing_core = [t for t in ["main_line", "sub_object", "peg_sub", "op_hours"] if not _has_table(conn, t)]
        if missing_core:
            st.warning(
                "Planner source tables are missing in this deployed database: "
                + ", ".join(missing_core)
                + ". Run your refresh/import process to load production data."
            )

        conn.commit()

ensure_schema()
st.cache_data.clear()

# -----------------------------
# Work center rules (must match refresh)
# -----------------------------
ASSY_CTRS = {"ASM", "ASSY"}
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

# ---- Dynamic sub status: derive from leftmost/next op in rem_ops ----
STATUS_NOT_STARTED = "not_started"
STATUS_WIP = "wip"
STATUS_PENDING_ASSY = "pending_assy"
STATUS_IN_TEST = "in_test"
STATUS_OTHER = "other"

def _tokenize_ops(rem_ops: Optional[str]) -> list[str]:
    if rem_ops is None:
        return []
    s = str(rem_ops).strip().upper()
    if not s or s in {"NONE", "NAN"}:
        return []
    # Split on whitespace; this matches the BU export formatting.
    toks = [t for t in s.replace("\t", " ").split() if t]
    return toks

def derive_sub_status(rem_ops: Optional[str]) -> tuple[str, str]:
    """Return (status, next_op) from Comp Remaining using LEFTMOST/NEXT op logic."""
    toks = _tokenize_ops(rem_ops)
    if not toks:
        return (STATUS_OTHER, "")

    # Leftmost token is treated as the next operation.
    next_op = toks[0]

    if next_op == "KIT":
        return (STATUS_NOT_STARTED, next_op)

    if next_op in TEST_CTRS:
        return (STATUS_IN_TEST, next_op)

    if next_op in ASSY_CTRS:
        return (STATUS_PENDING_ASSY, next_op)

    # Otherwise it's in process somewhere (machining/inspect/etc).
    return (STATUS_WIP, next_op)


# ---- Helper: ensure sub_status/next_op columns exist in a dataframe ----
def ensure_status_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure df has sub_status and next_op columns.

    This keeps the UI resilient if upstream data changes or if cached frames are missing
    derived columns.
    """
    if df is None or len(df) == 0:
        # still ensure columns exist for downstream column selection
        if df is None:
            df = pd.DataFrame()
        if "sub_status" not in df.columns:
            df["sub_status"] = pd.Series(dtype=str)
        if "next_op" not in df.columns:
            df["next_op"] = pd.Series(dtype=str)
        return df

    if "sub_status" in df.columns and "next_op" in df.columns:
        return df

    if "rem_ops" in df.columns:
        _status_next = df["rem_ops"].apply(lambda x: pd.Series(derive_sub_status(x), index=["sub_status", "next_op"]))
        # If df already has one of the columns, only fill missing
        if "sub_status" not in df.columns:
            df = pd.concat([df, _status_next[["sub_status"]]], axis=1)
        if "next_op" not in df.columns:
            df = pd.concat([df, _status_next[["next_op"]]], axis=1)
        # Ensure string dtype
        df["sub_status"] = df["sub_status"].fillna("").astype(str)
        df["next_op"] = df["next_op"].fillna("").astype(str)
        return df

    # No rem_ops available -> create blanks
    if "sub_status" not in df.columns:
        df["sub_status"] = ""
    if "next_op" not in df.columns:
        df["next_op"] = ""
    return df


# --- New helper functions for area status ---
def is_area_open_from_rem_ops(rem_ops: Optional[str], area: str) -> bool:
    """True if the requested area still has work remaining according to Comp Remaining."""
    toks = _tokenize_ops(rem_ops)
    a = (area or "").strip().upper()
    if a == "TEST":
        return any(t in TEST_CTRS for t in toks)
    # ASSY
    return any(t in ASSY_CTRS for t in toks)


def area_state_label(rem_ops: Optional[str], area: str) -> str:
    """Return 'OPEN' if area op still present in Comp Remaining, else 'COMPLETED'."""
    return "OPEN" if is_area_open_from_rem_ops(rem_ops, area) else "COMPLETED"


def iso_or_none(d: Optional[date]) -> Optional[str]:
    return d.isoformat() if isinstance(d, date) else None


def safe_date_input(label: str, value: Optional[str]):
    """
    value: ISO string or None
    returns: date or None
    """
    if not value:
        return st.date_input(label, value=None)
    try:
        dt = pd.to_datetime(value, errors="coerce")
        if pd.isna(dt):
            return st.date_input(label, value=None)
        return st.date_input(label, value=dt.date())
    except Exception:
        return st.date_input(label, value=None)


@st.cache_data(show_spinner=False)
def read_sql(sql: str, params: dict | tuple = ()) -> pd.DataFrame:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(sql, conn, params=params)


def exec_sql(sql: str, params: tuple = ()):
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute(sql, params)
        conn.commit()


def exec_many(sql: str, rows: Sequence[tuple]):
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.executemany(sql, rows)
        conn.commit()




def _sqlite_tables(conn: sqlite3.Connection) -> list[str]:
    return [r[0] for r in conn.execute("select name from sqlite_master where type='table';").fetchall()]

# Helper: check if a table exists in the SQLite DB
def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    return (
        conn.execute(
            "select 1 from sqlite_master where type='table' and name=?;",
            (name,),
        ).fetchone()
        is not None
    )


@st.cache_data(show_spinner=False)
def detect_dept_labor_source() -> dict:
    """Detect the Dept Labor table/columns in the SQLite DB.

    We expect something like the DeptLbr2026.xlsx import, with:
      - part number column
      - employee name column
      - department column that indicates Assembly/Test

    This returns a dict with keys:
      table, part_col, emp_col, dept_col, ts_col
    or an empty dict if not found.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys=ON;")
        tables = _sqlite_tables(conn)

        # Candidate table names (most likely first)
        candidates = [
            "dept_lbr",
            "dept_labor",
            "deptlabor",
            "deptlbr",
            "labor",
            "labor_log",
            "employee_labor",
        ]

        # Add any table that contains 'lbr' or 'labor'
        for t in tables:
            tl = t.lower()
            if ("lbr" in tl or "labor" in tl) and t not in candidates:
                candidates.append(t)

        # Column name candidates
        part_cols = [
            "part_nbr",
            "part",
            "part_num",
            "part_number",
            "item",
            "item_number",
            "pn",
        ]
        emp_cols = [
            "emp_name",
            "employee_name",
            "employee",
            "name",
            "emp",
            "worker",
            "operator",
        ]
        dept_cols = [
            "department",
            "dept",
            "dept_name",
            "op_dept",
            "operation_department",
        ]
        ts_cols = [
            "work_date",
            "date",
            "earned_date",
            "updated_at",
            "ts",
            "timestamp",
        ]

        for t in candidates:
            if t not in tables:
                continue
            cols = _table_cols(conn, t)

            part_col = next((c for c in part_cols if c in cols), None)
            emp_col = next((c for c in emp_cols if c in cols), None)
            dept_col = next((c for c in dept_cols if c in cols), None)
            ts_col = next((c for c in ts_cols if c in cols), None)

            if part_col and emp_col and dept_col:
                return {
                    "table": t,
                    "part_col": part_col,
                    "emp_col": emp_col,
                    "dept_col": dept_col,
                    "ts_col": ts_col,
                }

    return {}


def get_employee_options() -> list[str]:
    """Global employee list.

    Priority:
      1) Dept Labor source (what your team trusts)
      2) part_recent_employee (legacy)
      3) fallback empty

    NOTE: This returns ALL employees (no dept filtering).
    """
    src = detect_dept_labor_source()
    if src:
        t = src["table"]
        emp_col = src["emp_col"]
        df = read_sql(
            f"""
            select distinct trim({emp_col}) as emp
            from {t}
            where {emp_col} is not null and trim({emp_col}) <> ''
            order by emp;
            """
        )
        opts = df["emp"].dropna().astype(str).tolist()
        return [""] + opts

    # Legacy fallback
    try:
        df = read_sql(
            """
            select distinct trim(emp_name) as emp
            from part_recent_employee
            where emp is not null and emp <> ''
            order by emp;
            """
        )
        opts = df["emp"].dropna().astype(str).tolist()
        return [""] + opts
    except Exception:
        return [""]


def get_employee_options_by_area(area: str) -> list[str]:
    """Return [''] + all employees seen in Dept Labor for a given area.

    area: 'TEST' or 'ASSY'

    If Dept Labor source isn't available, falls back to global employee options.
    """
    a = (area or "").strip().upper()

    src = detect_dept_labor_source()
    if not src:
        return get_employee_options()

    t = src["table"]
    emp_col = src["emp_col"]
    dept_col = src["dept_col"]

    if a == "TEST":
        dept_pred = f"upper(trim({dept_col})) like '%TEST%'"
    else:
        # ASSY / ASSEMBLY
        dept_pred = f"(upper(trim({dept_col})) like '%ASSY%' or upper(trim({dept_col})) like '%ASSEMB%')"

    df = read_sql(
        f"""
        select distinct trim({emp_col}) as emp
        from {t}
        where {emp_col} is not null and trim({emp_col}) <> ''
          and {dept_pred}
        order by emp;
        """
    )

    opts = df["emp"].dropna().astype(str).tolist()
    return [""] + opts


# --- New: restricted employee allocation for mains and subs ---
@st.cache_data(show_spinner=False)
def _table_cols_cached(table: str) -> set[str]:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        return _table_cols(conn, table)



def get_recent_employee_suggestions(part: str | None, area: str, limit: int = 2) -> list[str]:
    """Return the last N employees who worked this part in the given area.

    STRICT: does not fall back to global/area lists.
    Returns [] if no history exists.

    area is 'TEST' or 'ASSY'
    """

    p = (part or "").strip()
    if not p:
        return []

    area_u = (area or "").strip().upper()
    dept = "test" if area_u == "TEST" else "assy"

    # 1) Preferred: curated mapping table created during refresh
    try:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("PRAGMA foreign_keys=ON;")
            if _has_table(conn, "part_recent_employee_dept"):
                cols = _table_cols(conn, "part_recent_employee_dept")
                if {"part_number", "dept"}.issubset(cols):
                    emp_col = "emp_name" if "emp_name" in cols else ("employee" if "employee" in cols else None)
                    rank_col = "rank" if "rank" in cols else None
                    if emp_col:
                        if rank_col:
                            sql = f"""
                                select trim({emp_col}) as emp
                                from part_recent_employee_dept
                                where trim(part_number) = :part
                                  and lower(trim(dept)) = :dept
                                  and {emp_col} is not null and trim({emp_col}) <> ''
                                  and {rank_col} <= :lim
                                order by {rank_col} asc;
                            """
                            df = pd.read_sql_query(sql, conn, params={"part": p, "dept": dept, "lim": int(limit)})
                        else:
                            sql = f"""
                                select trim({emp_col}) as emp
                                from part_recent_employee_dept
                                where trim(part_number) = :part
                                  and lower(trim(dept)) = :dept
                                  and {emp_col} is not null and trim({emp_col}) <> ''
                                order by rowid desc
                                limit :lim;
                            """
                            df = pd.read_sql_query(sql, conn, params={"part": p, "dept": dept, "lim": int(limit)})

                        opts = df.get("emp", pd.Series([], dtype=str)).dropna().astype(str).tolist()
                        opts = [o.strip() for o in opts if str(o).strip()]
                        if opts:
                            # de-dupe while preserving order
                            seen = set()
                            out = []
                            for o in opts:
                                if o not in seen:
                                    seen.add(o)
                                    out.append(o)
                            return out[: int(limit)]
    except Exception:
        pass

    # 2) Fallback: derive directly from Dept Labor raw table
    src = detect_dept_labor_source()
    if src:
        t = src["table"]
        part_col = src["part_col"]
        emp_col = src["emp_col"]
        dept_col = src["dept_col"]
        ts_col = src.get("ts_col")

        if area_u == "TEST":
            dept_pred = f"upper(trim({dept_col})) like '%TEST%'"
        else:
            dept_pred = f"(upper(trim({dept_col})) like '%ASSY%' or upper(trim({dept_col})) like '%ASSEMB%')"

        # Use max(date) when available, else max(rowid)
        order_expr = f"max({ts_col})" if ts_col else "max(rowid)"

        sql = f"""
            select trim({emp_col}) as emp
            from {t}
            where trim({part_col}) = :part
              and {emp_col} is not null and trim({emp_col}) <> ''
              and {dept_pred}
            group by trim({emp_col})
            order by {order_expr} desc
            limit :lim;
        """
        try:
            df = read_sql(sql, params={"part": p, "lim": int(limit)})
            opts = df.get("emp", pd.Series([], dtype=str)).dropna().astype(str).tolist()
            opts = [o.strip() for o in opts if str(o).strip()]
            if opts:
                seen = set()
                out = []
                for o in opts:
                    if o not in seen:
                        seen.add(o)
                        out.append(o)
                return out[: int(limit)]
        except Exception:
            pass

    return []


def employee_dropdown_options(part: str | None, area: str, limit: int = 2) -> tuple[list[str], str]:
    """Return (options, mode) for employee dropdown.

    mode:
      - 'experienced' => options are [''] + last N for part+area
      - 'fallback'    => no part history; options are [''] + ALL employees for that area

    This implements your rule:
      - show ONLY experienced people when available
      - ONLY when none exist, allow everyone (area-filtered)
    """
    recent = get_recent_employee_suggestions(part, area, limit=limit)
    if recent:
        return ([""] + recent, "experienced")

    # No history for this part+area -> allow everyone in that area
    return (get_employee_options_by_area(area), "fallback")


def validate_employee_for_part(part: str | None, area: str, emp: str | None) -> bool:
    """Validation rule:

    - If we HAVE history for (part, area): employee must be in last-N suggestions (or blank).
    - If we have NO history: allow any employee in that area list (or blank).
    """
    emp = (emp or "").strip()
    if emp == "":
        return True

    part = (part or "").strip()
    if not part:
        # No part context: do not allow assigning an employee.
        return False

    recent = get_recent_employee_suggestions(part, area, limit=2)
    if recent:
        return emp in set(recent)

    # No history -> allow any employee in the AREA list
    area_opts = [x for x in get_employee_options_by_area(area) if x]
    return emp in set(area_opts)


# --- New helper: Get BU options for filtering ---
@st.cache_data(show_spinner=False)
def get_bu_options() -> list[str]:
    df = read_sql(
        """
        select distinct trim(bu) as bu
        from main_line
        where is_active=1 and bu is not null and trim(bu) <> ''
        order by bu;
        """
    )
    opts = df["bu"].dropna().astype(str).tolist()
    return opts


def mains_core(filters: dict) -> pd.DataFrame:
    return read_sql(
        """
        select
          m.main_id,
          m.delivery,
          m.customer,
          m.parent_part,
          m.parent_wo,
          m.parent_wo_norm,
          m.bu,
          m.type,
          m.due_date,
          m.dock_date,
          m.wo_qty,
          m.line_value,
          m.short_free,

          t.assy_date,
          t.test_date,
          t.stock_date,
          t.assy_emp,
          t.test_emp

        from main_line m
        left join main_targets t on t.main_id = m.main_id
        where m.is_active = 1
          and (:exclude_rpo = 0 or m.parent_wo not like 'RPO%')
          and (:bu = '' or lower(m.bu) like '%' || lower(:bu) || '%')
          and (:customer = '' or lower(m.customer) like '%' || lower(:customer) || '%')
          and (:delivery = '' or lower(m.delivery) like '%' || lower(:delivery) || '%')
          and (:dock_from = '' or (m.dock_date is not null and m.dock_date >= :dock_from))
          and (:dock_to = '' or (m.dock_date is not null and m.dock_date <= :dock_to))
        order by m.due_date, m.delivery;
        """,
        params={
            "bu": filters.get("bu", ""),
            "customer": filters.get("customer", ""),
            "delivery": filters.get("delivery", ""),
            "dock_from": filters.get("dock_from", ""),
            "dock_to": filters.get("dock_to", ""),
            "exclude_rpo": 1 if filters.get("exclude_rpo", True) else 0,
        },
    )
def style_short_free_green(df: pd.DataFrame):
    """Highlight SHORT FREE mains in green (read-only tables only)."""
    if df is None or df.empty or "short_free" not in df.columns:
        return df

    def _row_style(row):
        try:
            is_sf = int(row.get("short_free", 0) or 0) == 1
        except Exception:
            is_sf = False
        return ["background-color: #C6EFCE" if is_sf else "" for _ in row]

    return df.style.apply(_row_style, axis=1)

def hours_rollup_for_mains() -> pd.DataFrame:
    # Aggregate hours at WO grain using normalized WO (handles (K) mismatch)
    # ASSY = ASM or ASSY; TEST = listed; ignore FASY.
    ctrs_test = tuple(sorted(TEST_CTRS))
    ctrs_assy = tuple(sorted(ASSY_CTRS))

    return read_sql(
        f"""
        select
          wo_pl_norm as wo_norm,
          sum(case when wk_ctr in {ctrs_assy} then rem_hrs else 0 end) as assy_hrs,
          sum(case when wk_ctr in {ctrs_test} then rem_hrs else 0 end) as test_hrs
        from op_hours
        where wk_ctr not in ('FASY')
        group by wo_pl_norm;
        """
    )


def hours_rollup_for_subs() -> pd.DataFrame:
    # For subs we join on digits extracted from Comp Source -> wo_pl_digits
    ctrs_test = tuple(sorted(TEST_CTRS))
    ctrs_assy = tuple(sorted(ASSY_CTRS))

    return read_sql(
        f"""
        select
          wo_pl_digits as source_digits,
          sum(case when wk_ctr in {ctrs_assy} then rem_hrs else 0 end) as assy_hrs,
          sum(case when wk_ctr in {ctrs_test} then rem_hrs else 0 end) as test_hrs
        from op_hours
        where wo_pl_digits is not null
          and wk_ctr not in ('FASY')
        group by wo_pl_digits;
        """
    )


def subs_for_main(main_id: int) -> pd.DataFrame:
    # "Comp. Remaining" from BU Shortages Details is stored on the sub object table when available.
    # We keep this backwards-compatible in case refresh_v2.py/db schema doesn't yet have the column.
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as _c:
        _c.execute("PRAGMA foreign_keys=ON;")
        if has_column(_c, "sub_object", "comp_remaining"):
            rem_sql = "s.comp_remaining as rem_ops,"
        elif has_column(_c, "sub_object", "comp_remaining_ops"):
            rem_sql = "s.comp_remaining_ops as rem_ops,"
        elif has_column(_c, "sub_object", "sub_remaining"):
            rem_sql = "s.sub_remaining as rem_ops,"
        else:
            rem_sql = "'' as rem_ops,"

    return read_sql(
        f"""
        select
          s.sub_key,
          s.sub_part,
          s.sub_source,
          s.source_digits,
          s.sub_resp,
          s.sub_eta_date,
          s.sub_desc,

          {rem_sql}

          p.clear_date,
          p.assy_date,
          p.test_date,
          p.stock_date,
          p.assy_emp,
          p.test_emp

        from peg_sub g
        join sub_object s on s.sub_key = g.sub_key
        left join sub_plan p on p.sub_key = s.sub_key
        where g.is_active = 1
          and s.is_active = 1
          and g.main_id = ?
        order by s.sub_eta_date, s.sub_part;
        """,
        params=(main_id,),
    )


def pegged_mains_for_sub(sub_key: str) -> pd.DataFrame:
    # show which mains this sub impacts + value + due date (your priority decision support)
    return read_sql(
        """
        select
          m.delivery,
          m.customer,
          m.parent_part,
          m.parent_wo,
          m.due_date,
          m.wo_qty,
          m.line_value,
          t.stock_date
        from peg_sub g
        join main_line m on m.main_id = g.main_id
        left join main_targets t on t.main_id = m.main_id
        where g.is_active = 1
          and m.is_active = 1
          and g.sub_key = ?
        order by m.due_date;
        """,
        params=(sub_key,),
    )


def business_days_between(start: date, end: date) -> list[date]:
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:  # Mon-Fri
            days.append(d)
        d += timedelta(days=1)
    return days


# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="Production Planning Workbench", layout="wide")

# -----------------------------
# Header image / branding
# -----------------------------
# Place your logo file here:
#   /Users/christianjohnson/planner_db_v2/ui/assets/company_header.png
# (You can change the filename/path below if needed.)
ASSETS_DIR = Path(__file__).resolve().parent / "assets"
HEADER_IMG = ASSETS_DIR / "company_header.png"

if HEADER_IMG.exists():
    st.image(str(HEADER_IMG), use_container_width=True)
else:
    # Fallback: no image found, show a small note (kept subtle)
    st.caption(f"(Header image not found at: {HEADER_IMG})")

st.title("Production Planning Workbench")

with st.sidebar:
    st.header("Filters")
    bu = st.text_input("BU contains", value="")
    customer = st.text_input("Customer contains", value="")
    delivery = st.text_input("Delivery contains", value="")
    exclude_rpo = st.checkbox(
        "Exclude RPO repair orders",
        value=True,
        help="Hide repair orders (WO starting with RPO)",
    )
    st.subheader("Dock date")
    c1, c2 = st.columns(2)
    with c1:
        dock_from = st.date_input("From", value=None, key="dock_from")
    with c2:
        dock_to = st.date_input("To", value=None, key="dock_to")

    st.divider()
    st.header("Capacity settings")
    hrs_per_emp = st.number_input("Hours per employee per day", min_value=1, max_value=16, value=10)
    test_headcount = st.number_input("Test headcount", min_value=1, max_value=500, value=10)
    assy_headcount = st.number_input("Assy headcount", min_value=1, max_value=500, value=10)

filters = {
    "bu": bu,
    "customer": customer,
    "delivery": delivery,
    "dock_from": dock_from.isoformat() if isinstance(dock_from, date) else "",
    "dock_to": dock_to.isoformat() if isinstance(dock_to, date) else "",
    "exclude_rpo": bool(exclude_rpo),
}

tabs = st.tabs(["Mains", "Subs", "Dashboards"])

# -------------
# Mains tab
# -------------
with tabs[0]:
    st.subheader("Mains (Sales Line × WO)")

    mains = mains_core(filters)

    # attach hours rollups
    h_main = hours_rollup_for_mains()
    mains = mains.merge(h_main, how="left", left_on="parent_wo_norm", right_on="wo_norm")
    mains["assy_hrs"] = mains["assy_hrs"].fillna(0.0)
    mains["test_hrs"] = mains["test_hrs"].fillna(0.0)

    # main list label includes customer + qty + value (you asked)
    mains["label"] = (
        mains["delivery"].astype(str)
        + " | "
        + mains["customer"].fillna("").astype(str)
        + " | "
        + mains["parent_wo"].fillna("").astype(str)
        + " | "
        + mains["parent_part"].fillna("").astype(str)
        + " | qty "
        + mains["wo_qty"].fillna(0).astype(int).astype(str)
        + " | $"
        + mains["line_value"].fillna(0).astype(float).map(lambda x: f"{x:,.0f}")
        + " | due "
        + mains["due_date"].fillna("")
    )

    # Tag SHORT-FREE mains so planners can schedule those first
    if "short_free" in mains.columns:
        mains["label"] = mains["label"] + mains["short_free"].fillna(0).astype(int).map(lambda x: " | SHORT FREE" if x == 1 else "")

    emp_opts = get_employee_options()
    colA, colB = st.columns([2, 3], gap="large")

    with colA:
        prioritize_short_free = st.checkbox("Prioritize SHORT FREE", value=False)
        if prioritize_short_free and "short_free" in mains.columns:
            mains = mains.sort_values(["short_free", "due_date", "delivery"], ascending=[False, True, True])

        sel = st.selectbox("Select a main", mains["label"].tolist())
        sel_row = mains.loc[mains["label"] == sel].iloc[0]
        main_id = int(sel_row["main_id"])

        st.markdown("### Main details")

        # Normalize values for clean display
        due_txt = str(sel_row["due_date"] or "")
        dock_txt = str(sel_row["dock_date"] or "")
        qty_i = int(pd.to_numeric(sel_row.get("wo_qty", 0), errors="coerce") or 0)
        val_f = float(pd.to_numeric(sel_row.get("line_value", 0), errors="coerce") or 0.0)
        assy_f = float(pd.to_numeric(sel_row.get("assy_hrs", 0), errors="coerce") or 0.0)
        test_f = float(pd.to_numeric(sel_row.get("test_hrs", 0), errors="coerce") or 0.0)

        # Summary badges
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.metric("Due", due_txt if due_txt else "—")
        with m2:
            st.metric("Dock", dock_txt if dock_txt else "—")
        with m3:
            st.metric("Value", f"${val_f:,.0f}")
        with m4:
            st.metric("Qty", f"{qty_i:,}")

        # Key identifiers + hours
        c1, c2 = st.columns([3, 2], gap="large")
        with c1:
            st.markdown(
                f"""
**Customer:** {sel_row.get("customer","") or "—"}  
**Sales line (Delivery):** {sel_row.get("delivery","") or "—"}  
**Main Part:** {sel_row.get("parent_part","") or "—"}  
**Work Order:** {sel_row.get("parent_wo","") or "—"}  
                """.strip()
            )
        with c2:
            st.markdown("**Hours remaining (from Scheduled Hours)**")
            h1, h2 = st.columns(2)
            with h1:
                st.metric("ASSY", f"{assy_f:.2f}")
            with h2:
                st.metric("TEST", f"{test_f:.2f}")

        st.markdown("### Update main schedule")

        with st.form("main_update_form", clear_on_submit=False):
            assy_dt = safe_date_input("Assy date", sel_row.get("assy_date"))
            test_dt = safe_date_input("Test date", sel_row.get("test_date"))
            stock_dt = safe_date_input("Stock date", sel_row.get("stock_date"))

            part_for_emp = str(sel_row.get("parent_part") or "").strip()
            assy_emp_opts, assy_mode = employee_dropdown_options(part_for_emp, "ASSY", limit=2)
            test_emp_opts, test_mode = employee_dropdown_options(part_for_emp, "TEST", limit=2)

            if assy_mode == "fallback":
                st.warning("No ASSY history for this part — showing all assemblers (fallback).")
            if test_mode == "fallback":
                st.warning("No TEST history for this part — showing all testers (fallback).")

            assy_emp = st.selectbox(
                "Assy employee (last 2 for part)",
                assy_emp_opts,
                index=assy_emp_opts.index(sel_row.get("assy_emp") or "") if (sel_row.get("assy_emp") or "") in assy_emp_opts else 0,
            )
            test_emp = st.selectbox(
                "Test employee (last 2 for part)",
                test_emp_opts,
                index=test_emp_opts.index(sel_row.get("test_emp") or "") if (sel_row.get("test_emp") or "") in test_emp_opts else 0,
            )

            save = st.form_submit_button("Save main schedule")
            if save:
                part_for_emp = str(sel_row.get("parent_part") or "").strip()
                if not validate_employee_for_part(part_for_emp, "ASSY", assy_emp):
                    st.error("Assy employee must be one of the last 2 recent assemblers for this part (or blank).")
                    st.stop()
                if not validate_employee_for_part(part_for_emp, "TEST", test_emp):
                    st.error("Test employee must be one of the last 2 recent testers for this part (or blank).")
                    st.stop()
                exec_sql(
                    """
                    INSERT INTO main_targets(main_id, assy_date, test_date, stock_date, assy_emp, test_emp, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(main_id) DO UPDATE SET
                      assy_date=excluded.assy_date,
                      test_date=excluded.test_date,
                      stock_date=excluded.stock_date,
                      assy_emp=excluded.assy_emp,
                      test_emp=excluded.test_emp,
                      updated_at=excluded.updated_at;
                    """,
                    (
                        main_id,
                        iso_or_none(assy_dt),
                        iso_or_none(test_dt),
                        iso_or_none(stock_dt),
                        assy_emp,
                        test_emp,
                        datetime.now().isoformat(timespec="seconds"),
                    ),
                )
                st.success("Saved. Refresh the page if needed.")

        # (Bulk update mains block moved to full-width after colB)

    with colB:
        st.markdown("### Subs pegged to this main")
        subs = subs_for_main(main_id)
        subs = ensure_status_cols(subs)

        # attach sub hours via source_digits join
        h_sub = hours_rollup_for_subs()
        subs = subs.merge(h_sub, how="left", on="source_digits")
        subs["assy_hrs"] = subs["assy_hrs"].fillna(0.0)
        subs["test_hrs"] = subs["test_hrs"].fillna(0.0)

        # Ensure sub_status/next_op columns exist (and derive if needed)
        subs = ensure_status_cols(subs)
        show = subs[
            [
                "sub_part",
                "sub_source",
                "sub_resp",
                "sub_eta_date",
                "rem_ops",
                "assy_hrs",
                "test_hrs",
                "sub_status",
                "next_op",
                "clear_date",
                "assy_date",
                "test_date",
                "stock_date",
                "assy_emp",
                "test_emp",
            ]
        ].copy()
        show["assy_state"] = show["rem_ops"].apply(lambda x: area_state_label(x, "ASSY"))
        show["test_state"] = show["rem_ops"].apply(lambda x: area_state_label(x, "TEST"))

        st.dataframe(
            show,
            use_container_width=True,
            height=240,
            column_config={
                "assy_state": st.column_config.TextColumn("ASSY status", help="OPEN if ASSY op exists in Comp Remaining; otherwise COMPLETED"),
                "test_state": st.column_config.TextColumn("TEST status", help="OPEN if TEST op exists in Comp Remaining; otherwise COMPLETED"),
            },
        )

        st.divider()
        st.markdown("### Update a sub plan (propagates across all pegged mains)")
        if len(subs) > 0:
            # Build a clean label (no UUID shown) but keep a mapping to sub_key
            _subs_pick = subs.copy()
            _subs_pick["sub_eta_date"] = pd.to_datetime(_subs_pick["sub_eta_date"], errors="coerce").dt.date

            # rem_ops comes from BU "Comp. Remaining" when available
            def _clean_rem_ops(v):
                if v is None:
                    return ""
                s = str(v).strip()
                return "" if s.lower() in {"none", "nan"} else s

            labels = []
            key_by_label = {}
            for _, r in _subs_pick.iterrows():
                eta = r.get("sub_eta_date")
                eta_txt = eta.isoformat() if isinstance(eta, date) else ""
                rem_txt = _clean_rem_ops(r.get("rem_ops"))
                # Example label: 60587-7 | 215298 | SHOP | ETA 2025-07-28 | RemOps PASS INSP ...
                label = (
                    f"{(r.get('sub_part') or '')} | {(r.get('sub_source') or '')} | {(r.get('sub_resp') or '')}"
                    + (f" | ETA {eta_txt}" if eta_txt else "")
                    + (f" | RemOps {rem_txt}" if rem_txt else "")
                ).strip()

                # Ensure uniqueness even if labels collide
                if label in key_by_label:
                    label = f"{label}  [dup:{r.get('sub_key')}]"

                labels.append(label)
                key_by_label[label] = r.get("sub_key")

            sel_sub = st.selectbox("Select sub", labels)
            sub_key = str(key_by_label.get(sel_sub))

            # load existing plan row
            plan_row = read_sql("select * from sub_plan where sub_key = ?;", params=(sub_key,))
            plan = plan_row.iloc[0] if len(plan_row) else None

            pegged = pegged_mains_for_sub(sub_key)

            # Show sub-level context (clean, high-signal)
            _sub_row = subs[subs["sub_key"] == sub_key]
            if len(_sub_row):
                _sr = _sub_row.iloc[0]
                rem_txt = str(_sr.get("rem_ops") or "").strip()
                if rem_txt.lower() in {"none", "nan"}:
                    rem_txt = ""
                desc_txt = str(_sr.get("sub_desc") or "").strip()

                cA, cB = st.columns([1, 3])
                with cA:
                    st.metric("Comp Remaining", rem_txt if rem_txt else "—")
                with cB:
                    if desc_txt:
                        st.markdown(f"**Sub description:** {desc_txt}")

            st.caption("Mains impacted by this sub:")
            st.dataframe(pegged, use_container_width=True, height=220)

            with st.form("sub_update_form", clear_on_submit=False):
                clear_dt = safe_date_input("Clear date", plan["clear_date"] if plan is not None else None)
                s_assy_dt = safe_date_input("Sub assy date", plan["assy_date"] if plan is not None else None)
                s_test_dt = safe_date_input("Sub test date", plan["test_date"] if plan is not None else None)
                s_stock_dt = safe_date_input("Sub stock date", plan["stock_date"] if plan is not None else None)

                sub_part_for_emp = str(_sr.get("sub_part") or "").strip() if '_sr' in locals() else ""
                rem_ops_val = _sr.get("rem_ops") if '_sr' in locals() else None
                assy_open = is_area_open_from_rem_ops(rem_ops_val, "ASSY")
                test_open = is_area_open_from_rem_ops(rem_ops_val, "TEST")

                # ASSY employee selection: only allowed if ASSY is still OPEN in Comp Remaining
                if not assy_open:
                    st.info("ASSY is COMPLETED for this sub (ASSY/ASM not present in Comp Remaining).")
                    s_assy_emp = ""
                else:
                    s_assy_emp_opts, s_assy_mode = employee_dropdown_options(sub_part_for_emp, "ASSY", limit=2)
                    if s_assy_mode == "fallback":
                        st.warning("No ASSY history for this sub part — showing all assemblers (fallback).")
                    s_assy_emp = st.selectbox("Sub assy employee (last 2 for part)", s_assy_emp_opts, index=0)

                # TEST employee selection: only allowed if TEST is still OPEN in Comp Remaining
                if not test_open:
                    st.info("TEST is COMPLETED for this sub (no TEST op present in Comp Remaining).")
                    s_test_emp = ""
                else:
                    s_test_emp_opts, s_test_mode = employee_dropdown_options(sub_part_for_emp, "TEST", limit=2)
                    if s_test_mode == "fallback":
                        st.warning("No TEST history for this sub part — showing all testers (fallback).")
                    s_test_emp = st.selectbox("Sub test employee (last 2 for part)", s_test_emp_opts, index=0)

                save_sub = st.form_submit_button("Save sub plan")
                if save_sub:
                    if not assy_open and (s_assy_emp or "").strip():
                        st.error("Cannot assign an ASSY employee: ASSY is COMPLETED (ASSY/ASM not present in Comp Remaining).")
                        st.stop()
                    if not test_open and (s_test_emp or "").strip():
                        st.error("Cannot assign a TEST employee: TEST is COMPLETED (no TEST op present in Comp Remaining).")
                        st.stop()
                    if not validate_employee_for_part(sub_part_for_emp, "ASSY", s_assy_emp):
                        st.error("Sub assy employee must be one of the last 2 recent assemblers for this part (or blank).")
                        st.stop()
                    if not validate_employee_for_part(sub_part_for_emp, "TEST", s_test_emp):
                        st.error("Sub test employee must be one of the last 2 recent testers for this part (or blank).")
                        st.stop()
                    exec_sql(
                        """
                        INSERT INTO sub_plan(sub_key, clear_date, assy_date, test_date, stock_date, assy_emp, test_emp, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(sub_key) DO UPDATE SET
                          clear_date=excluded.clear_date,
                          assy_date=excluded.assy_date,
                          test_date=excluded.test_date,
                          stock_date=excluded.stock_date,
                          assy_emp=excluded.assy_emp,
                          test_emp=excluded.test_emp,
                          updated_at=excluded.updated_at;
                        """,
                        (
                            sub_key,
                            iso_or_none(clear_dt),
                            iso_or_none(s_assy_dt),
                            iso_or_none(s_test_dt),
                            iso_or_none(s_stock_dt),
                            s_assy_emp,
                            s_test_emp,
                            datetime.now().isoformat(timespec="seconds"),
                        ),
                    )
                    st.success("Saved sub plan (shared across all pegged mains). Refresh if needed.")


# --- Bulk update mains (filtered list): full-width block ---
    st.divider()
    st.markdown("### Bulk update mains (filtered list)")
    st.caption(
        "Apply a single date/employee update to multiple mains, or edit rows directly (assy/test dates + employee allocation)."
    )

    # Build the bulk table (do NOT show main_id)
    bulk = mains[[
        "delivery",
        "customer",
        "parent_wo",
        "parent_part",
        "due_date",
        "wo_qty",
        "line_value",
        "short_free",
        "assy_date",
        "test_date",
        "stock_date",
        "assy_emp",
        "test_emp",
    ]].copy()
    bulk.insert(0, "select", False)

    # keep main_id aligned by index (hidden from UI)
    bulk_main_ids = mains["main_id"].reset_index(drop=True)
    bulk = bulk.reset_index(drop=True)

    # --- Suggested employees (last 2 for part) for bulk editor visibility ---
    # Build part lookup aligned to the bulk table rows
    bulk_parts = mains["parent_part"].reset_index(drop=True).fillna("").astype(str).str.strip()

    def _fmt_suggestions(part: str, area: str) -> str:
        recent = get_recent_employee_suggestions(part, area, limit=2)
        if recent:
            return ", ".join(recent)
        return "NO HISTORY"

    bulk["assy_suggestions"] = [
        _fmt_suggestions(p, "ASSY") if p else ""
        for p in bulk_parts.tolist()
    ]
    bulk["test_suggestions"] = [
        _fmt_suggestions(p, "TEST") if p else ""
        for p in bulk_parts.tolist()
    ]

    # Normalize date-like strings to date objects for editing
    for c in ["assy_date", "test_date", "stock_date", "due_date"]:
        bulk[c] = pd.to_datetime(bulk[c], errors="coerce").dt.date

    # --- Bulk apply controls FIRST (so the table can sit lower and be taller) ---
    st.markdown("**Bulk apply (uses the Select checkboxes below):**")
    with st.form("bulk_apply"):
        b1, b2, b3 = st.columns(3)
        with b1:
            b_assy_dt = st.date_input("Bulk assy date", value=None)
            b_assy_emp = st.text_input("Bulk assy employee (must be last 2 for part)", value="")
        with b2:
            b_test_dt = st.date_input("Bulk test date", value=None)
            b_test_emp = st.text_input("Bulk test employee (must be last 2 for part)", value="")
        with b3:
            b_stock_dt = st.date_input("Bulk stock date", value=None)

        apply_btn = st.form_submit_button("Apply to selected mains")

    st.markdown("**Bulk table (edit assy/test dates + employees directly, then click Save row edits):**")

    editable_cols = {"select", "assy_date", "test_date", "stock_date", "assy_emp", "test_emp"}

    edited = st.data_editor(
        bulk,
        hide_index=True,
        use_container_width=True,
        column_config={
            "select": st.column_config.CheckboxColumn("Select", width="small"),
            "delivery": st.column_config.TextColumn("Delivery", width="large"),
            "customer": st.column_config.TextColumn("Customer", width="large"),
            "parent_wo": st.column_config.TextColumn("Work Order", width="medium"),
            "parent_part": st.column_config.TextColumn("Parent Part", width="medium"),
            "line_value": st.column_config.NumberColumn("Value", format="$%,d", width="medium"),
            "wo_qty": st.column_config.NumberColumn("Qty", format="%d", width="small"),
            "due_date": st.column_config.DateColumn("Due", disabled=True, width="medium"),
            "assy_suggestions": st.column_config.TextColumn(
                "Assy sugg (last 2)",
                disabled=True,
                help="Last 2 assemblers for this part based on Dept Labor history.",
                width="medium",
            ),
            "test_suggestions": st.column_config.TextColumn(
                "Test sugg (last 2)",
                disabled=True,
                help="Last 2 testers for this part based on Dept Labor history.",
                width="medium",
            ),
            "assy_date": st.column_config.DateColumn("Assy date", width="medium"),
            "test_date": st.column_config.DateColumn("Test date", width="medium"),
            "stock_date": st.column_config.DateColumn("Stock date", width="medium"),
            "assy_emp": st.column_config.TextColumn(
                "Assy emp",
                help="Type one of the suggested employees (last 2). Blank allowed.",
                width="medium",
            ),
            "test_emp": st.column_config.TextColumn(
                "Test emp",
                help="Type one of the suggested employees (last 2). Blank allowed.",
                width="medium",
            ),
        },
        disabled=[c for c in bulk.columns if c not in editable_cols],
        height=1120,
    )

    # Selection for bulk apply
    chosen_rows = edited.index[edited["select"] == True].tolist()
    chosen_ids = bulk_main_ids.loc[chosen_rows].astype(int).tolist()

    if apply_btn:
        if not chosen_ids:
            st.warning("No rows selected. Tick the Select checkbox for the mains you want to update.")
        else:
            bad = []
            rows = []

            # Build main_id -> parent_part lookup
            part_lu = mains.set_index("main_id")["parent_part"].fillna("").astype(str).str.strip().to_dict()

            for mid in chosen_ids:
                part_for_emp = str(part_lu.get(int(mid), "") or "").strip()

                # Validate employee inputs (blank is allowed)
                assy_emp_use = (b_assy_emp or "").strip()
                test_emp_use = (b_test_emp or "").strip()

                if assy_emp_use and not validate_employee_for_part(part_for_emp, "ASSY", assy_emp_use):
                    bad.append((mid, part_for_emp, "ASSY", assy_emp_use, ", ".join(get_recent_employee_suggestions(part_for_emp, "ASSY", limit=2))))
                    assy_emp_use = ""  # do not overwrite

                if test_emp_use and not validate_employee_for_part(part_for_emp, "TEST", test_emp_use):
                    bad.append((mid, part_for_emp, "TEST", test_emp_use, ", ".join(get_recent_employee_suggestions(part_for_emp, "TEST", limit=2))))
                    test_emp_use = ""  # do not overwrite

                rows.append(
                    (
                        int(mid),
                        iso_or_none(b_assy_dt) if b_assy_dt else None,
                        iso_or_none(b_test_dt) if b_test_dt else None,
                        iso_or_none(b_stock_dt) if b_stock_dt else None,
                        assy_emp_use,
                        test_emp_use,
                        datetime.now().isoformat(timespec="seconds"),
                    )
                )

            exec_many(
                """
                INSERT INTO main_targets(main_id, assy_date, test_date, stock_date, assy_emp, test_emp, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(main_id) DO UPDATE SET
                  assy_date=coalesce(excluded.assy_date, main_targets.assy_date),
                  test_date=coalesce(excluded.test_date, main_targets.test_date),
                  stock_date=coalesce(excluded.stock_date, main_targets.stock_date),
                  assy_emp=case when excluded.assy_emp <> '' then excluded.assy_emp else main_targets.assy_emp end,
                  test_emp=case when excluded.test_emp <> '' then excluded.test_emp else main_targets.test_emp end,
                  updated_at=excluded.updated_at;
                """,
                rows,
            )
            st.success(f"Bulk update applied to {len(chosen_ids)} mains.")
            if bad:
                st.warning("Some employee assignments were skipped because the employee is not in the last-2 suggestions for that part.")
                st.dataframe(
                    pd.DataFrame(bad, columns=["main_id", "part", "area", "entered_emp", "allowed_last2"]),
                    use_container_width=True,
                    height=220,
                )
            st.cache_data.clear()
            st.rerun()

    # --- Save per-row edits (assy/test allocation and dates) ---
    if st.button("Save row edits", type="primary"):
        base = bulk.copy()
        changes = []

        # Compare edited vs base for editable columns (excluding select)
        for idx, row in edited.iterrows():
            mid = int(bulk_main_ids.iloc[idx])
            dirty = False

            # Normalize empty strings
            new_assy_emp = (row.get("assy_emp") or "").strip()
            new_test_emp = (row.get("test_emp") or "").strip()

            # Dates may be NaT/NaN
            new_assy_dt = row.get("assy_date")
            new_test_dt = row.get("test_date")
            new_stock_dt = row.get("stock_date")

            old = base.loc[idx]
            old_assy_emp = (old.get("assy_emp") or "").strip()
            old_test_emp = (old.get("test_emp") or "").strip()
            old_assy_dt = old.get("assy_date")
            old_test_dt = old.get("test_date")
            old_stock_dt = old.get("stock_date")

            if new_assy_emp != old_assy_emp:
                dirty = True
            if new_test_emp != old_test_emp:
                dirty = True
            if new_assy_dt != old_assy_dt:
                dirty = True
            if new_test_dt != old_test_dt:
                dirty = True
            if new_stock_dt != old_stock_dt:
                dirty = True

            if dirty:
                changes.append(
                    (
                        mid,
                        iso_or_none(new_assy_dt) if isinstance(new_assy_dt, date) else None,
                        iso_or_none(new_test_dt) if isinstance(new_test_dt, date) else None,
                        iso_or_none(new_stock_dt) if isinstance(new_stock_dt, date) else None,
                        new_assy_emp,
                        new_test_emp,
                        datetime.now().isoformat(timespec="seconds"),
                    )
                )

        # --- Validate employee allocations for each row ---
        invalid = []
        valid_changes = []
        for tup in changes:
            mid, a_dt, t_dt, s_dt, a_emp, t_emp, ts = tup
            # Look up the part for this main_id from the mains dataframe
            try:
                part_for_emp = str(mains.loc[mains["main_id"] == mid, "parent_part"].iloc[0] or "").strip()
            except Exception:
                part_for_emp = ""

            ok_a = validate_employee_for_part(part_for_emp, "ASSY", a_emp)
            ok_t = validate_employee_for_part(part_for_emp, "TEST", t_emp)
            if ok_a and ok_t:
                valid_changes.append(tup)
            else:
                invalid.append(
                    (
                        mid,
                        part_for_emp,
                        a_emp,
                        ", ".join(get_recent_employee_suggestions(part_for_emp, "ASSY", limit=2)),
                        t_emp,
                        ", ".join(get_recent_employee_suggestions(part_for_emp, "TEST", limit=2)),
                    )
                )

        if invalid:
            st.error(
                "Some rows were not saved because the selected employee is not in the last 2 recent workers for that part. "
                "Fix those rows and try again."
            )
            st.dataframe(
                pd.DataFrame(
                    invalid,
                    columns=["main_id", "part", "assy_emp", "assy_allowed_last2", "test_emp", "test_allowed_last2"],
                ),
                use_container_width=True,
                height=180,
            )

        changes = valid_changes

        if not changes:
            st.info("No row edits to save.")
        else:
            exec_many(
                """
                INSERT INTO main_targets(main_id, assy_date, test_date, stock_date, assy_emp, test_emp, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(main_id) DO UPDATE SET
                  assy_date=excluded.assy_date,
                  test_date=excluded.test_date,
                  stock_date=excluded.stock_date,
                  assy_emp=excluded.assy_emp,
                  test_emp=excluded.test_emp,
                  updated_at=excluded.updated_at;
                """,
                changes,
            )
            st.success(f"Saved {len(changes)} row edits.")
            st.cache_data.clear()
            st.rerun()


# -------------
# Subs tab (global view)
# -------------
with tabs[1]:
    st.subheader("Subs (global)")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as _c:
        _c.execute("PRAGMA foreign_keys=ON;")
        if has_column(_c, "sub_object", "comp_remaining"):
            rem_sql = "s.comp_remaining as rem_ops,"
        elif has_column(_c, "sub_object", "comp_remaining_ops"):
            rem_sql = "s.comp_remaining_ops as rem_ops,"
        elif has_column(_c, "sub_object", "sub_remaining"):
            rem_sql = "s.sub_remaining as rem_ops,"
        else:
            rem_sql = "'' as rem_ops,"

    subs = read_sql(
        f"""
        select
          s.sub_key, s.sub_part, s.sub_source, s.source_digits, s.sub_resp, s.sub_eta_date, s.sub_desc,
          {rem_sql}
          p.clear_date, p.assy_date, p.test_date, p.stock_date, p.assy_emp, p.test_emp,
          (select count(*) from peg_sub g where g.is_active=1 and g.sub_key=s.sub_key) as pegged_mains,
          (
            select group_concat(x, '\n')
            from (
              select
                coalesce(m.customer,'') || ' | ' ||
                coalesce(m.delivery,'') || ' | ' ||
                coalesce(m.parent_wo,'') || ' | dock ' || coalesce(m.dock_date,'') ||
                ' | qty ' || cast(coalesce(m.wo_qty,0) as int) ||
                ' | $' || cast(round(coalesce(m.line_value,0),0) as int)
                as x
              from peg_sub g
              join main_line m on m.main_id = g.main_id
              where g.is_active=1 and m.is_active=1 and g.sub_key = s.sub_key
              order by m.due_date, m.delivery
              limit 8
            )
          ) as pegged_preview
        from sub_object s
        left join sub_plan p on p.sub_key = s.sub_key
        where s.is_active=1
        order by pegged_mains desc, s.sub_eta_date, s.sub_part;
        """
    )
    subs = ensure_status_cols(subs)

    h_sub = hours_rollup_for_subs()
    subs = subs.merge(h_sub, how="left", on="source_digits")
    subs["assy_hrs"] = subs["assy_hrs"].fillna(0.0)
    subs["test_hrs"] = subs["test_hrs"].fillna(0.0)

    # Ensure sub_status/next_op columns exist (and derive if needed)
    subs = ensure_status_cols(subs)
    subs["assy_state"] = subs["rem_ops"].apply(lambda x: area_state_label(x, "ASSY"))
    subs["test_state"] = subs["rem_ops"].apply(lambda x: area_state_label(x, "TEST"))

    st.caption("Edit planning fields directly here. Changes save into sub_plan and apply across all pegged mains.")

    # Build an editable view. Keep sub_key as the index so it is not shown.
    view_cols = [
        "sub_key",
        "sub_part",
        "sub_source",
        "sub_resp",
        "sub_eta_date",
        "rem_ops",
        "pegged_mains",
        "pegged_preview",
        "assy_hrs",
        "test_hrs",
        "sub_status",
        "next_op",
        "assy_state",
        "test_state",
        "clear_date",
        "assy_date",
        "test_date",
        "stock_date",
        "assy_emp",
        "test_emp",
    ]

    subs_view = subs[view_cols].copy()

    # Suggested employees (last 2) for visibility in the subs editor
    def _fmt_suggestions(part: str, area: str) -> str:
        recent = get_recent_employee_suggestions(part, area, limit=2)
        if recent:
            return ", ".join(recent)
        return "NO HISTORY"
    _sub_parts = subs_view["sub_part"].fillna("").astype(str).str.strip()
    subs_view["assy_suggestions"] = [_fmt_suggestions(p, "ASSY") if p else "" for p in _sub_parts.tolist()]
    subs_view["test_suggestions"] = [_fmt_suggestions(p, "TEST") if p else "" for p in _sub_parts.tolist()]

    # Normalize date-like strings to date objects for the editor
    for c in ["sub_eta_date", "clear_date", "assy_date", "test_date", "stock_date"]:
        subs_view[c] = pd.to_datetime(subs_view[c], errors="coerce").dt.date

    # Set sub_key as index and hide it
    subs_view = subs_view.set_index("sub_key")

    emp_opts = get_employee_options()

    editable_cols = {"clear_date", "assy_date", "test_date", "stock_date", "assy_emp", "test_emp"}

    edited = st.data_editor(
        subs_view,
        use_container_width=True,
        height=650,
        hide_index=True,
        column_config={
            "sub_part": st.column_config.TextColumn("sub_part", disabled=True),
            "sub_source": st.column_config.TextColumn("sub_source", disabled=True),
            "sub_resp": st.column_config.TextColumn("sub_resp", disabled=True),
            "sub_eta_date": st.column_config.DateColumn("sub_eta_date", disabled=True),
            "rem_ops": st.column_config.TextColumn("rem_ops", disabled=True),
            "pegged_mains": st.column_config.NumberColumn("pegged_mains", disabled=True, format="%d"),
            "pegged_preview": st.column_config.TextColumn(
                "pegged_mains details",
                disabled=True,
                help="Hover to see up to 8 pegged mains: customer | order | WO | dock | qty | $value",
            ),
            "assy_hrs": st.column_config.NumberColumn("assy_hrs", disabled=True),
            "test_hrs": st.column_config.NumberColumn("test_hrs", disabled=True),
            "sub_status": st.column_config.TextColumn("sub_status", disabled=True),
            "next_op": st.column_config.TextColumn("next_op", disabled=True),
            "assy_state": st.column_config.TextColumn("ASSY status", disabled=True),
            "test_state": st.column_config.TextColumn("TEST status", disabled=True),
            "clear_date": st.column_config.DateColumn("clear_date"),
            "assy_date": st.column_config.DateColumn("assy_date"),
            "test_date": st.column_config.DateColumn("test_date"),
            "stock_date": st.column_config.DateColumn("stock_date"),
            "assy_suggestions": st.column_config.TextColumn(
                "Assy sugg (last 2)",
                disabled=True,
                help="Last 2 assemblers for this sub part based on Dept Labor history.",
            ),
            "test_suggestions": st.column_config.TextColumn(
                "Test sugg (last 2)",
                disabled=True,
                help="Last 2 testers for this sub part based on Dept Labor history.",
            ),
            "assy_emp": st.column_config.TextColumn(
                "assy_emp",
                help="Type one of the suggested employees (last 2). Blank allowed.",
            ),
            "test_emp": st.column_config.TextColumn(
                "test_emp",
                help="Type one of the suggested employees (last 2). Blank allowed.",
            ),
        },
        disabled=[c for c in subs_view.columns if c not in editable_cols],
    )

    # Save edits back to sub_plan
    if st.button("Save edits", type="primary"):
        # Compare only editable columns
        base = subs_view.copy()
        changes = []
        for sub_key, row in edited.iterrows():
            base_row = base.loc[sub_key]
            dirty = False
            vals = {}
            for c in editable_cols:
                newv = row[c]
                oldv = base_row[c]

                # normalize NaN/NaT
                if pd.isna(oldv):
                    oldv = None
                if pd.isna(newv):
                    newv = None

                if newv != oldv:
                    dirty = True
                vals[c] = newv

            if dirty:
                rem_ops_val = subs.loc[subs["sub_key"] == sub_key, "rem_ops"].iloc[0] if "rem_ops" in subs.columns else None
                assy_open = is_area_open_from_rem_ops(rem_ops_val, "ASSY")
                test_open = is_area_open_from_rem_ops(rem_ops_val, "TEST")

                assy_emp_val = (vals["assy_emp"] or "").strip()
                test_emp_val = (vals["test_emp"] or "").strip()

                # If the operation is completed, do not allow an assignment (auto-clear)
                if not assy_open:
                    assy_emp_val = ""
                if not test_open:
                    test_emp_val = ""

                changes.append(
                    (
                        str(sub_key),
                        iso_or_none(vals["clear_date"]),
                        iso_or_none(vals["assy_date"]),
                        iso_or_none(vals["test_date"]),
                        iso_or_none(vals["stock_date"]),
                        assy_emp_val,
                        test_emp_val,
                        datetime.now().isoformat(timespec="seconds"),
                    )
                )

        # --- Validate employee allocations for each row ---
        invalid = []
        valid_changes = []
        # Build a lookup from sub_key -> sub_part
        try:
            sub_part_lu = subs.set_index("sub_key")["sub_part"].astype(str).to_dict()
        except Exception:
            sub_part_lu = {}

        for tup in changes:
            sub_key, c_dt, a_dt, t_dt, s_dt, a_emp, t_emp, ts = tup
            part_for_emp = str(sub_part_lu.get(sub_key, "") or "").strip()
            ok_a = validate_employee_for_part(part_for_emp, "ASSY", a_emp)
            ok_t = validate_employee_for_part(part_for_emp, "TEST", t_emp)
            if ok_a and ok_t:
                valid_changes.append(tup)
            else:
                invalid.append(
                    (
                        sub_key,
                        part_for_emp,
                        a_emp,
                        ", ".join(get_recent_employee_suggestions(part_for_emp, "ASSY", limit=2)),
                        t_emp,
                        ", ".join(get_recent_employee_suggestions(part_for_emp, "TEST", limit=2)),
                    )
                )

        if invalid:
            st.error(
                "Some sub rows were not saved because the selected employee is not in the last 2 recent workers for that part."
            )
            st.dataframe(
                pd.DataFrame(
                    invalid,
                    columns=["sub_key", "part", "assy_emp", "assy_allowed_last2", "test_emp", "test_allowed_last2"],
                ),
                use_container_width=True,
                height=180,
            )

        changes = valid_changes

        if not changes:
            st.info("No changes to save.")
        else:
            exec_many(
                """
                INSERT INTO sub_plan(sub_key, clear_date, assy_date, test_date, stock_date, assy_emp, test_emp, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sub_key) DO UPDATE SET
                  clear_date=excluded.clear_date,
                  assy_date=excluded.assy_date,
                  test_date=excluded.test_date,
                  stock_date=excluded.stock_date,
                  assy_emp=excluded.assy_emp,
                  test_emp=excluded.test_emp,
                  updated_at=excluded.updated_at;
                """,
                changes,
            )
            st.success(f"Saved {len(changes)} sub plan updates.")
            st.caption("Note: If ASSY/TEST is COMPLETED (not present in Comp Remaining), employee assignments are automatically cleared.")
            st.cache_data.clear()
            st.rerun()


# -------------
# Dashboards tab
# -------------
with tabs[2]:
    st.subheader("Dashboards")

    dash_tabs = st.tabs([
        "Revenue by Stock Date",
        "Test Capacity by Date",
        "Daily Mains Overview",
        "Scheduled Lines",
        "Dispatch Report",
        "Historical Planning Approach",
        "ERP Work Order Lookup",
    ])
    # --- Historical Planning Approach (sheet-style view + export) ---
    with dash_tabs[5]:
        st.markdown("### Historical Planning Approach")
        st.caption(
            "Sheet-style view for planners who prefer Excel: SO#, customer, part, value, qty, WO, dock date, "
            "and the editable planning targets (assy/test/stock dates + assembler/tester)."
        )

        # Pull the same mains dataset the rest of the app uses (respects sidebar filters + Exclude RPO)
        mains = mains_core(filters)

        # Add a compact (truncated) list of sub shortages (sub part numbers) tied to each main.
        # This is for Excel-style users: quick visibility into what subs are holding the WO.
        subs_by_main = read_sql(
            """
            select
              g.main_id,
              group_concat(distinct s.sub_part) as sub_parts
            from peg_sub g
            join sub_object s on s.sub_key = g.sub_key
            where g.is_active = 1
              and s.is_active = 1
            group by g.main_id;
            """
        )

        if not subs_by_main.empty:
            subs_by_main["sub_parts"] = subs_by_main["sub_parts"].fillna("").astype(str)
            mains = mains.merge(subs_by_main, how="left", on="main_id")
        else:
            mains["sub_parts"] = ""

        def _truncate_csv(s: str, max_chars: int = 80) -> str:
            s = (s or "").strip()
            if len(s) <= max_chars:
                return s
            # Try to cut on a comma boundary for readability
            cut = s[:max_chars]
            if "," in cut:
                cut = cut.rsplit(",", 1)[0]
            return cut.rstrip() + " …"

        mains["sub_shortages"] = mains["sub_parts"].fillna("").astype(str).map(lambda x: _truncate_csv(x, 80))

        if mains.empty:
            st.info("No mains found for the current filters.")
        else:
            # Build the sheet-style view
            view = mains[[
                "main_id",
                "delivery",
                "customer",
                "parent_part",
                "line_value",
                "wo_qty",
                "parent_wo",
                "sub_shortages",
                "dock_date",
                "stock_date",
                "test_date",
                "assy_date",
                "test_emp",
                "assy_emp",
                "short_free",
            ]].copy()

            # Normalize datatypes for display/edit
            for c in ["dock_date", "stock_date", "test_date", "assy_date"]:
                view[c] = pd.to_datetime(view[c], errors="coerce").dt.date

            view["line_value"] = pd.to_numeric(view["line_value"], errors="coerce").fillna(0.0)
            view["wo_qty"] = pd.to_numeric(view["wo_qty"], errors="coerce").fillna(0).astype(int)

            # Use main_id as hidden index
            view = view.set_index("main_id")

            st.markdown("#### Planning sheet")
            st.caption("Edit ASSY/TEST/STOCK dates + assembler/tester, then click **Save planning edits**.")

            editable_cols = {"assy_date", "test_date", "stock_date", "assy_emp", "test_emp"}

            edited = st.data_editor(
                view,
                use_container_width=True,
                height=720,
                hide_index=True,
                column_config={
                    "delivery": st.column_config.TextColumn("SO#", disabled=True),
                    "customer": st.column_config.TextColumn("Customer", disabled=True),
                    "parent_part": st.column_config.TextColumn("Part Num", disabled=True),
                    "line_value": st.column_config.NumberColumn("Value", format="$%,d", disabled=True),
                    "wo_qty": st.column_config.NumberColumn("Qty", format="%d", disabled=True),
                    "parent_wo": st.column_config.TextColumn("Work Order", disabled=True),
                    "sub_shortages": st.column_config.TextColumn(
                        "Sub shortages (trunc)",
                        disabled=True,
                        help="Truncated list of sub part numbers pegged to this WO (from BU shortages).",
                    ),
                    "dock_date": st.column_config.DateColumn("Dock Date", disabled=True),
                    "stock_date": st.column_config.DateColumn("Stock Date"),
                    "test_date": st.column_config.DateColumn("Test Date"),
                    "assy_date": st.column_config.DateColumn("Assy Date"),
                    "test_emp": st.column_config.TextColumn("Tester", help="Type the last-2 suggested tester (blank allowed)."),
                    "assy_emp": st.column_config.TextColumn("Assembler", help="Type the last-2 suggested assembler (blank allowed)."),
                    "short_free": st.column_config.CheckboxColumn("Short Free", disabled=True),
                },
                disabled=[c for c in view.columns if c not in editable_cols],
            )

            # Save edits back to main_targets (same logic as Mains tab row edits)
            if st.button("Save planning edits", type="primary"):
                base = view.copy()
                changes = []
                invalid = []

                for mid, row in edited.iterrows():
                    try:
                        mid_i = int(mid)
                    except Exception:
                        continue

                    base_row = base.loc[mid]
                    dirty = False

                    new_assy_dt = row.get("assy_date")
                    new_test_dt = row.get("test_date")
                    new_stock_dt = row.get("stock_date")
                    new_assy_emp = (row.get("assy_emp") or "").strip()
                    new_test_emp = (row.get("test_emp") or "").strip()

                    old_assy_dt = base_row.get("assy_date")
                    old_test_dt = base_row.get("test_date")
                    old_stock_dt = base_row.get("stock_date")
                    old_assy_emp = (base_row.get("assy_emp") or "").strip()
                    old_test_emp = (base_row.get("test_emp") or "").strip()

                    if new_assy_dt != old_assy_dt:
                        dirty = True
                    if new_test_dt != old_test_dt:
                        dirty = True
                    if new_stock_dt != old_stock_dt:
                        dirty = True
                    if new_assy_emp != old_assy_emp:
                        dirty = True
                    if new_test_emp != old_test_emp:
                        dirty = True

                    if not dirty:
                        continue

                    # Validate employees using existing rules (blank allowed)
                    try:
                        part_for_emp = str(mains.loc[mains["main_id"] == mid_i, "parent_part"].iloc[0] or "").strip()
                    except Exception:
                        part_for_emp = ""

                    if new_assy_emp and not validate_employee_for_part(part_for_emp, "ASSY", new_assy_emp):
                        invalid.append((mid_i, part_for_emp, "ASSY", new_assy_emp, ", ".join(get_recent_employee_suggestions(part_for_emp, "ASSY", limit=2))))
                        new_assy_emp = ""
                    if new_test_emp and not validate_employee_for_part(part_for_emp, "TEST", new_test_emp):
                        invalid.append((mid_i, part_for_emp, "TEST", new_test_emp, ", ".join(get_recent_employee_suggestions(part_for_emp, "TEST", limit=2))))
                        new_test_emp = ""

                    changes.append(
                        (
                            mid_i,
                            iso_or_none(new_assy_dt) if isinstance(new_assy_dt, date) else None,
                            iso_or_none(new_test_dt) if isinstance(new_test_dt, date) else None,
                            iso_or_none(new_stock_dt) if isinstance(new_stock_dt, date) else None,
                            new_assy_emp,
                            new_test_emp,
                            datetime.now().isoformat(timespec="seconds"),
                        )
                    )

                if invalid:
                    st.warning("Some employee assignments were cleared because they were not in the last-2 suggestions for that part.")
                    st.dataframe(
                        pd.DataFrame(invalid, columns=["main_id", "part", "area", "entered_emp", "allowed_last2"]),
                        use_container_width=True,
                        height=220,
                    )

                if not changes:
                    st.info("No changes to save.")
                else:
                    exec_many(
                        """
                        INSERT INTO main_targets(main_id, assy_date, test_date, stock_date, assy_emp, test_emp, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(main_id) DO UPDATE SET
                          assy_date=excluded.assy_date,
                          test_date=excluded.test_date,
                          stock_date=excluded.stock_date,
                          assy_emp=excluded.assy_emp,
                          test_emp=excluded.test_emp,
                          updated_at=excluded.updated_at;
                        """,
                        changes,
                    )
                    st.success(f"Saved {len(changes)} planning edits.")
                    st.cache_data.clear()
                    st.rerun()

            st.divider()
            st.markdown("#### Export")
            st.caption("Download the current filtered view as an Excel file (including the editable planning columns).")

            export_df = edited.reset_index().copy()

            # Make an Excel in-memory
            bio_xlsx = io.BytesIO()
            with pd.ExcelWriter(bio_xlsx, engine="openpyxl") as writer:
                export_df.to_excel(writer, index=False, sheet_name="Historical Planning")
            bio_xlsx.seek(0)

            st.download_button(
                "Download Excel (Historical Planning Approach)",
                data=bio_xlsx.getvalue(),
                file_name="Historical_Planning_Approach.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    # --- ERP Work Order Lookup (ERP-like drilldown) ---
    with dash_tabs[6]:
        st.markdown("### ERP Work Order Lookup")
        st.caption(
            "Type a part number to see every Work Order tied to it. Select a WO to view Sales Orders pegged to it, "
            "material shortages with ETA + Comp Remaining, and editable planning targets (ASSY/TEST/STOCK + employees)."
        )

        part_q = st.text_input("Part number", value="", placeholder="e.g., 215298")
        part_q_norm = (part_q or "").strip()

        if not part_q_norm:
            st.info("Enter a part number to begin.")
        else:
            wo_hits = read_sql(
                """
                select
                  m.main_id,
                  m.delivery,
                  m.customer,
                  m.parent_part,
                  m.parent_wo,
                  m.parent_wo_norm,
                  m.bu,
                  m.type,
                  m.due_date,
                  m.dock_date,
                  m.wo_qty,
                  m.line_value,
                  m.short_free,
                  t.assy_date,
                  t.test_date,
                  t.stock_date,
                  t.assy_emp,
                  t.test_emp
                from main_line m
                left join main_targets t on t.main_id = m.main_id
                where m.is_active = 1
                  and (:exclude_rpo = 0 or m.parent_wo not like 'RPO%')
                  and upper(trim(m.parent_part)) = upper(trim(:part))
                order by m.parent_wo_norm, m.due_date, m.delivery;
                """,
                params={
                    "part": part_q_norm,
                    "exclude_rpo": 1 if filters.get("exclude_rpo", True) else 0,
                },
            )

            if wo_hits.empty:
                st.warning("No Work Orders found for that part number (in mains).")
            else:
                wo_list = (
                    wo_hits[["parent_wo_norm", "parent_wo"]]
                    .dropna()
                    .drop_duplicates(subset=["parent_wo_norm"])
                    .reset_index(drop=True)
                )
                wo_list["wo_label"] = wo_list["parent_wo"].astype(str)

                c0a, c0b = st.columns([2, 3], gap="large")
                with c0a:
                    sel_wo = st.selectbox("Work Order", wo_list["wo_label"].tolist())
                    sel_wo_norm = str(wo_list.loc[wo_list["wo_label"] == sel_wo, "parent_wo_norm"].iloc[0])

                wo_lines = wo_hits[wo_hits["parent_wo_norm"] == sel_wo_norm].copy()

                wo_lines["line_label"] = (
                    wo_lines["delivery"].fillna("").astype(str)
                    + " | "
                    + wo_lines["customer"].fillna("").astype(str)
                    + " | qty "
                    + pd.to_numeric(wo_lines["wo_qty"], errors="coerce").fillna(0).astype(int).astype(str)
                    + " | $"
                    + pd.to_numeric(wo_lines["line_value"], errors="coerce").fillna(0).astype(float).map(lambda x: f"{x:,.0f}")
                    + " | due "
                    + wo_lines["due_date"].fillna("").astype(str)
                )

                with c0b:
                    sel_line = st.selectbox("Sales line (SO) pegged to this WO", wo_lines["line_label"].tolist())

                sel_row = wo_lines.loc[wo_lines["line_label"] == sel_line].iloc[0]
                main_id = int(sel_row["main_id"])

                st.divider()

                due_txt = str(sel_row.get("due_date") or "")
                dock_txt = str(sel_row.get("dock_date") or "")
                qty_i = int(pd.to_numeric(sel_row.get("wo_qty", 0), errors="coerce") or 0)
                val_f = float(pd.to_numeric(sel_row.get("line_value", 0), errors="coerce") or 0.0)

                s1, s2, s3, s4 = st.columns(4)
                with s1:
                    st.metric("Work Order", str(sel_row.get("parent_wo") or "—"))
                with s2:
                    st.metric("Due", due_txt if due_txt else "—")
                with s3:
                    st.metric("Dock", dock_txt if dock_txt else "—")
                with s4:
                    st.metric("Value", f"${val_f:,.0f}")

                st.markdown(
                    f"""
**Customer:** {sel_row.get('customer','') or '—'}  
**SO# (Delivery):** {sel_row.get('delivery','') or '—'}  
**BU:** {sel_row.get('bu','') or '—'}  
**Part:** {sel_row.get('parent_part','') or '—'}
                    """.strip()
                )

                st.markdown("### Edit planning targets")
                with st.form("erp_main_targets_form", clear_on_submit=False):
                    assy_dt = safe_date_input("Assy date", sel_row.get("assy_date"))
                    test_dt = safe_date_input("Test date", sel_row.get("test_date"))
                    stock_dt = safe_date_input("Stock date", sel_row.get("stock_date"))

                    part_for_emp = str(sel_row.get("parent_part") or "").strip()
                    assy_emp_opts, assy_mode = employee_dropdown_options(part_for_emp, "ASSY", limit=2)
                    test_emp_opts, test_mode = employee_dropdown_options(part_for_emp, "TEST", limit=2)

                    if assy_mode == "fallback":
                        st.warning("No ASSY history for this part — showing all assemblers (fallback).")
                    if test_mode == "fallback":
                        st.warning("No TEST history for this part — showing all testers (fallback).")

                    assy_emp = st.selectbox(
                        "Assy employee (last 2 for part)",
                        assy_emp_opts,
                        index=assy_emp_opts.index(sel_row.get("assy_emp") or "") if (sel_row.get("assy_emp") or "") in assy_emp_opts else 0,
                    )
                    test_emp = st.selectbox(
                        "Test employee (last 2 for part)",
                        test_emp_opts,
                        index=test_emp_opts.index(sel_row.get("test_emp") or "") if (sel_row.get("test_emp") or "") in test_emp_opts else 0,
                    )

                    save = st.form_submit_button("Save targets")
                    if save:
                        if not validate_employee_for_part(part_for_emp, "ASSY", assy_emp):
                            st.error("Assy employee must be one of the last 2 recent assemblers for this part (or blank).")
                            st.stop()
                        if not validate_employee_for_part(part_for_emp, "TEST", test_emp):
                            st.error("Test employee must be one of the last 2 recent testers for this part (or blank).")
                            st.stop()

                        exec_sql(
                            """
                            INSERT INTO main_targets(main_id, assy_date, test_date, stock_date, assy_emp, test_emp, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(main_id) DO UPDATE SET
                              assy_date=excluded.assy_date,
                              test_date=excluded.test_date,
                              stock_date=excluded.stock_date,
                              assy_emp=excluded.assy_emp,
                              test_emp=excluded.test_emp,
                              updated_at=excluded.updated_at;
                            """,
                            (
                                main_id,
                                iso_or_none(assy_dt),
                                iso_or_none(test_dt),
                                iso_or_none(stock_dt),
                                assy_emp,
                                test_emp,
                                datetime.now().isoformat(timespec="seconds"),
                            ),
                        )
                        st.success("Saved targets.")
                        st.cache_data.clear()
                        st.rerun()

                st.divider()
                st.markdown("### Sales lines pegged to this Work Order")
                so_view = wo_lines[[
                    "delivery",
                    "customer",
                    "bu",
                    "due_date",
                    "dock_date",
                    "wo_qty",
                    "line_value",
                    "short_free",
                    "assy_date",
                    "test_date",
                    "stock_date",
                    "assy_emp",
                    "test_emp",
                ]].copy()
                st.dataframe(so_view, use_container_width=True, height=220)

                st.divider()
                st.markdown("### Shortages (Subs) tied to this Work Order")
                subs = subs_for_main(main_id)
                subs = ensure_status_cols(subs)

                if subs.empty:
                    st.info("No pegged subs/shortages found for this WO.")
                else:
                    h_sub = hours_rollup_for_subs()
                    subs = subs.merge(h_sub, how="left", on="source_digits")
                    subs["assy_hrs"] = subs["assy_hrs"].fillna(0.0)
                    subs["test_hrs"] = subs["test_hrs"].fillna(0.0)

                    show = subs[[
                        "sub_part",
                        "sub_source",
                        "sub_resp",
                        "sub_eta_date",
                        "rem_ops",
                        "assy_hrs",
                        "test_hrs",
                        "sub_status",
                        "next_op",
                        "clear_date",
                        "assy_date",
                        "test_date",
                        "stock_date",
                        "assy_emp",
                        "test_emp",
                    ]].copy()
                    show["assy_state"] = show["rem_ops"].apply(lambda x: area_state_label(x, "ASSY"))
                    show["test_state"] = show["rem_ops"].apply(lambda x: area_state_label(x, "TEST"))

                    st.dataframe(
                        show,
                        use_container_width=True,
                        height=280,
                        column_config={
                            "assy_state": st.column_config.TextColumn("ASSY status"),
                            "test_state": st.column_config.TextColumn("TEST status"),
                        },
                    )

                    st.markdown("#### Edit a shortage (sub plan)")
                    pick = subs.copy()
                    pick["sub_eta_date"] = pd.to_datetime(pick["sub_eta_date"], errors="coerce").dt.date

                    def _clean_rem_ops(v):
                        if v is None:
                            return ""
                        s = str(v).strip()
                        return "" if s.lower() in {"none", "nan"} else s

                    labels = []
                    key_by_label = {}
                    for _, r in pick.iterrows():
                        eta = r.get("sub_eta_date")
                        eta_txt = eta.isoformat() if isinstance(eta, date) else ""
                        rem_txt = _clean_rem_ops(r.get("rem_ops"))
                        label = (
                            f"{(r.get('sub_part') or '')} | {(r.get('sub_source') or '')} | {(r.get('sub_resp') or '')}"
                            + (f" | ETA {eta_txt}" if eta_txt else "")
                            + (f" | RemOps {rem_txt}" if rem_txt else "")
                        ).strip()
                        if label in key_by_label:
                            label = f"{label}  [dup:{r.get('sub_key')}]"
                        labels.append(label)
                        key_by_label[label] = r.get("sub_key")

                    sel_sub = st.selectbox("Select shortage (sub)", labels)
                    sub_key = str(key_by_label.get(sel_sub))

                    plan_row = read_sql("select * from sub_plan where sub_key = ?;", params=(sub_key,))
                    plan = plan_row.iloc[0] if len(plan_row) else None

                    _sub_row = subs[subs["sub_key"] == sub_key]
                    _sr = _sub_row.iloc[0] if len(_sub_row) else None

                    rem_ops_val = _sr.get("rem_ops") if _sr is not None else None
                    assy_open = is_area_open_from_rem_ops(rem_ops_val, "ASSY")
                    test_open = is_area_open_from_rem_ops(rem_ops_val, "TEST")

                    st.caption("Mains impacted by this sub:")
                    st.dataframe(pegged_mains_for_sub(sub_key), use_container_width=True, height=200)

                    with st.form("erp_sub_plan_form", clear_on_submit=False):
                        clear_dt = safe_date_input("Clear date", plan["clear_date"] if plan is not None else None)
                        s_assy_dt = safe_date_input("Sub assy date", plan["assy_date"] if plan is not None else None)
                        s_test_dt = safe_date_input("Sub test date", plan["test_date"] if plan is not None else None)
                        s_stock_dt = safe_date_input("Sub stock date", plan["stock_date"] if plan is not None else None)

                        sub_part_for_emp = str(_sr.get("sub_part") or "").strip() if _sr is not None else ""

                        if not assy_open:
                            st.info("ASSY is COMPLETED for this sub (ASSY/ASM not present in Comp Remaining).")
                            s_assy_emp = ""
                        else:
                            s_assy_emp_opts, s_assy_mode = employee_dropdown_options(sub_part_for_emp, "ASSY", limit=2)
                            if s_assy_mode == "fallback":
                                st.warning("No ASSY history for this sub part — showing all assemblers (fallback).")
                            s_assy_emp = st.selectbox("Sub assy employee (last 2 for part)", s_assy_emp_opts, index=0)

                        if not test_open:
                            st.info("TEST is COMPLETED for this sub (no TEST op present in Comp Remaining).")
                            s_test_emp = ""
                        else:
                            s_test_emp_opts, s_test_mode = employee_dropdown_options(sub_part_for_emp, "TEST", limit=2)
                            if s_test_mode == "fallback":
                                st.warning("No TEST history for this sub part — showing all testers (fallback).")
                            s_test_emp = st.selectbox("Sub test employee (last 2 for part)", s_test_emp_opts, index=0)

                        save_sub = st.form_submit_button("Save shortage plan")
                        if save_sub:
                            if not assy_open and (s_assy_emp or "").strip():
                                st.error("Cannot assign an ASSY employee: ASSY is COMPLETED.")
                                st.stop()
                            if not test_open and (s_test_emp or "").strip():
                                st.error("Cannot assign a TEST employee: TEST is COMPLETED.")
                                st.stop()
                            if not validate_employee_for_part(sub_part_for_emp, "ASSY", s_assy_emp):
                                st.error("Sub assy employee must be one of the last 2 recent assemblers for this part (or blank).")
                                st.stop()
                            if not validate_employee_for_part(sub_part_for_emp, "TEST", s_test_emp):
                                st.error("Sub test employee must be one of the last 2 recent testers for this part (or blank).")
                                st.stop()

                            exec_sql(
                                """
                                INSERT INTO sub_plan(sub_key, clear_date, assy_date, test_date, stock_date, assy_emp, test_emp, updated_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT(sub_key) DO UPDATE SET
                                  clear_date=excluded.clear_date,
                                  assy_date=excluded.assy_date,
                                  test_date=excluded.test_date,
                                  stock_date=excluded.stock_date,
                                  assy_emp=excluded.assy_emp,
                                  test_emp=excluded.test_emp,
                                  updated_at=excluded.updated_at;
                                """,
                                (
                                    sub_key,
                                    iso_or_none(clear_dt),
                                    iso_or_none(s_assy_dt),
                                    iso_or_none(s_test_dt),
                                    iso_or_none(s_stock_dt),
                                    s_assy_emp,
                                    s_test_emp,
                                    datetime.now().isoformat(timespec="seconds"),
                                ),
                            )
                            st.success("Saved shortage plan.")
                            st.cache_data.clear()
                            st.rerun()

    # --- Revenue by Stock Date ---
    with dash_tabs[0]:
        st.markdown("### Revenue by scheduled stock date (mains only)")

        df = read_sql(
            """
            select
              t.stock_date as d,
              sum(coalesce(m.line_value,0)) as revenue,
              sum(coalesce(m.wo_qty,0)) as qty,
              count(*) as mains
            from main_line m
            join main_targets t on t.main_id=m.main_id
            where m.is_active=1 and (:exclude_rpo = 0 or m.parent_wo not like 'RPO%')
              and t.stock_date is not null and t.stock_date <> ''
            group by t.stock_date
            order by t.stock_date;
            """,
            params={
                "exclude_rpo": 1 if filters.get("exclude_rpo", True) else 0,
            },
        )

        if df.empty:
            st.info("No stock dates scheduled yet. Schedule stock dates on the Mains tab.")
        else:
            df["d"] = pd.to_datetime(df["d"])
            st.dataframe(df, use_container_width=True, height=260)
            st.bar_chart(df.set_index("d")[["revenue"]], height=360)

        # ----------- New Visuals -----------
        st.divider()
        st.markdown("### Dock-date value: potential vs scheduled")
        st.caption(
            "Compares total line value by Dock Date (what we could ship) versus the subset that is scheduled "
            "to be stocked by an as-of date (what we are targeting). Uses the Dock Date filter window from the sidebar."
        )

        # As-of date for scheduled stock targets
        asof = st.date_input("Scheduled as-of (Stock Date <=)", value=date.today())

        dock_cmp = read_sql(
            """
            select
              m.dock_date as dock_date,
              sum(coalesce(m.line_value,0)) as booked_value,
              sum(case when t.stock_date is not null and t.stock_date <> '' and t.stock_date <= :asof
                       then coalesce(m.line_value,0) else 0 end) as scheduled_value
            from main_line m
            left join main_targets t on t.main_id = m.main_id
            where m.is_active = 1
              and m.dock_date is not null and m.dock_date <> ''
              and (:bu = '' or lower(m.bu) like '%' || lower(:bu) || '%')
              and (:customer = '' or lower(m.customer) like '%' || lower(:customer) || '%')
              and (:delivery = '' or lower(m.delivery) like '%' || lower(:delivery) || '%')
              and (:dock_from = '' or m.dock_date >= :dock_from)
              and (:dock_to = '' or m.dock_date <= :dock_to)
            group by m.dock_date
            order by m.dock_date;
            """,
            params={
                "asof": asof.isoformat(),
                "bu": filters.get("bu", ""),
                "customer": filters.get("customer", ""),
                "delivery": filters.get("delivery", ""),
                "dock_from": filters.get("dock_from", ""),
                "dock_to": filters.get("dock_to", ""),
                "exclude_rpo": 1 if filters.get("exclude_rpo", True) else 0,
            },
        )

        if dock_cmp.empty:
            st.info("No dock-date lines found for the current filter window.")
        else:
            dock_cmp["dock_date"] = pd.to_datetime(dock_cmp["dock_date"], errors="coerce")
            dock_cmp = dock_cmp.dropna(subset=["dock_date"]).sort_values("dock_date")

            cA, cB, cC = st.columns(3)
            with cA:
                st.metric("Booked (Dock-date)", f"${dock_cmp['booked_value'].sum():,.0f}")
            with cB:
                st.metric("Scheduled by as-of", f"${dock_cmp['scheduled_value'].sum():,.0f}")
            with cC:
                gap = float(dock_cmp["booked_value"].sum() - dock_cmp["scheduled_value"].sum())
                st.metric("Gap", f"${gap:,.0f}")

            st.dataframe(
                dock_cmp.assign(
                    dock_date=dock_cmp["dock_date"].dt.date,
                    gap=(dock_cmp["booked_value"] - dock_cmp["scheduled_value"]),
                ),
                use_container_width=True,
                height=220,
            )

            # Plot (line) chart: booked vs scheduled by dock_date
            plot_df = dock_cmp.copy()
            plot_df["dock_date"] = pd.to_datetime(plot_df["dock_date"], errors="coerce")
            plot_df = plot_df.dropna(subset=["dock_date"]).sort_values("dock_date")

            melted = plot_df.melt(
                id_vars=["dock_date"],
                value_vars=["booked_value", "scheduled_value"],
                var_name="series",
                value_name="value",
            )

            series_labels = {
                "booked_value": "Booked (dock-date)",
                "scheduled_value": "Scheduled by as-of",
            }
            melted["series"] = melted["series"].map(series_labels).fillna(melted["series"])

            chart = (
                alt.Chart(melted)
                .mark_line(point=True)
                .encode(
                    x=alt.X("dock_date:T", title="Dock date"),
                    y=alt.Y("value:Q", title="Value ($)", axis=alt.Axis(format="~s")),
                    color=alt.Color("series:N", title=""),
                    tooltip=[
                        alt.Tooltip("dock_date:T", title="Dock"),
                        alt.Tooltip("series:N", title=""),
                        alt.Tooltip("value:Q", title="Value", format=",.0f"),
                    ],
                )
                .properties(height=360)
            )

            st.altair_chart(chart, use_container_width=True)

        st.divider()
        st.markdown("### Past-due dollars by month (Dock Date < today)")
        st.caption("Past due = Dock Date is before today and the line is not stocked by today (no stock date or stock date after today).")

        past_due = read_sql(
            """
            with base as (
              select
                substr(m.dock_date, 1, 7) as month,
                coalesce(m.line_value,0) as line_value,
                coalesce(m.wo_qty,0) as qty,
                m.customer as customer,
                m.delivery as sales_order,
                m.parent_part as main_part,
                m.parent_wo as work_order
              from main_line m
              left join main_targets t on t.main_id = m.main_id
              where m.is_active = 1
                and m.dock_date is not null and m.dock_date <> ''
                and m.dock_date < :today
                and (t.stock_date is null or t.stock_date = '' or t.stock_date > :today)
            ), ranked as (
              select
                month,
                line_value,
                qty,
                customer,
                sales_order,
                main_part,
                work_order,
                row_number() over (partition by month order by line_value desc) as rn
              from base
            ), examples as (
              select
                month,
                group_concat(
                  customer || ' | ' || sales_order || ' | ' || main_part || ' | qty ' || cast(qty as int) || ' | $' || cast(round(line_value,0) as int),
                  '\n'
                ) as examples
              from ranked
              where rn <= 5
              group by month
            )
            select
              b.month,
              sum(b.line_value) as past_due_value,
              count(*) as lines,
              coalesce(e.examples, '') as examples
            from base b
            left join examples e on e.month = b.month
            group by b.month
            order by b.month;
            """,
            params={"today": date.today().isoformat()},
        )

        if past_due.empty:
            st.success("No past-due dollars based on Dock Date < today.")
        else:
            past_due["month"] = pd.to_datetime(past_due["month"] + "-01", errors="coerce")
            past_due = past_due.dropna(subset=["month"]).sort_values("month")
            past_due_disp = past_due.assign(month=past_due["month"].dt.strftime("%Y-%m"))
            st.dataframe(past_due_disp, use_container_width=True, height=220)

            chart = (
                alt.Chart(past_due_disp)
                .mark_bar()
                .encode(
                    x=alt.X("month:N", title="Month"),
                    y=alt.Y("past_due_value:Q", title="Past-due value"),
                    tooltip=[
                        alt.Tooltip("month:N", title="Month"),
                        alt.Tooltip("past_due_value:Q", title="Past-due $", format=",.0f"),
                        alt.Tooltip("lines:Q", title="Lines"),
                        alt.Tooltip("examples:N", title="Top 5 lines (preview)"),
                    ],
                )
            )
            st.altair_chart(chart, use_container_width=True)

            st.markdown("#### Drilldown: past-due lines for a month")
            pick_month = st.selectbox("Select month", past_due_disp["month"].tolist(), index=len(past_due_disp) - 1)

            past_due_lines = read_sql(
                """
                select
                  m.customer,
                  m.delivery as sales_order,
                  m.parent_part as main_part,
                  m.parent_wo as work_order,
                  m.wo_qty as qty,
                  m.line_value as value,
                  m.dock_date,
                  t.stock_date as stock_target_date
                from main_line m
                left join main_targets t on t.main_id = m.main_id
                where m.is_active = 1
                  and m.dock_date is not null and m.dock_date <> ''
                  and m.dock_date < :today
                  and (t.stock_date is null or t.stock_date = '' or t.stock_date > :today)
                  and substr(m.dock_date, 1, 7) = :month
                order by m.dock_date, m.customer, m.delivery;
                """,
                params={"today": date.today().isoformat(), "month": pick_month},
            )

            if past_due_lines.empty:
                st.info("No past-due lines for the selected month.")
            else:
                # Normalize for display
                for c in ["dock_date", "stock_target_date"]:
                    past_due_lines[c] = pd.to_datetime(past_due_lines[c], errors="coerce").dt.date
                past_due_lines["qty"] = pd.to_numeric(past_due_lines["qty"], errors="coerce").fillna(0).astype(int)
                past_due_lines["value"] = pd.to_numeric(past_due_lines["value"], errors="coerce").fillna(0.0)

                st.caption(
                    "Hover tooltips on the chart show month totals. Use this drilldown table for the line-level "
                    "details you requested (customer, order, part, qty)."
                )

                st.dataframe(
                    past_due_lines,
                    use_container_width=True,
                    height=520,
                    column_config={
                        "customer": st.column_config.TextColumn("Customer"),
                        "sales_order": st.column_config.TextColumn("Order"),
                        "main_part": st.column_config.TextColumn("Part"),
                        "qty": st.column_config.NumberColumn("Qty", format="%d"),
                        "work_order": st.column_config.TextColumn("Work order"),
                        "dock_date": st.column_config.DateColumn("Dock date"),
                        "stock_target_date": st.column_config.DateColumn("Stock target date"),
                        "value": st.column_config.NumberColumn("Value", format="$%,d"),
                    },
                )

    # --- Capacity by Date (ASSY/TEST, MAINS + SUBS) ---
    with dash_tabs[1]:
        st.markdown("### Capacity by scheduled date")
        st.caption(
            "Shows MAINS and SUBS grouped by day. Use the controls below to switch ASSY vs TEST and to filter by BU. "
            "Subs are counted once per sub (shared across pegged mains)."
        )

        # Controls
        c0, c1, c2, c3 = st.columns([1.2, 2.2, 1.2, 1.2])
        with c0:
            area = st.selectbox("Area", ["TEST", "ASSY"], index=0)
        with c1:
            bu_opts = get_bu_options()
            # default: all
            bu_sel = st.multiselect("BU (toggle)", options=bu_opts, default=bu_opts)
        with c2:
            include_mains = st.checkbox("Include mains", value=True)
        with c3:
            include_subs = st.checkbox("Include subs", value=True)

        date_col = "test_date" if area == "TEST" else "assy_date"
        headcount = float(test_headcount) if area == "TEST" else float(assy_headcount)
        capacity_per_day = headcount * float(hrs_per_emp)

        # -----------------
        # MAINS
        # -----------------
        mains_daily = pd.DataFrame(columns=["d", "mains_hrs", "mains_value", "mains_count"])
        if include_mains:
            mains = mains_core(filters)

            # BU filter (toggle)
            if bu_sel:
                mains = mains[mains["bu"].fillna("").astype(str).isin(bu_sel)]

            # attach hours rollups
            h_main = hours_rollup_for_mains()
            mains = mains.merge(h_main, how="left", left_on="parent_wo_norm", right_on="wo_norm")
            mains["assy_hrs"] = mains["assy_hrs"].fillna(0.0)
            mains["test_hrs"] = mains["test_hrs"].fillna(0.0)

            # scheduled date
            mains[date_col] = pd.to_datetime(mains[date_col], errors="coerce")
            sched_m = mains.dropna(subset=[date_col]).copy()

            if not sched_m.empty:
                sched_m["d"] = sched_m[date_col].dt.date
                hrs_field = "test_hrs" if area == "TEST" else "assy_hrs"
                sched_m["hrs"] = pd.to_numeric(sched_m[hrs_field], errors="coerce").fillna(0.0)
                sched_m["val"] = pd.to_numeric(sched_m["line_value"], errors="coerce").fillna(0.0)

                mains_daily = (
                    sched_m.groupby("d", as_index=False)
                    .agg(mains_hrs=("hrs", "sum"), mains_value=("val", "sum"), mains_count=("main_id", "count"))
                    .sort_values("d")
                )

        # -----------------
        # SUBS
        # -----------------
        subs_daily = pd.DataFrame(columns=["d", "subs_hrs", "subs_count"])
        subs_drill = pd.DataFrame()
        if include_subs:
            # Pull subs scheduled by sub_plan and attach a BU list from pegged mains
            with sqlite3.connect(DB_PATH) as _c:
                _c.execute("PRAGMA foreign_keys=ON;")
                if has_column(_c, "sub_object", "comp_remaining"):
                    rem_sql = "s.comp_remaining as rem_ops,"
                elif has_column(_c, "sub_object", "comp_remaining_ops"):
                    rem_sql = "s.comp_remaining_ops as rem_ops,"
                elif has_column(_c, "sub_object", "sub_remaining"):
                    rem_sql = "s.sub_remaining as rem_ops,"
                else:
                    rem_sql = "'' as rem_ops,"

            subs = read_sql(
                f"""
                select
                  s.sub_key,
                  s.sub_part,
                  s.sub_source,
                  s.sub_resp,
                  s.source_digits,
                  {rem_sql}
                  p.{date_col} as sched_date,
                  (
                    select group_concat(distinct trim(m.bu))
                    from peg_sub g
                    join main_line m on m.main_id = g.main_id
                    where g.is_active=1 and m.is_active=1 and g.sub_key = s.sub_key
                      and m.bu is not null and trim(m.bu) <> ''
                  ) as bu_list,
                  (
                    select count(*)
                    from peg_sub g
                    where g.is_active=1 and g.sub_key = s.sub_key
                  ) as pegged_mains
                from sub_object s
                join sub_plan p on p.sub_key = s.sub_key
                where s.is_active=1
                  and p.{date_col} is not null and p.{date_col} <> ''
                """,
                params={},
            )

            if not subs.empty:
                # BU filter (toggle): include sub if ANY of its pegged BU values is in selection
                if bu_sel:
                    def _sub_in_bu(bu_list: str | None) -> bool:
                        if not bu_list:
                            return False
                        toks = [x.strip() for x in str(bu_list).split(",") if x.strip()]
                        return any(t in set(bu_sel) for t in toks)

                    subs = subs[subs["bu_list"].apply(_sub_in_bu)]

                # Attach hours by digits
                h_sub = hours_rollup_for_subs()
                subs = subs.merge(h_sub, how="left", on="source_digits")
                subs["assy_hrs"] = subs["assy_hrs"].fillna(0.0)
                subs["test_hrs"] = subs["test_hrs"].fillna(0.0)

                subs["sched_date"] = pd.to_datetime(subs["sched_date"], errors="coerce")
                subs = subs.dropna(subset=["sched_date"]).copy()

                if not subs.empty:
                    subs["d"] = subs["sched_date"].dt.date
                    hrs_field = "test_hrs" if area == "TEST" else "assy_hrs"
                    subs["hrs"] = pd.to_numeric(subs[hrs_field], errors="coerce").fillna(0.0)
                    subs["pegged_mains"] = pd.to_numeric(subs["pegged_mains"], errors="coerce").fillna(0).astype(int)

                    subs_daily = (
                        subs.groupby("d", as_index=False)
                        .agg(subs_hrs=("hrs", "sum"), subs_count=("sub_key", "count"))
                        .sort_values("d")
                    )

                    # Keep drilldown details
                    subs_drill = subs[[
                        "d",
                        "sub_part",
                        "sub_source",
                        "sub_resp",
                        "hrs",
                        "pegged_mains",
                        "bu_list",
                        "rem_ops",
                    ]].copy()

        # -----------------
        # Combined daily
        # -----------------
        # Outer merge on date
        daily = pd.merge(mains_daily, subs_daily, how="outer", on="d")
        if daily.empty:
            st.info("No scheduled rows found for the current selection.")
            st.stop()

        for c in ["mains_hrs", "mains_value", "mains_count", "subs_hrs", "subs_count"]:
            if c in daily.columns:
                daily[c] = pd.to_numeric(daily[c], errors="coerce").fillna(0.0)

        daily["total_hrs"] = daily.get("mains_hrs", 0) + daily.get("subs_hrs", 0)
        daily["capacity"] = float(capacity_per_day)
        daily["over"] = daily["total_hrs"] - daily["capacity"]
        daily["flag"] = daily["over"].apply(lambda x: "OVER" if x > 0.01 else "")
        daily = daily.sort_values("d")

        # Metrics
        mc1, mc2, mc3, mc4 = st.columns(4)
        with mc1:
            st.metric("Total hours", f"{daily['total_hrs'].sum():,.1f}")
        with mc2:
            st.metric("Mains hours", f"{daily.get('mains_hrs', pd.Series([0])).sum():,.1f}")
        with mc3:
            st.metric("Subs hours", f"{daily.get('subs_hrs', pd.Series([0])).sum():,.1f}")
        with mc4:
            st.metric("Days OVER", f"{int((daily['over'] > 0.01).sum())}")

        # Table
        show_cols = [
            "d",
            "mains_hrs",
            "subs_hrs",
            "total_hrs",
            "capacity",
            "over",
            "flag",
        ]
        if "mains_value" in daily.columns:
            show_cols.insert(4, "mains_value")

        st.dataframe(
            daily[show_cols],
            use_container_width=True,
            height=260,
            column_config={
                "d": st.column_config.DateColumn("Day"),
                "mains_hrs": st.column_config.NumberColumn("Mains hrs", format="%.1f"),
                "subs_hrs": st.column_config.NumberColumn("Subs hrs", format="%.1f"),
                "total_hrs": st.column_config.NumberColumn("Total hrs", format="%.1f"),
                "capacity": st.column_config.NumberColumn("Capacity", format="%.1f"),
                "over": st.column_config.NumberColumn("Over/(under)", format="%.1f"),
                "flag": st.column_config.TextColumn("Flag"),
                "mains_value": st.column_config.NumberColumn("Mains $", format="$%,d"),
            },
        )

        # Chart: grouped bars (mains vs subs) with capacity line
        plot = daily[["d", "mains_hrs", "subs_hrs", "capacity"]].copy()
        plot["d"] = pd.to_datetime(plot["d"], errors="coerce")
        long = plot.melt(id_vars=["d", "capacity"], value_vars=["mains_hrs", "subs_hrs"], var_name="level", value_name="hrs")
        long["level"] = long["level"].map({"mains_hrs": "Mains", "subs_hrs": "Subs"}).fillna(long["level"])

        bars = (
            alt.Chart(long)
            .mark_bar()
            .encode(
                x=alt.X("d:T", title="Day"),
                y=alt.Y("hrs:Q", title="Hours"),
                color=alt.Color("level:N", title=""),
                tooltip=[
                    alt.Tooltip("d:T", title="Day"),
                    alt.Tooltip("level:N", title=""),
                    alt.Tooltip("hrs:Q", title="Hours", format=".1f"),
                    alt.Tooltip("capacity:Q", title="Capacity", format=".1f"),
                ],
            )
            .properties(height=360)
        )

        cap_line = (
            alt.Chart(plot)
            .mark_rule(strokeDash=[6, 4])
            .encode(
                x=alt.X("d:T"),
                y=alt.Y("capacity:Q"),
                tooltip=[alt.Tooltip("capacity:Q", title="Capacity", format=".1f")],
            )
        )

        st.altair_chart(bars + cap_line, use_container_width=True)

        # Overscheduled drilldown
        st.markdown("#### Overscheduled days (drilldown)")
        over_days = daily[daily["over"] > 0.01]
        if over_days.empty:
            st.success(f"No overscheduled {area} days based on current headcount.")
        else:
            # pick one day to drill
            pick = st.selectbox("Pick an OVER day", over_days["d"].astype(str).tolist())
            pick_d = pd.to_datetime(pick).date()

            if include_mains:
                st.markdown("**Mains driving load**")
                mains = mains_core(filters)
                if bu_sel:
                    mains = mains[mains["bu"].fillna("").astype(str).isin(bu_sel)]
                h_main = hours_rollup_for_mains()
                mains = mains.merge(h_main, how="left", left_on="parent_wo_norm", right_on="wo_norm")
                mains["assy_hrs"] = mains["assy_hrs"].fillna(0.0)
                mains["test_hrs"] = mains["test_hrs"].fillna(0.0)
                mains[date_col] = pd.to_datetime(mains[date_col], errors="coerce")
                sched_m = mains.dropna(subset=[date_col]).copy()
                sched_m["d"] = sched_m[date_col].dt.date
                hrs_field = "test_hrs" if area == "TEST" else "assy_hrs"
                sched_m["hrs"] = pd.to_numeric(sched_m[hrs_field], errors="coerce").fillna(0.0)

                drill_m = sched_m[sched_m["d"] == pick_d][
                    ["bu", "customer", "delivery", "parent_wo", "parent_part", "wo_qty", "line_value", "hrs"]
                ].sort_values("hrs", ascending=False)
                st.dataframe(drill_m, use_container_width=True, height=260)

            if include_subs and not subs_drill.empty:
                st.markdown("**Subs driving load**")
                drill_s = subs_drill[subs_drill["d"] == pick_d].sort_values("hrs", ascending=False)
                st.dataframe(drill_s, use_container_width=True, height=260)

    # --- Daily Mains Overview ---
    with dash_tabs[2]:
        st.markdown("### Daily mains overview (mains only)")

        date_field = st.selectbox("Group by date field", ["stock_date", "test_date", "assy_date"], index=0)

        df = read_sql(
            f"""
            select
              m.delivery,
              m.customer,
              m.parent_wo,
              m.parent_part,
              m.wo_qty,
              m.line_value,
              t.{date_field} as d
            from main_line m
            left join main_targets t on t.main_id=m.main_id
            where m.is_active=1
              and t.{date_field} is not null and t.{date_field} <> ''
            order by t.{date_field}, m.customer, m.delivery;
            """
        )

        if df.empty:
            st.info(f"No mains scheduled with {date_field} yet.")
        else:
            df["d"] = pd.to_datetime(df["d"]).dt.date

            summary = (
                df.groupby("d", as_index=False)
                .agg(revenue=("line_value", "sum"), qty=("wo_qty", "sum"), mains=("delivery", "count"))
                .sort_values("d")
            )

            st.dataframe(summary, use_container_width=True, height=220)

            pick = st.selectbox("Select a day to view mains", summary["d"].astype(str).tolist())
            pick_d = pd.to_datetime(pick).date()

            st.dataframe(
                df[df["d"] == pick_d][["customer", "delivery", "parent_wo", "parent_part", "wo_qty", "line_value"]],
                use_container_width=True,
                height=520,
            )

    # --- Scheduled Lines (Excel-like) ---
    with dash_tabs[3]:
        st.markdown("### Scheduled lines (dock-date window)")
        st.caption(
            "This is an Excel-like line list. It is filtered by the Dock Date range in the sidebar. "
            "Rows with an empty Stock Target Date are highlighted (not scheduled)."
        )

        df = read_sql(
            """
            select
              m.customer,
              t.stock_date as stock_target_date,
              m.parent_part as main_part,
              m.parent_wo as work_order,
              m.line_value as value,
              m.delivery as sales_order,
              m.dock_date,
              m.wo_qty as qty
            from main_line m
            left join main_targets t on t.main_id = m.main_id
            where m.is_active = 1
              and (:bu = '' or lower(m.bu) like '%' || lower(:bu) || '%')
              and (:customer = '' or lower(m.customer) like '%' || lower(:customer) || '%')
              and (:delivery = '' or lower(m.delivery) like '%' || lower(:delivery) || '%')
              and (:dock_from = '' or (m.dock_date is not null and m.dock_date >= :dock_from))
              and (:dock_to = '' or (m.dock_date is not null and m.dock_date <= :dock_to))
            order by m.dock_date, m.customer, m.delivery;
            """,
            params={
                "bu": filters.get("bu", ""),
                "customer": filters.get("customer", ""),
                "delivery": filters.get("delivery", ""),
                "dock_from": filters.get("dock_from", ""),
                "dock_to": filters.get("dock_to", ""),
            },
        )

        if df.empty:
            st.info("No lines found for the current Dock Date filter window.")
        else:
            # Normalize types for display
            for c in ["dock_date", "stock_target_date"]:
                df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
            df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0).astype(int)
            df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0.0)

            st.divider()
            st.markdown("#### Column filters")

            # A simple "filter by any column" control (choose column -> apply filter)
            col_names = [
                "customer",
                "stock_target_date",
                "main_part",
                "work_order",
                "value",
                "sales_order",
                "dock_date",
                "qty",
            ]
            fcol = st.selectbox("Filter column", col_names, index=0)

            df_f = df.copy()
            if fcol in {"customer", "main_part", "work_order", "sales_order"}:
                needle = st.text_input("Contains", value="")
                if needle.strip():
                    df_f = df_f[df_f[fcol].fillna("").astype(str).str.contains(needle, case=False, na=False)]
            elif fcol in {"dock_date", "stock_target_date"}:
                d1, d2 = st.columns(2)
                with d1:
                    start = st.date_input("Start", value=None, key=f"sched_{fcol}_start")
                with d2:
                    end = st.date_input("End", value=None, key=f"sched_{fcol}_end")
                if isinstance(start, date):
                    df_f = df_f[df_f[fcol].notna() & (df_f[fcol] >= start)]
                if isinstance(end, date):
                    df_f = df_f[df_f[fcol].notna() & (df_f[fcol] <= end)]
            elif fcol in {"value"}:
                n1, n2 = st.columns(2)
                with n1:
                    vmin = st.number_input("Min value", value=0.0, step=1000.0)
                with n2:
                    vmax = st.number_input("Max value", value=float(df_f["value"].max() if len(df_f) else 0.0), step=1000.0)
                df_f = df_f[(df_f["value"] >= float(vmin)) & (df_f["value"] <= float(vmax))]
            elif fcol in {"qty"}:
                q1, q2 = st.columns(2)
                with q1:
                    qmin = st.number_input("Min qty", value=0, step=1)
                with q2:
                    qmax = st.number_input("Max qty", value=int(df_f["qty"].max() if len(df_f) else 0), step=1)
                df_f = df_f[(df_f["qty"] >= int(qmin)) & (df_f["qty"] <= int(qmax))]

            # Optional: show only unscheduled rows
            only_unscheduled = st.checkbox("Show only rows missing Stock Target Date", value=False)
            if only_unscheduled:
                df_f = df_f[df_f["stock_target_date"].isna()]

            # Highlight rows where stock target date is empty
            def _highlight_unscheduled(row):
                return [
                    "background-color: rgba(255, 0, 0, 0.25)" if pd.isna(row["stock_target_date"]) else ""
                ] * len(row)

            styled = df_f.style.apply(_highlight_unscheduled, axis=1).format(
                {"value": "${:,.0f}"}
            )

            st.dataframe(
                styled,
                use_container_width=True,
                height=650,
                column_config={
                    "customer": st.column_config.TextColumn("Customer"),
                    "stock_target_date": st.column_config.DateColumn("Stock target date"),
                    "main_part": st.column_config.TextColumn("Main part"),
                    "work_order": st.column_config.TextColumn("Work order"),
                    "value": st.column_config.NumberColumn("Value", format="$%,d"),
                    "sales_order": st.column_config.TextColumn("Sales order"),
                    "dock_date": st.column_config.DateColumn("Dock date"),
                    "qty": st.column_config.NumberColumn("Qty", format="%d"),
                },
            )

            st.caption(f"Rows shown: {len(df_f):,} (Dock-date filtered)")

    # --- Dispatch Report (printable) ---
    with dash_tabs[4]:
        st.markdown("### Dispatch report (printable)")
        st.caption(
            "Pick a date range and what you want to dispatch (ASSY vs TEST, MAIN vs SUB). "
            "Then print or save to PDF from your browser."
        )

        r1, r2, r3, r4 = st.columns([1, 1, 1, 1])
        with r1:
            d_from = st.date_input("From", value=date.today())
        with r2:
            d_to = st.date_input("To", value=date.today() + timedelta(days=1))
        with r3:
            area = st.selectbox("Area", ["ASSY", "TEST"], index=0)
        with r4:
            level = st.selectbox("Level", ["MAIN", "SUB"], index=0)

        # Guards
        if isinstance(d_from, date) and isinstance(d_to, date) and d_to < d_from:
            st.error("To date must be on/after From date")
            st.stop()

        # Build MAIN dispatch
        def _main_dispatch(area_: str) -> pd.DataFrame:
            # area_ in {ASSY, TEST}
            date_col = "assy_date" if area_ == "ASSY" else "test_date"

            dfm = read_sql(
                f"""
                select
                  m.customer,
                  m.delivery,
                  m.parent_wo,
                  m.parent_wo_norm,
                  m.parent_part,
                  m.due_date,
                  m.dock_date,
                  m.wo_qty,
                  m.line_value,
                  t.{date_col} as sched_date,
                  t.assy_emp,
                  t.test_emp
                from main_line m
                left join main_targets t on t.main_id = m.main_id
                where m.is_active = 1
                  and t.{date_col} is not null and t.{date_col} <> ''
                  and t.{date_col} >= :d_from
                  and t.{date_col} <= :d_to
                order by t.{date_col}, m.customer, m.delivery;
                """,
                params={"d_from": d_from.isoformat(), "d_to": d_to.isoformat()},
            )

            if dfm.empty:
                return dfm

            # Attach hours (uses normalized WO to handle (K) mismatch)
            h = hours_rollup_for_mains()
            dfm = dfm.merge(h, how="left", left_on="parent_wo_norm", right_on="wo_norm")
            dfm["assy_hrs"] = pd.to_numeric(dfm.get("assy_hrs", 0), errors="coerce").fillna(0.0)
            dfm["test_hrs"] = pd.to_numeric(dfm.get("test_hrs", 0), errors="coerce").fillna(0.0)

            dfm["sched_date"] = pd.to_datetime(dfm["sched_date"], errors="coerce").dt.date
            dfm["due_date"] = pd.to_datetime(dfm["due_date"], errors="coerce").dt.date
            dfm["dock_date"] = pd.to_datetime(dfm["dock_date"], errors="coerce").dt.date
            dfm["wo_qty"] = pd.to_numeric(dfm["wo_qty"], errors="coerce").fillna(0).astype(int)
            dfm["line_value"] = pd.to_numeric(dfm["line_value"], errors="coerce").fillna(0.0)

            dfm["employee"] = dfm["assy_emp"].fillna("") if area_ == "ASSY" else dfm["test_emp"].fillna("")
            dfm["hours"] = dfm["assy_hrs"] if area_ == "ASSY" else dfm["test_hrs"]

            out = dfm[[
                "sched_date",
                "employee",
                "customer",
                "delivery",
                "parent_wo",
                "parent_part",
                "wo_qty",
                "hours",
                "line_value",
                "dock_date",
                "due_date",
            ]].copy()

            out.rename(
                columns={
                    "sched_date": "date",
                    "delivery": "sales_order",
                    "parent_wo": "work_order",
                    "parent_part": "part",
                    "wo_qty": "qty",
                    "line_value": "value",
                    "dock_date": "dock",
                    "due_date": "due",
                },
                inplace=True,
            )
            out.insert(0, "level", "MAIN")
            out.insert(1, "area", area_)
            return out

        # Build SUB dispatch
        def _sub_dispatch(area_: str) -> pd.DataFrame:
            date_col = "assy_date" if area_ == "ASSY" else "test_date"

            # Pull subs with a scheduled date in range
            with sqlite3.connect(DB_PATH) as _c:
                _c.execute("PRAGMA foreign_keys=ON;")
                if has_column(_c, "sub_object", "comp_remaining"):
                    rem_sql = "s.comp_remaining as rem_ops,"
                elif has_column(_c, "sub_object", "comp_remaining_ops"):
                    rem_sql = "s.comp_remaining_ops as rem_ops,"
                elif has_column(_c, "sub_object", "sub_remaining"):
                    rem_sql = "s.sub_remaining as rem_ops,"
                else:
                    rem_sql = "'' as rem_ops,"

            dfs = read_sql(
                f"""
                select
                  s.sub_key,
                  s.sub_part,
                  s.sub_source,
                  s.sub_resp,
                  s.source_digits,
                  s.sub_eta_date,
                  {rem_sql}
                  p.{date_col} as sched_date,
                  p.assy_emp,
                  p.test_emp,
                  (select count(*) from peg_sub g where g.is_active=1 and g.sub_key=s.sub_key) as pegged_mains
                from sub_object s
                join sub_plan p on p.sub_key = s.sub_key
                where s.is_active = 1
                  and p.{date_col} is not null and p.{date_col} <> ''
                  and p.{date_col} >= :d_from
                  and p.{date_col} <= :d_to
                order by p.{date_col}, s.sub_resp, s.sub_part;
                """,
                params={"d_from": d_from.isoformat(), "d_to": d_to.isoformat()},
            )

            if dfs.empty:
                return dfs

            # Attach hours by digits
            h = hours_rollup_for_subs()
            dfs = dfs.merge(h, how="left", on="source_digits")
            dfs["assy_hrs"] = pd.to_numeric(dfs.get("assy_hrs", 0), errors="coerce").fillna(0.0)
            dfs["test_hrs"] = pd.to_numeric(dfs.get("test_hrs", 0), errors="coerce").fillna(0.0)

            dfs["sched_date"] = pd.to_datetime(dfs["sched_date"], errors="coerce").dt.date
            dfs["sub_eta_date"] = pd.to_datetime(dfs["sub_eta_date"], errors="coerce").dt.date
            dfs["pegged_mains"] = pd.to_numeric(dfs["pegged_mains"], errors="coerce").fillna(0).astype(int)

            dfs["employee"] = dfs["assy_emp"].fillna("") if area_ == "ASSY" else dfs["test_emp"].fillna("")
            dfs["hours"] = dfs["assy_hrs"] if area_ == "ASSY" else dfs["test_hrs"]

            out = dfs[[
                "sched_date",
                "employee",
                "sub_part",
                "sub_source",
                "sub_resp",
                "pegged_mains",
                "hours",
                "sub_eta_date",
                "rem_ops",
            ]].copy()

            out.rename(
                columns={
                    "sched_date": "date",
                    "sub_source": "work_order",
                    "sub_part": "part",
                    "sub_resp": "owner",
                    "sub_eta_date": "eta",
                    "rem_ops": "comp_remaining",
                },
                inplace=True,
            )

            out.insert(0, "level", "SUB")
            out.insert(1, "area", area_)
            return out

        # Generate the report
        if level == "MAIN":
            report = _main_dispatch(area)
        else:
            report = _sub_dispatch(area)

        if report is None or report.empty:
            st.info("No scheduled rows found for the chosen date window.")
            st.stop()

        # Friendly ordering + formatting
        report = report.copy()
        report["date"] = pd.to_datetime(report["date"], errors="coerce").dt.date
        report = report.sort_values(["date", "employee"]).reset_index(drop=True)

        # Display + downloads
        st.markdown("#### Dispatch table")
        st.dataframe(report, use_container_width=True, height=650)

        csv_bytes = report.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV",
            data=csv_bytes,
            file_name=f"dispatch_{level.lower()}_{area.lower()}_{d_from.isoformat()}_{d_to.isoformat()}.csv",
            mime="text/csv",
        )

        # Printable HTML (use browser print -> Save as PDF)
        st.markdown("#### Printable view")
        st.caption("Click 'Print' then choose a printer or 'Save as PDF'.")

        def _fmt_money(x):
            try:
                return f"${float(x):,.0f}"
            except Exception:
                return ""

        printable = report.copy()
        if "value" in printable.columns:
            printable["value"] = printable["value"].apply(_fmt_money)

        # Limit long text in printable table
        for c in printable.columns:
            if printable[c].dtype == object:
                printable[c] = printable[c].fillna("").astype(str)

        html_table = printable.to_html(index=False, escape=True)

        html = f"""
        <html>
        <head>
          <meta charset='utf-8'/>
          <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Arial, sans-serif; padding: 18px; }}
            h2 {{ margin: 0 0 6px 0; }}
            .meta {{ margin: 0 0 14px 0; color: #555; }}
            table {{ border-collapse: collapse; width: 100%; font-size: 12px; }}
            th, td {{ border: 1px solid #ccc; padding: 6px 8px; vertical-align: top; }}
            th {{ background: #f3f3f3; }}
            .btn {{ margin: 12px 0; padding: 8px 12px; font-size: 14px; }}
            @media print {{ .btn {{ display: none; }} body {{ padding: 0; }} }}
          </style>
        </head>
        <body>
          <h2>Dispatch Report — {level} / {area}</h2>
          <div class='meta'>Date window: {d_from.isoformat()} → {d_to.isoformat()}</div>
          <button class='btn' onclick='window.print()'>Print</button>
          {html_table}
        </body>
        </html>
        """

        components.html(html, height=780, scrolling=True)
