PRAGMA foreign_keys=ON;

-- -----------------------
-- Stage tables (refreshed every run)
-- -----------------------
DROP TABLE IF EXISTS stg_bu_shortage_detail;
CREATE TABLE stg_bu_shortage_detail (
  snapshot_date TEXT,
  delivery TEXT, customer TEXT, bu TEXT, type TEXT,
  part_number TEXT, due_date TEXT, dock_date TEXT,
  bal_due TEXT, wo_qty TEXT, work_order TEXT, pr TEXT, value TEXT,
  comp_part_number TEXT, comp_level TEXT, comp_resp TEXT, comp_type TEXT,
  comp_source TEXT, comp_eta TEXT, comp_pr TEXT, comp_qty TEXT,
  comp_op TEXT, comp_description TEXT, comp_remaining TEXT,
  comp_po TEXT, comp_rem_hrs TEXT, comp_rem_odc TEXT, short_type TEXT,
  comp_wo_start_date TEXT, comp_wo_end_date TEXT,
  comp_op_start_date TEXT, comp_op_end_date TEXT,
  glg TEXT, glg_description TEXT, parent_short_count TEXT, action TEXT
);

DROP TABLE IF EXISTS stg_scheduled_hours;
CREATE TABLE stg_scheduled_hours (
  snapshot_date TEXT,
  wo_pl TEXT, priority TEXT, part_number TEXT, description TEXT,
  wk_ctr TEXT, op_seq TEXT, qty TEXT, rem_hrs TEXT,
  start_date TEXT, complete_date TEXT, rem_setup_hrs TEXT
);

-- -----------------------
-- Identity tables (sticky across refreshes)
-- -----------------------
DROP TABLE IF EXISTS main_line;
CREATE TABLE main_line (
  main_id INTEGER PRIMARY KEY,
  main_key TEXT UNIQUE NOT NULL,

  delivery TEXT,
  customer TEXT,
  bu TEXT,
  type TEXT,

  parent_part TEXT,
  parent_wo TEXT,
  parent_wo_digits TEXT,

  due_date TEXT,
  dock_date TEXT,

  bal_due REAL,
  wo_qty REAL,
  line_value REAL,

  parent_short_count INTEGER,
  action TEXT,

  is_active INTEGER NOT NULL DEFAULT 1,
  first_seen_date TEXT,
  last_seen_date TEXT
,
  short_free integer default 0
);

CREATE INDEX IF NOT EXISTS ix_main_line_active ON main_line(is_active);
CREATE INDEX IF NOT EXISTS ix_main_line_delivery_part ON main_line(delivery, parent_part);

DROP TABLE IF EXISTS supply_object;
CREATE TABLE supply_object (
  supply_id INTEGER PRIMARY KEY,
  supply_key TEXT UNIQUE NOT NULL,

  comp_part TEXT,
  resp TEXT,
  supply_type TEXT,      -- PO / WO / PLANNED
  po_ref TEXT,
  wo_ref TEXT,
  source_ref TEXT,

  op_code TEXT,
  description TEXT,

  eta_date TEXT,
  report_rem_hours REAL,
  wo_start_date TEXT,
  wo_end_date TEXT,

  is_active INTEGER NOT NULL DEFAULT 1,
  first_seen_date TEXT,
  last_seen_date TEXT
);

CREATE INDEX IF NOT EXISTS ix_supply_object_active ON supply_object(is_active);
CREATE INDEX IF NOT EXISTS ix_supply_object_comp_part ON supply_object(comp_part);

DROP TABLE IF EXISTS peg;
CREATE TABLE peg (
  peg_id INTEGER PRIMARY KEY,
  main_id INTEGER NOT NULL,
  supply_id INTEGER NOT NULL,

  comp_remaining REAL,
  comp_qty REAL,
  short_type TEXT,

  is_active INTEGER NOT NULL DEFAULT 1,
  first_seen_date TEXT,
  last_seen_date TEXT,

  UNIQUE(main_id, supply_id),
  FOREIGN KEY(main_id) REFERENCES main_line(main_id),
  FOREIGN KEY(supply_id) REFERENCES supply_object(supply_id)
);

CREATE INDEX IF NOT EXISTS ix_peg_active ON peg(is_active);
CREATE INDEX IF NOT EXISTS ix_peg_main ON peg(main_id);
CREATE INDEX IF NOT EXISTS ix_peg_supply ON peg(supply_id);

-- -----------------------
-- Planner inputs (sticky)
-- -----------------------
DROP TABLE IF EXISTS main_targets;
CREATE TABLE main_targets (
  main_id INTEGER PRIMARY KEY,
  kit_date TEXT,
  assy_date TEXT,
  test_date TEXT,
  stock_date TEXT,
  assigned_emp TEXT,
  updated_at TEXT,
  FOREIGN KEY(main_id) REFERENCES main_line(main_id)
);

DROP TABLE IF EXISTS supply_plan;
CREATE TABLE supply_plan (
  supply_id INTEGER PRIMARY KEY,
  clear_date TEXT,         -- when sub/PO is expected cleared/stocked
  status TEXT DEFAULT 'open',  -- open/in_progress/cleared/ignore
  hours_override REAL,
  assigned_emp TEXT,
  updated_at TEXT,
  FOREIGN KEY(supply_id) REFERENCES supply_object(supply_id)
);

-- -----------------------
-- Scheduled hours curated (only relevant workcenters)
-- -----------------------
DROP TABLE IF EXISTS wkctr_map;
CREATE TABLE wkctr_map (
  wk_ctr TEXT PRIMARY KEY,
  category TEXT,     -- ASSY / TEST / OTHER
  include INTEGER NOT NULL DEFAULT 0
);

DROP TABLE IF EXISTS op_hours;
CREATE TABLE op_hours (
  op_hours_id INTEGER PRIMARY KEY,
  snapshot_date TEXT,

  wo_pl TEXT,
  wo_pl_digits TEXT,

  part_number TEXT,
  wk_ctr TEXT,
  op_seq REAL,

  qty REAL,
  rem_hrs REAL,
  rem_setup_hrs REAL,

  start_date TEXT,
  complete_date TEXT
);

CREATE INDEX IF NOT EXISTS ix_op_hours_digits ON op_hours(wo_pl_digits);
CREATE INDEX IF NOT EXISTS ix_op_hours_wc ON op_hours(wk_ctr);

-- -----------------------
-- Dept labor validation output
-- -----------------------
DROP TABLE IF EXISTS part_recent_employee;
CREATE TABLE part_recent_employee (
  part_number TEXT NOT NULL,
  emp_num TEXT NOT NULL,
  emp_name TEXT NOT NULL,
  last_work_date TEXT,
  last_work_order TEXT,
  earned_hours_sum_180d REAL,
  recency_rank INTEGER NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(part_number, recency_rank)
);