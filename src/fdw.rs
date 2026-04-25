// Foreign Data Wrapper for TigerBeetle, built on raw pg_sys on pgrx 0.18.
//
// Exposes three foreign tables backed by the existing shmem ring:
//   tb_accounts         — reads via OP_LOOKUP_ACCOUNT / OP_QUERY_ACCOUNTS
//   tb_transfers        — reads via OP_LOOKUP_TRANSFER / OP_GET_ACCOUNT_TRANSFERS /
//                         OP_QUERY_TRANSFERS; INSERT via OP_CREATE_TRANSFER[_BATCH]
//   tb_account_balances — reads via OP_GET_ACCOUNT_BALANCES (account_id required)
//
// Planning path:
//   GetForeignRelSize → rough row estimate
//   GetForeignPaths   → one ForeignPath
//   GetForeignPlan    → extract pushable quals; stash in fdw_private
//   GetForeignUpperPaths (UPPERREL_FINAL) → pick up LIMIT
//
// Execution path:
//   BeginForeignScan  → pack Slot template from fdw_private, submit_and_wait,
//                       store ReadBack on per-scan state
//   IterateForeignScan → walk records, materialize one TupleTableSlot at a time
//   EndForeignScan    → free
//
// Modify path (tb_accounts and tb_transfers; balances is read-only):
//   PlanForeignModify → extract target attributes
//   BeginForeignModify → allocate modify state (enum over table kind)
//   ExecForeignInsert → single OP_CREATE_ACCOUNT / OP_CREATE_TRANSFER
//   ExecForeignBatchInsert → transfers only; packs legs into pool via submit_and_wait_with_legs
//   GetForeignModifyBatchSize → MAX_BATCH_LEGS for transfers, 1 for accounts
//
// Quals: only simple `column = const` equalities push down. Anything else is
// left on baserestrictinfo for Pg to filter locally.

use std::ffi::{CStr, c_int, c_void};

use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::{AllocatedByRust, PgBox};

use crate::pack::{unpack_account_row, unpack_balance_row, unpack_transfer_row};
use crate::shmem::{
    BATCH_LEG_LEN, MAX_BATCH_LEGS, MAX_QUERY_ROWS, OP_CREATE_ACCOUNT, OP_CREATE_TRANSFER,
    OP_CREATE_TRANSFER_BATCH, OP_GET_ACCOUNT_BALANCES, OP_GET_ACCOUNT_TRANSFERS, OP_LOOKUP_ACCOUNT,
    OP_LOOKUP_TRANSFER, OP_QUERY_ACCOUNTS, OP_QUERY_TRANSFERS, RECORD_LEN, Slot,
};
use crate::submit::{ReadBack, submit_and_wait, submit_and_wait_with_legs};

// -----------------------------------------------------------------------------
// Cross-version pg_sys shims
// -----------------------------------------------------------------------------
//
// Two PG APIs we touch changed shape between supported majors:
//   * TupleDescAttr was a macro before PG18 and a function in PG18. Older
//     versions need direct field access into the TupleDescData FAM.
//   * create_foreignscan_path picked up `fdw_restrictinfo` in PG17 and
//     `disabled_nodes` in PG18, so it has 10 / 11 / 12 args depending on
//     the build. The wrapper below accepts the PG18-shaped arg list and
//     drops what the older bindings don't take.

unsafe fn tuple_desc_attr(
    tupdesc: pg_sys::TupleDesc,
    i: i32,
) -> *mut pg_sys::FormData_pg_attribute {
    unsafe {
        #[cfg(feature = "pg18")]
        {
            pg_sys::TupleDescAttr(tupdesc, i)
        }
        #[cfg(not(feature = "pg18"))]
        {
            // TupleDescData ends with a FAM `attrs[]` of FormData_pg_attribute.
            // bindgen renders it as either a sized array or an
            // __IncompleteArrayField; addr_of_mut works in both cases.
            let base =
                std::ptr::addr_of_mut!((*tupdesc).attrs) as *mut pg_sys::FormData_pg_attribute;
            base.offset(i as isize)
        }
    }
}

// `Var.varno` is `Index` (u32) on PG15 and `int` (i32) on PG16+. The `as i32`
// cast is meaningful on PG15 and a no-op on later versions; clippy fires on
// the latter. Centralize the conversion so the allow only sits in one place.
unsafe fn varno_member(var: *mut pg_sys::Var, relids: *mut pg_sys::Bitmapset) -> bool {
    #[allow(clippy::unnecessary_cast)]
    let varno = unsafe { (*var).varno } as i32;
    unsafe { pg_sys::bms_is_member(varno, relids) }
}

#[allow(clippy::too_many_arguments)]
unsafe fn create_foreignscan_path_compat(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    target: *mut pg_sys::PathTarget,
    rows: f64,
    startup_cost: f64,
    total_cost: f64,
    pathkeys: *mut pg_sys::List,
    required_outer: *mut pg_sys::Bitmapset,
    fdw_outerpath: *mut pg_sys::Path,
    fdw_restrictinfo: *mut pg_sys::List,
    fdw_private: *mut pg_sys::List,
) -> *mut pg_sys::ForeignPath {
    unsafe {
        #[cfg(feature = "pg18")]
        {
            pg_sys::create_foreignscan_path(
                root,
                rel,
                target,
                rows,
                0, // disabled_nodes
                startup_cost,
                total_cost,
                pathkeys,
                required_outer,
                fdw_outerpath,
                fdw_restrictinfo,
                fdw_private,
            )
        }
        #[cfg(feature = "pg17")]
        {
            pg_sys::create_foreignscan_path(
                root,
                rel,
                target,
                rows,
                startup_cost,
                total_cost,
                pathkeys,
                required_outer,
                fdw_outerpath,
                fdw_restrictinfo,
                fdw_private,
            )
        }
        #[cfg(any(feature = "pg15", feature = "pg16"))]
        {
            // fdw_restrictinfo doesn't exist on PG <17; the join clauses we'd
            // pass via it are still in baserel->joininfo and get re-evaluated
            // by the executor, so dropping it here is safe (just less optimal).
            let _ = fdw_restrictinfo;
            pg_sys::create_foreignscan_path(
                root,
                rel,
                target,
                rows,
                startup_cost,
                total_cost,
                pathkeys,
                required_outer,
                fdw_outerpath,
                fdw_private,
            )
        }
    }
}

// Which TB entity this foreign table is bound to. Decided at planning time by
// matching the relation's relname against the fixed set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TableKind {
    Accounts,
    Transfers,
    Balances,
}

fn table_kind_from_oid(ftable_oid: pg_sys::Oid) -> Option<TableKind> {
    unsafe {
        let rel = pg_sys::RelationIdGetRelation(ftable_oid);
        if rel.is_null() {
            return None;
        }
        let name_ptr = (*(*rel).rd_rel).relname.data.as_ptr();
        let name = CStr::from_ptr(name_ptr).to_string_lossy().into_owned();
        pg_sys::RelationClose(rel);
        match name.as_str() {
            "tb_accounts" => Some(TableKind::Accounts),
            "tb_transfers" => Some(TableKind::Transfers),
            "tb_account_balances" => Some(TableKind::Balances),
            _ => None,
        }
    }
}

// Expected column layout per table, in catalog order. Pushdown indexing and
// row materialization both assume the user hasn't altered the shipped schema —
// added/dropped/reordered columns would silently push against the wrong attnum
// or leave tts_values entries uninitialized. validate_schema() rejects any
// mismatch at begin_foreign_scan / begin_foreign_modify before we touch memory.
fn expected_columns(kind: TableKind) -> &'static [(&'static str, pg_sys::Oid)] {
    match kind {
        TableKind::Accounts => &[
            ("id", pg_sys::UUIDOID),
            ("ledger", pg_sys::INT4OID),
            ("code", pg_sys::INT4OID),
            ("debits_posted", pg_sys::NUMERICOID),
            ("credits_posted", pg_sys::NUMERICOID),
            ("debits_pending", pg_sys::NUMERICOID),
            ("credits_pending", pg_sys::NUMERICOID),
            ("flags", pg_sys::INT4OID),
            ("timestamp", pg_sys::INT8OID),
        ],
        TableKind::Transfers => &[
            ("id", pg_sys::UUIDOID),
            ("debit_account_id", pg_sys::UUIDOID),
            ("credit_account_id", pg_sys::UUIDOID),
            ("amount", pg_sys::NUMERICOID),
            ("ledger", pg_sys::INT4OID),
            ("code", pg_sys::INT4OID),
            ("flags", pg_sys::INT4OID),
            ("timestamp", pg_sys::INT8OID),
            ("pending_id", pg_sys::UUIDOID),
        ],
        TableKind::Balances => &[
            ("account_id", pg_sys::UUIDOID),
            ("timestamp", pg_sys::INT8OID),
            ("debits_posted", pg_sys::NUMERICOID),
            ("credits_posted", pg_sys::NUMERICOID),
            ("debits_pending", pg_sys::NUMERICOID),
            ("credits_pending", pg_sys::NUMERICOID),
        ],
    }
}

// Walk the foreign table's tupdesc and compare against expected_columns. Any
// deviation — extra column, missing column, rename, type change, or dropped
// slot in the middle — fails loudly. Cheap: runs once per scan/modify start.
unsafe fn validate_schema(rel: pg_sys::Relation, kind: TableKind) {
    unsafe {
        let tupdesc = (*rel).rd_att;
        let natts = (*tupdesc).natts as usize;
        let expected = expected_columns(kind);
        if natts != expected.len() {
            pg_sys::error!(
                "tdw_fdw: {:?} has {} columns, expected {}",
                kind,
                natts,
                expected.len()
            );
        }
        for (i, (want_name, want_oid)) in expected.iter().enumerate() {
            let attr = tuple_desc_attr(tupdesc, i as i32);
            if (*attr).attisdropped {
                pg_sys::error!(
                    "tdw_fdw: column {} of {:?} is dropped; expected {}",
                    i + 1,
                    kind,
                    want_name
                );
            }
            let got_name = CStr::from_ptr((*attr).attname.data.as_ptr()).to_string_lossy();
            if got_name != *want_name {
                pg_sys::error!(
                    "tdw_fdw: column {} of {:?} is named {:?}, expected {:?}",
                    i + 1,
                    kind,
                    got_name,
                    want_name
                );
            }
            if (*attr).atttypid != *want_oid {
                pg_sys::error!(
                    "tdw_fdw: column {:?} of {:?} has type oid {}, expected {}",
                    want_name,
                    kind,
                    (*attr).atttypid.to_u32(),
                    want_oid.to_u32()
                );
            }
        }
    }
}

// Column attribute numbers for each table, in catalog order. Must match the
// SQL DDL exactly. 1-indexed per Pg convention. Some are unused by pushdown
// logic but kept for documentation of the schema contract.
#[allow(dead_code)]
mod attr {
    pub mod accounts {
        pub const ID: i16 = 1;
        pub const LEDGER: i16 = 2;
        pub const CODE: i16 = 3;
        pub const DEBITS_POSTED: i16 = 4;
        pub const CREDITS_POSTED: i16 = 5;
        pub const DEBITS_PENDING: i16 = 6;
        pub const CREDITS_PENDING: i16 = 7;
        pub const FLAGS: i16 = 8;
        pub const TIMESTAMP: i16 = 9;
    }
    pub mod transfers {
        pub const ID: i16 = 1;
        pub const DEBIT_ACCOUNT_ID: i16 = 2;
        pub const CREDIT_ACCOUNT_ID: i16 = 3;
        pub const AMOUNT: i16 = 4;
        pub const LEDGER: i16 = 5;
        pub const CODE: i16 = 6;
        pub const FLAGS: i16 = 7;
        pub const TIMESTAMP: i16 = 8;
        pub const PENDING_ID: i16 = 9;
    }
    pub mod balances {
        pub const ACCOUNT_ID: i16 = 1;
        pub const TIMESTAMP: i16 = 2;
        pub const DEBITS_POSTED: i16 = 3;
        pub const CREDITS_POSTED: i16 = 4;
        pub const DEBITS_PENDING: i16 = 5;
        pub const CREDITS_PENDING: i16 = 6;
    }
}

// TB FilterFlags: DEBITS(1) | CREDITS(2). Used by get_account_transfers /
// get_account_balances to indicate which side of the ledger to walk.
const FF_DEBITS: u32 = 1;
const FF_CREDITS: u32 = 2;
const FF_BOTH: u32 = FF_DEBITS | FF_CREDITS;

// Pushable predicates extracted during GetForeignPlan. All equalities.
#[derive(Default, Debug, Clone)]
struct PushedQuals {
    id: Option<[u8; 16]>,
    debit_account_id: Option<[u8; 16]>,
    credit_account_id: Option<[u8; 16]>,
    ledger: Option<u32>,
    code: Option<u16>,
    flags: Option<u32>,
    limit: Option<u32>,
}

// Tag identifying which PushedQuals field a runtime-evaluated expression fills.
// Stable int encoding so we can ship it through fdw_private.
#[derive(Clone, Copy, Debug)]
#[repr(i64)]
enum QualField {
    Id = 0,
    DebitAccountId = 1,
    CreditAccountId = 2,
    Ledger = 3,
    Code = 4,
    Flags = 5,
}

impl QualField {
    fn from_i64(v: i64) -> Option<Self> {
        Some(match v {
            0 => Self::Id,
            1 => Self::DebitAccountId,
            2 => Self::CreditAccountId,
            3 => Self::Ledger,
            4 => Self::Code,
            5 => Self::Flags,
            _ => return None,
        })
    }
}

// Expected Pg type for each pushable field. Used to gate whether a Const/Param
// RHS is compatible at plan time and to decode the Datum at runtime.
#[derive(Clone, Copy)]
enum PgType {
    Uuid,
    Int4,
}

fn field_info(kind: TableKind, attnum: i16) -> Option<(QualField, PgType)> {
    match kind {
        TableKind::Accounts => match attnum {
            attr::accounts::ID => Some((QualField::Id, PgType::Uuid)),
            attr::accounts::LEDGER => Some((QualField::Ledger, PgType::Int4)),
            attr::accounts::CODE => Some((QualField::Code, PgType::Int4)),
            attr::accounts::FLAGS => Some((QualField::Flags, PgType::Int4)),
            _ => None,
        },
        TableKind::Transfers => match attnum {
            attr::transfers::ID => Some((QualField::Id, PgType::Uuid)),
            attr::transfers::DEBIT_ACCOUNT_ID => Some((QualField::DebitAccountId, PgType::Uuid)),
            attr::transfers::CREDIT_ACCOUNT_ID => Some((QualField::CreditAccountId, PgType::Uuid)),
            attr::transfers::LEDGER => Some((QualField::Ledger, PgType::Int4)),
            attr::transfers::CODE => Some((QualField::Code, PgType::Int4)),
            attr::transfers::FLAGS => Some((QualField::Flags, PgType::Int4)),
            _ => None,
        },
        TableKind::Balances => match attnum {
            attr::balances::ACCOUNT_ID => Some((QualField::Id, PgType::Uuid)),
            _ => None,
        },
    }
}

// Scan-time state. Allocated in the executor's per-query MemoryContext by
// BeginForeignScan and freed by EndForeignScan. Held via raw pointer on
// ForeignScanState.fdw_state.
struct ScanState {
    kind: TableKind,
    readback: Option<ReadBack>,
    cursor: usize,
    // Pre-computed from the plan; re-packed into a Slot on each scan start.
    // Rescans re-evaluate Params into this struct before re-dispatching.
    quals: PushedQuals,
    // ExecInitExprList result for fdw_exprs, kept so rescan can re-eval without
    // rebuilding. Null when no Params were pushed.
    param_states: *mut pg_sys::List,
    // (field, expr_idx) pairs mapping Param evaluations back into quals.
    param_map: Vec<(QualField, i32)>,
}

// Modify-time state. The batch path packs directly from the slots array into
// the pool buffer, so no staging buffer is needed here. Held via ri_FdwState
// on the ResultRelInfo; the enum picks which attnum map to populate.
enum ModifyState {
    Transfers(TransferInsertAttnums),
    Accounts(AccountInsertAttnums),
}

#[derive(Default)]
struct TransferInsertAttnums {
    id: Option<c_int>,
    debit_account_id: Option<c_int>,
    credit_account_id: Option<c_int>,
    amount: Option<c_int>,
    ledger: Option<c_int>,
    code: Option<c_int>,
    flags: Option<c_int>,
    pending_id: Option<c_int>,
}

#[derive(Default)]
struct AccountInsertAttnums {
    id: Option<c_int>,
    ledger: Option<c_int>,
    code: Option<c_int>,
    flags: Option<c_int>,
}

// -----------------------------------------------------------------------------
// Handler entry point
// -----------------------------------------------------------------------------

// Exposed to SQL via extension_sql! below. The function name must match the
// HANDLER clause of CREATE FOREIGN DATA WRAPPER.
#[pg_extern(sql = "
    CREATE OR REPLACE FUNCTION tdw_fdw_handler()
    RETURNS fdw_handler
    LANGUAGE c
    AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
")]
fn tdw_fdw_handler() -> PgBox<pg_sys::FdwRoutine, AllocatedByRust> {
    let mut r = unsafe {
        PgBox::<pg_sys::FdwRoutine, AllocatedByRust>::alloc_node(pg_sys::NodeTag::T_FdwRoutine)
    };
    r.GetForeignRelSize = Some(get_foreign_rel_size);
    r.GetForeignPaths = Some(get_foreign_paths);
    r.GetForeignPlan = Some(get_foreign_plan);
    r.BeginForeignScan = Some(begin_foreign_scan);
    r.IterateForeignScan = Some(iterate_foreign_scan);
    r.ReScanForeignScan = Some(rescan_foreign_scan);
    r.EndForeignScan = Some(end_foreign_scan);
    r.GetForeignUpperPaths = Some(get_foreign_upper_paths);
    r.ExplainForeignScan = Some(explain_foreign_scan);

    // Modify path (transfers-only; other tables reject at plan time).
    r.AddForeignUpdateTargets = Some(add_foreign_update_targets);
    r.PlanForeignModify = Some(plan_foreign_modify);
    r.BeginForeignModify = Some(begin_foreign_modify);
    r.ExecForeignInsert = Some(exec_foreign_insert);
    r.ExecForeignBatchInsert = Some(exec_foreign_batch_insert);
    r.GetForeignModifyBatchSize = Some(get_foreign_modify_batch_size);
    r.EndForeignModify = Some(end_foreign_modify);
    r.IsForeignRelUpdatable = Some(is_foreign_rel_updatable);
    r
}

// Registers the wrapper once the handler function exists. Users still create
// their own SERVER and FOREIGN TABLEs — the DDL for those is shipped in the
// extension's sql/ directory.
extension_sql!(
    r#"
CREATE FOREIGN DATA WRAPPER tdw HANDLER tdw_fdw_handler NO VALIDATOR;
"#,
    name = "tdw_fdw_wrapper",
    requires = [tdw_fdw_handler],
);

// -----------------------------------------------------------------------------
// Planning
// -----------------------------------------------------------------------------

// Estimate output rows based on which quals we know we'll push. The planner
// uses this for join order / method choice; a flat 1000 was causing it to
// pick bad nested-loop orderings when joining tb_* against customer tables.
//
// Buckets (chosen for reasonable join planning, not precision):
//   id pushed (any form, Const or Param)        → 1
//   debit/credit_account_id pushed               → 50  (capped by MAX_QUERY_ROWS)
//   ledger + code pushed                         → 100
//   ledger only                                  → 500
//   nothing                                      → 1000 (fallback)
//
// We inspect baserel->baserestrictinfo for the same OpExpr(Var, Const|Param)
// shapes split_clauses accepts, without building the full quals struct.
#[pg_guard]
unsafe extern "C-unwind" fn get_foreign_rel_size(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    ftable_oid: pg_sys::Oid,
) {
    unsafe {
        let kind = match table_kind_from_oid(ftable_oid) {
            Some(k) => k,
            None => {
                (*baserel).rows = 1000.0;
                return;
            }
        };
        let mut saw_id = false;
        let mut saw_debit_or_credit = false;
        let mut saw_ledger = false;
        let mut saw_code = false;

        let ris = (*baserel).baserestrictinfo;
        if !ris.is_null() {
            let n = (*ris).length as usize;
            let cells = (*ris).elements;
            for i in 0..n {
                let ri = (*cells.add(i)).ptr_value as *mut pg_sys::RestrictInfo;
                if ri.is_null() {
                    continue;
                }
                let clause = (*ri).clause as *mut pg_sys::Node;
                if let Some(field) = classify_clause_field(clause, kind) {
                    match field {
                        QualField::Id => saw_id = true,
                        QualField::DebitAccountId | QualField::CreditAccountId => {
                            saw_debit_or_credit = true
                        }
                        QualField::Ledger => saw_ledger = true,
                        QualField::Code => saw_code = true,
                        QualField::Flags => {}
                    }
                }
            }
        }

        (*baserel).rows = if saw_id {
            1.0
        } else if saw_debit_or_credit {
            50.0
        } else if saw_ledger && saw_code {
            100.0
        } else if saw_ledger {
            500.0
        } else {
            1000.0
        };
    }
}

// Mirror of try_push_clause's shape-check but only reports which field would
// be filled — used for sizing before we've committed to the pushdown.
unsafe fn classify_clause_field(node: *mut pg_sys::Node, kind: TableKind) -> Option<QualField> {
    unsafe {
        if node.is_null() || (*node).type_ != pg_sys::NodeTag::T_OpExpr {
            return None;
        }
        let op = node as *mut pg_sys::OpExpr;
        let opname_ptr = pg_sys::get_opname((*op).opno);
        if opname_ptr.is_null() {
            return None;
        }
        let opname = CStr::from_ptr(opname_ptr).to_string_lossy();
        let is_eq = opname == "=";
        pg_sys::pfree(opname_ptr as *mut c_void);
        if !is_eq {
            return None;
        }
        let args = (*op).args;
        if args.is_null() || (*args).length != 2 {
            return None;
        }
        let a0 = (*(*args).elements.add(0)).ptr_value as *mut pg_sys::Node;
        let a1 = (*(*args).elements.add(1)).ptr_value as *mut pg_sys::Node;
        let var = if node_is_var(a0) {
            a0 as *mut pg_sys::Var
        } else if node_is_var(a1) {
            a1 as *mut pg_sys::Var
        } else {
            return None;
        };
        let other = if node_is_var(a0) { a1 } else { a0 };
        if !node_is_const(other) && !node_is_param(other) {
            return None;
        }
        field_info(kind, (*var).varattno).map(|(f, _)| f)
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn get_foreign_paths(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    ftable_oid: pg_sys::Oid,
) {
    unsafe {
        // Unparameterized path. No sort keys (TB returns in its own ordering).
        let startup_cost = 100.0;
        let total_cost = startup_cost + 10.0 * (*baserel).rows;
        let path = create_foreignscan_path_compat(
            root,
            baserel,
            std::ptr::null_mut(),
            (*baserel).rows,
            startup_cost,
            total_cost,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );
        pg_sys::add_path(baserel, path as *mut pg_sys::Path);

        // Parameterized paths for correlated joins. Without these the planner
        // treats `JOIN tb_transfers ON t.debit_account_id = u.account_uuid` as
        // a full-scan-then-hash-join — with our 10k pagination cap, that
        // silently truncates. Parameterized paths tell the planner it can do
        // an index-nested-loop shape where each outer row drives a TB lookup.
        if let Some(kind) = table_kind_from_oid(ftable_oid) {
            add_parameterized_paths(root, baserel, kind);
        }
    }
}

// Find join clauses of shape `our-Var = outer-Var` on our pushable columns
// and emit one parameterized ForeignPath per outer relation set.
//
// PG strips simple equality joins from baserel->joininfo and re-derives them
// from EquivalenceClasses, so a direct joininfo walk turns up empty for the
// common `JOIN ON a.x = b.y` case. generate_implied_equalities_for_column is
// the supported way back into the EC machinery: it returns RestrictInfos
// representing every implied `our-col = some-other-col` equality. We call it
// once per pushable column and group the results by outer relids.
unsafe fn add_parameterized_paths(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    kind: TableKind,
) {
    unsafe {
        let relids = (*baserel).relids;

        struct Group {
            outer_relids: *mut pg_sys::Bitmapset,
            clauses: *mut pg_sys::List,
            best_field: QualField,
        }
        let mut groups: Vec<Group> = Vec::new();

        // For each pushable column on this kind, ask PG for implied
        // equalities involving that column. The callback identifies whether
        // a given EquivalenceMember is the column we're asking about.
        for &(attno, field) in pushable_columns(kind) {
            let mut ctx = MatchCtx { attno };
            let ris = pg_sys::generate_implied_equalities_for_column(
                root,
                baserel,
                Some(ec_match_attno),
                &mut ctx as *mut MatchCtx as *mut c_void,
                std::ptr::null_mut(),
            );
            if ris.is_null() {
                continue;
            }
            let n = (*ris).length as usize;
            let cells = (*ris).elements;
            for i in 0..n {
                let ri = (*cells.add(i)).ptr_value as *mut pg_sys::RestrictInfo;
                if ri.is_null() {
                    continue;
                }
                let required = (*ri).required_relids;
                let outer = pg_sys::bms_difference(required, relids);
                if outer.is_null() || pg_sys::bms_num_members(outer) == 0 {
                    continue;
                }
                let existing = groups
                    .iter_mut()
                    .find(|g| pg_sys::bms_equal(g.outer_relids, outer));
                match existing {
                    Some(g) => {
                        g.clauses = pg_sys::lappend(g.clauses, ri as *mut c_void);
                        if matches!(field, QualField::Id) {
                            g.best_field = QualField::Id;
                        }
                    }
                    None => {
                        let mut clauses: *mut pg_sys::List = std::ptr::null_mut();
                        clauses = pg_sys::lappend(clauses, ri as *mut c_void);
                        groups.push(Group {
                            outer_relids: outer,
                            clauses,
                            best_field: field,
                        });
                    }
                }
            }
        }

        for g in &groups {
            let rows = match g.best_field {
                QualField::Id => 1.0,
                QualField::DebitAccountId | QualField::CreditAccountId => 50.0,
                _ => 100.0,
            };
            let startup_cost = 100.0;
            let total_cost = startup_cost + 10.0 * rows;
            let path = create_foreignscan_path_compat(
                root,
                baserel,
                std::ptr::null_mut(),
                rows,
                startup_cost,
                total_cost,
                std::ptr::null_mut(),
                g.outer_relids,
                std::ptr::null_mut(),
                g.clauses,
                std::ptr::null_mut(),
            );
            pg_sys::add_path(baserel, path as *mut pg_sys::Path);
        }
    }
}

#[repr(C)]
struct MatchCtx {
    attno: i16,
}

// EC callback: return true if this EquivalenceMember is the (Var on baserel
// at attnum=ctx.attno) we're hunting for. PG iterates EM's looking for a
// match; once found, it walks the rest of the EC and returns one RestrictInfo
// per (matched-EM = other-EM-with-disjoint-relids) pair.
unsafe extern "C-unwind" fn ec_match_attno(
    _root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    _ec: *mut pg_sys::EquivalenceClass,
    em: *mut pg_sys::EquivalenceMember,
    arg: *mut c_void,
) -> bool {
    unsafe {
        let ctx = &*(arg as *const MatchCtx);
        let expr = (*em).em_expr as *mut pg_sys::Node;
        if expr.is_null() || (*expr).type_ != pg_sys::NodeTag::T_Var {
            return false;
        }
        let v = expr as *mut pg_sys::Var;
        if !varno_member(v, (*rel).relids) {
            return false;
        }
        (*v).varattno == ctx.attno
    }
}

// Pushable (attno, field) tuples per table kind. Mirrors field_info but
// indexed by table for the EC walk above.
fn pushable_columns(kind: TableKind) -> &'static [(i16, QualField)] {
    match kind {
        TableKind::Accounts => &[
            (attr::accounts::ID, QualField::Id),
            (attr::accounts::LEDGER, QualField::Ledger),
            (attr::accounts::CODE, QualField::Code),
            (attr::accounts::FLAGS, QualField::Flags),
        ],
        TableKind::Transfers => &[
            (attr::transfers::ID, QualField::Id),
            (attr::transfers::DEBIT_ACCOUNT_ID, QualField::DebitAccountId),
            (
                attr::transfers::CREDIT_ACCOUNT_ID,
                QualField::CreditAccountId,
            ),
            (attr::transfers::LEDGER, QualField::Ledger),
            (attr::transfers::CODE, QualField::Code),
            (attr::transfers::FLAGS, QualField::Flags),
        ],
        TableKind::Balances => &[(attr::balances::ACCOUNT_ID, QualField::Id)],
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn get_foreign_plan(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    ftable_oid: pg_sys::Oid,
    _best_path: *mut pg_sys::ForeignPath,
    tlist: *mut pg_sys::List,
    scan_clauses: *mut pg_sys::List,
    outer_plan: *mut pg_sys::Plan,
) -> *mut pg_sys::ForeignScan {
    unsafe {
        let kind = table_kind_from_oid(ftable_oid).unwrap_or_else(|| {
            pg_sys::error!("tdw_fdw: unknown foreign table oid {:?}", ftable_oid);
        });

        // Extract pushable equalities from scan_clauses. Const RHS values land
        // in `pushed` directly; Param RHS and outer-relation Var RHS (the
        // latter present on parameterized paths used in nested loops) go into
        // fdw_exprs with an entry in param_map so begin_foreign_scan can
        // evaluate and patch `pushed` at execute time. Outer Vars get
        // rewritten to PARAM_EXEC by PG's replace_nestloop_params before exec,
        // so the runtime path is identical to plain prepared-statement Params.
        let actual = pg_sys::extract_actual_clauses(scan_clauses, false);
        let (pushed, param_map, fdw_exprs, remaining) = split_clauses(actual, kind, baserel);

        // Stash quals, table kind, and param map in fdw_private so execution
        // can rebuild them (including after plan-cache deserialization).
        let private = encode_private(kind, &pushed, &param_map);

        pg_sys::make_foreignscan(
            tlist,
            remaining,
            (*baserel).relid,
            fdw_exprs,            // fdw_exprs — Params we'll eval at run time
            private,              // fdw_private
            std::ptr::null_mut(), // fdw_scan_tlist
            std::ptr::null_mut(), // fdw_recheck_quals
            outer_plan,
        )
    }
}

// Walk the clause list, pulling out `Var = Const/Param/OuterVar` equalities on
// our columns. Const RHS values are resolved straight into PushedQuals. Param
// RHS and outer-relation-Var RHS (on parameterized paths) go into fdw_exprs
// with an entry in param_map; begin_foreign_scan evaluates them at execute
// time. Outer Vars are swapped to PARAM_EXEC by PG's replace_nestloop_params
// before execute, so runtime evaluation is identical to prepared-statement
// Params — that's what makes correlated / LATERAL joins push down.
unsafe fn split_clauses(
    clauses: *mut pg_sys::List,
    kind: TableKind,
    baserel: *mut pg_sys::RelOptInfo,
) -> (
    PushedQuals,
    Vec<(QualField, i32)>,
    *mut pg_sys::List,
    *mut pg_sys::List,
) {
    let mut pushed = PushedQuals::default();
    let mut param_map: Vec<(QualField, i32)> = Vec::new();
    let mut fdw_exprs: *mut pg_sys::List = std::ptr::null_mut();
    let mut leftover: *mut pg_sys::List = std::ptr::null_mut();

    unsafe {
        if clauses.is_null() {
            return (pushed, param_map, fdw_exprs, leftover);
        }
        let nelems = (*clauses).length as usize;
        let cells = (*clauses).elements;
        for i in 0..nelems {
            let node = (*cells.add(i)).ptr_value as *mut pg_sys::Node;
            if try_push_clause(
                node,
                kind,
                baserel,
                &mut pushed,
                &mut param_map,
                &mut fdw_exprs,
            ) {
                continue;
            }
            leftover = pg_sys::lappend(leftover, node as *mut c_void);
        }
    }
    (pushed, param_map, fdw_exprs, leftover)
}

// Returns true if the clause was consumed. OpExpr with equality over
// (our-Var, Const|Param|outer-Var). Anything else — function calls, casts,
// non-equality operators, subquery-returning Params wrapped in SubPlan —
// is left for Pg.
unsafe fn try_push_clause(
    node: *mut pg_sys::Node,
    kind: TableKind,
    baserel: *mut pg_sys::RelOptInfo,
    out: &mut PushedQuals,
    param_map: &mut Vec<(QualField, i32)>,
    fdw_exprs: &mut *mut pg_sys::List,
) -> bool {
    unsafe {
        if node.is_null() || (*node).type_ != pg_sys::NodeTag::T_OpExpr {
            return false;
        }
        let op = node as *mut pg_sys::OpExpr;
        let opname_ptr = pg_sys::get_opname((*op).opno);
        if opname_ptr.is_null() {
            return false;
        }
        let opname = CStr::from_ptr(opname_ptr).to_string_lossy();
        let is_eq = opname == "=";
        pg_sys::pfree(opname_ptr as *mut c_void);
        if !is_eq {
            return false;
        }

        let args = (*op).args;
        if args.is_null() || (*args).length != 2 {
            return false;
        }
        let a0 = (*(*args).elements.add(0)).ptr_value as *mut pg_sys::Node;
        let a1 = (*(*args).elements.add(1)).ptr_value as *mut pg_sys::Node;

        // Identify our-Var side: a Var whose varno belongs to this baserel.
        let relids = (*baserel).relids;
        let is_our_var = |n: *mut pg_sys::Node| -> Option<*mut pg_sys::Var> {
            if !node_is_var(n) {
                return None;
            }
            let v = n as *mut pg_sys::Var;
            if varno_member(v, relids) {
                Some(v)
            } else {
                None
            }
        };
        let (var, other) = if let Some(v) = is_our_var(a0) {
            (v, a1)
        } else if let Some(v) = is_our_var(a1) {
            (v, a0)
        } else {
            return false;
        };

        let (field, ty) = match field_info(kind, (*var).varattno) {
            Some(fi) => fi,
            None => return false,
        };

        if node_is_const(other) {
            let cst = other as *mut pg_sys::Const;
            if (*cst).constisnull {
                return false;
            }
            return assign_const(field, ty, cst, out);
        }
        if node_is_param(other) {
            let param = other as *mut pg_sys::Param;
            if !param_type_matches(param, ty) {
                return false;
            }
            *fdw_exprs = pg_sys::lappend(*fdw_exprs, other as *mut c_void);
            let idx = (*(*fdw_exprs)).length - 1;
            param_map.push((field, idx));
            return true;
        }
        // Outer-relation Var: valid driver for a parameterized path. PG will
        // rewrite it to PARAM_EXEC as part of the nested-loop plan before we
        // execute, so our ExecInitExprList + ExecEvalExpr pipeline handles it
        // exactly like a prepared-statement Param.
        if node_is_var(other) {
            let v = other as *mut pg_sys::Var;
            if varno_member(v, relids) {
                // Self-Var on our rel — not a pushable join driver.
                return false;
            }
            if !var_type_matches(v, ty) {
                return false;
            }
            *fdw_exprs = pg_sys::lappend(*fdw_exprs, other as *mut c_void);
            let idx = (*(*fdw_exprs)).length - 1;
            param_map.push((field, idx));
            return true;
        }
        false
    }
}

unsafe fn var_type_matches(var: *mut pg_sys::Var, ty: PgType) -> bool {
    unsafe {
        match ty {
            PgType::Uuid => (*var).vartype == pg_sys::UUIDOID,
            PgType::Int4 => (*var).vartype == pg_sys::INT4OID,
        }
    }
}

unsafe fn node_is_var(n: *mut pg_sys::Node) -> bool {
    unsafe { !n.is_null() && (*n).type_ == pg_sys::NodeTag::T_Var }
}
unsafe fn node_is_const(n: *mut pg_sys::Node) -> bool {
    unsafe { !n.is_null() && (*n).type_ == pg_sys::NodeTag::T_Const }
}
unsafe fn node_is_param(n: *mut pg_sys::Node) -> bool {
    unsafe { !n.is_null() && (*n).type_ == pg_sys::NodeTag::T_Param }
}

unsafe fn param_type_matches(param: *mut pg_sys::Param, ty: PgType) -> bool {
    unsafe {
        match ty {
            PgType::Uuid => (*param).paramtype == pg_sys::UUIDOID,
            PgType::Int4 => (*param).paramtype == pg_sys::INT4OID,
        }
    }
}

// Plan-time Const extraction — fills the resolved field directly.
unsafe fn assign_const(
    field: QualField,
    ty: PgType,
    cst: *mut pg_sys::Const,
    out: &mut PushedQuals,
) -> bool {
    unsafe {
        match (field, ty) {
            (QualField::Id, PgType::Uuid) => {
                if let Some(b) = uuid_from_const(cst) {
                    out.id = Some(b);
                    return true;
                }
            }
            (QualField::DebitAccountId, PgType::Uuid) => {
                if let Some(b) = uuid_from_const(cst) {
                    out.debit_account_id = Some(b);
                    return true;
                }
            }
            (QualField::CreditAccountId, PgType::Uuid) => {
                if let Some(b) = uuid_from_const(cst) {
                    out.credit_account_id = Some(b);
                    return true;
                }
            }
            (QualField::Ledger, PgType::Int4) => {
                if let Some(v) = int4_from_const(cst) {
                    out.ledger = Some(v as u32);
                    return true;
                }
            }
            (QualField::Code, PgType::Int4) => {
                if let Some(v) = int4_from_const(cst)
                    && (0..=u16::MAX as i32).contains(&v)
                {
                    out.code = Some(v as u16);
                    return true;
                }
            }
            (QualField::Flags, PgType::Int4) => {
                if let Some(v) = int4_from_const(cst)
                    && v >= 0
                {
                    out.flags = Some(v as u32);
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

// Runtime path: assign from a raw Datum produced by ExecEvalExpr on a Param.
// Returns true if the assignment took. Out-of-range values fall back to
// leaving the field unset, which degrades to a broader scan — better than a
// query-time error for a generic plan.
unsafe fn assign_datum(
    field: QualField,
    datum: pg_sys::Datum,
    isnull: bool,
    out: &mut PushedQuals,
) -> bool {
    if isnull {
        return false;
    }
    unsafe {
        match field {
            QualField::Id => {
                let ptr = datum.cast_mut_ptr::<u8>();
                if ptr.is_null() {
                    return false;
                }
                let mut b = [0u8; 16];
                std::ptr::copy_nonoverlapping(ptr, b.as_mut_ptr(), 16);
                out.id = Some(b);
            }
            QualField::DebitAccountId => {
                let ptr = datum.cast_mut_ptr::<u8>();
                if ptr.is_null() {
                    return false;
                }
                let mut b = [0u8; 16];
                std::ptr::copy_nonoverlapping(ptr, b.as_mut_ptr(), 16);
                out.debit_account_id = Some(b);
            }
            QualField::CreditAccountId => {
                let ptr = datum.cast_mut_ptr::<u8>();
                if ptr.is_null() {
                    return false;
                }
                let mut b = [0u8; 16];
                std::ptr::copy_nonoverlapping(ptr, b.as_mut_ptr(), 16);
                out.credit_account_id = Some(b);
            }
            QualField::Ledger => {
                out.ledger = Some(datum.value() as i32 as u32);
            }
            QualField::Code => {
                let v = datum.value() as i32;
                if (0..=u16::MAX as i32).contains(&v) {
                    out.code = Some(v as u16);
                } else {
                    return false;
                }
            }
            QualField::Flags => {
                let v = datum.value() as i32;
                if v >= 0 {
                    out.flags = Some(v as u32);
                } else {
                    return false;
                }
            }
        }
    }
    true
}

unsafe fn uuid_from_const(cst: *mut pg_sys::Const) -> Option<[u8; 16]> {
    unsafe {
        if (*cst).consttype != pg_sys::UUIDOID {
            return None;
        }
        // UUID Datum points at 16 raw bytes (pg_uuid_t layout).
        let ptr = (*cst).constvalue.cast_mut_ptr::<u8>();
        if ptr.is_null() {
            return None;
        }
        let mut out = [0u8; 16];
        std::ptr::copy_nonoverlapping(ptr, out.as_mut_ptr(), 16);
        Some(out)
    }
}

unsafe fn int4_from_const(cst: *mut pg_sys::Const) -> Option<i32> {
    unsafe {
        if (*cst).consttype != pg_sys::INT4OID {
            return None;
        }
        Some((*cst).constvalue.value() as i32)
    }
}

// Encode (kind, quals, param_map) into a List* so the plan survives the
// serialization Pg does when saving plans to cache. Layout:
//   [0]      int8  kind
//   [1]      bytea id (16 B payload) or NULL
//   [2]      bytea debit_account_id or NULL
//   [3]      bytea credit_account_id or NULL
//   [4]      int8  ledger or NULL
//   [5]      int8  code or NULL
//   [6]      int8  flags or NULL
//   [7]      int8  limit or NULL
//   [8]      int8  param_map length (N)
//   [9..9+2N] pairs of (int8 field_tag, int8 expr_idx) — field tag per
//             QualField::repr, expr_idx is the position in fdw_exprs.
unsafe fn encode_private(
    kind: TableKind,
    q: &PushedQuals,
    param_map: &[(QualField, i32)],
) -> *mut pg_sys::List {
    unsafe {
        let mut list: *mut pg_sys::List = std::ptr::null_mut();
        let push_int8 = |list: &mut *mut pg_sys::List, v: Option<i64>| {
            let (datum, isnull) = match v {
                Some(x) => (pg_sys::Datum::from(x), false),
                None => (pg_sys::Datum::null(), true),
            };
            let c = pg_sys::makeConst(
                pg_sys::INT8OID,
                -1,
                pg_sys::InvalidOid,
                8,
                datum,
                isnull,
                true, // byval
            );
            *list = pg_sys::lappend(*list, c as *mut c_void);
        };
        let push_bytea16 = |list: &mut *mut pg_sys::List, v: Option<[u8; 16]>| {
            let (datum, isnull) = match v {
                Some(bytes) => {
                    // Allocate a bytea in the current memory context.
                    let varsize = 16 + std::mem::size_of::<i32>();
                    let raw = pg_sys::palloc(varsize) as *mut u8;
                    // Set vl_len_ via SET_VARSIZE equivalent — 4-byte header.
                    let len_word = ((varsize as u32) << 2) as i32;
                    std::ptr::copy_nonoverlapping((&len_word as *const i32) as *const u8, raw, 4);
                    std::ptr::copy_nonoverlapping(bytes.as_ptr(), raw.add(4), 16);
                    (pg_sys::Datum::from(raw), false)
                }
                None => (pg_sys::Datum::null(), true),
            };
            let c = pg_sys::makeConst(
                pg_sys::BYTEAOID,
                -1,
                pg_sys::InvalidOid,
                -1,
                datum,
                isnull,
                false, // varlena, by-ref
            );
            *list = pg_sys::lappend(*list, c as *mut c_void);
        };

        push_int8(&mut list, Some(kind as i64));
        push_bytea16(&mut list, q.id);
        push_bytea16(&mut list, q.debit_account_id);
        push_bytea16(&mut list, q.credit_account_id);
        push_int8(&mut list, q.ledger.map(|v| v as i64));
        push_int8(&mut list, q.code.map(|v| v as i64));
        push_int8(&mut list, q.flags.map(|v| v as i64));
        push_int8(&mut list, q.limit.map(|v| v as i64));

        push_int8(&mut list, Some(param_map.len() as i64));
        for (field, idx) in param_map {
            push_int8(&mut list, Some(*field as i64));
            push_int8(&mut list, Some(*idx as i64));
        }

        list
    }
}

unsafe fn decode_private(
    list: *mut pg_sys::List,
) -> (TableKind, PushedQuals, Vec<(QualField, i32)>) {
    unsafe {
        let get = |i: usize| -> *mut pg_sys::Const {
            (*(*list).elements.add(i)).ptr_value as *mut pg_sys::Const
        };
        let read_int8 = |i: usize| -> Option<i64> {
            let c = get(i);
            if (*c).constisnull {
                None
            } else {
                Some((*c).constvalue.value() as i64)
            }
        };
        let read_bytea16 = |i: usize| -> Option<[u8; 16]> {
            let c = get(i);
            if (*c).constisnull {
                return None;
            }
            let raw = (*c).constvalue.cast_mut_ptr::<u8>();
            let mut out = [0u8; 16];
            std::ptr::copy_nonoverlapping(raw.add(4), out.as_mut_ptr(), 16);
            Some(out)
        };

        let kind = match read_int8(0).unwrap_or(-1) {
            0 => TableKind::Accounts,
            1 => TableKind::Transfers,
            2 => TableKind::Balances,
            _ => pg_sys::error!("tdw_fdw: invalid table kind in fdw_private"),
        };
        let q = PushedQuals {
            id: read_bytea16(1),
            debit_account_id: read_bytea16(2),
            credit_account_id: read_bytea16(3),
            ledger: read_int8(4).map(|v| v as u32),
            code: read_int8(5).map(|v| v as u16),
            flags: read_int8(6).map(|v| v as u32),
            limit: read_int8(7).map(|v| v as u32),
        };

        let n = read_int8(8).unwrap_or(0).max(0) as usize;
        let mut param_map = Vec::with_capacity(n);
        for i in 0..n {
            let tag = read_int8(9 + 2 * i).unwrap_or(-1);
            let idx = read_int8(10 + 2 * i).unwrap_or(-1);
            if let Some(f) = QualField::from_i64(tag) {
                param_map.push((f, idx as i32));
            }
        }

        (kind, q, param_map)
    }
}

// LIMIT / ORDER BY pushdown gets invoked at UPPERREL_FINAL. We peek at the
// planner's tuple_fraction and the query's limitCount to pick up a LIMIT N.
#[pg_guard]
unsafe extern "C-unwind" fn get_foreign_upper_paths(
    root: *mut pg_sys::PlannerInfo,
    stage: pg_sys::UpperRelationKind::Type,
    input_rel: *mut pg_sys::RelOptInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    _extra: *mut c_void,
) {
    unsafe {
        if stage != pg_sys::UpperRelationKind::UPPERREL_FINAL {
            return;
        }
        // Only push LIMIT if the whole plan below us is a ForeignScan on our FDW.
        // If the input rel is a join or has local quals that survived, we'd be
        // limiting after incomplete filtering — safer to let Pg LIMIT locally.
        if (*input_rel).reloptkind != pg_sys::RelOptKind::RELOPT_BASEREL {
            return;
        }
        let parse = (*root).parse;
        if parse.is_null() {
            return;
        }
        let limit_count = (*parse).limitCount;
        if limit_count.is_null() {
            return;
        }
        // Only pushable if limitCount is a plain Const of type BIGINT with
        // non-null value; expressions or parameters we skip for v1.
        if (*limit_count).type_ != pg_sys::NodeTag::T_Const {
            return;
        }
        let c = limit_count as *mut pg_sys::Const;
        if (*c).constisnull || (*c).consttype != pg_sys::INT8OID {
            return;
        }
        let lim_i64 = (*c).constvalue.value() as i64;
        if lim_i64 <= 0 {
            return;
        }
        // No clamp: dispatch_scan paginates across multiple TB round-trips,
        // so user LIMIT is honored exactly. Match Postgres-native behavior —
        // if you want a ceiling, use statement_timeout.
        let lim = lim_i64 as u32;

        // Mirror the existing scan path but lower its rows estimate and tag the
        // limit onto fdw_private. The simplest way to thread it through is to
        // mutate the existing best path's private list — but that's already
        // been built by GetForeignPlan for the base rel. Instead, create a new
        // ForeignPath for the upper rel with its own private list that only
        // carries the limit; BeginForeignScan combines the two at runtime.
        //
        // v1 shortcut: skip creating a proper upper path; just stash the limit
        // as a root-level hint through a static. A real implementation would
        // hang the limit on fdw_private of the upper ForeignPath — left as
        // future work because output_rel wiring is gnarly.
        UPPER_LIMIT_HINT::set(lim);
        let _ = (output_rel, input_rel); // silence unused warnings for v1
    }
}

// Single-writer, single-reader per backend. Read-consumed once per scan.
mod upper_limit {
    use std::cell::Cell;
    thread_local! {
        static HINT: Cell<Option<u32>> = const { Cell::new(None) };
    }
    pub fn set(v: u32) {
        HINT.with(|c| c.set(Some(v)));
    }
    pub fn take() -> Option<u32> {
        HINT.with(|c| c.take())
    }
}
use upper_limit as UPPER_LIMIT_HINT;

// -----------------------------------------------------------------------------
// Scan execution
// -----------------------------------------------------------------------------

#[pg_guard]
unsafe extern "C-unwind" fn begin_foreign_scan(node: *mut pg_sys::ForeignScanState, eflags: c_int) {
    unsafe {
        if eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as c_int != 0 {
            return;
        }
        let plan = (*node).ss.ps.plan as *mut pg_sys::ForeignScan;
        let (kind, mut quals, param_map) = decode_private((*plan).fdw_private);
        if let Some(lim) = UPPER_LIMIT_HINT::take() {
            quals.limit = Some(lim);
        }
        validate_schema((*node).ss.ss_currentRelation, kind);

        // InitPlans attached to an ancestor of this ForeignScan are initialized
        // during the ancestor's ExecInit, which runs AFTER our begin_foreign_scan.
        // So Params derived from those InitPlans can't be evaluated here —
        // prm->execPlan isn't set yet. We defer both ExprState init and TB
        // submission to the first iterate_foreign_scan, by which time the whole
        // executor tree is wired up. Prepared-statement PARAM_EXTERN Params work
        // either way (values are on EState from the caller), but correctness
        // demands we honor the order InitPlan + scalar-subquery Params need.
        let state = Box::new(ScanState {
            kind,
            readback: None,
            cursor: 0,
            quals,
            param_states: std::ptr::null_mut(),
            param_map,
        });
        (*node).fdw_state = Box::into_raw(state) as *mut c_void;
    }
}

// Initialize ExprStates lazily (once per scan start / rescan). InitPlans and
// scalar-subquery Params are only wired up after all children's ExecInit has
// completed, so this must be called from iterate_foreign_scan, not begin.
unsafe fn ensure_param_states(node: *mut pg_sys::ForeignScanState, state: &mut ScanState) {
    unsafe {
        if !state.param_states.is_null() || state.param_map.is_empty() {
            return;
        }
        let plan = (*node).ss.ps.plan as *mut pg_sys::ForeignScan;
        if (*plan).fdw_exprs.is_null() {
            return;
        }
        state.param_states =
            pg_sys::ExecInitExprList((*plan).fdw_exprs, node as *mut pg_sys::PlanState);
    }
}

unsafe fn apply_param_states(
    node: *mut pg_sys::ForeignScanState,
    states: *mut pg_sys::List,
    param_map: &[(QualField, i32)],
    quals: &mut PushedQuals,
) {
    unsafe {
        if states.is_null() {
            return;
        }
        let econtext = (*node).ss.ps.ps_ExprContext;
        if econtext.is_null() {
            return;
        }
        let nelems = (*states).length as usize;
        let cells = (*states).elements;
        for (field, idx) in param_map {
            let i = *idx as usize;
            if i >= nelems {
                continue;
            }
            let state = (*cells.add(i)).ptr_value as *mut pg_sys::ExprState;
            if state.is_null() {
                continue;
            }
            let mut isnull = false;
            let datum = pg_sys::ExecEvalExpr(state, econtext, &mut isnull);
            assign_datum(*field, datum, isnull, quals);
        }
    }
}

// Slot fill template for multi-row ops — captures the parameters that stay
// constant across pages (op, id/ledger/code, filter flags). `page_limit` and
// `timestamp_min` vary per iteration and are applied by drive_pagination.
struct PageFill {
    op: u8,
    id: [u8; 16],
    ledger: u32,
    code: u16,
    flags: u32,
}

unsafe fn dispatch_scan(kind: TableKind, q: &PushedQuals) -> ReadBack {
    // Lookups by id are always single-row; no pagination needed.
    if let Some(id) = q.id
        && matches!(kind, TableKind::Accounts | TableKind::Transfers)
    {
        let op = match kind {
            TableKind::Accounts => OP_LOOKUP_ACCOUNT,
            TableKind::Transfers => OP_LOOKUP_TRANSFER,
            _ => unreachable!(),
        };
        return match submit_and_wait(|slot| fill_lookup(slot, op, id)) {
            Ok(rb) => rb,
            Err(msg) if msg == "not found" => ReadBack {
                count: 0,
                bytes: vec![],
            },
            Err(msg) => error!("tdw_fdw: {}", msg),
        };
    }

    // Multi-row path. With no user LIMIT we drain until TB exhausts the
    // filter (PG-native behavior — `SELECT * FROM t` returns every row).
    // Use statement_timeout if you want a bound on long scans.
    let requested = q.limit.unwrap_or(u32::MAX);
    let fill = match kind {
        TableKind::Accounts => PageFill {
            op: OP_QUERY_ACCOUNTS,
            id: [0; 16],
            ledger: q.ledger.unwrap_or(0),
            code: q.code.unwrap_or(0),
            flags: q.flags.unwrap_or(0),
        },
        TableKind::Transfers => {
            if q.debit_account_id.is_some() || q.credit_account_id.is_some() {
                // get_account_transfers takes one account + filter flags
                // indicating which side. If both are supplied, neither gets a
                // clean pushdown — pick the one present and let Pg filter the
                // other locally.
                let (acct, side_flag) = match (q.debit_account_id, q.credit_account_id) {
                    (Some(d), Some(_c)) => (d, FF_DEBITS),
                    (Some(d), None) => (d, FF_DEBITS),
                    (None, Some(c)) => (c, FF_CREDITS),
                    (None, None) => unreachable!(),
                };
                let flag_bits = q.flags.unwrap_or(side_flag) | side_flag;
                PageFill {
                    op: OP_GET_ACCOUNT_TRANSFERS,
                    id: acct,
                    ledger: 0,
                    code: 0,
                    flags: flag_bits,
                }
            } else {
                PageFill {
                    op: OP_QUERY_TRANSFERS,
                    id: [0; 16],
                    ledger: q.ledger.unwrap_or(0),
                    code: q.code.unwrap_or(0),
                    flags: q.flags.unwrap_or(0),
                }
            }
        }
        TableKind::Balances => {
            let id = q.id.unwrap_or_else(|| {
                error!("tdw_fdw: tb_account_balances requires WHERE account_id = <uuid>");
            });
            let flag_bits = q.flags.unwrap_or(FF_BOTH) | FF_BOTH;
            PageFill {
                op: OP_GET_ACCOUNT_BALANCES,
                id,
                ledger: 0,
                code: 0,
                flags: flag_bits,
            }
        }
    };
    drive_pagination(kind, &fill, requested)
}

// Walk a multi-row result set in pages of up to MAX_QUERY_ROWS each, advancing
// TB's QueryFilter.timestamp_min past the last row returned until either the
// requested row count is satisfied or TB returns fewer rows than asked for
// (its signal that the filter is exhausted). Rows accumulate on the backend's
// transient heap — the shmem result buffer only has to hold one page at a
// time. User LIMIT is honored exactly; with no LIMIT, scans run until TB
// exhausts the filter (Postgres-native behavior).
fn drive_pagination(kind: TableKind, fill: &PageFill, requested: u32) -> ReadBack {
    let mut total: u32 = 0;
    let mut bytes: Vec<u8> = Vec::new();
    let mut timestamp_min: u64 = 0;
    loop {
        let remaining = requested.saturating_sub(total);
        if remaining == 0 {
            break;
        }
        let page_limit = remaining.min(MAX_QUERY_ROWS);
        let res = submit_and_wait(|slot| {
            slot.op = fill.op;
            slot.id = fill.id;
            slot.ledger = fill.ledger;
            slot.code = fill.code;
            slot.flags = fill.flags;
            slot.limit = page_limit;
            slot.timestamp_min = timestamp_min;
        });
        let rb = match res {
            Ok(rb) => rb,
            Err(msg) if msg == "not found" => break,
            Err(msg) => error!("tdw_fdw: {}", msg),
        };
        if rb.count == 0 {
            break;
        }
        let page_count = rb.count;
        let last_ts = last_record_ts(kind, &rb);
        bytes.extend_from_slice(&rb.bytes[..page_count as usize * RECORD_LEN]);
        total += page_count;
        // TB returned fewer than we asked for → no more rows match the filter.
        // Stop here rather than make another round-trip just to see count=0.
        if page_count < page_limit {
            break;
        }
        match last_ts {
            Some(t) if t < u64::MAX => timestamp_min = t + 1,
            // Last timestamp was u64::MAX (absurd but possible) or couldn't be
            // read — can't advance the cursor safely; stop to avoid looping
            // forever on the same page.
            _ => break,
        }
    }
    ReadBack {
        count: total,
        bytes,
    }
}

// Timestamp offset in each kind's packed record. Mirrors pack.rs byte layouts.
fn last_record_ts(kind: TableKind, rb: &ReadBack) -> Option<u64> {
    let i = (rb.count as usize).checked_sub(1)?;
    let rec = rb.record(i)?;
    let off = match kind {
        TableKind::Accounts => 88,
        TableKind::Transfers => 72,
        TableKind::Balances => 64,
    };
    let slice: [u8; 8] = rec[off..off + 8].try_into().ok()?;
    Some(u64::from_le_bytes(slice))
}

fn fill_lookup(slot: &mut Slot, op: u8, id: [u8; 16]) {
    slot.op = op;
    slot.id = id;
}

#[pg_guard]
unsafe extern "C-unwind" fn iterate_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {
    unsafe {
        let state_ptr = (*node).fdw_state as *mut ScanState;
        if state_ptr.is_null() {
            return std::ptr::null_mut();
        }
        let state = &mut *state_ptr;

        // Lazy first-call: evaluate any Params now (executor tree is fully
        // initialized, InitPlans linked) and dispatch the scan.
        if state.readback.is_none() {
            ensure_param_states(node, state);
            apply_param_states(node, state.param_states, &state.param_map, &mut state.quals);
            state.readback = Some(dispatch_scan(state.kind, &state.quals));
        }
        let rb = match state.readback.as_ref() {
            Some(r) => r,
            None => return std::ptr::null_mut(),
        };
        let scan_slot = (*node).ss.ss_ScanTupleSlot;
        pg_sys::ExecClearTuple(scan_slot);
        let i = state.cursor;
        let rec = match rb.record(i) {
            Some(r) => r,
            None => return scan_slot, // empty slot signals end of scan
        };
        state.cursor += 1;
        materialize_row(scan_slot, state.kind, rec);
        pg_sys::ExecStoreVirtualTuple(scan_slot);
        scan_slot
    }
}

unsafe fn materialize_row(slot: *mut pg_sys::TupleTableSlot, kind: TableKind, rec: &[u8]) {
    unsafe {
        let values = (*slot).tts_values;
        let nulls = (*slot).tts_isnull;
        match kind {
            TableKind::Accounts => write_account_row(values, nulls, rec),
            TableKind::Transfers => write_transfer_row(values, nulls, rec),
            TableKind::Balances => write_balance_row(values, nulls, rec),
        }
    }
}

unsafe fn write_account_row(values: *mut pg_sys::Datum, nulls: *mut bool, rec: &[u8]) {
    unsafe {
        let r = unpack_account_row(rec);
        // Columns, in order: id, ledger, code, debits_posted, credits_posted,
        // debits_pending, credits_pending, flags, timestamp.
        // 1-indexed attnums → 0-indexed array.
        set_datum_uuid(values, nulls, 0, r.0);
        set_datum_i32(values, nulls, 1, r.1);
        set_datum_i32(values, nulls, 2, r.2);
        set_datum_numeric(values, nulls, 3, r.3);
        set_datum_numeric(values, nulls, 4, r.4);
        set_datum_numeric(values, nulls, 5, r.5);
        set_datum_numeric(values, nulls, 6, r.6);
        set_datum_i32(values, nulls, 7, r.7);
        set_datum_i64(values, nulls, 8, r.8);
    }
}

unsafe fn write_transfer_row(values: *mut pg_sys::Datum, nulls: *mut bool, rec: &[u8]) {
    unsafe {
        let r = unpack_transfer_row(rec);
        set_datum_uuid(values, nulls, 0, r.0);
        set_datum_uuid(values, nulls, 1, r.1);
        set_datum_uuid(values, nulls, 2, r.2);
        set_datum_numeric(values, nulls, 3, r.3);
        set_datum_i32(values, nulls, 4, r.4);
        set_datum_i32(values, nulls, 5, r.5);
        set_datum_i32(values, nulls, 6, r.6);
        set_datum_i64(values, nulls, 7, r.7);
        // pending_id: not in unpack_transfer_row; the pack format doesn't
        // include it (TB returns it but the current pack drops it). Emit NULL
        // for now; future pack.rs extension can fill it in.
        *nulls.add(8) = true;
    }
}

unsafe fn write_balance_row(values: *mut pg_sys::Datum, nulls: *mut bool, rec: &[u8]) {
    unsafe {
        let r = unpack_balance_row(rec);
        // account_id is the pushed-down qual; we don't have it in the packed
        // record, so emit NULL. (Pg's quals filter already ensures only the
        // matching account's rows were returned.)
        *nulls.add(0) = true;
        set_datum_i64(values, nulls, 1, r.4);
        set_datum_numeric(values, nulls, 2, r.0);
        set_datum_numeric(values, nulls, 3, r.1);
        set_datum_numeric(values, nulls, 4, r.2);
        set_datum_numeric(values, nulls, 5, r.3);
    }
}

unsafe fn set_datum_i32(values: *mut pg_sys::Datum, nulls: *mut bool, idx: usize, v: i32) {
    unsafe {
        *values.add(idx) = pg_sys::Datum::from(v);
        *nulls.add(idx) = false;
    }
}

unsafe fn set_datum_i64(values: *mut pg_sys::Datum, nulls: *mut bool, idx: usize, v: i64) {
    unsafe {
        *values.add(idx) = pg_sys::Datum::from(v);
        *nulls.add(idx) = false;
    }
}

unsafe fn set_datum_uuid(values: *mut pg_sys::Datum, nulls: *mut bool, idx: usize, v: pgrx::Uuid) {
    unsafe {
        let raw = pg_sys::palloc(16) as *mut u8;
        std::ptr::copy_nonoverlapping(v.as_bytes().as_ptr(), raw, 16);
        *values.add(idx) = pg_sys::Datum::from(raw);
        *nulls.add(idx) = false;
    }
}

unsafe fn set_datum_numeric(
    values: *mut pg_sys::Datum,
    nulls: *mut bool,
    idx: usize,
    v: pgrx::AnyNumeric,
) {
    unsafe {
        // AnyNumeric is already heap-backed; into_datum is the blessed route.
        use pgrx::IntoDatum;
        match v.into_datum() {
            Some(d) => {
                *values.add(idx) = d;
                *nulls.add(idx) = false;
            }
            None => *nulls.add(idx) = true,
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn rescan_foreign_scan(node: *mut pg_sys::ForeignScanState) {
    unsafe {
        let state_ptr = (*node).fdw_state as *mut ScanState;
        if state_ptr.is_null() {
            return;
        }
        let state = &mut *state_ptr;
        state.cursor = 0;
        // Re-evaluate Params against the current econtext — for lateral /
        // correlated subquery params, values change between rescans.
        ensure_param_states(node, state);
        apply_param_states(node, state.param_states, &state.param_map, &mut state.quals);
        // Re-submit so the scan sees any new committed state.
        state.readback = Some(dispatch_scan(state.kind, &state.quals));
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn end_foreign_scan(node: *mut pg_sys::ForeignScanState) {
    unsafe {
        let state_ptr = (*node).fdw_state as *mut ScanState;
        if !state_ptr.is_null() {
            let _ = Box::from_raw(state_ptr);
            (*node).fdw_state = std::ptr::null_mut();
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn explain_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
    es: *mut pg_sys::ExplainState,
) {
    // Read quals from fdw_private on the plan rather than fdw_state: plain
    // EXPLAIN (without ANALYZE) skips begin_foreign_scan on EXEC_FLAG_EXPLAIN_ONLY,
    // so state may not exist. fdw_private is always populated at plan time.
    unsafe {
        let plan = (*node).ss.ps.plan as *mut pg_sys::ForeignScan;
        if plan.is_null() || (*plan).fdw_private.is_null() {
            return;
        }
        let (kind, quals, param_map) = decode_private((*plan).fdw_private);
        // A Param-sourced qual hasn't been evaluated yet at EXPLAIN time, but
        // we know which fields it WILL populate. Treat those as "present" for
        // labeling so generic prepared plans show the correct op.
        let has_field = |f: QualField| -> bool {
            match f {
                QualField::Id => quals.id.is_some(),
                QualField::DebitAccountId => quals.debit_account_id.is_some(),
                QualField::CreditAccountId => quals.credit_account_id.is_some(),
                QualField::Ledger => quals.ledger.is_some(),
                QualField::Code => quals.code.is_some(),
                QualField::Flags => quals.flags.is_some(),
            }
        };
        let param_has = |f: QualField| -> bool {
            param_map
                .iter()
                .any(|(pf, _)| std::mem::discriminant(pf) == std::mem::discriminant(&f))
        };
        let will_have = |f: QualField| has_field(f) || param_has(f);
        let op_label = match kind {
            TableKind::Accounts if will_have(QualField::Id) => "LOOKUP_ACCOUNT",
            TableKind::Accounts => "QUERY_ACCOUNTS",
            TableKind::Transfers if will_have(QualField::Id) => "LOOKUP_TRANSFER",
            TableKind::Transfers
                if will_have(QualField::DebitAccountId)
                    || will_have(QualField::CreditAccountId) =>
            {
                "GET_ACCOUNT_TRANSFERS"
            }
            TableKind::Transfers => "QUERY_TRANSFERS",
            TableKind::Balances => "GET_ACCOUNT_BALANCES",
        };
        let label = c"Tdw Op";
        let val = std::ffi::CString::new(op_label).unwrap();
        pg_sys::ExplainPropertyText(label.as_ptr(), val.as_ptr(), es);
        if let Some(lim) = quals.limit {
            let k = c"Tdw Limit";
            pg_sys::ExplainPropertyInteger(k.as_ptr(), std::ptr::null(), lim as i64, es);
        }
    }
}

// -----------------------------------------------------------------------------
// Modify (INSERT into tb_transfers only)
// -----------------------------------------------------------------------------

#[pg_guard]
unsafe extern "C-unwind" fn add_foreign_update_targets(
    _root: *mut pg_sys::PlannerInfo,
    _rtindex: pg_sys::Index,
    _target_rte: *mut pg_sys::RangeTblEntry,
    _target_relation: pg_sys::Relation,
) {
    // No hidden columns needed for INSERT. UPDATE/DELETE aren't supported.
}

#[pg_guard]
unsafe extern "C-unwind" fn plan_foreign_modify(
    _root: *mut pg_sys::PlannerInfo,
    plan: *mut pg_sys::ModifyTable,
    result_relation: pg_sys::Index,
    _subplan_index: c_int,
) -> *mut pg_sys::List {
    unsafe {
        if (*plan).operation != pg_sys::CmdType::CMD_INSERT {
            pg_sys::error!(
                "tdw_fdw: only INSERT is supported; got {:?}",
                (*plan).operation
            );
        }
        // rtable is on the parent Query; fetch via the PlannerInfo parent chain.
        // Simpler: just return empty private; we'll read relation OID from
        // ResultRelInfo at BeginForeignModify.
        let _ = result_relation;
        std::ptr::null_mut()
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn begin_foreign_modify(
    _mtstate: *mut pg_sys::ModifyTableState,
    rinfo: *mut pg_sys::ResultRelInfo,
    _fdw_private: *mut pg_sys::List,
    _subplan_index: c_int,
    _eflags: c_int,
) {
    unsafe {
        let rel = (*rinfo).ri_RelationDesc;
        let oid = (*(*rel).rd_rel).oid;
        let kind = table_kind_from_oid(oid).unwrap_or_else(|| {
            pg_sys::error!("tdw_fdw: unknown foreign table for INSERT");
        });
        validate_schema(rel, kind);
        let tupdesc = (*rel).rd_att;
        let natts = (*tupdesc).natts;
        let state = match kind {
            TableKind::Transfers => {
                let mut m = TransferInsertAttnums::default();
                for i in 0..natts {
                    let attr = tuple_desc_attr(tupdesc, i);
                    let name = CStr::from_ptr((*attr).attname.data.as_ptr()).to_string_lossy();
                    let a = (i + 1) as c_int;
                    match name.as_ref() {
                        "id" => m.id = Some(a),
                        "debit_account_id" => m.debit_account_id = Some(a),
                        "credit_account_id" => m.credit_account_id = Some(a),
                        "amount" => m.amount = Some(a),
                        "ledger" => m.ledger = Some(a),
                        "code" => m.code = Some(a),
                        "flags" => m.flags = Some(a),
                        "pending_id" => m.pending_id = Some(a),
                        _ => {}
                    }
                }
                ModifyState::Transfers(m)
            }
            TableKind::Accounts => {
                let mut m = AccountInsertAttnums::default();
                for i in 0..natts {
                    let attr = tuple_desc_attr(tupdesc, i);
                    let name = CStr::from_ptr((*attr).attname.data.as_ptr()).to_string_lossy();
                    let a = (i + 1) as c_int;
                    match name.as_ref() {
                        "id" => m.id = Some(a),
                        "ledger" => m.ledger = Some(a),
                        "code" => m.code = Some(a),
                        "flags" => m.flags = Some(a),
                        _ => {}
                    }
                }
                ModifyState::Accounts(m)
            }
            TableKind::Balances => {
                pg_sys::error!("tdw_fdw: tb_account_balances is read-only")
            }
        };
        (*rinfo).ri_FdwState = Box::into_raw(Box::new(state)) as *mut c_void;
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn exec_foreign_insert(
    _estate: *mut pg_sys::EState,
    rinfo: *mut pg_sys::ResultRelInfo,
    slot: *mut pg_sys::TupleTableSlot,
    plan_slot: *mut pg_sys::TupleTableSlot,
) -> *mut pg_sys::TupleTableSlot {
    unsafe {
        let state_ptr = (*rinfo).ri_FdwState as *mut ModifyState;
        if state_ptr.is_null() {
            pg_sys::error!("tdw_fdw: modify state not initialized");
        }
        let state = &*state_ptr;
        let res = match state {
            ModifyState::Transfers(m) => {
                let f = read_transfer_fields(slot, m);
                submit_and_wait(|s| {
                    s.op = OP_CREATE_TRANSFER;
                    s.id = f.id;
                    s.debit_id = f.debit_id;
                    s.credit_id = f.credit_id;
                    s.pending_id = f.pending_id;
                    s.amount = f.amount;
                    s.ledger = f.ledger;
                    s.code = f.code;
                    s.flags = f.flags;
                })
            }
            ModifyState::Accounts(m) => {
                let f = read_account_fields(slot, m);
                submit_and_wait(|s| {
                    s.op = OP_CREATE_ACCOUNT;
                    s.id = f.id;
                    s.ledger = f.ledger;
                    s.code = f.code;
                    s.flags = f.flags;
                })
            }
        };
        if let Err(msg) = res {
            pg_sys::error!("tdw_fdw: {}", msg);
        }
        let _ = plan_slot;
        slot
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn exec_foreign_batch_insert(
    _estate: *mut pg_sys::EState,
    rinfo: *mut pg_sys::ResultRelInfo,
    slots: *mut *mut pg_sys::TupleTableSlot,
    plan_slots: *mut *mut pg_sys::TupleTableSlot,
    num_slots: *mut c_int,
) -> *mut *mut pg_sys::TupleTableSlot {
    unsafe {
        let state_ptr = (*rinfo).ri_FdwState as *mut ModifyState;
        if state_ptr.is_null() {
            pg_sys::error!("tdw_fdw: modify state not initialized");
        }
        let state = &*state_ptr;
        let attnums = match state {
            ModifyState::Transfers(m) => m,
            // GetForeignModifyBatchSize returns 1 for accounts, so Pg shouldn't
            // call the batch path for them — defensive error anyway.
            ModifyState::Accounts(_) => {
                pg_sys::error!("tdw_fdw: batch insert not supported for tb_accounts");
            }
        };
        let n = *num_slots as usize;
        if n == 0 {
            return slots;
        }
        if n > MAX_BATCH_LEGS {
            pg_sys::error!(
                "tdw_fdw: batch of {} exceeds MAX_BATCH_LEGS={}",
                n,
                MAX_BATCH_LEGS
            );
        }

        let mut packed = vec![0u8; n * BATCH_LEG_LEN];
        for i in 0..n {
            let s = *slots.add(i);
            let f = read_transfer_fields(s, attnums);
            let base = i * BATCH_LEG_LEN;
            let b = &mut packed[base..base + BATCH_LEG_LEN];
            b[0..16].copy_from_slice(&f.id);
            b[16..32].copy_from_slice(&f.debit_id);
            b[32..48].copy_from_slice(&f.credit_id);
            b[48..64].copy_from_slice(&f.pending_id);
            b[64..80].copy_from_slice(&f.amount.to_le_bytes());
            b[80..84].copy_from_slice(&f.ledger.to_le_bytes());
            b[84..86].copy_from_slice(&f.code.to_le_bytes());
            let flags_u16 = (f.flags & 0xffff) as u16;
            b[86..88].copy_from_slice(&flags_u16.to_le_bytes());
        }
        let count = n as u32;
        match submit_and_wait_with_legs(
            |slot| {
                slot.op = OP_CREATE_TRANSFER_BATCH;
                slot.limit = count;
            },
            |buf| {
                buf[..packed.len()].copy_from_slice(&packed);
            },
        ) {
            Ok(_) => {}
            Err(msg) => pg_sys::error!("tdw_fdw: {}", msg),
        }
        let _ = plan_slots;
        slots
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn get_foreign_modify_batch_size(
    rinfo: *mut pg_sys::ResultRelInfo,
) -> c_int {
    unsafe {
        // Pg calls this before begin_foreign_modify, so ri_FdwState may be null.
        // Fall back to the relation name.
        let rel = (*rinfo).ri_RelationDesc;
        let name_ptr = (*(*rel).rd_rel).relname.data.as_ptr();
        let name = CStr::from_ptr(name_ptr).to_string_lossy();
        match name.as_ref() {
            // No OP_CREATE_ACCOUNT_BATCH; force single-row path.
            "tb_accounts" => 1,
            _ => MAX_BATCH_LEGS as c_int,
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn end_foreign_modify(
    _estate: *mut pg_sys::EState,
    rinfo: *mut pg_sys::ResultRelInfo,
) {
    unsafe {
        let state_ptr = (*rinfo).ri_FdwState as *mut ModifyState;
        if !state_ptr.is_null() {
            let _ = Box::from_raw(state_ptr);
            (*rinfo).ri_FdwState = std::ptr::null_mut();
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn is_foreign_rel_updatable(rel: pg_sys::Relation) -> c_int {
    // Returns a bitmask indexed by CmdType, per the FDW docs:
    //   (1 << CMD_INSERT) = 8, (1 << CMD_UPDATE) = 4, (1 << CMD_DELETE) = 16.
    // Only INSERT is supported, and only on tables TB lets us create rows in:
    // accounts and transfers. Balances are derived by TB and read-only.
    unsafe {
        let name_ptr = (*(*rel).rd_rel).relname.data.as_ptr();
        let name = CStr::from_ptr(name_ptr).to_string_lossy();
        match name.as_ref() {
            "tb_accounts" | "tb_transfers" => 1 << pg_sys::CmdType::CMD_INSERT,
            _ => 0,
        }
    }
}

struct TransferFields {
    id: [u8; 16],
    debit_id: [u8; 16],
    credit_id: [u8; 16],
    pending_id: [u8; 16],
    amount: u128,
    ledger: u32,
    code: u16,
    flags: u32,
}

unsafe fn read_transfer_fields(
    slot: *mut pg_sys::TupleTableSlot,
    m: &TransferInsertAttnums,
) -> TransferFields {
    unsafe {
        pg_sys::slot_getallattrs(slot);
        let values = (*slot).tts_values;
        let nulls = (*slot).tts_isnull;

        let read_uuid = |attnum: Option<c_int>, required: bool| -> [u8; 16] {
            let a = match attnum {
                Some(a) => a as usize - 1,
                None => {
                    if required {
                        pg_sys::error!("tdw_fdw: column missing in tuple descriptor");
                    }
                    return [0u8; 16];
                }
            };
            if *nulls.add(a) {
                if required {
                    pg_sys::error!("tdw_fdw: required UUID column is NULL");
                }
                return [0u8; 16];
            }
            let ptr = (*values.add(a)).cast_mut_ptr::<u8>();
            let mut out = [0u8; 16];
            std::ptr::copy_nonoverlapping(ptr, out.as_mut_ptr(), 16);
            out
        };
        let read_numeric_u128 = |attnum: Option<c_int>| -> u128 {
            let a = match attnum {
                Some(a) => a as usize - 1,
                None => pg_sys::error!("tdw_fdw: amount column missing"),
            };
            if *nulls.add(a) {
                pg_sys::error!("tdw_fdw: amount is NULL");
            }
            use pgrx::FromDatum;
            let n: pgrx::AnyNumeric = pgrx::AnyNumeric::from_datum(*values.add(a), false)
                .unwrap_or_else(|| pg_sys::error!("tdw_fdw: invalid numeric"));
            let s = n.to_string();
            s.parse::<u128>()
                .unwrap_or_else(|e| pg_sys::error!("tdw_fdw: amount {s}: {e}"))
        };
        let read_i32 = |attnum: Option<c_int>| -> i32 {
            let a = match attnum {
                Some(a) => a as usize - 1,
                None => pg_sys::error!("tdw_fdw: required int column missing"),
            };
            if *nulls.add(a) {
                0
            } else {
                (*values.add(a)).value() as i32
            }
        };

        let id = read_uuid(m.id, true);
        let debit_id = read_uuid(m.debit_account_id, true);
        let credit_id = read_uuid(m.credit_account_id, true);
        let pending_id = read_uuid(m.pending_id, false);
        let amount = read_numeric_u128(m.amount);
        let ledger = read_i32(m.ledger) as u32;
        let code_i = read_i32(m.code);
        if !(0..=u16::MAX as i32).contains(&code_i) {
            pg_sys::error!("tdw_fdw: code must fit in u16");
        }
        let code = code_i as u16;
        let flags = read_i32(m.flags) as u32;

        TransferFields {
            id,
            debit_id,
            credit_id,
            pending_id,
            amount,
            ledger,
            code,
            flags,
        }
    }
}

struct AccountFields {
    id: [u8; 16],
    ledger: u32,
    code: u16,
    flags: u32,
}

unsafe fn read_account_fields(
    slot: *mut pg_sys::TupleTableSlot,
    m: &AccountInsertAttnums,
) -> AccountFields {
    unsafe {
        pg_sys::slot_getallattrs(slot);
        let values = (*slot).tts_values;
        let nulls = (*slot).tts_isnull;

        let id_attnum =
            m.id.unwrap_or_else(|| pg_sys::error!("tdw_fdw: id column missing")) as usize - 1;
        if *nulls.add(id_attnum) {
            pg_sys::error!("tdw_fdw: id is NULL");
        }
        let ptr = (*values.add(id_attnum)).cast_mut_ptr::<u8>();
        let mut id = [0u8; 16];
        std::ptr::copy_nonoverlapping(ptr, id.as_mut_ptr(), 16);

        let read_i32 = |attnum: Option<c_int>| -> i32 {
            let a = match attnum {
                Some(a) => a as usize - 1,
                None => pg_sys::error!("tdw_fdw: required int column missing"),
            };
            if *nulls.add(a) {
                0
            } else {
                (*values.add(a)).value() as i32
            }
        };

        let ledger = read_i32(m.ledger) as u32;
        let code_i = read_i32(m.code);
        if !(0..=u16::MAX as i32).contains(&code_i) {
            pg_sys::error!("tdw_fdw: code must fit in u16");
        }
        let code = code_i as u16;
        let flags = read_i32(m.flags) as u32;

        AccountFields {
            id,
            ledger,
            code,
            flags,
        }
    }
}
