-- Foreign data wrapper bindings for TigerBeetle.
--
-- Run after `CREATE EXTENSION tbw`. The wrapper itself (`tbw`) is
-- registered by the extension; this file creates a server and the three
-- foreign tables mapped onto TB accounts, transfers, and balances.
--
-- All routing goes through the same shmem ring the tbw.* functions use —
-- there's only one TB connection per cluster and the server is purely a Pg
-- catalog requirement (no connection options need setting).
--
-- Multi-row reads (QUERY_*, GET_ACCOUNT_*) paginate across TB round-trips
-- under the hood, so large result sets work transparently. Behavior matches
-- vanilla Postgres: a query with LIMIT N returns at most N; a query without
-- LIMIT returns every matching row. Use statement_timeout if you want to
-- bound a long-running scan.

CREATE EXTENSION IF NOT EXISTS tbw;

CREATE SERVER IF NOT EXISTS tb_default FOREIGN DATA WRAPPER tbw;

-- Accounts. id, ledger, code are the primary filter axes:
--   WHERE id = $1                     → OP_LOOKUP_ACCOUNT
--   WHERE ledger = $1 [AND code = $2] → OP_QUERY_ACCOUNTS
-- Other predicates fall back to local Pg filtering.
CREATE FOREIGN TABLE IF NOT EXISTS tb_accounts (
    id               uuid      NOT NULL,
    ledger           int       NOT NULL,
    code             int       NOT NULL,
    debits_posted    numeric   NOT NULL,
    credits_posted   numeric   NOT NULL,
    debits_pending   numeric   NOT NULL,
    credits_pending  numeric   NOT NULL,
    flags            int       NOT NULL,
    "timestamp"      bigint    NOT NULL
) SERVER tb_default;

-- Transfers. Reads pick the most selective pushdown:
--   WHERE id = $1                           → OP_LOOKUP_TRANSFER
--   WHERE debit_account_id = $1             → OP_GET_ACCOUNT_TRANSFERS (debit side)
--   WHERE credit_account_id = $1            → OP_GET_ACCOUNT_TRANSFERS (credit side)
--   WHERE ledger = $1 [AND code = $2]       → OP_QUERY_TRANSFERS
-- pending_id is currently always NULL in reads (TB returns it but the on-wire
-- pack drops it); INSERT accepts it for POST_PENDING / VOID_PENDING flows.
CREATE FOREIGN TABLE IF NOT EXISTS tb_transfers (
    id                  uuid      NOT NULL,
    debit_account_id    uuid      NOT NULL,
    credit_account_id   uuid      NOT NULL,
    amount              numeric   NOT NULL,
    ledger              int       NOT NULL,
    code                int       NOT NULL,
    flags               int       NOT NULL,
    "timestamp"         bigint    NOT NULL,
    pending_id          uuid
) SERVER tb_default;

-- Account balance history. `account_id = $1` is required; TB's
-- get_account_balances has no bulk form.
CREATE FOREIGN TABLE IF NOT EXISTS tb_account_balances (
    account_id       uuid      NOT NULL,
    "timestamp"      bigint    NOT NULL,
    debits_posted    numeric   NOT NULL,
    credits_posted   numeric   NOT NULL,
    debits_pending   numeric   NOT NULL,
    credits_pending  numeric   NOT NULL
) SERVER tb_default;
