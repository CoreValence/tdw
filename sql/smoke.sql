-- End-to-end smoke test: post two accounts, post a transfer between them,
-- look both sides back up, and assert the balances + transfer metadata round-trip.
--
-- Idempotent: uses gen_random_uuid() so reruns never collide with TB state.

CREATE EXTENSION IF NOT EXISTS beetle;

DO $$
DECLARE
    debit       uuid := gen_random_uuid();
    credit      uuid := gen_random_uuid();
    missing     uuid := gen_random_uuid();
    transfer_id uuid;
    acct        record;
    xfer        record;
    miss_count  int;
BEGIN
    PERFORM post_account(debit,  1, 100);
    PERFORM post_account(credit, 1, 100);
    transfer_id := post_transfer(debit, credit, 1000, 1, 10);
    RAISE NOTICE 'smoke ok: transfer_id=%', transfer_id;

    SELECT * INTO acct FROM lookup_account(debit);
    IF acct.debits_posted <> 1000 THEN
        RAISE EXCEPTION 'debit account debits_posted=% expected 1000', acct.debits_posted;
    END IF;
    IF acct.ledger <> 1 OR acct.code <> 100 THEN
        RAISE EXCEPTION 'debit account ledger/code mismatch: ledger=% code=%', acct.ledger, acct.code;
    END IF;

    SELECT * INTO acct FROM lookup_account(credit);
    IF acct.credits_posted <> 1000 THEN
        RAISE EXCEPTION 'credit account credits_posted=% expected 1000', acct.credits_posted;
    END IF;

    SELECT * INTO xfer FROM lookup_transfer(transfer_id);
    IF xfer.debit <> debit OR xfer.credit <> credit THEN
        RAISE EXCEPTION 'transfer accounts mismatch: debit=% credit=%', xfer.debit, xfer.credit;
    END IF;
    IF xfer.amount <> 1000 OR xfer.ledger <> 1 OR xfer.code <> 10 THEN
        RAISE EXCEPTION 'transfer fields mismatch: amount=% ledger=% code=%',
            xfer.amount, xfer.ledger, xfer.code;
    END IF;

    SELECT count(*) INTO miss_count FROM lookup_account(missing);
    IF miss_count <> 0 THEN
        RAISE EXCEPTION 'expected 0 rows for missing account, got %', miss_count;
    END IF;
    SELECT count(*) INTO miss_count FROM lookup_transfer(missing);
    IF miss_count <> 0 THEN
        RAISE EXCEPTION 'expected 0 rows for missing transfer, got %', miss_count;
    END IF;

    RAISE NOTICE 'smoke reads ok';
END $$;

-- account_transfers / account_balances / query_* smoke.
-- Picks a random ledger+code per run so query_{accounts,transfers} scoped to
-- them finds exactly the rows this run inserts — TB state persists across
-- reruns in the tigerbeetle-data volume, and a fixed coordinate would
-- accumulate stragglers from prior runs.
DO $$
DECLARE
    hist_ledger  int := (random() * 2000000000)::int + 100000;
    hist_code    int := (random() * 60000)::int + 1000;
    debit        uuid := gen_random_uuid();
    credit       uuid := gen_random_uuid();
    missing      uuid := gen_random_uuid();
    xfer1        uuid;
    xfer2        uuid;
    row_count    int;
    bal_count    int;
    qa_count     int;
    qt_count     int;
BEGIN
    -- TB_ACCOUNT_HISTORY = 8: enables per-transfer balance snapshots for
    -- the debit account so get_account_balances returns non-empty.
    PERFORM post_account(debit,  hist_ledger, hist_code, 8);
    PERFORM post_account(credit, hist_ledger, hist_code);

    xfer1 := post_transfer(debit, credit, 100, hist_ledger, hist_code);
    xfer2 := post_transfer(debit, credit, 250, hist_ledger, hist_code);

    -- account_transfers: both sides, default limit should return both xfers.
    SELECT count(*) INTO row_count FROM account_transfers(debit);
    IF row_count <> 2 THEN
        RAISE EXCEPTION 'account_transfers(debit) expected 2 rows, got %', row_count;
    END IF;
    SELECT count(*) INTO row_count FROM account_transfers(credit);
    IF row_count <> 2 THEN
        RAISE EXCEPTION 'account_transfers(credit) expected 2 rows, got %', row_count;
    END IF;

    -- flags=1 (DEBITS only): debit account has 2 outgoing transfers, credit has 0 debits.
    SELECT count(*) INTO row_count FROM account_transfers(debit, 10, 1);
    IF row_count <> 2 THEN
        RAISE EXCEPTION 'account_transfers(debit, DEBITS) expected 2, got %', row_count;
    END IF;
    SELECT count(*) INTO row_count FROM account_transfers(credit, 10, 1);
    IF row_count <> 0 THEN
        RAISE EXCEPTION 'account_transfers(credit, DEBITS) expected 0, got %', row_count;
    END IF;

    -- limit truncates.
    SELECT count(*) INTO row_count FROM account_transfers(debit, 1);
    IF row_count <> 1 THEN
        RAISE EXCEPTION 'account_transfers(debit, limit=1) expected 1, got %', row_count;
    END IF;

    -- Missing account returns zero rows (not an error).
    SELECT count(*) INTO row_count FROM account_transfers(missing);
    IF row_count <> 0 THEN
        RAISE EXCEPTION 'account_transfers(missing) expected 0, got %', row_count;
    END IF;

    -- account_balances requires HISTORY flag on the account.
    SELECT count(*) INTO bal_count FROM account_balances(debit);
    IF bal_count <> 2 THEN
        RAISE EXCEPTION 'account_balances(debit with HISTORY) expected 2 rows, got %', bal_count;
    END IF;
    -- credit has no HISTORY flag → empty history.
    SELECT count(*) INTO bal_count FROM account_balances(credit);
    IF bal_count <> 0 THEN
        RAISE EXCEPTION 'account_balances(credit without HISTORY) expected 0, got %', bal_count;
    END IF;

    -- query_accounts scoped by ledger+code finds both of ours.
    SELECT count(*) INTO qa_count FROM query_accounts(hist_ledger, hist_code, 10);
    IF qa_count <> 2 THEN
        RAISE EXCEPTION 'query_accounts(%, %) expected 2, got %', hist_ledger, hist_code, qa_count;
    END IF;

    -- query_transfers scoped by ledger+code finds both transfers.
    SELECT count(*) INTO qt_count FROM query_transfers(hist_ledger, hist_code, 10);
    IF qt_count <> 2 THEN
        RAISE EXCEPTION 'query_transfers(%, %) expected 2, got %', hist_ledger, hist_code, qt_count;
    END IF;

    RAISE NOTICE 'smoke query ops ok';
END $$;
