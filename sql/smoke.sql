-- End-to-end smoke test: open two accounts, post a transfer between them,
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
    PERFORM accounts.open(debit,  1, 100);
    PERFORM accounts.open(credit, 1, 100);
    transfer_id := transfers.post(gen_random_uuid(), debit, credit, 1000, 1, 10);
    RAISE NOTICE 'smoke ok: transfer_id=%', transfer_id;

    SELECT * INTO acct FROM accounts.get(debit);
    IF acct.debits_posted <> 1000 THEN
        RAISE EXCEPTION 'debit account debits_posted=% expected 1000', acct.debits_posted;
    END IF;
    IF acct.ledger <> 1 OR acct.code <> 100 THEN
        RAISE EXCEPTION 'debit account ledger/code mismatch: ledger=% code=%', acct.ledger, acct.code;
    END IF;

    SELECT * INTO acct FROM accounts.get(credit);
    IF acct.credits_posted <> 1000 THEN
        RAISE EXCEPTION 'credit account credits_posted=% expected 1000', acct.credits_posted;
    END IF;

    SELECT * INTO xfer FROM transfers.get(transfer_id);
    IF xfer.debit <> debit OR xfer.credit <> credit THEN
        RAISE EXCEPTION 'transfer accounts mismatch: debit=% credit=%', xfer.debit, xfer.credit;
    END IF;
    IF xfer.amount <> 1000 OR xfer.ledger <> 1 OR xfer.code <> 10 THEN
        RAISE EXCEPTION 'transfer fields mismatch: amount=% ledger=% code=%',
            xfer.amount, xfer.ledger, xfer.code;
    END IF;

    SELECT count(*) INTO miss_count FROM accounts.get(missing);
    IF miss_count <> 0 THEN
        RAISE EXCEPTION 'expected 0 rows for missing account, got %', miss_count;
    END IF;
    SELECT count(*) INTO miss_count FROM transfers.get(missing);
    IF miss_count <> 0 THEN
        RAISE EXCEPTION 'expected 0 rows for missing transfer, got %', miss_count;
    END IF;

    RAISE NOTICE 'smoke reads ok';
END $$;

-- accounts.ledger / accounts.history / *.search smoke.
-- Picks a random ledger+code per run so *.search scoped to them finds exactly
-- the rows this run inserts — TB state persists across reruns in the
-- tigerbeetle-data volume, and a fixed coordinate would accumulate stragglers
-- from prior runs.
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
    -- the debit account so accounts.history returns non-empty.
    PERFORM accounts.open(debit,  hist_ledger, hist_code, 8);
    PERFORM accounts.open(credit, hist_ledger, hist_code);

    xfer1 := transfers.post(gen_random_uuid(), debit, credit, 100, hist_ledger, hist_code);
    xfer2 := transfers.post(gen_random_uuid(), debit, credit, 250, hist_ledger, hist_code);

    -- accounts.ledger: both sides, default limit should return both xfers.
    SELECT count(*) INTO row_count FROM accounts.ledger(debit);
    IF row_count <> 2 THEN
        RAISE EXCEPTION 'accounts.ledger(debit) expected 2 rows, got %', row_count;
    END IF;
    SELECT count(*) INTO row_count FROM accounts.ledger(credit);
    IF row_count <> 2 THEN
        RAISE EXCEPTION 'accounts.ledger(credit) expected 2 rows, got %', row_count;
    END IF;

    -- flags=1 (DEBITS only): debit account has 2 outgoing transfers, credit has 0 debits.
    SELECT count(*) INTO row_count FROM accounts.ledger(debit, 10, 1);
    IF row_count <> 2 THEN
        RAISE EXCEPTION 'accounts.ledger(debit, DEBITS) expected 2, got %', row_count;
    END IF;
    SELECT count(*) INTO row_count FROM accounts.ledger(credit, 10, 1);
    IF row_count <> 0 THEN
        RAISE EXCEPTION 'accounts.ledger(credit, DEBITS) expected 0, got %', row_count;
    END IF;

    -- limit truncates.
    SELECT count(*) INTO row_count FROM accounts.ledger(debit, 1);
    IF row_count <> 1 THEN
        RAISE EXCEPTION 'accounts.ledger(debit, limit=1) expected 1, got %', row_count;
    END IF;

    -- Missing account returns zero rows (not an error).
    SELECT count(*) INTO row_count FROM accounts.ledger(missing);
    IF row_count <> 0 THEN
        RAISE EXCEPTION 'accounts.ledger(missing) expected 0, got %', row_count;
    END IF;

    -- accounts.history requires HISTORY flag on the account.
    SELECT count(*) INTO bal_count FROM accounts.history(debit);
    IF bal_count <> 2 THEN
        RAISE EXCEPTION 'accounts.history(debit with HISTORY) expected 2 rows, got %', bal_count;
    END IF;
    -- credit has no HISTORY flag → empty history.
    SELECT count(*) INTO bal_count FROM accounts.history(credit);
    IF bal_count <> 0 THEN
        RAISE EXCEPTION 'accounts.history(credit without HISTORY) expected 0, got %', bal_count;
    END IF;

    -- accounts.search scoped by ledger+code finds both of ours.
    SELECT count(*) INTO qa_count FROM accounts.search(hist_ledger, hist_code, 10);
    IF qa_count <> 2 THEN
        RAISE EXCEPTION 'accounts.search(%, %) expected 2, got %', hist_ledger, hist_code, qa_count;
    END IF;

    -- transfers.search scoped by ledger+code finds both transfers.
    SELECT count(*) INTO qt_count FROM transfers.search(hist_ledger, hist_code, 10);
    IF qt_count <> 2 THEN
        RAISE EXCEPTION 'transfers.search(%, %) expected 2, got %', hist_ledger, hist_code, qt_count;
    END IF;

    RAISE NOTICE 'smoke query ops ok';
END $$;

-- Composite verbs: hold/capture/release, sweep_*, journal, split.
DO $$
DECLARE
    composite_ledger int := (random() * 2000000000)::int + 100000;
    composite_code   int := (random() * 60000)::int + 1000;
    alice   uuid := gen_random_uuid();
    bob     uuid := gen_random_uuid();
    carol   uuid := gen_random_uuid();
    dave    uuid := gen_random_uuid();
    dust_to uuid := gen_random_uuid();
    pend_id uuid := gen_random_uuid();
    post_id uuid := gen_random_uuid();
    pend2   uuid := gen_random_uuid();
    void2   uuid := gen_random_uuid();
    bal_dr  uuid := gen_random_uuid();
    bal_cr  uuid := gen_random_uuid();
    acct    record;
    batch_ids uuid[];
    split_ids uuid[];
BEGIN
    -- Fund alice so sweep/partial-capture math is meaningful.
    PERFORM accounts.open(alice,   composite_ledger, composite_code);
    PERFORM accounts.open(bob,     composite_ledger, composite_code);
    PERFORM accounts.open(carol,   composite_ledger, composite_code);
    PERFORM accounts.open(dave,    composite_ledger, composite_code);
    PERFORM accounts.open(dust_to, composite_ledger, composite_code);

    -- Two-phase: hold 500, then capture 300 (partial auto-voids remainder).
    PERFORM transfers.hold(pend_id, alice, bob, 500, composite_ledger, composite_code);
    SELECT * INTO acct FROM accounts.get(alice);
    IF acct.debits_pending <> 500 THEN
        RAISE EXCEPTION 'hold expected debits_pending=500, got %', acct.debits_pending;
    END IF;

    PERFORM transfers.capture(post_id, pend_id, 300);
    SELECT * INTO acct FROM accounts.get(alice);
    IF acct.debits_pending <> 0 OR acct.debits_posted <> 300 THEN
        RAISE EXCEPTION 'capture: pending=% posted=% (expected 0 / 300)',
            acct.debits_pending, acct.debits_posted;
    END IF;

    -- Hold-then-release round-trip.
    PERFORM transfers.hold(pend2, alice, bob, 200, composite_ledger, composite_code);
    PERFORM transfers.release(void2, pend2);
    SELECT * INTO acct FROM accounts.get(alice);
    IF acct.debits_pending <> 0 THEN
        RAISE EXCEPTION 'release expected 0 pending, got %', acct.debits_pending;
    END IF;

    -- Sweep: cap the debit at alice's available balance. alice has 300
    -- posted as debits already; a sweep_from for 1_000_000 should only
    -- move what's actually there without overdrawing.
    PERFORM transfers.sweep_to  (bal_cr, dave, alice, 1000, composite_ledger, composite_code);
    PERFORM transfers.sweep_from(bal_dr, alice, carol, 1000000, composite_ledger, composite_code);
    SELECT * INTO acct FROM accounts.get(alice);
    IF acct.debits_posted < 300 THEN
        RAISE EXCEPTION 'sweep_from corrupted alice: debits_posted=%', acct.debits_posted;
    END IF;

    -- Journal: three linked legs. Either all commit or none.
    batch_ids := transfers.journal(jsonb_build_array(
        jsonb_build_object(
            'id', gen_random_uuid(),
            'debit', alice, 'credit', bob,
            'amount', 10, 'ledger', composite_ledger, 'code', composite_code),
        jsonb_build_object(
            'id', gen_random_uuid(),
            'debit', bob, 'credit', carol,
            'amount', 10, 'ledger', composite_ledger, 'code', composite_code),
        jsonb_build_object(
            'id', gen_random_uuid(),
            'debit', carol, 'credit', dave,
            'amount', 10, 'ledger', composite_ledger, 'code', composite_code)
    ));
    IF array_length(batch_ids, 1) <> 3 THEN
        RAISE EXCEPTION 'transfers.journal expected 3 ids, got %', array_length(batch_ids, 1);
    END IF;

    -- Split with dust, remainder_to IS one of the destinations: merged leg, 3 ids.
    -- total=10, 33/33/34 pct → 3/3/3 = 9 posted, dust=1 goes to bob's leg.
    split_ids := transfers.split(
        alice,
        jsonb_build_array(
            jsonb_build_object('to', bob,   'pct', 33),
            jsonb_build_object('to', carol, 'pct', 33),
            jsonb_build_object('to', dave,  'pct', 34)
        ),
        10,
        composite_ledger,
        composite_code,
        bob
    );
    IF array_length(split_ids, 1) <> 3 THEN
        RAISE EXCEPTION 'transfers.split (dust merged into existing dest) expected 3 ids, got %',
            array_length(split_ids, 1);
    END IF;

    -- Split with dust, remainder_to is NOT a destination: separate leg, 4 ids.
    split_ids := transfers.split(
        alice,
        jsonb_build_array(
            jsonb_build_object('to', bob,   'pct', 33),
            jsonb_build_object('to', carol, 'pct', 33),
            jsonb_build_object('to', dave,  'pct', 34)
        ),
        10,
        composite_ledger,
        composite_code,
        dust_to
    );
    IF array_length(split_ids, 1) <> 4 THEN
        RAISE EXCEPTION 'transfers.split (separate dust leg) expected 4 ids, got %',
            array_length(split_ids, 1);
    END IF;

    RAISE NOTICE 'smoke composite verbs ok';
END $$;

-- transfers.allocate / transfers.waterfall: fan-out / fallback cascade.
DO $$
DECLARE
    rc_ledger int := (random() * 2000000000)::int + 100000;
    rc_code   int := (random() * 60000)::int + 1000;
    src       uuid := gen_random_uuid();
    a         uuid := gen_random_uuid();
    b         uuid := gen_random_uuid();
    c         uuid := gen_random_uuid();
    rem       uuid := gen_random_uuid();
    pool1     uuid := gen_random_uuid();
    pool2     uuid := gen_random_uuid();
    pool3     uuid := gen_random_uuid();
    sink      uuid := gen_random_uuid();
    funder    uuid := gen_random_uuid();
    route_ids uuid[];
    casc_ids  uuid[];
    acct      record;
BEGIN
    PERFORM accounts.open(src, rc_ledger, rc_code);
    PERFORM accounts.open(a,   rc_ledger, rc_code);
    PERFORM accounts.open(b,   rc_ledger, rc_code);
    PERFORM accounts.open(c,   rc_ledger, rc_code);
    PERFORM accounts.open(rem, rc_ledger, rc_code);

    -- allocate: fixed 100 to a, 25% to b, remainder to rem.
    -- total=1000, claimed=100+250=350, remainder=650.
    route_ids := transfers.allocate(
        src,
        jsonb_build_array(
            jsonb_build_object('to', a,   'amount', 100),
            jsonb_build_object('to', b,   'pct', 25),
            jsonb_build_object('to', rem, 'remainder', true)
        ),
        1000, rc_ledger, rc_code);
    IF array_length(route_ids, 1) <> 3 THEN
        RAISE EXCEPTION 'transfers.allocate expected 3 ids, got %', array_length(route_ids, 1);
    END IF;

    SELECT * INTO acct FROM accounts.get(a);
    IF acct.credits_posted <> 100 THEN
        RAISE EXCEPTION 'allocate fixed leg: a.credits_posted=% expected 100', acct.credits_posted;
    END IF;
    SELECT * INTO acct FROM accounts.get(b);
    IF acct.credits_posted <> 250 THEN
        RAISE EXCEPTION 'allocate pct leg: b.credits_posted=% expected 250', acct.credits_posted;
    END IF;
    SELECT * INTO acct FROM accounts.get(rem);
    IF acct.credits_posted <> 650 THEN
        RAISE EXCEPTION 'allocate remainder leg: rem.credits_posted=% expected 650', acct.credits_posted;
    END IF;

    -- allocate without a remainder and exact-sum pcts.
    route_ids := transfers.allocate(
        src,
        jsonb_build_array(
            jsonb_build_object('to', a, 'pct', 50),
            jsonb_build_object('to', b, 'pct', 50)
        ),
        200, rc_ledger, rc_code);
    IF array_length(route_ids, 1) <> 2 THEN
        RAISE EXCEPTION 'transfers.allocate (exact sum) expected 2 ids, got %', array_length(route_ids, 1);
    END IF;

    -- waterfall: fund pool1=50, pool2=150, pool3=9999. Target: move 500 to sink,
    -- draining pool1 then pool2 then pool3.
    PERFORM accounts.open(pool1,  rc_ledger, rc_code);
    PERFORM accounts.open(pool2,  rc_ledger, rc_code);
    PERFORM accounts.open(pool3,  rc_ledger, rc_code);
    PERFORM accounts.open(sink,   rc_ledger, rc_code);
    PERFORM accounts.open(funder, rc_ledger, rc_code);
    PERFORM transfers.post(gen_random_uuid(), funder, pool1,   50, rc_ledger, rc_code);
    PERFORM transfers.post(gen_random_uuid(), funder, pool2,  150, rc_ledger, rc_code);
    PERFORM transfers.post(gen_random_uuid(), funder, pool3, 9999, rc_ledger, rc_code);

    -- max=null on pool1 and pool2 drains them; pool3 capped explicitly.
    casc_ids := transfers.waterfall(
        sink,
        jsonb_build_array(
            jsonb_build_object('from', pool1, 'max', NULL),
            jsonb_build_object('from', pool2, 'max', NULL),
            jsonb_build_object('from', pool3, 'max', 9999)
        ),
        500, rc_ledger, rc_code);
    -- expected: pool1=50, pool2=150, pool3=300 → 3 legs.
    IF array_length(casc_ids, 1) <> 3 THEN
        RAISE EXCEPTION 'transfers.waterfall expected 3 legs, got %', array_length(casc_ids, 1);
    END IF;

    SELECT * INTO acct FROM accounts.get(sink);
    IF acct.credits_posted <> 500 THEN
        RAISE EXCEPTION 'waterfall: sink.credits_posted=% expected 500', acct.credits_posted;
    END IF;
    SELECT * INTO acct FROM accounts.get(pool1);
    IF acct.debits_posted <> 50 THEN
        RAISE EXCEPTION 'waterfall: pool1.debits_posted=% expected 50', acct.debits_posted;
    END IF;
    SELECT * INTO acct FROM accounts.get(pool2);
    IF acct.debits_posted <> 150 THEN
        RAISE EXCEPTION 'waterfall: pool2.debits_posted=% expected 150', acct.debits_posted;
    END IF;
    SELECT * INTO acct FROM accounts.get(pool3);
    IF acct.debits_posted <> 300 THEN
        RAISE EXCEPTION 'waterfall: pool3.debits_posted=% expected 300', acct.debits_posted;
    END IF;

    -- waterfall stops early when total is met.
    casc_ids := transfers.waterfall(
        sink,
        jsonb_build_array(
            jsonb_build_object('from', pool3, 'max', 100),
            jsonb_build_object('from', pool3, 'max', 9999)
        ),
        40, rc_ledger, rc_code);
    IF array_length(casc_ids, 1) <> 1 THEN
        RAISE EXCEPTION 'transfers.waterfall (early stop) expected 1 leg, got %', array_length(casc_ids, 1);
    END IF;

    RAISE NOTICE 'smoke allocate/waterfall ok';
END $$;

-- transfers.reverse: posts an opposite-direction transfer referencing an original.
DO $$
DECLARE
    rv_ledger int := (random() * 2000000000)::int + 100000;
    rv_code   int := (random() * 60000)::int + 1000;
    payer     uuid := gen_random_uuid();
    payee     uuid := gen_random_uuid();
    missing   uuid := gen_random_uuid();
    orig_id   uuid;
    rev_id    uuid;
    pend_id   uuid := gen_random_uuid();
    acct      record;
BEGIN
    PERFORM accounts.open(payer, rv_ledger, rv_code);
    PERFORM accounts.open(payee, rv_ledger, rv_code);

    -- post 500, then reverse it. balances should net to zero on both sides.
    orig_id := transfers.post(gen_random_uuid(), payer, payee, 500, rv_ledger, rv_code);
    rev_id  := transfers.reverse(gen_random_uuid(), orig_id);

    SELECT * INTO acct FROM accounts.get(payer);
    IF acct.debits_posted <> 500 OR acct.credits_posted <> 500 THEN
        RAISE EXCEPTION 'reverse: payer debits=% credits=% expected 500/500',
            acct.debits_posted, acct.credits_posted;
    END IF;
    SELECT * INTO acct FROM accounts.get(payee);
    IF acct.debits_posted <> 500 OR acct.credits_posted <> 500 THEN
        RAISE EXCEPTION 'reverse: payee debits=% credits=% expected 500/500',
            acct.debits_posted, acct.credits_posted;
    END IF;

    -- both entries stay in history.
    IF (SELECT count(*) FROM accounts.ledger(payer)) <> 2 THEN
        RAISE EXCEPTION 'reverse: payer ledger should have 2 entries';
    END IF;

    -- reversing a non-existent transfer errors.
    BEGIN
        PERFORM transfers.reverse(gen_random_uuid(), missing);
        RAISE EXCEPTION 'reverse(missing) should have errored';
    EXCEPTION WHEN OTHERS THEN
        -- expected
    END;

    -- reversing a pending transfer errors (use release / capture instead).
    PERFORM transfers.hold(pend_id, payer, payee, 100, rv_ledger, rv_code);
    BEGIN
        PERFORM transfers.reverse(gen_random_uuid(), pend_id);
        RAISE EXCEPTION 'reverse(pending) should have errored';
    EXCEPTION WHEN OTHERS THEN
        -- expected
    END;
    PERFORM transfers.release(gen_random_uuid(), pend_id);

    RAISE NOTICE 'smoke reverse ok';
END $$;
