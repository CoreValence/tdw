-- Seed 128 debit and 128 credit accounts for bench_transfer.sql.
--
-- Debit  ids: 00000000-0000-0000-0000-0000000000{01..80}
-- Credit ids: 00000000-0000-0000-0000-0000000027{11..90}  (i.e. i + 10000)
--
-- Idempotent: if accounts already exist in TB (common when re-running bench
-- against a persistent tigerbeetle-data volume), the "Exists" error is
-- swallowed per-account so the bench can proceed.

CREATE EXTENSION IF NOT EXISTS beetle;

DO $$
DECLARE
    i int;
    created int := 0;
    existed int := 0;
BEGIN
    FOR i IN 1..128 LOOP
        BEGIN
            PERFORM post_account(
                ('00000000-0000-0000-0000-' || lpad(to_hex(i), 12, '0'))::uuid,
                1, 100);
            created := created + 1;
        EXCEPTION WHEN OTHERS THEN
            IF SQLERRM LIKE '%Exists%' THEN
                existed := existed + 1;
            ELSE
                RAISE;
            END IF;
        END;

        BEGIN
            PERFORM post_account(
                ('00000000-0000-0000-0000-' || lpad(to_hex(i + 10000), 12, '0'))::uuid,
                1, 100);
            created := created + 1;
        EXCEPTION WHEN OTHERS THEN
            IF SQLERRM LIKE '%Exists%' THEN
                existed := existed + 1;
            ELSE
                RAISE;
            END IF;
        END;
    END LOOP;
    RAISE NOTICE 'bench seed: created=% already_existed=%', created, existed;
END $$;
