-- Benchmark transfers.post RPC round-trip (SQL → shmem slot → bgworker → TB → back).
--
-- Usage:
--   psql -v N=1000 -f sql/bench.sql
--
-- Slot is single-shot in the PoC, so this is meaningfully single-client only.

\if :{?N}
\else
    \set N 1000
\endif

CREATE EXTENSION IF NOT EXISTS tdw;
SET tdw.bench_n = :'N';

DO $$
DECLARE
    n         int := current_setting('tdw.bench_n')::int;
    debit     uuid := gen_random_uuid();
    credit    uuid := gen_random_uuid();
    t0        timestamptz;
    t1        timestamptz;
    total_t0  timestamptz;
    lat       bigint[] := array_fill(0::bigint, ARRAY[n]);
    total_ms  numeric;
    p50       bigint; p95 bigint; p99 bigint; pmax bigint;
BEGIN
    PERFORM accounts.open(debit,  1, 100);
    PERFORM accounts.open(credit, 1, 100);

    total_t0 := clock_timestamp();
    FOR i IN 1..n LOOP
        t0 := clock_timestamp();
        PERFORM transfers.post(gen_random_uuid(), debit, credit, 1, 1, 10);
        t1 := clock_timestamp();
        lat[i] := (extract(epoch from (t1 - t0)) * 1000000)::bigint;
    END LOOP;
    total_ms := extract(epoch from (clock_timestamp() - total_t0)) * 1000;

    SELECT array_agg(x ORDER BY x) INTO lat FROM unnest(lat) AS x;
    p50  := lat[greatest(1, (n * 0.50)::int)];
    p95  := lat[greatest(1, (n * 0.95)::int)];
    p99  := lat[greatest(1, (n * 0.99)::int)];
    pmax := lat[n];

    RAISE NOTICE '';
    RAISE NOTICE 'transfers.post RPC bench';
    RAISE NOTICE '  N       = %', n;
    RAISE NOTICE '  total   = % ms', round(total_ms, 2);
    RAISE NOTICE '  tps     = %', round((n::numeric) / (total_ms / 1000), 0);
    RAISE NOTICE '  p50     = % µs', p50;
    RAISE NOTICE '  p95     = % µs', p95;
    RAISE NOTICE '  p99     = % µs', p99;
    RAISE NOTICE '  max     = % µs', pmax;
END $$;
