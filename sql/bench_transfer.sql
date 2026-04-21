-- pgbench-driven multi-client bench for post_transfer.
--
-- Usage (after accounts are seeded, see bench_setup.sql):
--   pgbench -n -c 16 -j 4 -T 10 -f sql/bench_transfer.sql \
--           -h /tmp -p 28818 -d beetle
--
-- Each client posts a transfer between two pre-seeded accounts chosen at random
-- from the seeded range. Concurrency is what lets the group-commit batcher
-- actually batch — with -c 1 you're measuring single-slot round-trip latency.

\set a random(1, 128)
\set b random(1, 128)
\set amt 1

SELECT post_transfer(
    ('00000000-0000-0000-0000-' || lpad(to_hex(:a), 12, '0'))::uuid,
    ('00000000-0000-0000-0000-' || lpad(to_hex(:b + 10000), 12, '0'))::uuid,
    :amt,
    1,
    10
);
