![](./assets/banner.png)

# beetle

A SQL interface to [TigerBeetle](https://tigerbeetle.com/), packaged as a [PostgreSQL extension](https://www.postgresql.org/docs/current/extend-extensions.html).

`beetle` lets you post accounts, post transfers, and query balances or history from plain SQL, while TigerBeetle stays authoritative for the double-entry state. A background worker inside Postgres batches requests from regular backends into the TB client, so each connection only pays for a shared-memory enqueue instead of holding its own socket to the cluster.

## Requirements

- PostgreSQL 15, 16, 17, or 18
- A running TigerBeetle cluster (single replica for dev, three for production)
- `shared_preload_libraries = 'beetle'` in `postgresql.conf`

## Quick start

The fastest way to try it is the bundled `compose.yml`, which brings up a three-replica TB cluster, a Postgres 18 image with `beetle` preloaded, and a one-shot smoke/bench runner.

```sh
docker compose up -d postgres
psql -h localhost -p 28819 -U postgres -d beetle -f sql/smoke.sql
```

Run the bundled pgbench profile:

```sh
CLIENTS=128 DURATION=60 docker compose run --rm bench
```

## Install (from release tarball)

Each tagged release ships prebuilt bundles on the [Releases page](https://github.com/CoreValence/beetle/releases), one per Postgres major (15–18) and arch (amd64, arm64). Pick the file matching your setup:

```
beetle-pg<major>-linux-<arch>.tar.gz
```

The tarball uses Debian's standard Postgres layout (`/usr/lib/postgresql/<major>/lib/` for the extension `.so`, `/usr/share/postgresql/<major>/extension/` for the `.control` and `.sql`). On Debian/Ubuntu with the PGDG packages, extract from the filesystem root:

```sh
sudo tar xzf beetle-pg18-linux-amd64.tar.gz -C /
```

For a non-Debian install, check `pg_config --pkglibdir` and `pg_config --sharedir` and move the extracted files into those directories instead.

The amd64 bundle contains two shared libraries: `beetle.so` and `libtb_client.so`. Keep them in the same directory — `beetle.so` resolves `libtb_client.so` via `$ORIGIN`. arm64 bundles contain only `beetle.so` (the TigerBeetle client links statically on that arch).

Then in `postgresql.conf`:

```
shared_preload_libraries = 'beetle'
beetle.tb_addr           = '172.30.0.10:3000,172.30.0.11:3000,172.30.0.12:3000'
beetle.tb_cluster_id     = '1'
```

Restart Postgres, then per database:

```sql
CREATE EXTENSION beetle;
```

## Docker

The release tarball drops into the stock `postgres:<major>-bookworm` layout unchanged, so a runnable image is a two-line `Dockerfile`:

```dockerfile
FROM postgres:18-bookworm
ADD https://github.com/CoreValence/beetle/releases/download/v0.1.0/beetle-pg18-linux-amd64.tar.gz /tmp/beetle.tgz
RUN tar xzf /tmp/beetle.tgz -C / && rm /tmp/beetle.tgz
```

Swap the Postgres major (`15`–`18`) and arch (`amd64`, `arm64`) in both the `FROM` and the tarball filename to match your target.

Run it against an existing TigerBeetle cluster:

```sh
docker build -t beetle:pg18 .
docker run --rm \
  --security-opt seccomp=unconfined \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  -e POSTGRES_DB=beetle \
  -p 5432:5432 \
  beetle:pg18 \
  postgres \
    -c shared_preload_libraries=beetle \
    -c beetle.tb_addr=10.0.0.10:3000,10.0.0.11:3000,10.0.0.12:3000 \
    -c beetle.tb_cluster_id=1
```

`seccomp=unconfined` is required: TigerBeetle's Zig client uses `io_uring` for TCP, which Docker's default seccomp profile blocks.

For a full multi-service setup (three TB replicas + Postgres built from source), see the bundled `compose.yml` and `Dockerfile` at the repo root — `docker compose up -d postgres` brings the whole stack up.

## Configuration

All settings are standard Postgres GUCs.

| GUC                    | Context    | Default | Description                                                                                                       |
| ---------------------- | ---------- | ------- | ----------------------------------------------------------------------------------------------------------------- |
| `beetle.tb_addr`       | Postmaster | `3000`  | Comma-separated TB replica addresses (`port`, `ip:port`, or `host:port`). Hostnames resolved once at worker start |
| `beetle.tb_cluster_id` | Postmaster | `0`     | TigerBeetle cluster id (u128, decimal). Must match the value the replica was formatted with                       |
| `beetle.batch_wait_ms` | Sighup     | `1`     | Worker idle wait between batch drains, ms. Lower = lower latency, higher = larger batches                         |
| `beetle.batch_max`     | Sighup     | `8189`  | Max slots drained per TB request. Capped by TB's 8189-per-message limit                                           |

## Usage

A round-trip: post two accounts, move money between them, and read the balances back.

```sql
CREATE EXTENSION beetle;

DO $$
DECLARE
    alice uuid := gen_random_uuid();
    bob   uuid := gen_random_uuid();
    xfer  uuid;
BEGIN
    PERFORM accounts.open(alice, 1, 100);
    PERFORM accounts.open(bob,   1, 100);
    xfer := transfers.post(gen_random_uuid(), alice, bob, 1000, 1, 10);
    RAISE NOTICE 'transfer %', xfer;
END $$;

SELECT debits_posted, credits_posted, ledger, code FROM accounts.get(alice);
--  debits_posted | credits_posted | ledger | code
-- ---------------+----------------+--------+------
--           1000 |              0 |      1 |  100
```

Per-transfer balance snapshots (the TB `HISTORY` flag = 8 must be set at account creation):

```sql
PERFORM accounts.open(alice, 1, 100, 8);
-- … post a few transfers against alice …
SELECT timestamp, debits_posted, credits_posted FROM accounts.history(alice) LIMIT 10;
```

Account-scoped transfer history with a TB flag bitfield (`DEBITS = 1`, `CREDITS = 2`):

```sql
-- outgoing transfers from alice only
SELECT id, credit, amount FROM accounts.ledger(alice, 50, 1);
```

Global scan by ledger/code coordinate:

```sql
SELECT id, debit, credit, amount FROM transfers.search(1, 10, 100);
```

## SQL API

Client-provided ids are the idempotency primitive: re-posting a transfer with the same id returns the existing row rather than creating a duplicate (TB's `exists` error). Generate them with `gen_random_uuid()`.

All functions live in one of two schemas: `accounts` or `transfers`.

```sql
-- ─── accounts ────────────────────────────────────────────────────────────
-- Create an account. flags is the raw TB bitfield (HISTORY = 8, LINKED = 1, …).
accounts.open(id uuid, ledger int, code int, flags int default 0) → uuid

-- Lookups. Return 0 rows if not found.
accounts.get(id uuid) → table(...)

-- Account-scoped history. flags: DEBITS(1) | CREDITS(2) | REVERSED(4).
accounts.ledger (account_id uuid, limit int default 10, flags int default 3) → setof record
accounts.history(account_id uuid, limit int default 10, flags int default 3) → setof record

-- Query-by-coordinate.
accounts.search(ledger int default 0, code int default 0, limit int default 10, flags int default 0) → setof record

-- ─── transfers ───────────────────────────────────────────────────────────
-- Post a transfer. Returns the same id that was passed in.
transfers.post(id uuid, debit uuid, credit uuid, amount bigint, ledger int, code int,
               flags int default 0) → uuid

-- Two-phase: reserve funds, then capture or release. amount NULL on capture
-- captures the full held amount; a smaller amount captures a partial and
-- auto-releases the rest.
transfers.hold   (id uuid, debit uuid, credit uuid, amount bigint, ledger int, code int) → uuid
transfers.capture(id uuid, pending_id uuid, amount bigint default NULL) → uuid
transfers.release(id uuid, pending_id uuid) → uuid

-- Sweep transfers: TB caps amount at the counterparty's available balance,
-- so you can say "move up to this much" and TB does the math.
transfers.sweep_from(id uuid, debit uuid, credit uuid, amount bigint, ledger int, code int) → uuid
transfers.sweep_to  (id uuid, debit uuid, credit uuid, amount bigint, ledger int, code int) → uuid

-- Atomic chain of transfers in a single TB call. Each leg is a JSON object:
--   {"id":uuid, "debit":uuid, "credit":uuid, "amount":int, "ledger":int, "code":int,
--    "pending_id"?:uuid, "flags"?:int}
-- The LINKED flag is applied automatically so all legs commit or none do.
transfers.journal(legs jsonb) → uuid[]

-- N-way split by integer percent. Percentages must sum to exactly 100;
-- floor-division rounding dust goes to remainder_to.
transfers.split(source uuid, destinations jsonb, total bigint, ledger int, code int,
                remainder_to uuid) → uuid[]

-- Fan-out with mixed fixed/percent/remainder destinations. Each entry is
-- exactly one of: {"to":uuid,"amount":int} | {"to":uuid,"pct":int}
-- | {"to":uuid,"remainder":true}. At most one remainder. Atomic LINKED chain.
transfers.allocate(source uuid, destinations jsonb, total bigint, ledger int, code int) → uuid[]

-- Fallback cascade: pull `total` from sources in order, each contributing up
-- to its `max` cap. Entries: {"from":uuid,"max":int|null} — max=null drains
-- the source's available balance. Errors if caps don't cover the total.
transfers.waterfall(destination uuid, sources jsonb, total bigint, ledger int, code int) → uuid[]

-- Lookups. Return 0 rows if not found.
transfers.get(id uuid) → table(...)

-- Query-by-coordinate.
transfers.search(ledger int default 0, code int default 0, limit int default 10, flags int default 0) → setof record

-- Post an opposite-direction transfer referencing an original. Same amount,
-- ledger, code; debit/credit swapped. Rejects two-phase originals — use
-- transfers.release / transfers.capture instead.
transfers.reverse(id uuid, original_id uuid) → uuid
```

See `sql/smoke.sql` for a full round-trip example covering every function.

## Architecture

![](./assets/banner-arch.png)

Each `post_*` call packs its request into a fixed-size slot in the shared-memory ring, wakes the worker's latch, and waits on a per-slot condvar for the outcome. The worker drains up to `batch_max` slots per TB round-trip.

## Development

Build against a specific Postgres major:

```sh
cargo pgrx init --pg18 $(which pg_config)
cargo pgrx run pg18
```

Package an installable bundle (what the release workflow does):

```sh
cargo pgrx package --features pg18 --pg-config $(which pg_config)
```

The `.github/workflows/release.yml` CI matrix builds `pg15..pg18 × {amd64, arm64}` tarballs and attaches them to tagged releases.

## License

TBD.
