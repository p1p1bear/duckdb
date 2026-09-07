# Persistent SORTED BY and Background Reclustering

This document describes the implemented feature on `feature/sorted-by`. It is the
entry point for code review and design discussion. The series is based on DuckDB
`22226bba7f8b3fa32b9b1c0777c2caf048cb4cef`.

`SORTED BY` is a physical organization property of native persistent tables. It
narrows row-group zonemaps to improve selective analytical scans. It does not
guarantee SQL result order, remove an explicit `ORDER BY`, or provide an index.

## SQL and Controls

```sql
ATTACH 'events.duckdb' AS events_db (STORAGE_VERSION 'v2.0.0');
USE events_db;

CREATE TABLE events (
    event_id BIGINT,
    tenant_id BIGINT,
    event_time TIMESTAMP,
    payload VARCHAR
) SORTED BY (tenant_id, event_time, event_id);

ALTER TABLE events SET SORTED BY (tenant_id, event_time);
ALTER TABLE events RESET SORTED BY;
```

Keys must be stored column references using `ASC NULLS LAST`. Expressions such as
`day(event_time)`, other directions/null orders and collations are not supported.
Current restrictions also exclude temporary/in-memory tables, indexes,
PRIMARY KEY, UNIQUE and FOREIGN KEY constraints. UPDATE is rejected while the
property is enabled, including MERGE/ON CONFLICT/trigger UPDATE paths. Disabling
direct sorted writes does not remove these table-level restrictions.

| Global setting | Default | Effect |
| --- | --- | --- |
| `enable_sorted_write` | `true` | Permit sufficiently large INSERT/COPY/CTAS/public Appender writes to sort before appending |
| `auto_recluster` | `true` | Queue maintenance after relevant commits and successful checkpoints |
| `recluster_trigger_checkpoint` | `false` | Let background maintenance request rate-limited checkpoints when it needs fresh checkpointed inputs |

Background-only physical organization:

```sql
SET enable_sorted_write = false;
SET auto_recluster = true;
SET recluster_trigger_checkpoint = true;
```

With direct sorting disabled, new rows take the ordinary unsorted append path.
The table's sort definition and existing sorted runs remain intact. A write does
not synchronously invoke reclustering. The scheduler performs the later rewrite.
Without `recluster_trigger_checkpoint`, it waits for a normal or explicit
checkpoint when no eligible persisted input is available. With both direct sorting
and automatic maintenance disabled, data stays unsorted until explicit maintenance.

The direct-write setting is read when the physical sink is initialized, not when
the statement is prepared. Reusing a prepared statement observes the current
setting. Public Appenders observe it at each flush. A running write keeps its
chosen path. Settings are runtime configuration, not persistent sort metadata;
`RESET enable_sorted_write` restores the default. Explicit SQL sorting still runs.

Manual maintenance remains available in all direct-write modes:

```sql
CALL recluster('main.events', max_tasks=1, max_bytes='1GB', create_checkpoint=true);
CALL recluster('main.events', mode='full');
SELECT * FROM duckdb_recluster_status() WHERE table_name='events';
```

The default incremental call has a one-task, 1GB call budget and does not create
a checkpoint unless requested. FULL defaults to continuing until complete or
blocked, can checkpoint between successful levels, and still obeys memory limits.
Both modes reject explicit user transactions and read-only databases.

## Persistent and Runtime Model

| Identity/state | Purpose |
| --- | --- |
| Persistent table UUID | Identify the table across catalog replacement and WAL replay |
| Persistent column IDs | Bind keys and schemas across column renames and supported ALTER operations |
| `sort_order_id` | Identify the current rule; old rules do not become current again after RESET/SET |
| `run_id` | Identify a sorted sequence of adjacent RGs under the same rule |
| Layout version and visibility timestamp | Choose the correct physical layout for each transaction |
| Checkpoint physical identities | Prove that task inputs match recoverable persisted data |
| Replacement manifest | Describe durable private output and its exact input contract |

A run can contain many RGs. Adjacency means adjacency in the layout's RG sequence,
not dense numeric row IDs. Checkpoint vacuum can remove a whole RG and leave a
valid row-ID gap. Input ranges must remain ordered and non-overlapping. Replacement
rows are dense within their published range and cannot exceed the actual input
row count; row-ID gaps do not count as rows.

Unsorted RGs use zero sort/run IDs. Current catalog metadata retains the current
definition and monotonic next IDs; reading the older development payload with
multiple definitions remains supported. Runtime layouts, task state and ownership
objects are separate from catalog metadata. Ordinary tables use lazy sidecars so
unused sorting does not allocate complete maintenance state.

## Direct Write Path

`PhysicalInsert` and `PhysicalBatchInsert` check the global setting together with
the table property and binder permission when constructing sink state. All normal
write entry points, including public Appenders, converge on these checks.

When permitted, `AdaptiveSortedWrite` buffers up to a row group. Smaller statements
retain the unsorted path. Once the threshold is reached, it reuses DuckDB's Sort
engine for the complete statement. Sort output partitions are written through
optimistic collections and combined in partition order under one run ID. Default
RG-size output can drain in parallel; custom RG sizes use a single collection.

Trigger-generated writes, MERGE INSERT and InternalAppender paths remain
conservatively unsorted. They are not upgraded by setting `enable_sorted_write`.
Transaction-local changes that invalidate organization conservatively downgrade
their local sort tags. The property does not weaken constraint checking or
transaction rollback.

## Recluster Task

The built-in scheduler admits one task at a time per table. The internal range
registry also validates non-overlapping reservations. Candidate analysis shares
one layout/scheduling snapshot across status, backlog estimation and sizing retries.

Candidates are selected in this order:

1. Convert checkpointed unsorted/old-rule RGs.
2. Clean up sufficiently deleted current runs.
3. Merge complete current runs. Incremental mode prioritizes first-key zonemap
   overlap; FULL uses deterministic multi-level merging.

Current runs are indivisible task units. Candidate selection, task startup and
final publication each check the relevant identities. A task scans a consistent
read snapshot while foreground INSERT and DELETE can continue.

```text
STARTING -> PREPARING -> CATCHING_UP_DELETES -> PREPARED
         -> FINALIZING -> COMMITTING -> PUBLISHED

Before COMMITTING: cancellation can detach and discard private output.
After COMMITTING: the existing transaction commit/revert protocol decides the result.
```

Unsorted conversion uses the existing Sort engine. Sorted inputs use streaming
k-way merge with `create_sort_key` byte keys and deterministic tie handling. Source
data uses fixed slots and dictionary output views. After roughly eight source
capacities of copied rows, a refill resets variable-length backing stores and
copies only unread suffixes. This avoids retaining every prior VARCHAR/LIST value.

Output keeps a bounded compression batch in flight while producing the next batch.
Persisted RG metadata is written and loaded output columns are released promptly.
The output writer uses task-private block ownership; it does not publish through
an ordinary checkpoint writer. Cross-RG partial-block packing is intentionally
disabled because its durable shared-reference protocol is not implemented here.

## DELETE, Publication and Recovery

The task keeps an old-to-new row-ID remap for its entire input. It is an RG-chunked
array, normally four bytes per physical row using relative offsets; large row-ID
spans use eight bytes. The mapping remains until publication because a concurrent
DELETE can refer to a previously scanned input row. It is not currently spillable.

DELETE commit preflight reserves journal slots. Slots resolve to committed or
aborted; catch-up consumes only a resolved prefix. Exhausting journal capacity
cancels the maintenance task instead of failing the foreground DELETE. Catch-up
writes a self-contained durable metadata/manifest revision before releasing the
preceding private revision.

Finalize prepares layout patches, exact input checks, DELETE remapping, WAL chunks
and block-retention reservations outside the exclusive table write gate. Inside
the gate it rechecks object identities, DELETE sequence and WAL generation, then
uses an ordinary maintenance transaction to publish the layout and write/flush WAL.
The normal transaction visibility protocol controls when readers can see it.
WAL serialization/flush still contributes to the write pause; this is not a
zero-allocation or zero-I/O critical section.

Readers that started earlier keep the old layout. These protections have distinct
lifetimes and must not be collapsed into a single generic cleanup list:

| Protection | Release condition |
| --- | --- |
| Column ownership tokens | The concrete shared column/block ownership no longer requires the resource |
| Layout retirement | Checkpoint has persisted the drop and old-layout readers have released their references |
| WAL block retention | A successful checkpoint has retired the relevant WAL generation/position |

Recovery validates the manifest framing, checksum, table/task/range/schema and old
RG identities, applies the replacement plus final DELETE records, and registers
the corresponding ownership. A torn uncommitted maintenance record is not applied.
Publication rollback failures invalidate the database rather than hiding an
inconsistent layout. Shutdown drains/cancels scheduler work; crash correctness
depends on durable records, not destructor cleanup.

## Scheduling and Resource Limits

Commit wake-ups name only modified sorted tables; checkpoint wake-ups name only
tables that installed a snapshot. Pending requests coalesce. A regular scheduler
task executes an eligible bounded task and queues further work when appropriate.

Successful automatic checkpoints are separated by at least 60 seconds. A pending
cooldown or checkpoint-lock conflict uses one lazily created timer to requeue
work; the timer itself does not run checkpoint/recluster. Lock conflicts retry
after approximately one second, including when the blocker ends by ROLLBACK.

| Budget | Incremental/automatic | Explicit FULL |
| --- | --- | --- |
| Conversion target | 32 RGs | 32 RGs |
| Remap budget | `memory_limit / 64` | `memory_limit / 8` |
| Input-byte budget | Normally `memory_limit / 8`, also constrained by the call budget | Conversion retains the input budget; already sorted merges use the call budget |
| Merge fan-in | Up to 4 | Up to 32 |
| Per-stage parallelism | Automatic scan/sort/compression uses at most 2 task partitions; explicit calls use connection threads | Connection threads |

The merge producer can overlap a compression batch, so the per-stage cap is not
a hard limit of two busy CPUs across the complete task. The sizing code retains
minimum-RG/single-RG admission exceptions. These are
task budgets, not a process RSS guarantee. A run that cannot fit remains blocked;
increasing `max_bytes` alone does not bypass the other limits. Status reports
coverage, run count, backlog, blocked reason, active work, retired bytes and errors.
COMPLETE means no selected organization work remains, not necessarily zero retired
bytes before the next checkpoint.

Unified CPU/I/O/temp/retired-layout admission, journal node reclamation and FULL
time slicing are not implemented. A long reader can retain old blocks, and large
FULL tasks can delay DDL. The current scope keeps in-memory remap and bounded
tasks; unlimited-scale convergence and index coexistence require separate designs.

## Review Map

The history is grouped into eight review modules. They form one integrated feature,
not eight independent PRs. Shared native files stay with their principal owner;
cross-module call sites must be read against the complete series. Final-tree tests
are the acceptance boundary; intermediate commits are not claimed to be standalone
buildable releases.

| Module | Principal files |
| --- | --- |
| 1. SQL, catalog and configuration | `table_sort_metadata.*`, `table_sort_bind.*`, parser/catalog/binder changes, generated settings |
| 2. Versioned layouts and native row access | `row_group_layout.*`, `layout_row_group_cursor.cpp`, DataTable/RowGroup/collection/scan changes |
| 3. Private blocks and column ownership | allocator reservations, metadata/partial-block managers, `column_drop_ownership*` |
| 4. Switchable sorted writes | `adaptive_sorted_write.*`, INSERT operators/plans, local/optimistic storage |
| 5. Bounded task execution | checkpoint identity, candidates, task/context/manager, sorter/merger/output and explicit CALL |
| 6. Transactional publication and recovery | DELETE journal/catch-up, commit, WAL/replay, retirement/retention and transaction hooks |
| 7. Automatic maintenance and status | `recluster_auto_scheduler.cpp`, `recluster_status.cpp`, status table function |
| 8. Build integration, benchmarks and documentation | CMake wiring, order benchmarks and this review guide |

Tests follow their owning module where possible. `test_recluster_output.cpp` covers
both output and the publication/recovery boundary; `test_recluster_manager.cpp`
covers snapshot coordination and automatic scheduling.

## Verification and Performance

Build the complete series and matching test extensions before running tests;
extension binaries from a different source ID are rejected.

```sh
make reldebug
build/reldebug/test/unittest
build/reldebug/test/run --workers=4 --test-flags='--max-threads 8' '*'
```

The test flag sets the default DuckDB thread count for SQL test databases. CPU
affinity alone does not change that count; on large hosts the default can exceed
the parallelism assumed by tests with deliberately small memory limits.

Targeted entry points include `sorted_write_setting.test`, `adaptive_sorted_write.test`,
`recluster_explicit.test`, `recluster_run_gaps.test`, and C++ tags
`[row_group_layout]`, `[recluster_sort]`, `[recluster_auto]`, `[replacement_manifest]`,
`[recluster_finalize]` and `[recluster_wal]`.

The setting tests cover execution-time prepared statement changes, parallel and
batch INSERT, COPY, CTAS, public Appender flushes, existing run preservation and
automatic background-only convergence. Other regressions cover NULL/nested data,
concurrent INSERT/DELETE, old readers, DDL conflicts, gap-containing inputs,
bounded payload buffers, failed publication, torn WAL and block retention.

Representative existing measurements are developer experiments on a shared host,
not release guarantees. The controlled 50M workload uses 12 columns, 8 physical
CPUs, an 8GB DuckDB limit, fresh input copies, warm-up and complete fingerprints.

| Comparison | Observation |
| --- | --- |
| Earlier ordinary vs direct sorted INSERT | Approximately 2x write wall time for sorting on the tested wide workload |
| Earlier unsorted vs FULL layout | Selective range/Top-N queries improve substantially; full-width ORDER BY improved about 13.5%; full scans had little benefit |
| Latest merge-buffer fix, 13 runs to 1 | FULL median 24.731s -> 22.540s; CPU task-clock 72.255s -> 62.981s; peak RSS 9.617GiB -> 5.160GiB |
| Resource tradeoff | Write volume remained about 4.274GiB and disk saturation remained possible; no temp spill in that FULL case |

The new OFF switch has functional coverage; prior deferred-write timings used the
older unsorted-table/ALTER workflow and are not relabeled as measurements of this
new setting. The in-tree microbenchmarks are reusable workload definitions, while
the detailed 50M protocol and raw records remain in the accompanying design folder.

Before production enablement, independently review the storage/recovery changes,
validate sustained mixed workloads and resource failure behavior, and define
storage-version and rollback policy. Existing development builds need not accept
newly valid manifest states such as gapped inputs; unchanged field numbers are not
a downgrade compatibility guarantee.
