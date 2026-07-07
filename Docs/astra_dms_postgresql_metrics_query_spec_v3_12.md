# Astra DMS — PostgreSQL Metrics Collection Query Specification

**Target platform:** Azure Database for PostgreSQL Flexible Server
**Primary consumer:** Azure Function metrics collector
**Test context:** DMS performance test with an 8-hour k6 load profile
**Document status:** Collector execution specification — consolidated
**Version:** 3.12 — 6 July 2026

> **What version 3 is.** This document keeps the consolidated PostgreSQL coverage
> from version 2 and adds **PgBouncer connection-pool collection** for before-after
> analysis when the application is switched from direct PostgreSQL connections to
> PgBouncer. Existing PostgreSQL query layout is preserved. PgBouncer coverage is
> separated into **PB01–PB06** entries because those commands are executed against
> the PgBouncer admin console rather than PostgreSQL system catalogs.

> **Version 3.02 implementation correction.** The startup/configuration inventory
> is simplified into one logical **Q01** group with four independently executable
> statements: **Q01A**, **Q01B**, **Q01C**, and **Q01D**. Legacy **M03A/M03B/M03C**
> and **C01** are no longer standalone query IDs. Their parameter coverage is
> merged by source catalog so the collector does not run overlapping configuration
> scans.

> **Version 3.03 implementation correction.** **Q02** is simplified into one
> independently executable SQL statement. The live wait/activity logic and the
> parallel-worker-pool logic are now physically merged inside Q02, so the
> collector no longer needs any separate 15-second parallel-worker statement.

> **Version 3.04 implementation correction.** **Q03** keeps the same blocking and
> lock-chain logic, but the SQL comment tag is normalized from the legacy
> `w02_replaces_q03` lineage tag to the operational `q03` collector tag.

> **Version 3.09 implementation correction.** Standalone memory collector entries
> are normalized into the standard Q-series: former M01, M02, M04, and M05 were
> mapped to Q13, Q14, Q15, and Q16 in v3.09. Legacy M03 remains folded into
> Q01A–Q01D and is not reintroduced as a standalone collector query. The memory
> query SQL is now PostgreSQL 18-only; fallback variants for older PostgreSQL
> versions are removed. Version 3.10 supersedes the former Q16 mapping by moving
> backend memory-context logging to EQ01.



> **Version 3.10 implementation correction.** The former **Q16** backend
> memory-context logging entry is retired from the implemented collector and moved
> to the excluded-query registry as **EQ01** because Azure Database for PostgreSQL
> Flexible Server customer admin roles may not be able to grant or execute
> `pg_log_backend_memory_contexts(integer)`. The operational Q-series now continues
> from the former W04/W05/W06, C04, and PB01–PB06 entries: W04→Q16, W05→Q17,
> W06→Q18, C04→Q19, and PB01–PB06→Q20–Q25.

> **Version 3.11 layout correction.** Collector execution targets are now explicit:
> PostgreSQL server-wide, PostgreSQL database-scoped, Azure system database
> `azure_sys`, dictionary/reference, PgBouncer admin console, and excluded
> queries. The former **Q18** wait-event dictionary is moved to **DICT01** because
> it is metadata rather than a runtime metric. PgBouncer commands are no longer
> coded as Q-series entries; former **Q20–Q25** are restored as **PB01–PB06**. The
> PgBouncer section also adds a .NET/Npgsql smoke-test checklist because PgBouncer
> admin console `SHOW` commands require protocol behavior that must be validated
> before implementing the collector in .NET.

> **Version 3.12 scope correction.** **Q05** is corrected from database-scoped
> execution to server-wide `pg_stat_statements` collection. Q05 must not exclude
> `current_user`, must not filter to `current_database()`, and must not be run
> once per application database; database attribution is done through `dbid` and
> `datname`. **Q12** is also corrected to server-wide execution because
> PostgreSQL progress views already expose the database attribution for active
> maintenance backends. Q03 and Q02 receive guardrail notes for cross-database
> relation-name resolution and collector `application_name` reservation.

> **Faithfulness note.** The PostgreSQL SQL blocks consolidated in version 2 are
> preserved from the reviewed source documents, except for the Q01 configuration
> inventory simplification introduced in version 3.02 and the Q02 single-query
> consolidation introduced in version 3.03, and the Q03 comment-tag normalization
> introduced in version 3.04. The PgBouncer blocks are
> PgBouncer admin-console `SHOW` commands and do not require PostgreSQL catalog
> SQL; they are coded as PB01–PB06 rather than Q-series entries. Q01 remains a logical configuration group. Q02 is now one operational SQL
> statement with no separate parallel-worker execution path. Q13–Q15 are the
> standard Q-series replacements for the implemented standalone memory collector entries, while DICT01 and PB01–PB06 are separate non-runtime/non-PostgreSQL-catalog groups and the former Q16 backend memory-context entry is retained as EQ01 in the excluded-query registry.

---

## 1. Purpose

Dokumen ini mendefinisikan query PostgreSQL yang dijalankan oleh Azure Function
untuk mengumpulkan evidence terkait:

- wait state, specific wait event, dan blocking/lock chain;
- CPU workload attribution, planning overhead, JIT overhead, dan parallel-query behavior;
- database dan query I/O;
- memory-pressure indicators, shared-buffer occupancy, dan memory override/forensic;
- connection pressure;
- PgBouncer pool state, pool saturation, and before-after pooling evidence;
- cache effectiveness;
- checkpoint, WAL, temp spill, table, index, dan SLRU behavior.

Query disusun supaya satu sumber tidak dipolling berkali-kali hanya karena dipakai
untuk kategori analisis yang berbeda. Contohnya, satu snapshot `pg_stat_activity`
(Q02) dipakai sekaligus untuk wait state, wait event, connection, long query, long
transaction, parallel-worker pool, dan parallel-worker visibility. PgBouncer snapshots
(PB01–PB06) melengkapi Q02 dengan sisi pooler: client queue, server pool usage,
server assignment, dan wait time di layer PgBouncer.

Dokumen ini **tidak** menjadikan seluruh query sebagai polling 15 detik. Frekuensi
dibedakan berdasarkan sifat data, biaya query, dan granularitas sumber.

PostgreSQL built-in statistics dan Azure PostgreSQL Query Store **tidak** menyediakan
exact per-query CPU time. `total_exec_time`, `active_cpu_or_running_sessions`, dan
Query Store `total_time` adalah elapsed/aggregate proxy, bukan CPU time. CPU
attribution tetap correlation-based (lihat Section 9).

---

## 2. Consolidation and Query Lineage

### 2.1 How v1 and the addenda map into version 3.11

| v3.11 ID | Execution target category | Role | Sources merged | Treatment |
|---|---|---|---|---|
| **Q01** | PG-CFG / PG-DB | Configuration, capability, and override inventory | v1 Q01 + legacy M03 + legacy C01 parameter coverage | Implemented as Q01A–Q01D by source catalog; Q01D may need to run per application database |
| **Q02** | PG-SRV | Unified activity / wait / connection / long query / long transaction / parallel-worker snapshot | W01 (replaced v1 Q02) + CPU/parallelism addendum parallel-worker logic | Implemented as one SQL statement using a single `pg_stat_activity` scan; no separate parallel-worker query ID |
| **Q03** | PG-SRV | Blocking pair, lock detail, exact lock-wait duration, blocking chain | W02 (replaced v1 Q03) | Conditional diagnostic |
| **Q04** | PG-SRV | Database-level transactions, cache, temp, deadlock, I/O timing | v1 Q04 | Server-wide view with per-database rows |
| **Q05** | PG-SRV | Query-level workload, CPU candidate, planning, JIT, parallel, I/O, temp spill | C03 (replaced v1 Q05) | Server-wide `pg_stat_statements`; run once from one database where the extension view is available |
| **Q06** | PG-SRV | `pg_stat_statements` capacity and reset health | v1 Q06 | Unchanged |
| **Q07** | PG-SRV | Cluster I/O by backend type, object, context | v1 Q07 | PostgreSQL 18-only |
| **Q08** | PG-SRV | Checkpoint, background writer, WAL | v1 Q08 | PostgreSQL 18-only |
| **Q09** | PG-DB | Table access, DML, dead tuples, vacuum, table I/O | v1 Q09 | Run on each application database |
| **Q10** | PG-DB | Index usage, index I/O, size, validity | v1 Q10 | Run on each application database |
| **Q11** | AZ-SYS | Query Store historical waits enriched with runtime and query text | W03 (replaced v1 Q11) | Must connect to database `azure_sys` |
| **Q12** | PG-SRV | Active vacuum and analyze progress | v1 Q12 | Server-wide progress views; run once from one application DB / monitoring DB |
| **Q13** | PG-SRV | Shared buffer cache summary and usage-count distribution | former M01 | PostgreSQL 18-only; requires `pg_buffercache` in a designated database |
| **Q14** | PG-DB | Top relations occupying shared buffers | former M02 | PostgreSQL 18-only; run per application database that needs relation mapping |
| **Q15** | PG-SRV | Main shared-memory allocation snapshot | former M04 | PostgreSQL 18-only |
| **EQ01** | EQ | Excluded backend memory-context logging | former M05 / former Q16 | Retired from implemented collector; retained in excluded-query registry because Azure Flexible Server customer admin may not be able to grant/execute `pg_log_backend_memory_contexts(integer)` |
| **Q16** | PG-SRV | SLRU I/O statistics | W04 | Renamed from W04 to standard Q-series |
| **Q17** | PG-CFG | Lock and deadlock logging configuration validation | W05 | Startup/config validation |
| **DICT01** | DICT | Wait event dictionary snapshot | former W06 / former Q18 | Moved out of runtime Q-series because it is metadata/reference data, not a metric |
| **Q19** | AZ-SYS | Query Store parallel-plan inventory | C04 | Must connect to database `azure_sys`; conditional / phase-boundary query |
| **PB01** | PB | PgBouncer capability and configuration snapshot | PgBouncer admin console | Former Q20 / original PB01; PgBouncer `SHOW` commands on virtual database `pgbouncer`, port `6432` |
| **PB02** | PB | PgBouncer pool runtime snapshot | PgBouncer `SHOW POOLS` | Former Q21 / original PB02 |
| **PB03** | PB | PgBouncer statistics snapshot | PgBouncer `SHOW STATS` / totals / averages | Former Q22 / original PB03 |
| **PB04** | PB | PgBouncer client diagnostic snapshot | PgBouncer `SHOW CLIENTS` | Former Q23 / original PB04; conditional |
| **PB05** | PB | PgBouncer server diagnostic snapshot | PgBouncer `SHOW SERVERS` | Former Q24 / original PB05; conditional |
| **PB06** | PB | PgBouncer internal lists, memory, and state snapshot | PgBouncer `SHOW LISTS` / `SHOW MEM` / `SHOW STATE` | Former Q25 / original PB06 |

### 2.2 Reading inline references

The implemented PostgreSQL runtime collector remains the Q-series. The Q-series is
now reserved for SQL executed against PostgreSQL databases (`PG-SRV`, `PG-DB`,
`PG-CFG`, and `AZ-SYS` categories). Non-runtime reference metadata and PgBouncer
admin-console commands have their own IDs.

Legacy references resolve as follows:

- **W01 → Q02**, **W02 → Q03**, **W03 → Q11**, **C03 → Q05**;
- parallel-worker-pool logic is folded directly into **Q02** output; there is no standalone parallel-worker collector query ID;
- legacy **M03A/M03B/M03C** and **C01** parameter coverage is folded into **Q01A–Q01D**;
- **Q13–Q15** are the implemented memory collector entries; the former backend memory-context logging entry is **EQ01**, not Q16;
- legacy **W06 / former Q18** is now **DICT01** because it is a dictionary/reference snapshot;
- legacy **Q20–Q25** are now **PB01–PB06** because PgBouncer admin-console commands are not PostgreSQL catalog SQL.

When older notes say "correlate with Q21/Q22/Q23/Q24/Q25" in a PgBouncer context,
read them as **PB02/PB03/PB04/PB05/PB06** respectively.

---

## 3. Collection Architecture

### 3.1 Collector execution target taxonomy

The collector implementation should be split by execution target. This prevents the
program from accidentally sending a PgBouncer `SHOW` command to an application
database, or sending an Azure Query Store query to the wrong database.

| Category | Meaning | Connection target | Implementation implication |
|---|---|---|---|
| **PG-SRV** | PostgreSQL server-wide or cluster-wide SQL | One application database or designated monitoring database | Result may describe all databases/backends even though the connection is opened to one database. |
| **PG-DB** | PostgreSQL database-scoped SQL | Each application database that must be analyzed | Collector may need to loop through target databases; result is tied to `current_database()`. |
| **AZ-SYS** | Azure PostgreSQL system database SQL | Database **`azure_sys`** | Dedicated connection profile. Do not run these queries from the DMS application database. |
| **PG-CFG** | PostgreSQL configuration / validation SQL | One application database, sometimes each application database for routine-local settings | Startup and pre-test validation; not high-frequency runtime polling. |
| **DICT** | Dictionary / reference SQL | Application database or platform-specific dictionary source | Metadata snapshot only; not a performance metric or counter. |
| **PB** | PgBouncer admin-console command | Virtual database **`pgbouncer`**, normally port **6432** | Separate collector path; commands are PgBouncer `SHOW` commands, not PostgreSQL catalog queries. |
| **EQ** | Excluded / retired query | Not executed by the implemented collector | Kept only to document why a query was removed or intentionally excluded. |

### 3.2 Direct PostgreSQL and Azure system-database collection paths

Azure Function terhubung langsung ke Azure Database for PostgreSQL untuk mengambil
data yang membutuhkan resolusi lebih cepat daripada Azure Monitor atau membutuhkan
detail query/session. Query Store queries are separated as **AZ-SYS** because they
must connect to database `azure_sys`.

Consolidated scheduler:

| Scheduler class | Interval | ID | Target category | Connection target |
|---|---:|---|---|---|
| Startup/configuration inventory | Function start, every 6 hours, and after restart/scale/failover/config change | Q01A–Q01D | PG-CFG / PG-DB | One application DB; Q01D per application DB if routine-local settings matter |
| Fast activity + wait + parallel snapshot | 15 seconds | Q02 | PG-SRV | One application DB / monitoring DB |
| Conditional lock/blocking diagnostic | Immediately on lock detection, then 15 seconds while blocking persists | Q03 | PG-SRV | One application DB / monitoring DB |
| Database cumulative counters | 30 seconds | Q04 | PG-SRV | One application DB / monitoring DB |
| Query cumulative counters (+ planning/JIT/parallel) | 60 seconds, plus full snapshot at each phase boundary | Q05 | PG-SRV | One DB where `pg_stat_statements` view is available |
| `pg_stat_statements` health | 5 minutes | Q06 | PG-SRV | One application DB / monitoring DB |
| Cluster I/O counters | 30 seconds | Q07 | PG-SRV | One application DB / monitoring DB |
| Checkpoint/background writer/WAL | 60 seconds | Q08 | PG-SRV | One application DB / monitoring DB |
| Table access and table I/O | 5 minutes and each phase boundary | Q09 | PG-DB | Each application DB |
| Index access and index I/O | 15 minutes and each phase boundary | Q10 | PG-DB | Each application DB |
| Query Store historical waits + runtime | 15 minutes, with 2-minute persistence lag | Q11 | AZ-SYS | `azure_sys` |
| Maintenance progress | Conditional, 30 seconds while maintenance is active | Q12 | PG-SRV | One application DB / monitoring DB |
| Shared-buffer summary | 5 minutes and each phase boundary | Q13 | PG-SRV | Designated DB with `pg_buffercache` |
| Top relation cache residency | 15 minutes, phase boundary, or conditional | Q14 | PG-DB | Each application DB needing relation mapping |
| Main shared-memory allocations | Startup, restart/scale/extension/static-config change, before/after test | Q15 | PG-SRV | One application DB / monitoring DB |
| SLRU cumulative I/O | 60 seconds | Q16 | PG-SRV | One application DB / monitoring DB |
| Wait/deadlock logging validation | Startup, 6 hours, config change | Q17 | PG-CFG | One application DB / monitoring DB |
| Wait event dictionary | Startup/version change | DICT01 | DICT | One application DB / monitoring DB |
| Query Store parallel-plan inventory | Phase boundary and conditional | Q19 | AZ-SYS | `azure_sys` |

PgBouncer commands use the separate **PB** collector path in Section 3.4.

### 3.3 Slow path — Azure Monitor metrics from Storage Account

Infrastructure metrics seperti `cpu_percent`, `memory_percent`, disk consumed
percentage, IOPS, throughput, dan storage dikirim oleh Diagnostic Settings ke
Storage Account. File yang sudah terbentuk diringkas dan dimasukkan ke metrics
collection database **setiap 2 jam**.

Konsekuensinya:

1. Azure Function fast-path tidak perlu memanggil Azure Monitor Metrics API untuk metric yang sudah diekspor ke Storage Account.
2. Azure Monitor metric mempunyai native time grain 1 menit; polling file 2 jam tidak mengubah resolusi metric, hanya menambah ingestion latency ke consolidation database.
3. Metric IOPS/throughput yang diberi tanda `^` oleh Microsoft diproses dalam batch lima menit sehingga visibility dapat terlambat sampai sekitar lima menit.
4. `postmaster_process_cpu_usage_percent` tercatat sebagai **DS Export = No**. Metric tersebut tidak akan tersedia melalui export Diagnostic Settings. Jika metric ini dianggap mandatory, diperlukan pengecualian arsitektur berupa Azure Monitor Metrics API; jika tidak, gunakan `cpu_percent` sebagai metric CPU utama dan SQL collection (Q02/Q05/Q19) untuk workload attribution.

---


### 3.4 PgBouncer admin-console collection path

PgBouncer pool metrics are collected through a separate connection to the PgBouncer
admin console, not through PostgreSQL system catalogs. For Azure Database for
PostgreSQL Flexible Server built-in PgBouncer, the collector connects to database
`pgbouncer` on port `6432` using a role listed in `pgbouncer.stats_users` or an
equivalent administrative role.

Recommended collector connection behavior:

```text
host=<server>.postgres.database.azure.com
port=6432
dbname=pgbouncer
sslmode=require
user=<pgbouncer_stats_user>
application_name=dms_metrics_collector_pgbouncer
```

Important implementation notes:

1. PgBouncer admin commands are `SHOW` commands. They are not ordinary PostgreSQL
   catalog queries and should be executed on the `pgbouncer` virtual database.
2. Use a driver mode that can issue simple-query protocol commands to the admin
   console. Validate this during smoke test; if the driver always uses extended
   query protocol, the admin console connection can fail even when normal database
   connections work.
3. Store PgBouncer samples with the same `source_collected_at` and
   `collector_received_at` convention used for PostgreSQL samples.
4. In before-after analysis, compare three layers separately:
   - application client connections to PgBouncer;
   - PgBouncer server connections to PostgreSQL;
   - PostgreSQL backend connections visible in Q02 / Azure Monitor.
5. PgBouncer collection is mandatory only for a run where PgBouncer is part of the
   tested architecture. For a direct-connection baseline, PB01–PB06 can be marked
   `not_applicable`, but Q02 and Azure Monitor connection metrics remain required.


### 3.4.1 .NET / Npgsql smoke test requirement

If the PgBouncer collector is implemented in .NET, validate the admin-console path
before building PB01–PB06. The validation is mandatory because PgBouncer admin
console commands are `SHOW` commands on the virtual database `pgbouncer`, and the
admin console only supports simple-query protocol. Some PostgreSQL drivers use
extended-query protocol for all commands; those drivers will fail against the admin
console even when normal PostgreSQL connections work.

Minimum .NET smoke test:

1. Use a dedicated connection string for the PgBouncer admin console:

```text
Host=<server>.postgres.database.azure.com;
Port=6432;
Database=pgbouncer;
Username=<pgbouncer_stats_user>;
Password=<password>;
Ssl Mode=Require;
Pooling=false;
Timeout=10;
Command Timeout=10;
Application Name=dms_metrics_collector_pgbouncer
```

2. Use raw command text such as `SHOW HELP;`, `SHOW VERSION;`, and `SHOW POOLS;`.
   Do not use Entity Framework, LINQ, prepared statements, `Prepare()`, command
   parameters, or multi-statement batching for PB01–PB06.
3. Read the returned columns dynamically by name. Do not hard-code an exact column
   order because PgBouncer output can vary by version and Azure platform behavior.
4. Treat these as distinct failure classes during smoke test:
   - network/firewall/private-endpoint or port `6432` access failure;
   - wrong database target, for example connecting to the DMS DB instead of `pgbouncer`;
   - user not listed in `pgbouncer.stats_users` or equivalent admin configuration;
   - unsupported driver/protocol behavior against the PgBouncer admin console;
   - parser failure because the result schema differs from the collector expectation.
5. If Npgsql cannot execute the admin-console `SHOW` commands successfully in the
   selected runtime/version, use a dedicated fallback for PB collection, such as a
   `psql` helper process/container or another PostgreSQL client library that is
   verified to issue simple-query protocol for these commands.

Example smoke-test code shape:

```csharp
using Npgsql;

var connectionString =
    "Host=<server>.postgres.database.azure.com;" +
    "Port=6432;" +
    "Database=pgbouncer;" +
    "Username=<pgbouncer_stats_user>;" +
    "Password=<password>;" +
    "Ssl Mode=Require;" +
    "Pooling=false;" +
    "Timeout=10;" +
    "Command Timeout=10;" +
    "Application Name=dms_metrics_collector_pgbouncer";

await using var connection = new NpgsqlConnection(connectionString);
await connection.OpenAsync();

await using var command = new NpgsqlCommand("SHOW POOLS;", connection);
await using var reader = await command.ExecuteReaderAsync();

while (await reader.ReadAsync())
{
    for (var ordinal = 0; ordinal < reader.FieldCount; ordinal++)
    {
        var columnName = reader.GetName(ordinal);
        var value = reader.GetValue(ordinal);
        // Store as raw key/value or map by column name.
    }
}
```

Reference facts for this smoke test:

- Azure built-in PgBouncer uses port `6432`, and the admin console is reached by
  connecting to database `pgbouncer` with a user listed in `pgbouncer.stats_users`.
- PgBouncer admin console `SHOW` commands expose pool, database, and statistics
  state, but they are not ordinary PostgreSQL catalog queries.
- PgBouncer admin console supports simple-query protocol only; driver protocol
  behavior must therefore be tested explicitly.

---

## 4. Measurement Principles

### 3.1 Cumulative counter must be converted to delta

View seperti berikut menyimpan counter kumulatif:

- `pg_stat_database`;
- `pg_stat_statements`;
- `pg_stat_io`;
- `pg_stat_bgwriter`;
- `pg_stat_checkpointer`;
- `pg_stat_wal`;
- `pg_stat_user_tables`;
- `pg_stat_user_indexes`;
- PgBouncer `SHOW STATS` total counters from PB03 (`total_xact_count`, `total_query_count`, `total_wait_time`, traffic counters, and related totals).

Azure Function atau downstream summarizer harus menghitung:

```text
delta_value = current_value - previous_value
rate_per_second = delta_value / elapsed_seconds
```

Jika `stats_reset` berubah, server restart terdeteksi, atau current counter lebih kecil daripada previous counter, sample tersebut menjadi awal baseline baru dan delta tidak boleh dihitung melintasi reset.

### 3.2 Use short, independent transactions

Setiap polling cycle harus menggunakan autocommit atau transaksi yang segera ditutup. PostgreSQL dapat menyimpan statistics snapshot sampai akhir transaksi; transaksi collector yang panjang dapat membuat data terlihat tidak berubah.

Recommended session settings:

```sql
SET application_name = 'dms_metrics_collector';
SET default_transaction_read_only = on;
SET statement_timeout = '10s';
SET lock_timeout = '1s';
SET idle_in_transaction_session_timeout = '15s';
```

### 3.3 Store two timestamps

Setiap sample minimal menyimpan:

- `source_collected_at`: timestamp dari `clock_timestamp()` di PostgreSQL;
- `collector_received_at`: timestamp ketika Azure Function menerima hasil.

Gunakan `source_collected_at` untuk korelasi dengan phase k6.

### 3.4 Do not reset PostgreSQL statistics during the official run

Jangan menjalankan `pg_stat_reset()` saat official performance test. Selain memutus continuity counter, PostgreSQL juga memperingatkan bahwa reset table counters dapat memengaruhi informasi yang digunakan autovacuum untuk menentukan vacuum/analyze.

### 3.5 CPU and memory limitations of SQL views

- PostgreSQL tidak menyediakan per-query CPU time dari `pg_stat_statements`; `total_exec_time` adalah elapsed execution time, bukan CPU time.
- Total server memory usage tidak dapat diambil secara akurat dari SQL system view pada managed service. `memory_percent` dari Azure Monitor adalah sumber utama.
- `pg_backend_memory_contexts` hanya memperlihatkan memory context session yang menjalankan query dan **bukan** total memory semua backend. Karena itu view tersebut tidak dimasukkan sebagai regular collector query.

---

## 5. Proposed Threshold Model

RFP meminta metric dan statistik MIN, MAX, AVG, P90, dan P99, tetapi tidak menetapkan absolute SLO. Threshold pada dokumen ini adalah **proposed engineering thresholds** untuk investigation dan production-readiness interpretation, bukan threshold resmi Microsoft atau nilai kelulusan RFP.

Severity convention:

| Severity | Meaning |
|---|---|
| Informational | Evidence untuk analisis; tidak otomatis menunjukkan bottleneck |
| Warning | Perlu korelasi dengan throughput, latency, dan phase load |
| High | Kemungkinan sudah mengurangi headroom atau menimbulkan degradation |
| Critical | Immediate bottleneck/event; harus dimasukkan ke incident timeline |

### 4.1 Infrastructure threshold summary

| Metric | Warning | High | Critical / test interpretation |
|---|---|---|---|
| `cpu_percent` | AVG ≥ 70% selama 15 menit | AVG ≥ 80% selama 15 menit | AVG ≥ 90% selama 5 menit atau continuous ≥ 90% selama 15 menit. Target sustain: AVG < 80% agar tersedia headroom. |
| `memory_percent` | ≥ 80% selama 15 menit **dan masih meningkat** | ≥ 90% selama 15 menit | ≥ 95% selama 5 menit; atau slope > 5 percentage-points/hour selama stable sustain. Memory tinggi yang plateau harus dibedakan dari memory leak. |
| Memory growth during sustain | Slope > 2 percentage-points/hour selama ≥ 60 menit | Slope > 5 percentage-points/hour | Tidak plateau sampai akhir sustain dan tetap meningkat walaupun load konstan. |
| Disk IOPS consumed % | AVG ≥ 70% selama 15 menit | AVG ≥ 80% selama 15 menit | AVG ≥ 90% selama 5 menit atau saturation berulang bersama latency/error increase. |
| Disk bandwidth consumed % | AVG ≥ 70% selama 15 menit | AVG ≥ 80% selama 15 menit | AVG ≥ 90% selama 5 menit. |
| Disk queue depth | > 2× baseline selama 10 menit | > 3× baseline bersama consumed % ≥ 80% | Terus meningkat atau tidak kembali setelah ramp-down. Tidak ada universal absolute queue-depth threshold. |
| Connection utilization | ≥ 70% max_connections selama 5 menit | ≥ 85% selama 5 menit | ≥ 95% atau failed connection meningkat. |
| Cache hit ratio | < 99% selama 15 menit pada workload OLTP yang cukup besar | < 97% selama 15 menit | < 95% dan bersamaan dengan I/O pressure. Workload-dependent; bukan universal pass/fail. |
| Deadlock | — | Setiap delta > 0 | Critical event untuk RFP evidence; capture exact timestamp dan query pair. |
| Lock wait | Active lock wait > 5 detik | > 30 detik | > 60 detik atau blocking chain bertambah. |
| Idle in transaction | > 60 detik | > 300 detik | Menahan vacuum/lock dan terus muncul pada sustain. |
| Long active query | > 5 detik | > 30 detik | > 60 detik pada OLTP path, kecuali memang batch/reporting yang disetujui. |
| Temp spill | > 100 MB/min per database | > 1 GB/min | Terus meningkat dan berkorelasi dengan memory wait/I/O pressure. |

### 4.2 Final 8-hour report interpretation

Untuk setiap sustain phase, khususnya 2000 concurrent users:

- laporkan AVG, MAX, P90, dan P99 CPU/memory/I/O;
- laporkan durasi total dan longest continuous period di atas 70%, 80%, dan 90%;
- jangan hanya melaporkan overall 8-hour average;
- tandai ramp-up, sustain, scaling event, ramp-down, dan 30–60 minute cool-down;
- memory dianggap sehat jika naik saat load naik lalu plateau; tidak harus kembali persis ke baseline karena PostgreSQL/OS cache dapat dipertahankan.


### 5.1 PgBouncer threshold summary

PgBouncer thresholds below are **engineering investigation thresholds** for pooler
behavior. They are not official RFP pass/fail thresholds. Interpret them together
with k6 latency/error, Q02 PostgreSQL backend connections, Azure CPU/memory, and
Azure connection-failure metrics.

| Metric | Warning | High | Critical / test interpretation |
|---|---|---|---|
| `cl_waiting` from PB02 | > 0 for two consecutive 15-second samples | > 0 for 5 minutes or during every ramp-up step | Any waiting together with k6 timeout/error spike, or queue does not clear during sustain. |
| `maxwait` from PB02 | > 1 second | > 5 seconds | > 30 seconds, or approaches `pgbouncer.query_wait_timeout`. |
| Server pool not-immediately-idle percent | ≥ 80% for 5 minutes | ≥ 90% for 5 minutes | ≥ 95% with `cl_waiting > 0`, meaning pool capacity is probably exhausted or PostgreSQL is not returning connections fast enough. |
| `sv_idle = 0` with `cl_waiting > 0` | Warning if repeated | High if sustained for 5 minutes | Critical if paired with rising GraphQL P99/error rate. |
| PgBouncer `avg_wait_time` from PB03, normalized to milliseconds | > 5 ms for 5 minutes | > 20 ms for 5 minutes | > 100 ms or sustained increase across same-load baseline/optimized comparison. |
| PgBouncer client count vs `max_client_conn` | ≥ 70% | ≥ 85% | ≥ 95% or login failures appear. |
| Server connections to PostgreSQL after PgBouncer | Informational | High if not materially lower than direct-connection baseline | PgBouncer is not providing effective connection shielding if PostgreSQL backend count remains close to app client connection count. |
| `paused` / `disabled` from PB01 | — | — | Any non-zero value during official run is critical unless it is an intentional test event. |
| PgBouncer restart/failover event | Informational if planned and marked | High if unplanned | Critical if it causes sustained app errors or connection storm. |

For before-after PgBouncer success, the target pattern is:

- PostgreSQL backend connection count decreases materially compared with direct
  connection baseline;
- PgBouncer `cl_waiting` remains zero or near-zero during steady sustain;
- PgBouncer `maxwait` remains near zero;
- k6 throughput and P90/P99 do not regress materially;
- DB CPU/memory and connection utilization improve or at least remain stable.


---

## 6. PostgreSQL Query Specifications

All session settings, delta handling, and the no-reset rule in Section 4 apply to
every query below. Query comment tags use the operational collector ID where the query has been normalized into the Q-series.

### 6.A Core collection queries (Q01–Q12)

## Q01 — Configuration, Capability, and Override Inventory

Q01 is the only startup/configuration inventory group. It replaces the older
layout where Q01 core, M03A/M03B/M03C, and C01 were shown as separate named
components.

Implementation rule:

- **Q01A** reads capability/version/extension availability.
- **Q01B** reads effective server/session settings from `pg_settings` using one
  unified parameter list: Q01 core + memory/worker parameters from legacy M03 +
  CPU/planner/parallel/JIT parameters from legacy C01.
- **Q01C** reads database, role, and role-database overrides from
  `pg_db_role_setting` using the same unified parameter list.
- **Q01D** reads function/procedure-local `SET` overrides from `pg_proc.proconfig`
  using the same unified parameter list.

Do not run legacy M03A/M03B/M03C or C01 as separate collector query IDs. Their
parameter coverage is preserved inside Q01B/Q01C/Q01D.

### Purpose

Menentukan versi server, feature availability, extension availability, effective
configuration, database/role overrides, dan routine-local overrides yang
memengaruhi validitas collector dan interpretasi performance test.

Q01 digunakan untuk menjawab:

- apakah view/extension yang dibutuhkan collector tersedia;
- apakah setting observability dasar aktif;
- apakah setting memory, worker, CPU planner cost, parallel query, JIT, dan
  `pg_stat_statements` sudah sesuai;
- apakah ada override di level database, role, role-database, function, atau
  procedure yang membuat application role berbeda dari nilai yang dilihat oleh
  collector.

### Frequency

- saat Azure Function start;
- setiap **6 jam**;
- segera setelah database restart, scale, failover, parameter change,
  application-role configuration change, atau deployment function/procedure;
- tepat sebelum baseline dan optimized official run.

### Connection target

Salah satu application database. Jalankan Q01D pada setiap application database
jika function/procedure-local setting perlu dicapture per database.

### Unified parameter list

The unified parameter list below is used by Q01B, Q01C, and Q01D. It intentionally
combines the older Q01, M03, and C01 parameter coverage so the collector does not
query `pg_settings` or `pg_db_role_setting` twice for overlapping information.

```text
track_activities
track_counts
track_io_timing
track_wal_io_timing
track_activity_query_size
compute_query_id
pg_stat_statements.max
pg_stat_statements.track
pg_stat_statements.track_planning
pg_stat_statements.save
pg_qs.query_capture_mode
pg_qs.interval_length_minutes
pg_qs.max_captured_queries
pgms_wait_sampling.query_capture_mode
pgms_wait_sampling.history_period
metrics.collector_database_activity
metrics.autovacuum_diagnostics
max_connections
shared_buffers
effective_cache_size
work_mem
hash_mem_multiplier
temp_buffers
maintenance_work_mem
autovacuum_work_mem
logical_decoding_work_mem
temp_file_limit
log_temp_files
max_worker_processes
max_parallel_workers
max_parallel_workers_per_gather
max_parallel_maintenance_workers
autovacuum_max_workers
max_prepared_transactions
huge_pages
huge_page_size
parallel_leader_participation
parallel_setup_cost
parallel_tuple_cost
min_parallel_table_scan_size
min_parallel_index_scan_size
cpu_tuple_cost
cpu_index_tuple_cost
cpu_operator_cost
seq_page_cost
random_page_cost
jit
jit_above_cost
jit_inline_above_cost
jit_optimize_above_cost
enable_gathermerge
enable_parallel_append
enable_parallel_hash
```

### SQL statement Q01A — capability, version, and extension snapshot

```sql
/* dms_metrics_collector:q01a_capability */
SELECT
    clock_timestamp() AS collected_at,
    current_database() AS database_name,
    current_user AS collector_user,
    current_setting('server_version_num')::integer AS server_version_num,
    version() AS server_version,
    EXISTS (
        SELECT 1
        FROM pg_extension
        WHERE extname = 'pg_stat_statements'
    ) AS has_pg_stat_statements_extension,
    to_regclass('public.pg_stat_statements') IS NOT NULL
        AS has_pg_stat_statements_view,
    to_regclass('public.pg_stat_statements_info') IS NOT NULL
        AS has_pg_stat_statements_info_view,
    to_regclass('pg_catalog.pg_stat_io') IS NOT NULL
        AS has_pg_stat_io,
    to_regclass('pg_catalog.pg_stat_checkpointer') IS NOT NULL
        AS has_pg_stat_checkpointer;
```

### SQL statement Q01B — effective server/session settings

Q01B is the replacement for the older Q01 core settings probe, M03A, and the
`server_settings` portion of C01. This statement should normally return rows from
`pg_settings`.

```sql
/* dms_metrics_collector:q01b_effective_settings */
SELECT
    clock_timestamp() AS collected_at,
    'server_effective_for_collector'::text AS setting_scope,
    current_database()::text AS database_name,
    current_user::text AS role_name,
    NULL::text AS object_identity,
    s.name AS parameter_name,
    s.setting AS parameter_value,
    s.unit,
    s.source,
    s.context,
    s.pending_restart
FROM pg_settings s
WHERE s.name IN (
    'track_activities',
    'track_counts',
    'track_io_timing',
    'track_wal_io_timing',
    'track_activity_query_size',
    'compute_query_id',
    'pg_stat_statements.max',
    'pg_stat_statements.track',
    'pg_stat_statements.track_planning',
    'pg_stat_statements.save',
    'pg_qs.query_capture_mode',
    'pg_qs.interval_length_minutes',
    'pg_qs.max_captured_queries',
    'pgms_wait_sampling.query_capture_mode',
    'pgms_wait_sampling.history_period',
    'metrics.collector_database_activity',
    'metrics.autovacuum_diagnostics',
    'max_connections',
    'shared_buffers',
    'effective_cache_size',
    'work_mem',
    'hash_mem_multiplier',
    'temp_buffers',
    'maintenance_work_mem',
    'autovacuum_work_mem',
    'logical_decoding_work_mem',
    'temp_file_limit',
    'log_temp_files',
    'max_worker_processes',
    'max_parallel_workers',
    'max_parallel_workers_per_gather',
    'max_parallel_maintenance_workers',
    'autovacuum_max_workers',
    'max_prepared_transactions',
    'huge_pages',
    'huge_page_size',
    'parallel_leader_participation',
    'parallel_setup_cost',
    'parallel_tuple_cost',
    'min_parallel_table_scan_size',
    'min_parallel_index_scan_size',
    'cpu_tuple_cost',
    'cpu_index_tuple_cost',
    'cpu_operator_cost',
    'seq_page_cost',
    'random_page_cost',
    'jit',
    'jit_above_cost',
    'jit_inline_above_cost',
    'jit_optimize_above_cost',
    'enable_gathermerge',
    'enable_parallel_append',
    'enable_parallel_hash'
)
ORDER BY parameter_name;
```

### SQL statement Q01C — database, role, and role-database overrides

Q01C is the replacement for older M03B and the `db_role_settings` portion of C01.
It reads only override rows. A zero-row result is valid evidence and means no
matching `ALTER DATABASE`, `ALTER ROLE`, or role-database override was detected.

```sql
/* dms_metrics_collector:q01c_db_role_overrides */
SELECT
    clock_timestamp() AS collected_at,
    CASE
        WHEN drs.setdatabase = 0 AND drs.setrole = 0
            THEN 'all_database_all_role'
        WHEN drs.setdatabase = 0
            THEN 'all_database_specific_role'
        WHEN drs.setrole = 0
            THEN 'specific_database_all_role'
        ELSE 'specific_database_specific_role'
    END AS setting_scope,
    CASE
        WHEN drs.setdatabase = 0 THEN 'ALL DATABASES'
        ELSE d.datname::text
    END AS database_name,
    CASE
        WHEN drs.setrole = 0 THEN 'ALL ROLES'
        ELSE r.rolname::text
    END AS role_name,
    NULL::text AS object_identity,
    split_part(x.cfg, '=', 1) AS parameter_name,
    substring(x.cfg FROM position('=' IN x.cfg) + 1) AS parameter_value,
    ps.unit,
    'pg_db_role_setting'::text AS source,
    ps.context,
    NULL::boolean AS pending_restart
FROM pg_db_role_setting drs
LEFT JOIN pg_database d
  ON d.oid = drs.setdatabase
LEFT JOIN pg_roles r
  ON r.oid = drs.setrole
CROSS JOIN LATERAL unnest(drs.setconfig) AS x(cfg)
LEFT JOIN pg_settings ps
  ON ps.name = split_part(x.cfg, '=', 1)
WHERE position('=' IN x.cfg) > 0
  AND split_part(x.cfg, '=', 1) IN (
      'track_activities',
      'track_counts',
      'track_io_timing',
      'track_wal_io_timing',
      'track_activity_query_size',
      'compute_query_id',
      'pg_stat_statements.max',
      'pg_stat_statements.track',
      'pg_stat_statements.track_planning',
      'pg_stat_statements.save',
      'pg_qs.query_capture_mode',
      'pg_qs.interval_length_minutes',
      'pg_qs.max_captured_queries',
      'pgms_wait_sampling.query_capture_mode',
      'pgms_wait_sampling.history_period',
      'metrics.collector_database_activity',
      'metrics.autovacuum_diagnostics',
      'max_connections',
      'shared_buffers',
      'effective_cache_size',
      'work_mem',
      'hash_mem_multiplier',
      'temp_buffers',
      'maintenance_work_mem',
      'autovacuum_work_mem',
      'logical_decoding_work_mem',
      'temp_file_limit',
      'log_temp_files',
      'max_worker_processes',
      'max_parallel_workers',
      'max_parallel_workers_per_gather',
      'max_parallel_maintenance_workers',
      'autovacuum_max_workers',
      'max_prepared_transactions',
      'huge_pages',
      'huge_page_size',
      'parallel_leader_participation',
      'parallel_setup_cost',
      'parallel_tuple_cost',
      'min_parallel_table_scan_size',
      'min_parallel_index_scan_size',
      'cpu_tuple_cost',
      'cpu_index_tuple_cost',
      'cpu_operator_cost',
      'seq_page_cost',
      'random_page_cost',
      'jit',
      'jit_above_cost',
      'jit_inline_above_cost',
      'jit_optimize_above_cost',
      'enable_gathermerge',
      'enable_parallel_append',
      'enable_parallel_hash'
  )
ORDER BY
    parameter_name,
    setting_scope,
    database_name,
    role_name;
```

### SQL statement Q01D — function/procedure-local settings

Q01D is the replacement for older M03C. It also checks the CPU/planner/parallel/JIT
parameters that were unique to C01, because function/procedure-local `SET` can
change execution behavior inside routine calls. A zero-row result is valid evidence
and means no matching routine-local override was detected in the current database.

```sql
/* dms_metrics_collector:q01d_routine_local_settings */
SELECT
    clock_timestamp() AS collected_at,
    'routine_local_setting'::text AS setting_scope,
    current_database()::text AS database_name,
    owner_role.rolname::text AS role_name,
    pr.oid::regprocedure::text AS object_identity,
    split_part(x.cfg, '=', 1) AS parameter_name,
    substring(x.cfg FROM position('=' IN x.cfg) + 1) AS parameter_value,
    ps.unit,
    'pg_proc.proconfig'::text AS source,
    ps.context,
    NULL::boolean AS pending_restart
FROM pg_proc pr
JOIN pg_namespace n
  ON n.oid = pr.pronamespace
JOIN pg_roles owner_role
  ON owner_role.oid = pr.proowner
CROSS JOIN LATERAL unnest(pr.proconfig) AS x(cfg)
LEFT JOIN pg_settings ps
  ON ps.name = split_part(x.cfg, '=', 1)
WHERE pr.proconfig IS NOT NULL
  AND position('=' IN x.cfg) > 0
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname NOT LIKE 'pg_toast%'
  AND n.nspname NOT LIKE 'pg_temp_%'
  AND split_part(x.cfg, '=', 1) IN (
      'track_activities',
      'track_counts',
      'track_io_timing',
      'track_wal_io_timing',
      'track_activity_query_size',
      'compute_query_id',
      'pg_stat_statements.max',
      'pg_stat_statements.track',
      'pg_stat_statements.track_planning',
      'pg_stat_statements.save',
      'pg_qs.query_capture_mode',
      'pg_qs.interval_length_minutes',
      'pg_qs.max_captured_queries',
      'pgms_wait_sampling.query_capture_mode',
      'pgms_wait_sampling.history_period',
      'metrics.collector_database_activity',
      'metrics.autovacuum_diagnostics',
      'max_connections',
      'shared_buffers',
      'effective_cache_size',
      'work_mem',
      'hash_mem_multiplier',
      'temp_buffers',
      'maintenance_work_mem',
      'autovacuum_work_mem',
      'logical_decoding_work_mem',
      'temp_file_limit',
      'log_temp_files',
      'max_worker_processes',
      'max_parallel_workers',
      'max_parallel_workers_per_gather',
      'max_parallel_maintenance_workers',
      'autovacuum_max_workers',
      'max_prepared_transactions',
      'huge_pages',
      'huge_page_size',
      'parallel_leader_participation',
      'parallel_setup_cost',
      'parallel_tuple_cost',
      'min_parallel_table_scan_size',
      'min_parallel_index_scan_size',
      'cpu_tuple_cost',
      'cpu_index_tuple_cost',
      'cpu_operator_cost',
      'seq_page_cost',
      'random_page_cost',
      'jit',
      'jit_above_cost',
      'jit_inline_above_cost',
      'jit_optimize_above_cost',
      'enable_gathermerge',
      'enable_parallel_append',
      'enable_parallel_hash'
  )
ORDER BY
    parameter_name,
    setting_scope,
    database_name,
    role_name,
    object_identity;
```

### Q01 implementation notes

- Store Q01B, Q01C, and Q01D in one logical configuration table or dataset using
  the common columns `setting_scope`, `database_name`, `role_name`,
  `object_identity`, `parameter_name`, `parameter_value`, `unit`, `source`,
  `context`, and `pending_restart`.
- Q01B should normally return rows. Missing expected parameters are data-quality
  signals and may indicate PostgreSQL version/platform differences.
- Q01C and Q01D may legitimately return zero rows. Zero rows means no matching
  override was detected from that source; it does **not** mean Q01 failed.
- Do not create database objects named `memory_parameters`, `target_parameters`,
  `db_role_settings`, or `routine_settings`. Those were CTE names in older
  draft implementations only.
- Do not reintroduce standalone C01, M03A, M03B, or M03C collector query IDs.

### Threshold / validation

Critical pre-test validation failure if:

- `track_activities = off`;
- `track_counts = off`;
- `pg_stat_statements` is required but unavailable;
- `track_io_timing = off` while block read/write time is required;
- `pg_qs.query_capture_mode = none` or wait sampling is disabled while Query Store historical waits are required;
- monitoring role does not have visibility to other sessions.

Configuration review triggers:

- Any database/role/routine override for memory, worker, parallel, JIT, planner
  cost, or `pg_stat_statements` setting is informational evidence and must be
  reviewed.
- Override `work_mem`, `temp_buffers`, `maintenance_work_mem`,
  `autovacuum_work_mem`, or `logical_decoding_work_mem` > 2× server default:
  warning; > 4×: high.
- High-risk combination: application `work_mem` override > 2× default,
  `hash_mem_multiplier > 1`, and `max_parallel_workers_per_gather > 0`,
  especially when concurrent active sessions are high.
- `max_parallel_workers > max_worker_processes`: high configuration issue because
  the excess value cannot be used.
- `max_parallel_workers_per_gather > max_parallel_workers`: warning configuration
  inconsistency.
- `max_parallel_workers_per_gather = 0`: informational; parallel query is disabled.
- `max_parallel_workers_per_gather >= 4` on a high-concurrency OLTP workload:
  warning review trigger, not automatic fault. Validate through Q02/Q05 and k6
  results.
- Application override that reduces `parallel_setup_cost`, `parallel_tuple_cost`,
  `min_parallel_table_scan_size`, or `min_parallel_index_scan_size` to less than
  50% of server setting: warning for potentially over-aggressive parallel
  planning.
- `jit = on` with `jit_above_cost` substantially below the approved baseline:
  warning because short queries may incur compilation overhead.
- `pg_stat_statements.track_planning = on`: informational but requires overhead
  validation. Treat as warning if enabled during high-concurrency official test
  without a controlled comparison, because planning-stat collection can add
  contention.
- `track_io_timing = off`: data-quality warning because CPU-vs-I/O attribution
  becomes weaker.
- `log_temp_files = -1`: warning evidence gap.
- `log_temp_files = 0` on a high-throughput test: warning for potential logging
  overhead; perform a controlled overhead check before the official run.
- Any unexpected `pending_restart = true` immediately before a test: high
  configuration-control issue.

---

## Q02 — Unified Activity, Wait, Connection, Long Query, Long Transaction, and Parallel-Worker Snapshot

### Purpose

Q02 adalah satu query operasional untuk mengambil satu snapshot `pg_stat_activity`
setiap 15 detik. Query ini menghasilkan satu row per sample dan mencakup:

- connection and session-state summary;
- active wait count per `wait_event_type`;
- active wait count per specific `wait_event`;
- live wait attribution per `query_id`, database, dan application;
- wait background process seperti checkpointer, WAL writer, background writer, autovacuum, dan parallel worker;
- long-running query dan long-running transaction;
- selected session detail untuk root-cause investigation;
- parallel-worker pool summary;
- worker count per parallel-query leader;
- parallel-worker wait distribution;
- parallel-query group detail.

Q02 menggantikan kebutuhan query live activity/wait dan parallel-worker snapshot yang sebelumnya ditulis terpisah. Collector tidak perlu menjalankan query parallel-worker terpisah pada cadence 15 detik.

### Frequency

Setiap **15 detik** selama performance test dan cool-down.

### Connection target

Salah satu application database. `pg_stat_activity` bersifat server-wide.

### SQL

```sql
/* dms_metrics_collector:q02 */
WITH a AS MATERIALIZED (
    SELECT
        datid,
        datname,
        pid,
        leader_pid,
        usesysid,
        usename,
        application_name,
        client_addr,
        backend_start,
        xact_start,
        query_start,
        state_change,
        wait_event_type,
        wait_event,
        state,
        backend_xid,
        backend_xmin,
        query_id,
        backend_type,
        CASE
            WHEN state = 'active' AND query_start IS NOT NULL
            THEN EXTRACT(EPOCH FROM (clock_timestamp() - query_start))
        END AS active_query_age_seconds,
        CASE
            WHEN xact_start IS NOT NULL
            THEN EXTRACT(EPOCH FROM (clock_timestamp() - xact_start))
        END AS transaction_age_seconds,
        CASE
            WHEN state LIKE 'idle in transaction%' AND state_change IS NOT NULL
            THEN EXTRACT(EPOCH FROM (clock_timestamp() - state_change))
        END AS idle_in_transaction_age_seconds,
        LEFT(query, 2000) AS query_text
    FROM pg_stat_activity
    WHERE pid <> pg_backend_pid()
      AND application_name IS DISTINCT FROM 'dms_metrics_collector'
),
settings AS (
    SELECT
        current_setting('max_connections')::integer AS max_connections,
        current_setting('max_worker_processes')::integer AS max_worker_processes,
        current_setting('max_parallel_workers')::integer AS max_parallel_workers,
        current_setting('max_parallel_workers_per_gather')::integer
            AS max_parallel_workers_per_gather
),
state_counts AS (
    SELECT COALESCE(state, 'unknown') AS key, COUNT(*) AS value
    FROM a
    WHERE backend_type = 'client backend'
    GROUP BY COALESCE(state, 'unknown')
),
wait_type_counts AS (
    SELECT
        COALESCE(wait_event_type, 'CPU_or_not_waiting') AS wait_event_type,
        COUNT(*) AS session_count
    FROM a
    WHERE backend_type = 'client backend'
      AND state = 'active'
    GROUP BY COALESCE(wait_event_type, 'CPU_or_not_waiting')
),
wait_event_counts AS (
    SELECT
        wait_event_type,
        wait_event,
        COUNT(*) AS session_count,
        MAX(active_query_age_seconds) AS longest_query_age_seconds
    FROM a
    WHERE backend_type = 'client backend'
      AND state = 'active'
      AND wait_event_type IS NOT NULL
    GROUP BY wait_event_type, wait_event
),
wait_query_counts AS (
    SELECT
        datid,
        datname,
        application_name,
        query_id,
        wait_event_type,
        wait_event,
        COUNT(*) AS session_count,
        MAX(active_query_age_seconds) AS longest_query_age_seconds
    FROM a
    WHERE backend_type = 'client backend'
      AND state = 'active'
      AND wait_event_type IS NOT NULL
    GROUP BY
        datid,
        datname,
        application_name,
        query_id,
        wait_event_type,
        wait_event
),
backend_counts AS (
    SELECT COALESCE(backend_type, 'unknown') AS key, COUNT(*) AS value
    FROM a
    GROUP BY COALESCE(backend_type, 'unknown')
),
system_backend_wait_counts AS (
    SELECT
        backend_type,
        wait_event_type,
        wait_event,
        COUNT(*) AS process_count
    FROM a
    WHERE backend_type <> 'client backend'
      AND wait_event_type IS NOT NULL
    GROUP BY backend_type, wait_event_type, wait_event
),
worker_groups AS (
    SELECT
        leader_pid,
        COUNT(*) AS launched_worker_count,
        COUNT(*) FILTER (WHERE wait_event_type IS NULL)
            AS workers_running_or_not_waiting,
        COUNT(*) FILTER (WHERE wait_event_type IS NOT NULL)
            AS workers_waiting,
        COUNT(*) FILTER (WHERE wait_event_type = 'IO')
            AS workers_waiting_io,
        COUNT(*) FILTER (WHERE wait_event_type = 'LWLock')
            AS workers_waiting_lwlock,
        COUNT(*) FILTER (WHERE wait_event_type = 'Lock')
            AS workers_waiting_lock,
        COUNT(*) FILTER (WHERE wait_event_type = 'IPC')
            AS workers_waiting_ipc,
        COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'worker_pid', pid,
                    'state', state,
                    'wait_event_type', wait_event_type,
                    'wait_event', wait_event
                )
                ORDER BY pid
            ),
            '[]'::jsonb
        ) AS workers
    FROM a
    WHERE backend_type = 'parallel worker'
      AND leader_pid IS NOT NULL
    GROUP BY leader_pid
),
leader_groups AS (
    SELECT
        l.pid AS leader_pid,
        l.datid,
        l.datname,
        l.usename,
        l.application_name,
        l.state AS leader_state,
        l.wait_event_type AS leader_wait_event_type,
        l.wait_event AS leader_wait_event,
        l.query_id,
        EXTRACT(EPOCH FROM (clock_timestamp() - l.query_start))
            AS query_age_seconds,
        COALESCE(w.launched_worker_count, 0) AS launched_worker_count,
        COALESCE(w.workers_running_or_not_waiting, 0)
            AS workers_running_or_not_waiting,
        COALESCE(w.workers_waiting, 0) AS workers_waiting,
        COALESCE(w.workers_waiting_io, 0) AS workers_waiting_io,
        COALESCE(w.workers_waiting_lwlock, 0) AS workers_waiting_lwlock,
        COALESCE(w.workers_waiting_lock, 0) AS workers_waiting_lock,
        COALESCE(w.workers_waiting_ipc, 0) AS workers_waiting_ipc,
        COALESCE(w.workers, '[]'::jsonb) AS workers,
        l.query_text
    FROM worker_groups w
    JOIN a l
      ON l.pid = w.leader_pid
)
SELECT
    clock_timestamp() AS collected_at,
    s.max_connections,
    COUNT(*) FILTER (WHERE a.backend_type = 'client backend')
        AS client_connections,
    COUNT(*) FILTER (
        WHERE a.backend_type = 'client backend' AND a.state = 'active'
    ) AS active_client_connections,
    COUNT(*) FILTER (
        WHERE a.backend_type = 'client backend' AND a.state = 'idle'
    ) AS idle_client_connections,
    COUNT(*) FILTER (
        WHERE a.backend_type = 'client backend'
          AND a.state LIKE 'idle in transaction%'
    ) AS idle_in_transaction_connections,
    COUNT(*) FILTER (
        WHERE a.backend_type = 'client backend'
          AND a.state = 'active'
          AND a.wait_event_type IS NOT NULL
    ) AS active_waiting_sessions,
    COUNT(*) FILTER (
        WHERE a.backend_type = 'client backend'
          AND a.state = 'active'
          AND a.wait_event_type = 'Lock'
    ) AS active_lock_waiting_sessions,
    COUNT(*) FILTER (
        WHERE a.backend_type = 'client backend'
          AND a.state = 'active'
          AND a.wait_event_type = 'IO'
    ) AS active_io_waiting_sessions,
    COUNT(*) FILTER (
        WHERE a.backend_type = 'client backend'
          AND a.state = 'active'
          AND a.wait_event_type = 'LWLock'
    ) AS active_lwlock_waiting_sessions,
    COUNT(*) FILTER (
        WHERE a.backend_type = 'client backend'
          AND a.state = 'active'
          AND a.wait_event_type = 'BufferPin'
    ) AS active_bufferpin_waiting_sessions,
    COUNT(*) FILTER (
        WHERE a.backend_type = 'client backend'
          AND a.state = 'active'
          AND a.wait_event_type IS NULL
    ) AS active_cpu_or_running_sessions,
    MAX(a.active_query_age_seconds) AS longest_active_query_seconds,
    MAX(a.transaction_age_seconds) AS longest_transaction_seconds,
    MAX(a.idle_in_transaction_age_seconds)
        AS longest_idle_in_transaction_seconds,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE a.backend_type = 'client backend')
        / NULLIF(s.max_connections::numeric, 0),
        2
    ) AS connection_utilization_percent,
    (SELECT COUNT(*) FROM wait_query_counts) AS wait_query_group_count,
    COALESCE(
        (SELECT jsonb_object_agg(key, value) FROM state_counts),
        '{}'::jsonb
    ) AS sessions_by_state,
    COALESCE(
        (
            SELECT jsonb_object_agg(wait_event_type, session_count)
            FROM wait_type_counts
        ),
        '{}'::jsonb
    ) AS active_sessions_by_wait_type,
    COALESCE(
        (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'wait_event_type', wait_event_type,
                    'wait_event', wait_event,
                    'session_count', session_count,
                    'longest_query_age_seconds', longest_query_age_seconds
                )
                ORDER BY session_count DESC, wait_event_type, wait_event
            )
            FROM wait_event_counts
        ),
        '[]'::jsonb
    ) AS active_sessions_by_wait_event,
    COALESCE(
        (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'datid', q.datid,
                    'datname', q.datname,
                    'application_name', q.application_name,
                    'query_id', q.query_id,
                    'wait_event_type', q.wait_event_type,
                    'wait_event', q.wait_event,
                    'session_count', q.session_count,
                    'longest_query_age_seconds', q.longest_query_age_seconds
                )
                ORDER BY q.session_count DESC,
                         q.longest_query_age_seconds DESC NULLS LAST
            )
            FROM (
                SELECT *
                FROM wait_query_counts
                ORDER BY session_count DESC,
                         longest_query_age_seconds DESC NULLS LAST
                LIMIT 500
            ) q
        ),
        '[]'::jsonb
    ) AS active_waits_by_query_event,
    COALESCE(
        (SELECT jsonb_object_agg(key, value) FROM backend_counts),
        '{}'::jsonb
    ) AS sessions_by_backend_type,
    COALESCE(
        (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'backend_type', backend_type,
                    'wait_event_type', wait_event_type,
                    'wait_event', wait_event,
                    'process_count', process_count
                )
                ORDER BY backend_type, wait_event_type, wait_event
            )
            FROM system_backend_wait_counts
        ),
        '[]'::jsonb
    ) AS system_backend_waits,
    COALESCE(
        (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'datname', d.datname,
                    'pid', d.pid,
                    'leader_pid', d.leader_pid,
                    'usename', d.usename,
                    'application_name', d.application_name,
                    'client_addr', d.client_addr,
                    'backend_type', d.backend_type,
                    'state', d.state,
                    'wait_event_type', d.wait_event_type,
                    'wait_event', d.wait_event,
                    'query_id', d.query_id,
                    'active_query_age_seconds', d.active_query_age_seconds,
                    'transaction_age_seconds', d.transaction_age_seconds,
                    'idle_in_transaction_age_seconds',
                        d.idle_in_transaction_age_seconds,
                    'query_text', d.query_text
                )
                ORDER BY
                    COALESCE(
                        d.active_query_age_seconds,
                        d.transaction_age_seconds,
                        0
                    ) DESC
            )
            FROM (
                SELECT *
                FROM a
                WHERE state LIKE 'idle in transaction%'
                   OR (
                        backend_type = 'client backend'
                        AND state = 'active'
                        AND (
                            wait_event_type IS NOT NULL
                            OR active_query_age_seconds >= 2
                        )
                   )
                   OR backend_type IN (
                        'autovacuum worker',
                        'parallel worker',
                        'checkpointer',
                        'background writer',
                        'walwriter',
                        'startup'
                   )
                ORDER BY
                    COALESCE(
                        active_query_age_seconds,
                        transaction_age_seconds,
                        0
                    ) DESC
                LIMIT 200
            ) d
        ),
        '[]'::jsonb
    ) AS interesting_sessions,
    s.max_worker_processes,
    s.max_parallel_workers,
    s.max_parallel_workers_per_gather,
    COUNT(*) FILTER (WHERE a.backend_type = 'parallel worker')
        AS active_parallel_workers,
    COUNT(*) FILTER (
        WHERE a.backend_type = 'parallel worker'
          AND a.wait_event_type IS NULL
    ) AS parallel_workers_running_or_not_waiting,
    COUNT(*) FILTER (
        WHERE a.backend_type = 'parallel worker'
          AND a.wait_event_type IS NOT NULL
    ) AS parallel_workers_waiting,
    COUNT(DISTINCT a.leader_pid) FILTER (
        WHERE a.backend_type = 'parallel worker'
          AND a.leader_pid IS NOT NULL
    ) AS active_parallel_query_groups,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE a.backend_type = 'parallel worker')
        / NULLIF(s.max_parallel_workers::numeric, 0),
        2
    ) AS parallel_worker_pool_utilization_percent,
    COALESCE(
        (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'leader_pid', g.leader_pid,
                    'datid', g.datid,
                    'datname', g.datname,
                    'usename', g.usename,
                    'application_name', g.application_name,
                    'query_id', g.query_id,
                    'query_age_seconds', g.query_age_seconds,
                    'leader_state', g.leader_state,
                    'leader_wait_event_type', g.leader_wait_event_type,
                    'leader_wait_event', g.leader_wait_event,
                    'launched_worker_count', g.launched_worker_count,
                    'workers_running_or_not_waiting',
                        g.workers_running_or_not_waiting,
                    'workers_waiting', g.workers_waiting,
                    'workers_waiting_io', g.workers_waiting_io,
                    'workers_waiting_lwlock', g.workers_waiting_lwlock,
                    'workers_waiting_lock', g.workers_waiting_lock,
                    'workers_waiting_ipc', g.workers_waiting_ipc,
                    'workers', g.workers,
                    'query_text', g.query_text
                )
                ORDER BY
                    g.launched_worker_count DESC,
                    g.query_age_seconds DESC NULLS LAST
            )
            FROM leader_groups g
        ),
        '[]'::jsonb
    ) AS parallel_query_groups
FROM settings s
LEFT JOIN a ON TRUE
GROUP BY
    s.max_connections,
    s.max_worker_processes,
    s.max_parallel_workers,
    s.max_parallel_workers_per_gather;
```

### Output columns that must be stored

Q02 returns one row per sample. Downstream storage should preserve these top-level columns at minimum:

- `collected_at`, `max_connections`, `client_connections`, `active_client_connections`, `idle_client_connections`, `idle_in_transaction_connections`, `connection_utilization_percent`;
- `active_waiting_sessions`, `active_lock_waiting_sessions`, `active_io_waiting_sessions`, `active_lwlock_waiting_sessions`, `active_bufferpin_waiting_sessions`, `active_cpu_or_running_sessions`;
- `longest_active_query_seconds`, `longest_transaction_seconds`, `longest_idle_in_transaction_seconds`;
- `sessions_by_state`, `active_sessions_by_wait_type`, `active_sessions_by_wait_event`, `active_waits_by_query_event`, `sessions_by_backend_type`, `system_backend_waits`, `interesting_sessions`;
- `max_worker_processes`, `max_parallel_workers`, `max_parallel_workers_per_gather`, `active_parallel_workers`, `parallel_workers_running_or_not_waiting`, `parallel_workers_waiting`, `active_parallel_query_groups`, `parallel_worker_pool_utilization_percent`, `parallel_query_groups`.

### Derived metrics

For each 15-second sample:

```text
wait_type_share_percent =
    100 * sessions_waiting_for_type / active_client_connections

wait_event_share_percent =
    100 * sessions_waiting_for_event / active_client_connections

sampled_wait_occupancy_ms =
    session_count * actual_sampling_interval_ms

parallel_worker_wait_share_percent =
    100 * parallel_workers_waiting / active_parallel_workers

parallel_running_demand =
    parallel_workers_running_or_not_waiting
    + active non-worker parallel-query leaders with no reported wait
```

`sampled_wait_occupancy_ms` adalah **session occupancy estimate**, bukan wall-clock duration. Lima session yang menunggu selama satu interval 15 detik menghasilkan sekitar 75,000 session-ms.

### Threshold and interpretation

- Lock wait share > 5% dari active client sessions: warning; > 15%: high.
- I/O wait share > 20%: warning; > 40%: high, terutama jika disk consumed percentage ≥ 80%.
- LWLock share > 20%: warning; specific LWLock event > 10% selama dua sample berturut-turut: high investigation trigger.
- BufferPin wait yang muncul pada dua sample berturut-turut: warning; jika > 5% active sessions: high.
- Active `ClientRead` biasanya client think-time atau idle protocol wait; jangan diklasifikasikan sebagai database bottleneck tanpa korelasi.
- Active `ClientWrite` > 10% selama 5 menit: warning untuk client/network backpressure.
- `wait_query_group_count > 500`: data-volume warning karena detail per-query hanya menyimpan top 500, walaupun total per-type/per-event tetap lengkap.
- Suatu event yang meningkat > 2× dibanding baseline pada load level yang sama selama minimal dua sample adalah regression evidence.
- Parallel worker pool utilization ≥ 70% selama 5 menit: warning.
- Parallel worker pool utilization ≥ 85% selama 5 menit: high.
- Parallel worker pool utilization ≥ 95% selama dua sample berturut-turut bersama CPU ≥ 80%, throughput plateau, atau P99 naik: critical worker saturation evidence.
- Parallel-worker wait share > 20% selama dua sample berturut-turut: warning; identify the dominant wait event.
- Satu parallel query memakai ≥ 50% active worker pool saat high-frequency OLTP requests degrade: warning concentration.
- Satu parallel query memakai ≥ 75% active worker pool selama sustain dan menyebabkan query lain kehilangan throughput: high concentration.
- Parallel query age > 30 detik pada OLTP request path: high kecuali query tersebut approved reporting/batch operation.
- High IPC wait among workers is not automatically CPU pressure. Correlate leader consumption, tuple transfer, and plan shape.
- A pool at 100% is not by itself proof of worker starvation. In PostgreSQL 18, correlate Q02 pool utilization with Q05 `parallel_workers_to_launch` / `parallel_workers_launched` counters, Q19 plan evidence, CPU, throughput, and latency.
- The `application_name IS DISTINCT FROM 'dms_metrics_collector'` filter is only for collector self-noise. Reserve that exact `application_name` for the collector; application workloads must not use it, otherwise Q02 would hide real sessions.

---

## Q03 — Blocking Pair, Lock Detail, Exact Lock-Wait Duration, and Blocking Chain


### Purpose

Menggantikan Q03 agar collector menggunakan `pg_locks.waitstart` sebagai sumber **actual lock wait start**.

Umur query dan umur transaksi tetap disimpan sebagai context, tetapi tidak lagi dipakai sebagai pengganti lock-wait duration.

Query juga menghitung:

- direct blockers;
- blocker count;
- maximum blocking-chain depth;
- lock object and requested mode;
- blocked and blocking query pair.

### Frequency

Conditional:

1. segera ketika Q02 menemukan `active_lock_waiting_sessions > 0`;
2. ulangi setiap 15 detik selama blocking masih ada;
3. satu final sample setelah blocking hilang.

### Connection target

Salah satu application database. Q03 is a server-wide lock diagnostic because `pg_locks` and `pg_stat_activity` expose all waiting/blocking backends visible to the monitoring role. Cross-database lock rows remain captured through `blocked.datname`, `locked_relation_oid`, and lock metadata. `locked_relation_name` is only reliable for relations that can be resolved from the database where the collector is connected; for other databases it may be null and should be enriched downstream if needed.

### SQL

```sql
/* dms_metrics_collector:q03 */
WITH RECURSIVE waiting_lock AS (
    SELECT
        l.pid,
        l.locktype,
        l.database,
        l.relation,
        l.page,
        l.tuple,
        l.virtualxid,
        l.transactionid,
        l.classid,
        l.objid,
        l.objsubid,
        l.virtualtransaction,
        l.mode,
        l.waitstart
    FROM pg_locks l
    WHERE l.granted = false
),
edges AS (
    SELECT
        w.pid AS blocked_pid,
        x.blocker_pid
    FROM waiting_lock w
    CROSS JOIN LATERAL
        unnest(pg_blocking_pids(w.pid)) AS x(blocker_pid)
),
chain AS (
    SELECT
        e.blocked_pid AS root_blocked_pid,
        e.blocked_pid,
        e.blocker_pid,
        1 AS depth,
        ARRAY[e.blocked_pid, e.blocker_pid]::integer[] AS path
    FROM edges e

    UNION ALL

    SELECT
        c.root_blocked_pid,
        e.blocked_pid,
        e.blocker_pid,
        c.depth + 1,
        c.path || e.blocker_pid
    FROM chain c
    JOIN edges e
      ON e.blocked_pid = c.blocker_pid
    WHERE c.depth < 32
      AND NOT e.blocker_pid = ANY(c.path)
),
chain_depth AS (
    SELECT
        root_blocked_pid,
        MAX(depth) AS maximum_chain_depth
    FROM chain
    GROUP BY root_blocked_pid
),
blocker_summary AS (
    SELECT
        blocked_pid,
        COUNT(*) AS direct_blocker_count,
        ARRAY_AGG(blocker_pid ORDER BY blocker_pid) AS direct_blocker_pids
    FROM edges
    GROUP BY blocked_pid
),
waiter_count AS (
    SELECT COUNT(*) AS waiting_lock_count
    FROM waiting_lock
)
SELECT
    clock_timestamp() AS collected_at,
    current_database() AS collector_database,
    wc.waiting_lock_count,
    w.pid AS blocked_pid,
    blocked.datname,
    blocked.usename AS blocked_user,
    blocked.application_name AS blocked_application_name,
    blocked.client_addr AS blocked_client_addr,
    blocked.query_id AS blocked_query_id,
    blocked.wait_event_type AS blocked_wait_event_type,
    blocked.wait_event AS blocked_wait_event,
    w.locktype,
    w.mode AS requested_lock_mode,
    w.waitstart,
    EXTRACT(EPOCH FROM (clock_timestamp() - w.waitstart))
        AS actual_lock_wait_seconds,
    EXTRACT(EPOCH FROM (clock_timestamp() - blocked.query_start))
        AS blocked_query_age_seconds,
    EXTRACT(EPOCH FROM (clock_timestamp() - blocked.xact_start))
        AS blocked_transaction_age_seconds,
    CASE
        WHEN w.relation IS NOT NULL
         AND (w.database = (SELECT oid FROM pg_database WHERE datname = current_database())
              OR w.database = 0)
        THEN format('%I.%I', n.nspname, c.relname)
    END AS locked_relation_name,
    w.relation AS locked_relation_oid,
    w.page,
    w.tuple,
    w.transactionid,
    w.virtualxid,
    bs.direct_blocker_count,
    bs.direct_blocker_pids,
    COALESCE(cd.maximum_chain_depth, 0) AS maximum_chain_depth,
    blocker.pid AS blocking_pid,
    blocker.usename AS blocking_user,
    blocker.application_name AS blocking_application_name,
    blocker.client_addr AS blocking_client_addr,
    blocker.state AS blocking_state,
    blocker.query_id AS blocking_query_id,
    EXTRACT(EPOCH FROM (clock_timestamp() - blocker.query_start))
        AS blocking_query_age_seconds,
    EXTRACT(EPOCH FROM (clock_timestamp() - blocker.xact_start))
        AS blocking_transaction_age_seconds,
    LEFT(blocked.query, 2000) AS blocked_query_text,
    LEFT(blocker.query, 2000) AS blocking_query_text
FROM waiting_lock w
JOIN pg_stat_activity blocked
  ON blocked.pid = w.pid
LEFT JOIN blocker_summary bs
  ON bs.blocked_pid = w.pid
LEFT JOIN chain_depth cd
  ON cd.root_blocked_pid = w.pid
LEFT JOIN edges e
  ON e.blocked_pid = w.pid
LEFT JOIN pg_stat_activity blocker
  ON blocker.pid = e.blocker_pid
LEFT JOIN pg_class c
  ON c.oid = w.relation
LEFT JOIN pg_namespace n
  ON n.oid = c.relnamespace
CROSS JOIN waiter_count wc
ORDER BY
    actual_lock_wait_seconds DESC NULLS LAST,
    w.pid,
    blocker.pid;
```

### Threshold and interpretation

- Any returned row is persistent evidence.
- Actual lock wait > 5 seconds: warning.
- Actual lock wait > 30 seconds: high.
- Actual lock wait > 60 seconds: critical.
- `direct_blocker_count > 1`: warning for fan-in contention.
- `maximum_chain_depth >= 3`: high; `>= 5`: critical contention chain.
- Waiting lock count increasing for three consecutive samples: high, walaupun individual waits masih pendek.
- `waitstart` dapat bernilai null untuk periode sangat singkat sesudah wait dimulai; jangan menganggap null sebagai zero-duration wait.
- Query ini tidak menggantikan deadlock log. Deadlock dapat dideteksi dan salah satu transaksi dibatalkan sebelum sampler 15 detik sempat mengambil row.

---

## Q04 — Database-Level Transactions, Cache, Temp, Deadlock, and I/O Timing


### Purpose

Mengambil cumulative counters per database untuk:

- TPS and rollback rate;
- database buffer hit ratio;
- temp file generation;
- deadlock count;
- database-wide block read/write time;
- tuple activity;
- session time and active time.

### Frequency

Setiap **30 detik**.

### Connection target

Salah satu application database. View mencakup seluruh database dalam server.

### SQL

```sql
/* dms_metrics_collector:q04 */
SELECT
    clock_timestamp() AS collected_at,
    datid,
    datname,
    numbackends,
    xact_commit,
    xact_rollback,
    blks_read,
    blks_hit,
    tup_returned,
    tup_fetched,
    tup_inserted,
    tup_updated,
    tup_deleted,
    conflicts,
    temp_files,
    temp_bytes,
    deadlocks,
    blk_read_time,
    blk_write_time,
    session_time,
    active_time,
    idle_in_transaction_time,
    sessions,
    sessions_abandoned,
    sessions_fatal,
    sessions_killed,
    stats_reset
FROM pg_stat_database
WHERE datname IS NOT NULL
  AND datname NOT IN ('template0', 'template1')
ORDER BY datid;
```

### Derived metrics

Compute from deltas:

```text
tps = (delta_xact_commit + delta_xact_rollback) / elapsed_seconds
rollback_percent = 100 * delta_xact_rollback / delta_total_transactions
cache_hit_percent = 100 * delta_blks_hit / (delta_blks_hit + delta_blks_read)
temp_bytes_per_second = delta_temp_bytes / elapsed_seconds
deadlock_count = delta_deadlocks
read_io_time_ms_per_second = delta_blk_read_time / elapsed_seconds
write_io_time_ms_per_second = delta_blk_write_time / elapsed_seconds
```

Only calculate interval cache hit ratio when `delta_blks_hit + delta_blks_read` is sufficiently large, for example at least 10,000 block accesses, so a tiny sample does not create a misleading percentage.

### Threshold and interpretation

- Any `delta_deadlocks > 0` is critical evidence.
- OLTP cache hit below 99% for 15 minutes is a warning; below 97% is high; below 95% together with disk saturation is critical.
- `temp_bytes` > 100 MB/min is warning; > 1 GB/min is high. The top spilling query must be found through Q05.
- Rollback rate > 1% is warning and > 5% is high, but classify expected business rollback separately from technical failure.
- `blk_read_time` and `blk_write_time` are meaningful only if `track_io_timing = on` for the collection period.

---

## Q05 — Query-Level Workload, CPU Candidate, Planning, JIT, Parallel, I/O, and Temp Spill

### Purpose

Menggantikan Q05 sambil mempertahankan seluruh fungsi Q05 dan menambahkan:

- JIT function count and JIT compilation time;
- query planning time and plan frequency interpretation;
- PostgreSQL 18 `parallel_workers_to_launch` and `parallel_workers_launched`;
- PostgreSQL 18 I/O timing columns;
- direct input untuk JIT overhead, planning overhead, and parallel worker launch fulfillment.

### Frequency

- setiap **60 detik**, regular top-N capture;
- full snapshot pada setiap phase boundary;
- full snapshot tepat sebelum dan sesudah official test.

Recommended regular `$1 = 500`. Bind `$1 = NULL` untuk no limit pada phase boundary.

### Connection target

Jalankan **sekali** dari salah satu database tempat view `pg_stat_statements` tersedia. Q05 membaca statistik `pg_stat_statements` yang berisi row lintas database pada server, sehingga collector **tidak boleh** menjalankan Q05 per application database dan **tidak boleh** memfilter ke `current_database()`. Database attribution dilakukan melalui `dbid` dan `database_name`.

### PostgreSQL version target

PostgreSQL **18** only. Query memakai PostgreSQL 18 `pg_stat_statements` output, termasuk expanded I/O timing columns dan `parallel_workers_to_launch` / `parallel_workers_launched`.

### SQL

```sql
/* dms_metrics_collector:q05 */
WITH raw AS MATERIALIZED (
    SELECT
        s.*,
        d.datname AS database_name,
        r.rolname AS role_name,
        to_jsonb(s) AS j
    FROM pg_stat_statements s
    LEFT JOIN pg_database d
      ON d.oid = s.dbid
    LEFT JOIN pg_roles r
      ON r.oid = s.userid
),
normalized AS (
    SELECT
        dbid,
        database_name,
        userid,
        role_name,
        queryid,
        toplevel,
        plans,
        total_plan_time,
        min_plan_time,
        max_plan_time,
        mean_plan_time,
        stddev_plan_time,
        calls,
        total_exec_time,
        min_exec_time,
        max_exec_time,
        mean_exec_time,
        stddev_exec_time,
        rows,
        shared_blks_hit,
        shared_blks_read,
        shared_blks_dirtied,
        shared_blks_written,
        local_blks_hit,
        local_blks_read,
        local_blks_dirtied,
        local_blks_written,
        temp_blks_read,
        temp_blks_written,
        COALESCE(
            NULLIF(j ->> 'blk_read_time', '')::double precision,
            NULLIF(j ->> 'shared_blk_read_time', '')::double precision,
            0
        ) AS shared_or_legacy_blk_read_time,
        COALESCE(
            NULLIF(j ->> 'blk_write_time', '')::double precision,
            NULLIF(j ->> 'shared_blk_write_time', '')::double precision,
            0
        ) AS shared_or_legacy_blk_write_time,
        COALESCE(
            NULLIF(j ->> 'temp_blk_read_time', '')::double precision,
            0
        ) AS temp_blk_read_time,
        COALESCE(
            NULLIF(j ->> 'temp_blk_write_time', '')::double precision,
            0
        ) AS temp_blk_write_time,
        wal_records,
        wal_fpi,
        wal_bytes,
        COALESCE(NULLIF(j ->> 'wal_buffers_full', '')::bigint, 0)
            AS wal_buffers_full,
        COALESCE(NULLIF(j ->> 'jit_functions', '')::bigint, 0)
            AS jit_functions,
        COALESCE(NULLIF(j ->> 'jit_generation_time', '')::double precision, 0)
            AS jit_generation_time,
        COALESCE(NULLIF(j ->> 'jit_inlining_count', '')::bigint, 0)
            AS jit_inlining_count,
        COALESCE(NULLIF(j ->> 'jit_inlining_time', '')::double precision, 0)
            AS jit_inlining_time,
        COALESCE(NULLIF(j ->> 'jit_optimization_count', '')::bigint, 0)
            AS jit_optimization_count,
        COALESCE(NULLIF(j ->> 'jit_optimization_time', '')::double precision, 0)
            AS jit_optimization_time,
        COALESCE(NULLIF(j ->> 'jit_emission_count', '')::bigint, 0)
            AS jit_emission_count,
        COALESCE(NULLIF(j ->> 'jit_emission_time', '')::double precision, 0)
            AS jit_emission_time,
        COALESCE(NULLIF(j ->> 'jit_deform_count', '')::bigint, 0)
            AS jit_deform_count,
        COALESCE(NULLIF(j ->> 'jit_deform_time', '')::double precision, 0)
            AS jit_deform_time,
        NULLIF(j ->> 'parallel_workers_to_launch', '')::bigint
            AS parallel_workers_to_launch,
        NULLIF(j ->> 'parallel_workers_launched', '')::bigint
            AS parallel_workers_launched,
        NULLIF(j ->> 'stats_since', '')::timestamptz AS stats_since,
        NULLIF(j ->> 'minmax_stats_since', '')::timestamptz
            AS minmax_stats_since,
        LEFT(query, 6000) AS normalized_query_text
    FROM raw
)
SELECT
    clock_timestamp() AS collected_at,
    current_setting('server_version_num')::integer AS server_version_num,
    n.*,
    (
        n.jit_generation_time
        + n.jit_inlining_time
        + n.jit_optimization_time
        + n.jit_emission_time
        + n.jit_deform_time
    ) AS total_jit_time
FROM normalized n
ORDER BY
    total_exec_time DESC,
    shared_blks_read DESC,
    temp_blks_written DESC
LIMIT $1;
```

### Implementation notes

- Q05 must capture all roles and all databases represented in `pg_stat_statements`. Do not exclude `current_user` and do not filter on `current_database()`.
- Join to `pg_database` and `pg_roles` is for labeling only. The stable counter identity remains `(dbid, userid, queryid, toplevel)`.
- Collector self-noise should be handled downstream or through an explicitly reviewed exclusion list in the summarizer. Do not put an in-query role exclusion in Q05, because the collector may be executed by the same role used by the application workload during manual checks.
- If SQL text visibility for other users is limited, validate monitoring privileges before the official run and record the data-quality limitation.

### Derived interval metrics

Use `(dbid, userid, queryid, toplevel)` as the counter key.

```text
calls_per_second = delta_calls / elapsed_seconds

avg_exec_ms_interval = delta_total_exec_time / delta_calls

avg_plan_ms_interval = delta_total_plan_time / delta_plans

plan_to_call_ratio = delta_plans / delta_calls

planning_share_percent =
    100 * delta_total_plan_time /
    (delta_total_plan_time + delta_total_exec_time)

jit_time_interval =
    delta_jit_generation_time
    + delta_jit_inlining_time
    + delta_jit_optimization_time
    + delta_jit_emission_time
    + delta_jit_deform_time

jit_overhead_percent =
    100 * jit_time_interval / delta_total_exec_time

jit_ms_per_call = jit_time_interval / delta_calls

parallel_worker_launch_fulfillment_percent_pg18 =
    100 * delta_parallel_workers_launched /
    delta_parallel_workers_to_launch
```

Only calculate a ratio when its denominator is positive. Do not calculate deltas across a reset, server restart, or changed `stats_since` baseline.

### CPU candidate interpretation

A query becomes a stronger CPU candidate when:

1. Azure Monitor `cpu_percent` is high;
2. Q02 has high `active_cpu_or_running_sessions`;
3. Q02 shows leader/worker demand, if parallel plans are involved;
4. Q05 has high delta `total_exec_time`, call rate, planning, or JIT time;
5. Q11 and Q05 show low relative I/O/lock/client wait contribution;
6. k6 throughput no longer increases proportionally or P90/P99 degrades.

### Threshold and interpretation

#### Planning overhead

- Planning share > 10% with at least 1 second of planning time in the interval: warning.
- Planning share > 25%: high.
- `plan_to_call_ratio ≥ 0.90`, `calls ≥ 1,000/minute`, and average planning time ≥ 1 ms: warning for excessive replanning/high-frequency planning overhead.
- Same condition with average planning time ≥ 5 ms: high.
- Do not interpret planning columns if `pg_stat_statements.track_planning = off`.

#### JIT overhead

- JIT time > 10% of query execution time and total JIT time > 1 second/window: warning.
- JIT time > 25%: high.
- JIT > 5 ms/call for a query whose average execution is below 100 ms and call rate is material: warning for short-query JIT overhead.
- JIT > 10 ms/call for a high-frequency OLTP query: high.
- JIT counters > 0 are not automatically bad; JIT is often beneficial for long-running CPU-bound analytical queries.

#### Parallel-worker launch fulfillment — PostgreSQL 18+

- Evaluate only if `delta_parallel_workers_to_launch ≥ 20` in the analysis window.
- Fulfillment < 80% for two consecutive windows: warning.
- Fulfillment < 50%: high worker-availability issue.
- Fulfillment < 50% together with Q02 parallel-worker pool utilization ≥ 85%, CPU ≥ 80%, and latency regression: critical worker starvation evidence.
- On PostgreSQL 18 these columns are expected to be available; if they are null, treat it as a data-quality issue and validate the installed `pg_stat_statements` version.

#### Existing Q05 thresholds retained

Retain the Q05 investigation thresholds for execution time, top-query concentration, temp spill, physical reads, and same-load regression.

---

## Q06 — pg_stat_statements Capacity and Reset Health


### Purpose

Mendeteksi apakah `pg_stat_statements.max` terlalu kecil dan query fingerprints mulai dideallocate, serta mendeteksi reset yang memutus interval delta.

### Frequency

Setiap **5 menit** dan pada setiap phase boundary.

### Connection target

Salah satu application database.

### SQL

```sql
/* dms_metrics_collector:q06 */
SELECT
    clock_timestamp() AS collected_at,
    dealloc,
    stats_reset
FROM pg_stat_statements_info;
```

### Threshold and interpretation

- Any positive `delta_dealloc` is warning karena fingerprint berfrekuensi rendah dapat hilang.
- High jika deallocation muncul pada dua sample berturut-turut.
- Critical data-quality issue jika deallocation terus meningkat selama official sustain phase; pertimbangkan meningkatkan `pg_stat_statements.max` sebelum official rerun.
- Jika `stats_reset` berubah, Q05 counter baseline harus diinisialisasi ulang.

---

## Q07 — Cluster I/O by Backend Type, Object, and Context


### Purpose

Mendapatkan I/O cluster-level yang lebih detail daripada `pg_stat_database`, termasuk:

- client backend vs autovacuum/checkpointer/background writer;
- normal, bulk read, bulk write, dan vacuum I/O;
- temp relation I/O;
- shared-buffer hit, eviction, reuse, writeback, dan fsync.

### Frequency

Setiap **30 detik**.

### PostgreSQL version target

PostgreSQL **18** only. Q07 uses PostgreSQL 18 `pg_stat_io` byte counters
(`read_bytes`, `write_bytes`, and `extend_bytes`) and does not use the removed
`op_bytes` column. Jalankan hanya jika Q01 menghasilkan `has_pg_stat_io = true`.

### Connection target

Salah satu application database; data bersifat cluster-wide.

### SQL

```sql
/* dms_metrics_collector:q07 */
SELECT
    clock_timestamp() AS collected_at,
    backend_type,
    object,
    context,
    reads,
    read_bytes,
    read_time,
    writes,
    write_bytes,
    write_time,
    writebacks,
    writeback_time,
    extends,
    extend_bytes,
    extend_time,
    hits,
    evictions,
    reuses,
    fsyncs,
    fsync_time,
    stats_reset
FROM pg_stat_io
ORDER BY backend_type, object, context;
```

### Derived metrics

```text
read_bytes_per_second = delta_read_bytes / elapsed_seconds
write_bytes_per_second = delta_write_bytes / elapsed_seconds
extend_bytes_per_second = delta_extend_bytes / elapsed_seconds
io_hit_percent = 100 * delta_hits / (delta_hits + delta_reads)
evictions_per_second = delta_evictions / elapsed_seconds
client_backend_write_share = client_backend_delta_writes / all_backend_delta_writes
```

### Threshold and interpretation

- High client-backend writes or fsyncs are evidence that backends are doing work expected to be absorbed by checkpointer/background writer; correlate with Q08.
- Eviction rate does not have a universal threshold. Warning when normal-context eviction rate rises > 2× baseline for 15 minutes together with lower cache hit and higher read I/O.
- `bulkread` is not automatically bad; it can be an expected sequential scan or batch operation.
- `vacuum` I/O must be correlated with Q12 before classifying it as application I/O.
- Time columns are valid only when `track_io_timing = on`.

---

## Q08 — Checkpoint, Background Writer, and WAL Snapshot


### Purpose

Mendeteksi checkpoint pressure, background-writer saturation, WAL buffer pressure, dan WAL write/sync time. Backend writes/fsyncs dikorelasikan melalui Q07 `pg_stat_io` pada PostgreSQL 18.

### Frequency

Setiap **60 detik**.

### Connection target

Salah satu application database; views bersifat cluster-wide.

### SQL — PostgreSQL 18

```sql
/* dms_metrics_collector:q08 */
SELECT
    clock_timestamp() AS collected_at,
    'pg_stat_bgwriter'::text AS source_view,
    to_jsonb(bg) AS payload
FROM pg_stat_bgwriter bg

UNION ALL

SELECT
    clock_timestamp() AS collected_at,
    'pg_stat_checkpointer'::text AS source_view,
    to_jsonb(cp) AS payload
FROM pg_stat_checkpointer cp

UNION ALL

SELECT
    clock_timestamp() AS collected_at,
    'pg_stat_wal'::text AS source_view,
    to_jsonb(wal) AS payload
FROM pg_stat_wal wal;
```

### Derived metrics

For PostgreSQL 18:

```text
requested_checkpoint_percent = 100 * delta_num_requested /
                               (delta_num_requested + delta_num_timed)
checkpoint_busy_percent = 100 *
    (delta_write_time + delta_sync_time) /
    interval_milliseconds
wal_bytes_per_second = delta_wal_bytes / elapsed_seconds
```

Use `pg_stat_checkpointer` for checkpoint and restartpoint counters, `pg_stat_bgwriter`
for background-writer cleaning counters, and `pg_stat_wal` for WAL counters. Backend
write/fsync evidence should be correlated through Q07 `pg_stat_io` client-backend
write/fsync rows in PostgreSQL 18.

### Threshold and interpretation

- Requested checkpoint ratio > 20% over 15 minutes: warning.
- Requested checkpoint ratio > 50%: high, investigate `max_wal_size`, checkpoint interval, and write workload.
- `wal_buffers_full` delta repeatedly > 0: warning; persistent growth with write latency is high.
- Q07 client-backend fsync delta > 0: high evidence because backend normally should not need to fsync itself.
- `maxwritten_clean` repeatedly increases: background writer is reaching its cleaning limit; correlate with client backend writes and I/O saturation.
- Do not tune checkpoint parameters only from one short spike; use at least one sustain window.

---

## Q09 — Table Access, DML, Dead Tuples, Vacuum, and Table I/O


### Purpose

Satu query menggabungkan `pg_stat_user_tables` dan `pg_statio_user_tables` untuk:

- sequential scan and index scan behavior;
- DML volume;
- HOT update effectiveness;
- live/dead tuples and analyze backlog;
- table/index/TOAST cache effectiveness;
- last vacuum/autovacuum/analyze timestamps.

### Frequency

Setiap **5 menit** dan setiap phase boundary.

### Connection target

Jalankan pada **setiap application database**, karena table statistics bersifat current-database.

### SQL

```sql
/* dms_metrics_collector:q09 */
SELECT
    clock_timestamp() AS collected_at,
    st.relid,
    st.schemaname,
    st.relname,
    st.seq_scan,
    st.seq_tup_read,
    st.idx_scan,
    st.idx_tup_fetch,
    st.n_tup_ins,
    st.n_tup_upd,
    st.n_tup_del,
    st.n_tup_hot_upd,
    st.n_live_tup,
    st.n_dead_tup,
    st.n_mod_since_analyze,
    st.last_vacuum,
    st.last_autovacuum,
    st.last_analyze,
    st.last_autoanalyze,
    st.vacuum_count,
    st.autovacuum_count,
    st.analyze_count,
    st.autoanalyze_count,
    io.heap_blks_read,
    io.heap_blks_hit,
    io.idx_blks_read,
    io.idx_blks_hit,
    io.toast_blks_read,
    io.toast_blks_hit,
    io.tidx_blks_read,
    io.tidx_blks_hit
FROM pg_stat_user_tables st
LEFT JOIN pg_statio_user_tables io
       ON io.relid = st.relid
ORDER BY st.relid;
```

### Derived metrics

```text
rows_read_per_seq_scan = delta_seq_tup_read / delta_seq_scan
hot_update_percent = 100 * delta_n_tup_hot_upd / delta_n_tup_upd
dead_tuple_percent = 100 * n_dead_tup / (n_live_tup + n_dead_tup)
heap_hit_percent = 100 * delta_heap_blks_hit /
                   (delta_heap_blks_hit + delta_heap_blks_read)
index_hit_percent = 100 * delta_idx_blks_hit /
                    (delta_idx_blks_hit + delta_idx_blks_read)
```

### Threshold and interpretation

- Dead tuple ratio > 10%: warning; > 20%: high, subject to absolute row count and workload pattern.
- A table is a sequential-scan candidate if interval `delta_seq_scan > 0` and average rows read per sequential scan > 100,000, especially when Azure I/O is high.
- Low table/index hit ratio is only evaluated when interval block access is sufficiently large.
- `n_mod_since_analyze` high relative to `n_live_tup` can indicate statistics staleness; warning when > 10%, but actual analyze threshold follows PostgreSQL autovacuum settings.
- Do not conclude an index is missing solely from `seq_scan`; small tables are often correctly scanned sequentially.

---

## Q10 — Index Usage, Index I/O, Size, and Validity


### Purpose

Menggabungkan index usage dan index I/O tanpa menjalankan query terpisah untuk CPU/I/O analysis. Digunakan untuk melihat index yang banyak dibaca, jarang dipakai, tidak valid, atau berukuran besar.

### Frequency

- setiap **15 menit**;
- setiap phase boundary;
- full snapshot before/after tuning.

### Connection target

Jalankan pada **setiap application database**.

### SQL

```sql
/* dms_metrics_collector:q10 */
SELECT
    clock_timestamp() AS collected_at,
    s.relid,
    s.indexrelid,
    s.schemaname,
    s.relname,
    s.indexrelname,
    s.idx_scan,
    s.idx_tup_read,
    s.idx_tup_fetch,
    io.idx_blks_read,
    io.idx_blks_hit,
    pg_relation_size(s.indexrelid) AS index_size_bytes,
    i.indisprimary,
    i.indisunique,
    i.indisvalid,
    i.indisready
FROM pg_stat_user_indexes s
LEFT JOIN pg_statio_user_indexes io
       ON io.indexrelid = s.indexrelid
JOIN pg_index i
     ON i.indexrelid = s.indexrelid
ORDER BY s.indexrelid;
```

### Threshold and interpretation

- `idx_scan = 0` selama satu performance test **bukan** alasan otomatis untuk drop index.
- Mark as review candidate only when all are true:
  - delta `idx_scan = 0` selama seluruh representative test;
  - bukan PK/unique-supporting index;
  - index size significant, suggested > 100 MB;
  - write workload is material;
  - production workload is represented by the test.
- Index hit ratio below 95% with high access volume and disk pressure warrants investigation.
- Any `indisvalid = false` or `indisready = false` is a configuration/operation issue and should be reported.

---

## Q11 — Query Store Historical Waits Enriched with Runtime and Query Text


### Purpose

Menggantikan Q11 agar historical wait sample langsung dikorelasikan dengan:

- Query Store runtime window yang sama;
- representative normalized query text;
- execution calls;
- total and mean execution time;
- shared block reads/hits;
- temp blocks;
- block read/write time.

Ini menghindari asumsi bahwa `query_id` Query Store selalu dapat dipetakan secara aman ke query dictionary dari sumber lain.

### Frequency

Setiap **15 menit**, sekitar **2 menit setelah** Query Store window closure.

### Connection target

Database **`azure_sys`**.

### Bind parameter

`$1` = last successfully persisted `end_time`.

### SQL

```sql
/* dms_metrics_collector:q11 */
WITH settings AS (
    SELECT
        current_setting('pgms_wait_sampling.history_period')::numeric
            AS history_period_ms
),
waits AS (
    SELECT
        w.start_time,
        w.end_time,
        w.user_id,
        w.db_id,
        w.query_id,
        w.event_type,
        w.event,
        w.calls::bigint AS wait_sample_count,
        w.calls::numeric * s.history_period_ms
            AS estimated_sampled_wait_ms
    FROM query_store.pgms_wait_sampling_view w
    CROSS JOIN settings s
    WHERE w.end_time > $1::timestamp
      AND w.end_time <=
          (clock_timestamp() AT TIME ZONE 'UTC') - INTERVAL '2 minutes'
),
runtime AS (
    SELECT
        q.start_time,
        q.end_time,
        q.user_id,
        q.db_id,
        q.query_id,
        MAX(q.query_sql_text) AS query_sql_text,
        BOOL_OR(q.is_system_query) AS is_system_query,
        SUM(q.calls) AS runtime_calls,
        SUM(q.total_time) AS total_exec_time_ms,
        CASE
            WHEN SUM(q.calls) > 0
            THEN SUM(q.total_time) / SUM(q.calls)
        END AS mean_exec_time_ms,
        MAX(q.max_time) AS max_exec_time_ms,
        SUM(q.shared_blks_hit) AS shared_blks_hit,
        SUM(q.shared_blks_read) AS shared_blks_read,
        SUM(q.shared_blks_dirtied) AS shared_blks_dirtied,
        SUM(q.shared_blks_written) AS shared_blks_written,
        SUM(q.temp_blks_read) AS temp_blks_read,
        SUM(q.temp_blks_written) AS temp_blks_written,
        SUM(q.blk_read_time) AS blk_read_time_ms,
        SUM(q.blk_write_time) AS blk_write_time_ms
    FROM query_store.qs_view q
    WHERE q.end_time > $1::timestamp
      AND q.end_time <=
          (clock_timestamp() AT TIME ZONE 'UTC') - INTERVAL '2 minutes'
    GROUP BY
        q.start_time,
        q.end_time,
        q.user_id,
        q.db_id,
        q.query_id
),
joined AS (
    SELECT
        w.*,
        r.query_sql_text,
        r.is_system_query,
        r.runtime_calls,
        r.total_exec_time_ms,
        r.mean_exec_time_ms,
        r.max_exec_time_ms,
        r.shared_blks_hit,
        r.shared_blks_read,
        r.shared_blks_dirtied,
        r.shared_blks_written,
        r.temp_blks_read,
        r.temp_blks_written,
        r.blk_read_time_ms,
        r.blk_write_time_ms,
        SUM(w.wait_sample_count) OVER (
            PARTITION BY w.start_time, w.end_time
        ) AS all_window_wait_samples,
        SUM(w.wait_sample_count) OVER (
            PARTITION BY
                w.start_time,
                w.end_time,
                w.user_id,
                w.db_id,
                w.query_id
        ) AS query_window_wait_samples
    FROM waits w
    LEFT JOIN runtime r
      ON r.start_time = w.start_time
     AND r.end_time = w.end_time
     AND r.user_id = w.user_id
     AND r.db_id = w.db_id
     AND r.query_id = w.query_id
)
SELECT
    clock_timestamp() AS collected_at,
    start_time,
    end_time,
    user_id,
    db_id,
    query_id,
    event_type,
    event,
    CASE
        WHEN event_type = 'Activity'
            THEN 'background_or_idle_activity'
        WHEN event_type = 'Client' AND event = 'ClientRead'
            THEN 'client_idle_or_think_time'
        WHEN event_type = 'Client'
            THEN 'client_or_network_backpressure'
        ELSE 'database_resource_wait'
    END AS wait_classification,
    wait_sample_count,
    estimated_sampled_wait_ms,
    ROUND(
        100.0 * wait_sample_count
        / NULLIF(all_window_wait_samples, 0),
        2
    ) AS event_share_of_all_window_wait_percent,
    ROUND(
        100.0 * wait_sample_count
        / NULLIF(query_window_wait_samples, 0),
        2
    ) AS event_share_within_query_percent,
    CASE
        WHEN runtime_calls > 0
        THEN ROUND(wait_sample_count::numeric / runtime_calls, 4)
    END AS wait_samples_per_runtime_call,
    CASE
        WHEN runtime_calls > 0
        THEN ROUND(estimated_sampled_wait_ms / runtime_calls, 2)
    END AS estimated_wait_ms_per_runtime_call,
    runtime_calls,
    total_exec_time_ms,
    mean_exec_time_ms,
    max_exec_time_ms,
    shared_blks_hit,
    shared_blks_read,
    shared_blks_dirtied,
    shared_blks_written,
    temp_blks_read,
    temp_blks_written,
    blk_read_time_ms,
    blk_write_time_ms,
    is_system_query,
    LEFT(query_sql_text, 6000) AS query_sql_text
FROM joined
ORDER BY
    end_time,
    wait_sample_count DESC,
    db_id,
    query_id,
    event_type,
    event;
```

### Threshold and interpretation

- Lock share > 5% of relevant wait samples in a window: warning; > 15%: high.
- I/O share > 20%: warning; > 40%: high, khususnya jika disk consumed percentage ≥ 80%.
- LWLock share > 20%: warning dan wajib dianalisis per specific event.
- Satu query menyumbang > 20% dari database-resource wait samples pada satu window: warning; > 40%: high concentration.
- `estimated_wait_ms_per_runtime_call` meningkat > 2× baseline pada load level yang sama selama dua Query Store windows: regression evidence.
- Wait row tanpa matching runtime/query text: data-quality warning; jika > 5% wait samples pada suatu window tidak mempunyai runtime match, status high data-quality issue.
- `runtime_calls` Query Store pada parallel query dapat menghitung leader dan worker; jangan selalu mengartikannya sebagai jumlah business executions.
- `estimated_sampled_wait_ms` adalah sampled occupancy estimate, bukan exact elapsed duration dan bukan wall-clock duration.

---

## Q12 — Active Vacuum and Analyze Progress


### Purpose

Membedakan application-driven I/O dari maintenance-driven I/O dan melihat apakah vacuum/analyze aktif, stagnan, atau terlalu lama pada saat I/O pressure.

### Frequency

Conditional:

- setiap 30 detik ketika Q02 melihat `autovacuum worker` aktif;
- atau ketika Q09 menunjukkan dead tuple pressure;
- atau saat disk consumed percentage tinggi dan maintenance activity perlu dikonfirmasi.

Stop polling ketika tidak ada row selama dua consecutive samples.

### Connection target

Jalankan **sekali** dari salah satu application database atau designated monitoring database. Q12 reads server-wide progress views and should not be looped per application database, because the rows already carry database attribution through `datname` / progress payload.

### SQL

```sql
/* dms_metrics_collector:q12 */
SELECT
    clock_timestamp() AS collected_at,
    'vacuum'::text AS operation_type,
    to_jsonb(v) AS progress,
    a.datname,
    a.usename,
    a.application_name,
    a.wait_event_type,
    a.wait_event,
    EXTRACT(EPOCH FROM (clock_timestamp() - a.query_start))
        AS operation_age_seconds
FROM pg_stat_progress_vacuum v
LEFT JOIN pg_stat_activity a ON a.pid = v.pid

UNION ALL

SELECT
    clock_timestamp() AS collected_at,
    'analyze'::text AS operation_type,
    to_jsonb(an) AS progress,
    a.datname,
    a.usename,
    a.application_name,
    a.wait_event_type,
    a.wait_event,
    EXTRACT(EPOCH FROM (clock_timestamp() - a.query_start))
        AS operation_age_seconds
FROM pg_stat_progress_analyze an
LEFT JOIN pg_stat_activity a ON a.pid = an.pid;
```

### Threshold and interpretation

- Maintenance active is informational, not automatically bad.
- Warning when operation age > 30 minutes and disk consumed percentage ≥ 70%.
- High when progress counters do not change for 5 minutes while the backend remains active/waiting.
- Autovacuum blocked by a long transaction should be correlated with Q02/Q03.

---

### 6.B Memory collector queries (Q13–Q15)

Q13 and Q14 require the `pg_buffercache` extension on one designated
monitoring/application database (see Section 13). These entries are PostgreSQL
18-only; older fallback variants are intentionally not included.

---

## Q13 — Shared Buffer Cache Summary and Usage-Count Distribution

### Purpose

Mengambil keadaan aktual `shared_buffers` untuk melihat:

- jumlah buffer used dan unused;
- dirty buffers;
- pinned buffers;
- average `usagecount`;
- distribution buffer berdasarkan `usagecount`.

Query ini melengkapi cache hit ratio dari Q04/Q05/Q09. Cache hit ratio adalah
counter historis, sedangkan Q13 menunjukkan keadaan buffer pool pada waktu
snapshot.

### Frequency

- setiap **5 menit** dan setiap phase boundary;
- tetap aktif selama 30–60 minute cool-down.

### PostgreSQL version target

PostgreSQL **18** only. Query menggunakan `pg_buffercache_summary()` dan
`pg_buffercache_usage_counts()`. Tidak ada fallback scan langsung ke
`pg_buffercache` untuk versi lama.

### Connection target

Designated database tempat extension `pg_buffercache` dibuat. Data buffer cache
bersifat server-wide.

### SQL

```sql
/* dms_metrics_collector:q13 */
WITH summary AS (
    SELECT *
    FROM pg_buffercache_summary()
),
usage_distribution AS (
    SELECT
        COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'usage_count', usage_count,
                    'buffers', buffers,
                    'dirty', dirty,
                    'pinned', pinned
                )
                ORDER BY usage_count
            ),
            '[]'::jsonb
        ) AS usage_counts
    FROM pg_buffercache_usage_counts()
)
SELECT
    clock_timestamp() AS collected_at,
    current_setting('server_version_num')::integer AS server_version_num,
    s.buffers_used,
    s.buffers_unused,
    s.buffers_dirty,
    s.buffers_pinned,
    s.usagecount_avg,
    ROUND(
        100.0 * s.buffers_used
        / NULLIF(s.buffers_used + s.buffers_unused, 0),
        2
    ) AS buffer_used_percent,
    ROUND(
        100.0 * s.buffers_dirty
        / NULLIF(s.buffers_used, 0),
        2
    ) AS dirty_buffer_percent_of_used,
    ROUND(
        100.0 * s.buffers_pinned
        / NULLIF(s.buffers_used, 0),
        2
    ) AS pinned_buffer_percent_of_used,
    u.usage_counts
FROM summary s
CROSS JOIN usage_distribution u;
```

### Threshold and interpretation

- `buffer_used_percent` mendekati 100% setelah warm-up adalah **normal** dan bukan bukti memory pressure.
- Warning jika `usagecount_avg` turun lebih dari 50% dibanding same-load baseline selama minimal 15 menit dan pada window yang sama Q07 menunjukkan eviction meningkat > 2× baseline serta Azure read I/O meningkat.
- High jika pola di atas berlangsung minimal 30 menit dan cache hit Q04 < 97% atau disk consumed percentage ≥ 80%.
- `dirty_buffer_percent_of_used` > 10% selama 10 menit: warning jika write throughput dan backend writes juga meningkat; > 20% bersama write saturation: high.
- `pinned_buffer_percent_of_used` > 1% selama dua consecutive samples: warning; > 5%: high investigation trigger.
- Jangan menganggap `usagecount` rendah sebagai masalah tanpa korelasi. Sequential/bulk workload dapat secara sah menghasilkan buffer ber-usage rendah.

---

## Q14 — Top Relations Occupying Shared Buffers

### Purpose

Menentukan table atau index pada current database yang paling banyak menempati
`shared_buffers`.

Query digunakan untuk:

- menemukan satu relation yang mendominasi cache;
- membandingkan cache residency antar phase;
- mengorelasikan cache crowding dengan Q04 cache hit, Q07 eviction, Q09 table I/O, dan Azure read I/O.

### Frequency

- setiap **15 menit** jika overhead test menunjukkan query aman;
- mandatory pada setiap phase boundary;
- conditional ketika Q13 menunjukkan cache churn atau Q04 cache hit turun;
- jangan dijalankan lebih sering dari 5 menit.

### PostgreSQL version target

PostgreSQL **18** only.

### Connection target

Jalankan pada setiap application database yang perlu dianalisis. Query hanya
memetakan relation milik current database.

### SQL

```sql
/* dms_metrics_collector:q14 */
WITH current_db AS (
    SELECT
        oid AS database_oid,
        dattablespace AS database_default_tablespace_oid
    FROM pg_database
    WHERE datname = current_database()
),
cache AS MATERIALIZED (
    SELECT
        b.reldatabase,
        b.reltablespace,
        b.relfilenode,
        b.relforknumber,
        COUNT(*) AS cached_buffers,
        COUNT(*) FILTER (WHERE b.isdirty) AS dirty_buffers,
        COUNT(*) FILTER (WHERE b.pinning_backends > 0) AS pinned_buffers,
        AVG(b.usagecount) AS avg_usagecount
    FROM pg_buffercache b
    CROSS JOIN current_db d
    WHERE b.relfilenode IS NOT NULL
      AND b.relforknumber = 0
      AND b.reldatabase = d.database_oid
    GROUP BY
        b.reldatabase,
        b.reltablespace,
        b.relfilenode,
        b.relforknumber
),
mapped AS (
    SELECT
        c.oid AS relation_oid,
        n.nspname AS schemaname,
        c.relname,
        c.relkind,
        cache.cached_buffers,
        cache.dirty_buffers,
        cache.pinned_buffers,
        cache.avg_usagecount,
        cache.cached_buffers
            * current_setting('block_size')::bigint AS cached_bytes,
        pg_relation_size(c.oid) AS relation_main_fork_size_bytes
    FROM cache
    CROSS JOIN current_db d
    JOIN pg_class c
      ON pg_relation_filenode(c.oid) = cache.relfilenode
     AND COALESCE(
            NULLIF(c.reltablespace, 0),
            d.database_default_tablespace_oid
         ) = cache.reltablespace
    JOIN pg_namespace n
      ON n.oid = c.relnamespace
    WHERE n.nspname NOT LIKE 'pg_temp_%'
)
SELECT
    clock_timestamp() AS collected_at,
    current_database() AS database_name,
    relation_oid,
    schemaname,
    relname,
    relkind,
    cached_buffers,
    cached_bytes,
    relation_main_fork_size_bytes,
    ROUND(
        100.0 * cached_bytes
        / NULLIF(relation_main_fork_size_bytes, 0),
        2
    ) AS relation_cached_percent,
    ROUND(
        100.0 * cached_buffers
        / NULLIF(SUM(cached_buffers) OVER (), 0),
        2
    ) AS share_of_current_database_cached_buffers_percent,
    dirty_buffers,
    pinned_buffers,
    avg_usagecount
FROM mapped
ORDER BY cached_buffers DESC
LIMIT 100;
```

### Threshold and interpretation

- Tidak ada universal maximum cache share untuk satu relation.
- Warning jika satu relation memakai > 30% current-database cached buffers dan cache share tersebut meningkat > 2× same-load baseline saat cache hit database turun.
- High jika satu relation memakai > 50% cache, Q07 eviction meningkat, dan relation lain yang penting menunjukkan peningkatan disk reads.
- `relation_cached_percent` > 100% dapat terjadi bila relation mapping atau size berubah selama concurrent activity. Treat as snapshot inconsistency and data-quality warning, bukan performance event.
- Relation besar yang memang hot dapat secara sah mendominasi cache. Jangan membuat tuning decision hanya dari cache share.

---

## Q15 — Main Shared-Memory Allocation Snapshot

### Purpose

Mengambil allocation inventory dari main shared-memory segment PostgreSQL untuk
melihat memory yang dialokasikan oleh:

- core PostgreSQL components;
- shared buffer structures;
- lock and transaction structures;
- installed extensions yang menggunakan main shared memory.

View ini tidak mencakup dynamic shared memory dan tidak menggantikan Azure
`memory_percent`.

### Frequency

- saat collector start;
- setelah restart, scale, failover, extension install/update, atau static parameter change;
- tepat sebelum dan sesudah official test;
- tidak perlu dipolling setiap beberapa menit.

### PostgreSQL version target

PostgreSQL **18** only.

### Connection target

Salah satu application database. Data bersifat cluster-wide.

### SQL

```sql
/* dms_metrics_collector:q15 */
WITH a AS (
    SELECT
        COALESCE(name, '<unused>') AS allocation_name,
        off,
        size,
        allocated_size
    FROM pg_shmem_allocations
),
summary AS (
    SELECT
        SUM(allocated_size) AS total_main_shared_memory_bytes,
        SUM(allocated_size) FILTER (
            WHERE allocation_name <> '<unused>'
        ) AS allocated_named_and_anonymous_bytes,
        SUM(allocated_size) FILTER (
            WHERE allocation_name = '<unused>'
        ) AS unused_main_shared_memory_bytes
    FROM a
)
SELECT
    clock_timestamp() AS collected_at,
    a.allocation_name,
    a.off,
    a.size,
    a.allocated_size,
    s.total_main_shared_memory_bytes,
    s.allocated_named_and_anonymous_bytes,
    s.unused_main_shared_memory_bytes,
    ROUND(
        100.0 * s.allocated_named_and_anonymous_bytes
        / NULLIF(s.total_main_shared_memory_bytes, 0),
        2
    ) AS main_shared_memory_allocated_percent,
    ROUND(
        100.0 * a.allocated_size
        / NULLIF(s.total_main_shared_memory_bytes, 0),
        4
    ) AS allocation_share_percent
FROM a
CROSS JOIN summary s
ORDER BY a.allocated_size DESC, a.allocation_name;
```

### Threshold and interpretation

- Tidak ada universal memory-pressure threshold untuk `pg_shmem_allocations`.
- Total main shared memory biasanya stabil di antara restart/configuration events.
- Perubahan total atau named allocation > 5% tanpa restart, scale, extension, atau static parameter change: warning data/configuration anomaly; > 10%: high.
- Satu third-party extension memakai > 10% main shared-memory segment: warning review; > 20%: high review, kecuali sudah direncanakan dan didokumentasikan.
- `main_shared_memory_allocated_percent` tinggi bukan otomatis host memory pressure. Selalu korelasikan dengan Azure `memory_percent`.
- Query failure karena privilege adalah evidence gap. Role `pg_monitor`/`pg_read_all_stats` harus divalidasi sebelum test.

---

### 6.C Wait-statistics additions (Q16–Q17 plus DICT01)

---

## Q16 — SLRU I/O Statistics

### Purpose

Mengambil cumulative counters dari `pg_stat_slru` untuk mengorelasikan wait events terkait transaction status, subtransaction, multixact, serializable state, notification, dan commit timestamp dengan I/O cache SLRU.

Query ini membantu root-cause analysis jika W01 atau W03 menunjukkan event seperti SLRU-related I/O/LWLock waits.

### Frequency

Setiap **60 detik**.

### Connection target

Salah satu application database. Data bersifat cluster-wide.

### SQL

```sql
/* dms_metrics_collector:q16 */
SELECT
    clock_timestamp() AS collected_at,
    name,
    blks_zeroed,
    blks_hit,
    blks_read,
    blks_written,
    blks_exists,
    flushes,
    truncates,
    stats_reset
FROM pg_stat_slru
ORDER BY name;
```

### Derived metrics

```text
slru_accesses = delta_blks_hit + delta_blks_read

slru_hit_percent =
    100 * delta_blks_hit / (delta_blks_hit + delta_blks_read)

slru_reads_per_second = delta_blks_read / elapsed_seconds
slru_writes_per_second = delta_blks_written / elapsed_seconds
slru_flushes_per_second = delta_flushes / elapsed_seconds
```

Jangan menghitung delta melintasi perubahan `stats_reset` atau counter yang menurun.

### Threshold and interpretation

Tidak ada universal SLRU threshold. Gunakan minimum activity floor dan baseline-relative interpretation:

- Evaluasi hit ratio hanya jika `delta_blks_hit + delta_blks_read >= 1,000` dalam interval analisis.
- Hit ratio < 95% selama 15 menit: warning; < 90%: high, jika bersamaan dengan SLRU-related waits.
- Read rate > 2× same-load baseline selama 15 menit: warning; > 5×: high.
- Persistent write/flush growth bersama I/O saturation dan SLRU wait: high.
- `MultixactMember` atau `MultixactOffset` activity yang meningkat tajam harus dikorelasikan dengan concurrent row locking dan transaction design.
- `Subtransaction` activity tinggi dapat menunjukkan penggunaan savepoint/subtransaction yang berlebihan.

---

## Q17 — Lock and Deadlock Logging Configuration Validation

### Purpose

Memastikan collector mempunyai evidence untuk lock waits dan deadlocks yang selesai di antara dua polling cycle.

SQL polling tidak dapat menjamin menangkap deadlock pair karena PostgreSQL dapat membatalkan salah satu transaksi sebelum interval 15 detik berikutnya. Karena itu, `PostgreSQLLogs` tetap menjadi source wajib untuk exact deadlock timeline.

### Frequency

- saat Azure Function start;
- setiap 6 jam;
- setelah restart, scale, failover, atau parameter change;
- sebelum official performance test.

### Connection target

Salah satu application database.

### SQL

```sql
/* dms_metrics_collector:q17 */
SELECT
    clock_timestamp() AS collected_at,
    name,
    setting,
    unit,
    source,
    pending_restart
FROM pg_settings
WHERE name IN (
    'deadlock_timeout',
    'log_lock_waits',
    'log_min_messages',
    'log_min_error_statement',
    'log_error_verbosity',
    'log_line_prefix'
)
ORDER BY name;
```

### Threshold and validation

- `log_lock_waits = off`: **critical evidence gap** jika RFP membutuhkan lock/deadlock waiting-time evidence.
- `deadlock_timeout = 1s` adalah recommended starting point untuk official test.
- `deadlock_timeout > 1s`: warning untuk observability resolution; > 5s: high evidence delay.
- Jangan menurunkan `deadlock_timeout` di bawah 500 ms tanpa controlled overhead test, karena deadlock checks dan lock-wait logging dapat menjadi lebih sering.
- `log_min_error_statement` harus memungkinkan statement pada severity `ERROR`; setting yang lebih restrictive daripada `ERROR` adalah critical evidence gap.
- `log_error_verbosity = terse`: warning karena diagnostic detail lebih sedikit; `default` atau `verbose` lebih sesuai untuk test evidence.
- `PostgreSQLLogs` Diagnostic Settings category harus dikirim ke Storage Account atau Log Analytics. Hal ini tidak dapat divalidasi melalui SQL dan harus masuk infrastructure pre-test checklist.

---

### 6.D Dictionary / reference queries (DICT)

Dictionary/reference queries are retained for collector metadata and data-quality mapping. They are not runtime performance metrics and do not produce threshold events by themselves.

## DICT01 — Wait Event Dictionary Snapshot for PostgreSQL 17+

### Purpose

Menyimpan official dictionary `wait_event_type`, `wait_event`, dan description dari versi PostgreSQL yang benar-benar digunakan.

Ini mencegah collector bergantung pada hard-coded event list yang dapat berubah antar major/minor version.

### Frequency

- sekali saat Azure Function start;
- setelah major/minor PostgreSQL version change;
- setelah failover ke server dengan version/build berbeda.

### Version requirement

PostgreSQL **17+**. Jalankan hanya jika:

```sql
SELECT to_regclass('pg_catalog.pg_wait_events') IS NOT NULL;
```

### Connection target

Salah satu application database.

### SQL

```sql
/* dms_metrics_collector:dict01 */
SELECT
    clock_timestamp() AS collected_at,
    current_setting('server_version_num')::integer AS server_version_num,
    type AS wait_event_type,
    name AS wait_event,
    description
FROM pg_wait_events
ORDER BY type, name;
```

### Threshold and validation

- Tidak ada performance threshold.
- Setiap `(wait_event_type, wait_event)` yang diamati oleh Q02 tetapi tidak ditemukan di dictionary: data-quality warning.
- Jika unmapped event mewakili > 1% active wait samples: high data-quality issue.
- PostgreSQL 18 menyediakan `pg_wait_events`, sehingga tidak diperlukan static fallback dictionary untuk versi lama.

---

### 6.E Azure Query Store / CPU and parallelism additions (Q19, AZ-SYS target)

---

## Q19 — Query Store Parallel-Plan Inventory

### Purpose

Mengambil plan yang disimpan oleh Azure PostgreSQL Query Store untuk mengidentifikasi:

- query dengan `Gather` or `Gather Merge`;
- parallel sequential/index scans;
- parallel hash/join/append nodes;
- planned worker count yang tertulis dalam plan;
- plan change pada query yang sama;
- parallel plan yang muncul pada high-frequency short OLTP queries.

Q19 memberi plan-shape evidence tanpa menjalankan `EXPLAIN ANALYZE` secara otomatis terhadap application query.

### Enablement

Set server parameter:

```text
pg_qs.query_capture_mode = top or all
pg_qs.store_query_plans = on
```

Query Store data hanya tersedia setelah window dipersist dan dibaca dari database `azure_sys`.

### Frequency

- setiap **phase boundary**;
- setiap 15 menit hanya jika plan-volume dan overhead terbukti aman;
- conditional ketika CPU ≥ 80% selama 15 menit;
- conditional ketika Q02 parallel-worker pool utilization ≥ 85%;
- sebelum dan sesudah parallelism tuning.

### Connection target

Database **`azure_sys`**.

### Bind parameters

- `$1` = analysis window start timestamp;
- `$2` = analysis window end timestamp;
- `$3` = top-N, recommended 200.

### SQL

```sql
/* dms_metrics_collector:q19 */
WITH runtime AS (
    SELECT
        q.db_id,
        q.query_id,
        q.plan_id,
        MIN(q.start_time) AS first_window_start,
        MAX(q.end_time) AS last_window_end,
        SUM(q.calls) AS query_store_calls,
        SUM(q.total_time) AS total_exec_time_ms,
        CASE
            WHEN SUM(q.calls) > 0
            THEN SUM(q.total_time) / SUM(q.calls)
        END AS mean_time_ms,
        MAX(q.max_time) AS max_time_ms,
        MAX(q.query_sql_text) AS query_sql_text
    FROM query_store.qs_view q
    WHERE q.end_time > $1::timestamp
      AND q.start_time < $2::timestamp
      AND NOT q.is_system_query
    GROUP BY
        q.db_id,
        q.query_id,
        q.plan_id
),
plans AS (
    SELECT
        p.plan_id,
        p.db_id,
        p.query_id,
        p.plan_text,
        p.plan_text LIKE '%Gather%' AS has_gather,
        p.plan_text LIKE '%Gather Merge%' AS has_gather_merge,
        p.plan_text LIKE '%Parallel Seq Scan%'
            AS has_parallel_seq_scan,
        p.plan_text LIKE '%Parallel Index Scan%'
            AS has_parallel_index_scan,
        p.plan_text LIKE '%Parallel Index Only Scan%'
            AS has_parallel_index_only_scan,
        p.plan_text LIKE '%Parallel Hash%'
            AS has_parallel_hash,
        p.plan_text LIKE '%Parallel Append%'
            AS has_parallel_append,
        NULLIF(
            substring(p.plan_text FROM 'Workers Planned: ([0-9]+)'),
            ''
        )::integer AS workers_planned_from_plan
    FROM query_store.query_plans_view p
)
SELECT
    clock_timestamp() AS collected_at,
    r.db_id,
    r.query_id,
    r.plan_id,
    r.first_window_start,
    r.last_window_end,
    r.query_store_calls,
    r.total_exec_time_ms,
    r.mean_time_ms,
    r.max_time_ms,
    p.has_gather,
    p.has_gather_merge,
    p.has_parallel_seq_scan,
    p.has_parallel_index_scan,
    p.has_parallel_index_only_scan,
    p.has_parallel_hash,
    p.has_parallel_append,
    p.workers_planned_from_plan,
    LEFT(r.query_sql_text, 6000) AS query_sql_text,
    LEFT(p.plan_text, 10000) AS plan_text
FROM runtime r
JOIN plans p
  ON p.db_id = r.db_id
 AND p.query_id = r.query_id
 AND p.plan_id = r.plan_id
WHERE p.has_gather
   OR p.has_parallel_seq_scan
   OR p.has_parallel_index_scan
   OR p.has_parallel_index_only_scan
   OR p.has_parallel_hash
   OR p.has_parallel_append
ORDER BY
    r.total_exec_time_ms DESC,
    r.query_store_calls DESC
LIMIT $3;
```

### Threshold and interpretation

- Any parallel plan is informational evidence, not automatically a problem.
- Parallel plan with `mean_time_ms < 50 ms` and `query_store_calls > 1,000` in a 15-minute window: warning review trigger for possibly unnecessary parallel startup/coordination overhead.
- `workers_planned_from_plan ≥ 4` on a high-frequency OLTP query: warning; correlate with Q02 parallel-worker pool concentration and k6 P99.
- Same query has more than one `plan_id` at the same load level: warning for plan variability; high if mean time differs by > 2×.
- A plan changes from nonparallel to parallel after tuning while throughput decreases or P99 increases: high regression evidence.
- A parallel plan decreases single-query latency but reduces total successful RPS at 2000 users: treat as workload-level regression.
- Query Store `calls` for a parallel query can include leader and worker contributions and must not always be interpreted as business-execution count.
- Q19 plan is normalized `EXPLAIN` text, not `EXPLAIN ANALYZE`; it does not prove actual workers launched. Use Q02 or PostgreSQL 18 counters in Q05.


### 6.F PgBouncer connection-pool commands (PB01–PB06)

PgBouncer commands are collected from the PgBouncer admin console database
`pgbouncer`, normally on port `6432` for Azure Database for PostgreSQL Flexible
Server built-in PgBouncer. These commands complement PostgreSQL Q02 connection
snapshots. They do not replace Q02 because Q02 is the source of truth for backend
connections that actually reached PostgreSQL.

## PB01 — PgBouncer Capability and Configuration Snapshot

### Purpose

Menentukan apakah PgBouncer admin console bisa diakses dan menangkap konfigurasi
yang memengaruhi analisa connection pool before-after, termasuk:

- PgBouncer version;
- effective runtime configuration;
- database-level pool size, reserve pool, pool mode, client/server limits;
- user-level pool override and current client/server connection count;
- paused/disabled database state.

### Frequency

- saat Azure Function start;
- setiap 6 jam;
- setelah PgBouncer enablement/configuration change;
- setelah database scale, restart, failover, atau PgBouncer restart;
- tepat sebelum baseline direct-connection run dan PgBouncer optimized run.

### Connection target

PgBouncer admin console database `pgbouncer`, port `6432`, using a stats/admin
user allowed by `pgbouncer.stats_users` or equivalent PgBouncer admin configuration.

### SQL

```sql
/* dms_metrics_collector:pb01_version */
SHOW VERSION;
```

```sql
/* dms_metrics_collector:pb01_config */
SHOW CONFIG;
```

```sql
/* dms_metrics_collector:pb01_databases */
SHOW DATABASES;
```

```sql
/* dms_metrics_collector:pb01_users */
SHOW USERS;
```

### Threshold / validation

Critical pre-test validation failure if:

- connection to database `pgbouncer` on port `6432` fails when PgBouncer is in scope;
- collector user is not allowed to run `SHOW` commands;
- `SHOW CONFIG` does not expose expected PgBouncer settings;
- expected application database is missing from `SHOW DATABASES`;
- `paused = 1` or `disabled = 1` for an application database during official test;
- `pool_mode` differs from the approved test design;
- `max_client_conn` is lower than expected peak client connections;
- per-database `pool_size + reserve_pool_size` multiplied by active database/user pools can exceed the PostgreSQL `max_connections` budget after reserving room for admin, monitoring, autovacuum, replication, and emergency access.

Configuration values to store at minimum:

```text
pgbouncer.enabled
pgbouncer.default_pool_size
pgbouncer.min_pool_size
pgbouncer.reserve_pool_size if exposed
pgbouncer.max_client_conn
pgbouncer.max_prepared_statements
pgbouncer.pool_mode
pgbouncer.query_wait_timeout
pgbouncer.server_idle_timeout
pgbouncer.stats_users
pool_size from SHOW DATABASES
min_pool_size from SHOW DATABASES
reserve_pool_size from SHOW DATABASES
max_connections from SHOW DATABASES
max_client_connections from SHOW DATABASES
pool_size / reserve_pool_size / pool_mode override from SHOW USERS
```

---

## PB02 — PgBouncer Pool Runtime Snapshot

### Purpose

Mengambil runtime state per pool dari `SHOW POOLS` untuk melihat:

- client connections aktif;
- client connections yang menunggu server connection;
- server connections active/idle/used/tested/login;
- pool mode yang sedang berlaku;
- `maxwait`, yaitu oldest queued client wait time;
- apakah PgBouncer benar-benar mengurangi backend connection yang masuk ke PostgreSQL.

### Frequency

Setiap **15 detik** selama:

- direct baseline jika PgBouncer sudah aktif tetapi belum dipakai aplikasi;
- PgBouncer before-after test;
- official 8-hour run jika aplikasi memakai PgBouncer;
- 30–60 minute cool-down.

### Connection target

PgBouncer admin console database `pgbouncer`, port `6432`.

### SQL

```sql
/* dms_metrics_collector:pb02_pools */
SHOW POOLS;
```

### Derived metrics

Compute per pool and all-pool aggregate:

```text
pgbouncer_client_connections =
    cl_active + cl_waiting + cl_active_cancel_req + cl_waiting_cancel_req

pgbouncer_server_connections =
    sv_active + sv_idle + sv_used + sv_tested + sv_login
    + sv_active_cancel + sv_being_canceled

pgbouncer_server_busy_connections =
    sv_active + sv_used + sv_tested + sv_login
    + sv_active_cancel + sv_being_canceled

server_pool_not_immediately_idle_percent =
    100 * pgbouncer_server_busy_connections
    / NULLIF(pgbouncer_server_connections, 0)

oldest_client_wait_seconds =
    maxwait + (maxwait_us / 1000000.0)

pool_queue_depth = cl_waiting

client_to_server_pooling_ratio =
    pgbouncer_client_connections
    / NULLIF(pgbouncer_server_connections, 0)

postgres_connection_shielding_percent =
    100 * (1 - postgresql_client_connections_from_q02
              / NULLIF(pgbouncer_client_connections, 0))
```

For before-after comparison:

```text
postgres_backend_connection_reduction_percent =
    100 * (baseline_direct_postgres_client_connections_p95
           - after_pgbouncer_postgres_client_connections_p95)
    / NULLIF(baseline_direct_postgres_client_connections_p95, 0)
```

### Threshold and interpretation

- `cl_waiting > 0` for two consecutive samples: warning.
- `cl_waiting > 0` for 5 minutes: high pool queue evidence.
- `oldest_client_wait_seconds > 1`: warning.
- `oldest_client_wait_seconds > 5`: high.
- `oldest_client_wait_seconds > 30`: critical, especially if k6 P99 or error rate rises.
- `sv_idle = 0` and `cl_waiting > 0`: high evidence of pool exhaustion or slow PostgreSQL response.
- `server_pool_not_immediately_idle_percent >= 90%` for 5 minutes: high; if paired with queueing, treat as critical pool saturation evidence.
- `client_to_server_pooling_ratio` near 1 during high client concurrency means PgBouncer is not multiplexing effectively. This can be normal in session pooling, but it weakens the value of PgBouncer for connection reduction.
- PgBouncer success is not proven by lower PostgreSQL connections alone; k6 throughput, GraphQL P90/P99, DB execution time, and error rate must not materially regress.

---

## PB03 — PgBouncer Statistics Snapshot

### Purpose

Mengambil cumulative PgBouncer counters and stat-period averages untuk melihat:

- transaction and query throughput handled by PgBouncer;
- bytes received/sent;
- server assignment rate;
- transaction and query time seen by PgBouncer;
- client wait time before server assignment;
- prepared-statement parse/bind behavior when `max_prepared_statements` is used.

### Frequency

Setiap **60 detik** selama PgBouncer test and cool-down. Capture full snapshot at
phase boundary and before/after PgBouncer configuration changes.

### Connection target

PgBouncer admin console database `pgbouncer`, port `6432`.

### SQL

```sql
/* dms_metrics_collector:pb03_stats */
SHOW STATS;
```

```sql
/* dms_metrics_collector:pb03_stats_totals */
SHOW STATS_TOTALS;
```

```sql
/* dms_metrics_collector:pb03_stats_averages */
SHOW STATS_AVERAGES;
```

```sql
/* dms_metrics_collector:pb03_totals */
SHOW TOTALS;
```

### Derived metrics

For cumulative columns, compute deltas between samples. Do not calculate deltas
across PgBouncer restart or counter decrease.

```text
pgbouncer_xact_per_second = delta_total_xact_count / elapsed_seconds
pgbouncer_query_per_second = delta_total_query_count / elapsed_seconds
server_assignment_per_second = delta_total_server_assignment_count / elapsed_seconds
received_bytes_per_second = delta_total_received / elapsed_seconds
sent_bytes_per_second = delta_total_sent / elapsed_seconds
wait_ms_per_assigned_client = delta_total_wait_time / 1000.0
                              / NULLIF(delta_total_server_assignment_count, 0)
query_ms_per_query = delta_total_query_time / 1000.0
                     / NULLIF(delta_total_query_count, 0)
xact_ms_per_xact = delta_total_xact_time / 1000.0
                   / NULLIF(delta_total_xact_count, 0)
server_assignment_per_xact = delta_total_server_assignment_count
                             / NULLIF(delta_total_xact_count, 0)
```

`SHOW STATS` average columns are updated according to PgBouncer `stats_period`.
Store both raw PgBouncer averages and derived collector-interval deltas so the
final report can explain any difference between PgBouncer's stat period and the
collector's 60-second cadence.

### Threshold and interpretation

- `avg_wait_time / 1000.0 > 5 ms` for 5 minutes: warning.
- `avg_wait_time / 1000.0 > 20 ms` for 5 minutes: high.
- `avg_wait_time / 1000.0 > 100 ms`: critical if correlated with PB02 queueing or k6 latency.
- `wait_ms_per_assigned_client` increasing > 2× same-load baseline: warning regression.
- `pgbouncer_xact_per_second` flat while k6 offered load increases and `cl_waiting` rises: high pool or PostgreSQL bottleneck evidence.
- `server_assignment_per_xact` interpretation depends on pool mode. In transaction pooling it can be close to one assignment per transaction; in session pooling it can be lower. Do not compare this metric across different `pool_mode` without labeling the change.
- PgBouncer `total_query_time` and `total_xact_time` are PgBouncer-observed timing, not a replacement for PostgreSQL Q05 execution statistics or k6 GraphQL response time.

---

## PB04 — PgBouncer Client Diagnostic Snapshot

### Purpose

Menangkap detail client connection saat pool wait, timeout, reconnect storm, or
application-side connection behavior perlu dianalisis.

This command is intentionally conditional because `SHOW CLIENTS` can be large
when many app pods or virtual users connect to PgBouncer.

### Frequency

Conditional:

1. immediately when PB02 reports `cl_waiting > 0` or `maxwait > 0`;
2. every 15 seconds while client queue persists;
3. during ramp-up 400 → 800 and 800 → 2000 if connection storm is suspected;
4. during scaling/failover/restart event;
5. one final sample after queue clears.

### Connection target

PgBouncer admin console database `pgbouncer`, port `6432`.

### SQL

```sql
/* dms_metrics_collector:pb04_clients */
SHOW CLIENTS;
```

### Derived metrics

```text
waiting_client_count = count(*) where state = 'waiting'
active_client_count = count(*) where state = 'active'
idle_client_count = count(*) where state = 'idle'
max_client_wait_seconds = max(wait + wait_us / 1000000.0)
client_count_by_application_name = count(*) group by application_name
client_count_by_addr = count(*) group by addr
waiting_client_count_by_application_name = count(*) where state='waiting' group by application_name
```

### Threshold and interpretation

- Any `state = 'waiting'` row is worth persisting.
- `max_client_wait_seconds > 1`: warning.
- `max_client_wait_seconds > 5`: high.
- `max_client_wait_seconds > 30`: critical, especially if near `pgbouncer.query_wait_timeout`.
- Many waiting clients from one `application_name`, pod, or address can indicate application-side pool storm or uneven traffic distribution.
- Empty or generic `application_name` is a data-quality warning because it weakens root-cause attribution. If possible, set application connection string to include service/pod/component identity.
- `state = 'idle'` client count can be high without being a database bottleneck. Interpret it with PB02 server connections and Q02 PostgreSQL backend connections.

Security note:

`SHOW CLIENTS` includes client address and application name. Treat it as operational
metadata; do not expose raw client details in an executive report unless approved.

---

## PB05 — PgBouncer Server Diagnostic Snapshot

### Purpose

Menangkap detail server-side connections from PgBouncer to PostgreSQL during:

- pool saturation;
- server connection churn;
- scaling/failover/restart;
- `close_needed` transition after reload/reconnect;
- suspected mismatch between PgBouncer server connections and PostgreSQL Q02 backend connections.

### Frequency

Conditional:

1. when PB02 reports `sv_idle = 0` and `cl_waiting > 0`;
2. when PB02 server pool not-immediately-idle percent ≥ 90%;
3. when PB03 wait time increases materially;
4. during scale/failover/restart event;
5. after PgBouncer `RELOAD`/configuration change if applicable;
6. every 15 seconds while the condition persists, then one final sample.

### Connection target

PgBouncer admin console database `pgbouncer`, port `6432`.

### SQL

```sql
/* dms_metrics_collector:pb05_servers */
SHOW SERVERS;
```

### Derived metrics

```text
server_count_by_state = count(*) group by state
server_active_count = count(*) where state = 'active'
server_idle_count = count(*) where state = 'idle'
server_used_count = count(*) where state = 'used'
server_new_count = count(*) where state = 'new'
server_close_needed_count = count(*) where close_needed = 1
server_count_by_database_user = count(*) group by database, user
server_count_by_application_name = count(*) group by application_name
```

Join or correlate with Q02 using:

```text
PB05.remote_pid <-> Q02.pid / pg_stat_activity.pid when available and valid
PB05.application_name <-> Q02.application_name
PB05.database/user <-> Q02.datname/usename
```

### Threshold and interpretation

- `server_idle_count = 0` with `cl_waiting > 0`: high pool saturation evidence.
- `server_new_count > 0` for more than 30 seconds: warning; > 5 minutes is high and can indicate slow authentication/server connection establishment.
- `close_needed_count > 0` after configuration reload/reconnect is informational; if it persists through a full sustain window, high configuration-transition evidence.
- A large number of server connections mapped to one database/user can indicate hot pool concentration.
- PB05 server connections should reconcile approximately with PostgreSQL backend connections visible through Q02/Azure active connections, allowing for timing differences and admin/monitoring sessions.

Security note:

`SHOW SERVERS` can include backend PID, address, and application name. Store raw
values in the metrics database, but mask them in stakeholder-facing reports if required.

---

## PB06 — PgBouncer Internal Lists, Memory, and State Snapshot

### Purpose

Mengambil low-frequency PgBouncer process state and internal object counts untuk
menganalisis:

- PgBouncer process state;
- number of pools, databases, users;
- used/free client and server objects;
- login clients;
- DNS cache activity;
- approximate internal memory allocation if `SHOW MEM` is available.

PB06 is not the primary pool-saturation signal. PB02 and PB03 are the primary
pool-runtime sources. PB06 gives process-level context.

### Frequency

- every **5 minutes** during PgBouncer test;
- every phase boundary;
- after PgBouncer restart/reload/configuration change;
- conditional when PB02/PB03 indicate pool queueing or connection churn.

### Connection target

PgBouncer admin console database `pgbouncer`, port `6432`.

### SQL

```sql
/* dms_metrics_collector:pb06_lists */
SHOW LISTS;
```

```sql
/* dms_metrics_collector:pb06_state */
SHOW STATE;
```

```sql
/* dms_metrics_collector:pb06_mem */
SHOW MEM;
```

### Derived metrics

```text
used_client_object_percent =
    100 * used_clients / NULLIF(used_clients + free_clients, 0)

used_server_object_percent =
    100 * used_servers / NULLIF(used_servers + free_servers, 0)

pool_count = pools
login_clients = login_clients
inflight_dns_queries = dns_queries
```

For `SHOW MEM`, preserve raw rows because the output is low-level and can change
between PgBouncer versions. Use same-load baseline comparison rather than a fixed
absolute threshold.

### Threshold and interpretation

- PgBouncer state not equal to `active` during official run: critical unless this is an intentional PAUSE/SUSPEND test event.
- `login_clients > 0` for two consecutive 5-minute samples: warning; if paired with app connection errors, high.
- `used_client_object_percent >= 85%`: warning if near `max_client_conn`; >= 95% is high.
- `dns_queries > 0` persistently during connection churn: warning context; correlate with logs and network/DNS behavior.
- `SHOW MEM` growth > 2× same-load baseline: warning; > 5× is high, but confirm with PgBouncer logs and host/process metrics before calling it a leak.

---

## 7. Azure Monitor Slow-Path Metrics


The following metrics are not collected by SQL queries and should be summarized from files in Storage Account every two hours.

| Metric ID | Native grain | Main use | Proposed threshold |
|---|---:|---|---|
| `cpu_percent` | 1 minute | Actual server CPU utilization | See Section 4.1 |
| `memory_percent` | 1 minute | Actual server memory utilization | See Section 4.1 |
| `disk_iops_consumed_percentage` | 1 minute; up to 5-minute visibility delay | IOPS saturation | 70/80/90% |
| `disk_bandwidth_consumed_percentage` | 1 minute; up to 5-minute visibility delay | Throughput saturation | 70/80/90% |
| `disk_queue_depth` | 1 minute | Outstanding disk operations | Baseline-relative |
| `read_iops` / `write_iops` | 1 minute; up to 5-minute visibility delay | Read/write operation rate | Compare to provisioned or autoscale capacity |
| `read_throughput` / `write_throughput` | 1 minute; up to 5-minute visibility delay | Disk bytes/sec | Compare to bandwidth capacity |
| `active_connections` | 1 minute | Total connections, all states | Correlate with Q02 |
| `connections_failed` | 1 minute | Connection failure | Any increase during scale/ramp-up is high |
| `storage_percent` | 1 minute | Capacity safety | Warning ≥ 70%, high ≥ 80%, critical ≥ 90% |
| `storage_free` | 1 minute | Remaining bytes | Must be interpreted together with growth rate |
| `sessions_by_state` | 1 minute, enhanced | State count | Optional validation of Q02 |
| `sessions_by_wait_event_type` | 1 minute, enhanced | Wait class count | Optional validation of Q02/Q11 |
| `longest_query_time_sec` | 1 minute, enhanced | Long query | Optional validation of Q02 |
| `longest_transaction_time_sec` | 1 minute, enhanced | Long transaction | Optional validation of Q02 |
| `deadlocks` | 1 minute, enhanced | Deadlock count | Any delta > 0 |
| `temp_bytes` / `temp_files` | 1 minute, enhanced | Temp spill | Optional validation of Q04/Q05 |
| `postmaster_process_cpu_usage_percent` | 1 minute | PostgreSQL postmaster process CPU | **Not exportable by Diagnostic Settings**; optional Metrics API exception only |
| `client_connections_active` | 1 minute | PgBouncer active client connections | Correlate with PB02 `cl_active` |
| `client_connections_waiting` | 1 minute | PgBouncer waiting client connections | Any sustained value > 0 is warning/high depending on duration |
| `server_connections_active` | 1 minute | PgBouncer active server connections to PostgreSQL | Correlate with PB02 `sv_active` and Q02 backend count |
| `server_connections_idle` | 1 minute | PgBouncer idle server connections | `0` with waiting clients is high |
| `total_pooled_connections` | 1 minute | PgBouncer total pooled connections | Before-after connection shielding evidence |
| `num_pools` | 1 minute | Number of PgBouncer pools | Context for pool sizing and database/user fan-out |

### 6.1 Two-hour summarizer output

For each 1-minute metric series and each test phase, compute:

- MIN;
- MAX;
- AVG;
- P50;
- P90;
- P95;
- P99;
- number of samples;
- total minutes above 70%, 80%, 90%;
- longest continuous duration above each threshold;
- first and last threshold breach timestamps;
- linear trend/slope for memory during each sustain phase.

Do not collapse the raw 1-minute series into only one two-hour average before phase analysis.

---

## 8. Phase Boundary Capture


Write an explicit phase marker into the metrics database at these timestamps:

| Boundary | Test time |
|---|---:|
| Test start / ramp-up 0 → 400 | 00:00 |
| Sustain 400 start | 00:15 |
| Ramp-up 400 → 800 start | 02:00 |
| Sustain 800 start | 02:15 |
| Ramp-up 800 → 2000 start | 04:00 |
| Sustain 2000 start | 04:30 |
| Ramp-down start | 07:45 |
| Official test end | 08:00 |
| Cool-down end | 08:30–09:00 |

At each boundary:

- run full Q05 snapshot;
- run Q06, Q09, and Q10;
- preserve exact timestamp from the k6 controller;
- store scale/failover events as separate markers rather than inferring them from metric changes.

If PgBouncer is in scope for the run:

- run PB01 at test start and after any PgBouncer configuration/restart/failover event;
- run PB02/PB03 phase-boundary snapshots even if their regular cadence is active;
- run PB04/PB05 at the start and end of each ramp-up if connection storm is suspected;
- mark the timestamp when application traffic is switched from direct PostgreSQL port `5432` to PgBouncer port `6432`.

---

## 9. Derived Cross-Source Metrics — No Additional PostgreSQL Query

The following metrics use output already collected by Azure Monitor, Q02, Q05, Q19,
and k6, and must be computed in the metrics collection database or two-hour
summarizer rather than by a new poll.

### 9.1 CPU demand proxy per vCore

Collector configuration must store the provisioned database vCore count. For the RFP baseline this is expected to be 48 cores, but the value must be updated after scale events.

```text
running_demand_per_vcore =
    active_cpu_or_running_sessions / provisioned_vcores
```

Interpretation:

- ≥ 0.70 for 5 minutes together with CPU ≥ 70%: warning.
- ≥ 1.00 for 5 minutes together with CPU ≥ 80%: high CPU-demand evidence.
- ≥ 1.50 together with CPU ≥ 90%, throughput plateau, or P99 regression: critical saturation evidence.

`active_cpu_or_running_sessions` remains a proxy. A backend with no wait event is running or not currently reporting a wait; it is not an exact runnable-queue counter.

### 9.2 CPU efficiency

```text
successful_rps_per_cpu_percent =
    successful_requests_per_second / cpu_percent
```

Compare only:

- same application version;
- same k6 script;
- same data state or documented data delta;
- same user load and phase;
- same server size unless normalized per vCore.

Interpretation:

- > 10% decline from same-load baseline for 15 minutes: warning.
- > 20% decline: high efficiency regression.
- Improvement in single-query runtime that reduces successful RPS/CPU efficiency is not a successful workload tuning result.

### 9.3 Parallelism benefit test

For each tested configuration:

```text
parallelism_benefit =
    change in successful RPS,
    GraphQL P90/P99,
    DB execution time,
    CPU percent,
    CPU efficiency,
    worker-pool utilization,
    worker-launch fulfillment,
    and error rate
```

A configuration is considered better only when workload-level result improves without creating material regression elsewhere.

Recommended result rules:

- **Positive:** successful RPS improves ≥ 5% or P99 improves ≥ 10%, with error rate unchanged and CPU headroom not materially worse.
- **Neutral:** improvement < 5% and no material regression; prefer simpler/conservative configuration.
- **Negative:** P99 worsens ≥ 10%, successful RPS decreases ≥ 5%, worker pool saturates, or error rate increases.

These are proposed engineering interpretation bands, not official RFP pass/fail thresholds.


### 9.4 PgBouncer before-after effectiveness

These metrics use PB02/PB03/PB04/PB05, PostgreSQL Q02, Azure Monitor connection
metrics, and k6 output. They are derived downstream and do not require additional
polling commands.

```text
postgres_backend_connection_reduction_percent =
    100 * (baseline_direct_postgres_client_connections_p95
           - after_pgbouncer_postgres_client_connections_p95)
    / NULLIF(baseline_direct_postgres_client_connections_p95, 0)

pgbouncer_multiplexing_ratio =
    pgbouncer_client_connections_p95
    / NULLIF(pgbouncer_server_connections_p95, 0)

queue_free_sample_percent =
    100 * count(samples where cl_waiting = 0)
    / total_pgbouncer_pool_samples

pool_wait_p99_seconds =
    p99(oldest_client_wait_seconds from PB02)

pgbouncer_latency_regression_percent =
    100 * (after_pgbouncer_k6_p99 - baseline_direct_k6_p99)
    / NULLIF(baseline_direct_k6_p99, 0)

connection_shielding_efficiency =
    postgres_backend_connection_reduction_percent
    - max(0, pgbouncer_latency_regression_percent)
```

Recommended interpretation:

- **Positive PgBouncer outcome:** PostgreSQL backend P95 connection count drops at least 50%, `queue_free_sample_percent >= 99%`, `pool_wait_p99_seconds <= 1`, and k6 P99 does not regress more than 10% at the same load.
- **Neutral:** PostgreSQL backend connections drop, but throughput/latency is unchanged and there is no meaningful queueing. Keep PgBouncer if operational simplicity or failover/reconnect behavior improves.
- **Negative:** PgBouncer reduces PostgreSQL connections but introduces sustained `cl_waiting`, `maxwait`, timeout, or P99 regression. Tune pool size/mode/application pool behavior before using it for the official optimized result.

Do not compare direct baseline and PgBouncer run unless the k6 script, data state,
server size, phase schedule, app version, and observability overhead are the same
or explicitly documented.


---

## 10. Non-SQL Evidence Requirements

### 10.1 Deadlocks cannot be fully covered by a polling query

Kombinasi berikut tetap wajib:

1. Q04 `delta_deadlocks` untuk authoritative cumulative deadlock count;
2. W02 untuk blocking pairs dan actual lock-wait duration yang masih aktif;
3. PostgreSQL logs untuk exact deadlock pair, statement, timestamp, dan event yang selesai sebelum sampler menangkapnya;
4. k6/application error logs untuk dampak deadlock terhadap business transaction.

`deadlock waiting time` pada final report harus diberi definisi eksplisit. Recommended interpretation:

- untuk ordinary blocking: actual wait dari `pg_locks.waitstart`;
- untuk deadlock: time from first observable/logged wait until deadlock resolution/error, jika timeline tersedia;
- jangan menggunakan query age sebagai lock-wait duration.


### 10.2 PgBouncer logs and application connection errors

PgBouncer admin-console snapshots are not enough to explain every connection
failure. For PgBouncer runs, collect out-of-band evidence from:

1. Azure PgBouncer logs, for authentication failure, connection lifecycle, pool
   exhaustion, server disconnect, and PgBouncer restart/failover events;
2. application logs for connection acquisition timeout, pool timeout, retry storm,
   prepared-statement incompatibility, or transaction-pooling compatibility issues;
3. k6 error logs for GraphQL/API failures that align with PB02 queueing or PB03 wait time;
4. Azure Monitor PgBouncer metrics as 1-minute validation for PB02/PB03 trend.

For final report, separate:

- PgBouncer client waiting time;
- PostgreSQL lock/query wait time;
- application-side connection pool wait time;
- network/DNS/authentication failures.

These are different waits and should not be summed unless the timeline proves they
belong to the same request path.


---

## 11. Collector Failure and Overlap Rules


1. A collector failure must not fail the performance test workload.
2. Do not retry a failed metrics query aggressively. Recommended retry: one retry after 2 seconds, then wait for the next scheduled cycle.
3. Do not allow two executions of the same query ID for the same server to overlap.
4. Keep one database connection per target database per polling cycle, not one connection per result row.
5. Use connection pooling conservatively; the collector must not materially increase server connections.
6. Add jitter of 0–2 seconds for 30/60-second queries so all collector queries do not start at exactly the same millisecond.
7. Record collector query duration. Warning if a regular collector query takes > 2 seconds; high if > 5 seconds.
8. If Q09/Q10 become expensive because of a very large object count, reduce their cadence before reducing Q02/Q04/Q07 resolution.


9. The conditional/low-frequency additions (Q14, Q15, Q17, DICT01, Q19, PB04, PB05, PB06) must never pre-empt the 15-second Q02 or the 30/60-second counter cycles; if the host is constrained, defer them rather than the high-resolution path.
10. Q19 and the PgBouncer diagnostic queries PB04/PB05/PB06 are gated by the conditions in their own sections; do not promote them to unconditional high-frequency polling.

---

## 12. Destination Data Model — Minimum Keys


Recommended logical tables:

| Table | Natural key / important columns |
|---|---|
| `collector_run` | server_id, query_id, scheduled_at, started_at, completed_at, status, row_count, duration_ms, error |
| `activity_snapshot` | server_id, collected_at; summary columns + JSON interesting_sessions |
| `blocking_event` | server_id, collected_at, blocked_pid, blocking_pid |
| `database_stats` | server_id, datid, collected_at, stats_reset |
| `query_stats` | server_id, dbid, userid, queryid, toplevel, collected_at |
| `query_dictionary` | server_id, dbid, userid, queryid, normalized_query_text, first_seen, last_seen |
| `pgss_info` | server_id, collected_at, stats_reset |
| `io_stats` | server_id, backend_type, object, context, collected_at, stats_reset |
| `writer_wal_stats` | server_id, source_view, collected_at, payload |
| `table_stats` | server_id, database_name, relid, collected_at |
| `index_stats` | server_id, database_name, indexrelid, collected_at |
| `query_store_waits` | server_id, start_time, end_time, db_id, user_id, query_id, event_type, event |
| `maintenance_progress` | server_id, database_name, operation_type, pid, collected_at |
| `azure_metric_raw` | resource_id, metric_id, dimension_key, time_utc |
| `test_phase_marker` | test_run_id, phase_name, start_time, end_time, user_load |

Preserve raw cumulative values and compute deltas in a derived layer. Do not overwrite raw samples with only rates.


Additional tables and columns required by the merged coverage:

| Table | Natural key / important columns |
|---|---|
| `activity_snapshot` (extend) | add active_sessions_by_wait_event, active_waits_by_query_event, system_backend_waits, wait_query_group_count, active_parallel_workers, parallel_worker_pool_utilization_percent, active_parallel_query_groups, worker wait counts |
| `blocking_event` (extend) | add waitstart, actual_lock_wait_seconds, direct_blocker_count, maximum_chain_depth |
| `query_stats` (extend) | add planning counters, JIT counters/time, PostgreSQL 18 parallel_workers_to_launch/launched, stats_since |
| `query_store_waits` (extend) | add runtime_calls, execution time, block stats, query text, wait classifications and shares |
| `slru_stats` | server_id, name, collected_at, all cumulative counters, stats_reset |
| `wait_event_dictionary` | server_id/version, wait_event_type, wait_event, description |
| `collector_configuration` | server_id, collected_at, scope, role/database/routine override, capability flags, CPU/parallel/JIT/memory/logging settings, validation status |
| `buffercache_summary` | server_id, collected_at, buffers_used/unused/dirty/pinned, usagecount_avg, usage distribution |
| `buffercache_top_relations` | server_id, database_name, relation_oid, collected_at, cached_buffers, cached_bytes, shares |
| `shmem_allocations` | server_id, allocation_name, collected_at, size, allocated_size, shares |
| `backend_memory_context_log` | server_id, target_pid, collected_at, parsed used_bytes per context (from PostgreSQLLogs) |
| `query_plan_inventory` | server_id, db_id, query_id, plan_id, plan flags, workers_planned, plan_text, first/last seen |
| `server_capacity_history` | server_id, effective_from/to, provisioned_vcores, memory, SKU, scale event ID |
| `phase_summary` | test_run_id, phase, CPU-demand/vCore, RPS/CPU, worker utilization, JIT share, planning share |

For PgBouncer samples, include:

```text
pgbouncer_instance_id
pgbouncer_host
pgbouncer_port
pgbouncer_database = 'pgbouncer'
source_command = PB01/PB02/PB03/PB04/PB05/PB06
pool_database
pool_user
pool_mode
client_state
server_state
application_name
phase_id
run_id
source_collected_at
collector_received_at
```

Preserve raw cumulative values and compute deltas in a derived layer. Do not
overwrite raw samples with only rates.

---

## 13. Permissions and Enablement


Recommended monitoring user:

```sql
CREATE ROLE dms_monitoring LOGIN;
GRANT pg_monitor TO dms_monitoring;
```

`pg_monitor` includes read access to many monitoring views through built-in monitoring roles. Validate that the role can see query text and statistics for other users. Query Store views in `azure_sys` may require explicit access depending on the Azure configuration.

Pre-test configuration to validate:

- `track_activities = on`;
- `track_counts = on`;
- `track_io_timing = on` if I/O timing is required;
- `track_wal_io_timing = on` if WAL write/sync timing is required;
- `pg_stat_statements` allowed in `azure.extensions`, extension created in each required database, and server-side preload/configuration active;
- `compute_query_id = auto` or `on`;
- `pg_qs.query_capture_mode = top` or `all`;
- `pgms_wait_sampling.query_capture_mode = all`;
- `metrics.collector_database_activity = on` if enhanced Azure metrics are also required.

Do not enable Query Store on Burstable tier; Microsoft documents potential performance impact on that tier. The RFP target is a large 48-core database and should not be Burstable.


Additional enablement required by the merged coverage:

- `pgms_wait_sampling.query_capture_mode = all` and a defined `pgms_wait_sampling.history_period` for Q02/Q11 wait sampling.
- `pg_stat_statements.track_planning = on` only if planning overhead in Q05 is required; validate its overhead under load before the official test.
- `track_wal_io_timing = on` if WAL write/sync timing is required in Q08.
- `pg_buffercache` listed in `azure.extensions` and created on one designated database for Q13/Q14:

```sql
CREATE EXTENSION IF NOT EXISTS pg_buffercache;
```

  The collector role uses `pg_monitor`; it must not be granted buffer-eviction functions.
- For Q19 plan inventory: `pg_qs.query_capture_mode = top` or `all`, and `pg_qs.store_query_plans = on`. Query Store data is read from database `azure_sys` after the window persists.
- Former Q16 backend memory-context logging is retired as EQ01 and is not part of the implemented collector. Do not require `GRANT EXECUTE` on `pg_log_backend_memory_contexts(integer)` for the official test. If future manual DBA-controlled forensic execution is approved and possible, label that evidence as EQ01.
- `PostgreSQLLogs` Diagnostic Settings category must be sent to Storage Account or Log Analytics for deadlock, lock-wait, and temp-file evidence. This cannot be validated through SQL and belongs on the infrastructure pre-test checklist.


PgBouncer enablement required when PgBouncer is in scope:

- Azure built-in PgBouncer enabled through `pgbouncer.enabled = true`.
- Application connection string points to port `6432` for the PgBouncer run, while direct PostgreSQL baseline uses port `5432` unless the baseline explicitly includes PgBouncer pass-through.
- `pgbouncer.stats_users` includes the collector user or a dedicated statistics user allowed to connect to database `pgbouncer`.
- `metrics.pgbouncer_diagnostics = on` if Azure Monitor PgBouncer metrics are required in the slow path.
- PgBouncer logs are exported through Diagnostic Settings when PgBouncer pool behavior is part of final evidence.
- Collector driver is validated against the PgBouncer admin console and can execute `SHOW` commands successfully.
- PgBouncer pool mode and prepared-statement compatibility are approved before the official run.


---

## 14. Conditional-Only and Deliberately Excluded Queries

This section separates query logic that is intentionally not part of the standard collector cadence from query logic that has been retired/removed from the implemented Q-series.

### 14.1 Retired / Excluded Query Registry

| Excluded code | Former code | Status | Reason | Replacement evidence |
|---|---|---|---|---|
| **EQ01** | Former **Q16** / legacy **M05** | Retired from implemented collector | Azure Flexible Server customer admin may not be able to grant or execute `pg_log_backend_memory_contexts(integer)`; function output goes to PostgreSQLLogs rather than SQL result rows | Azure `memory_percent`, Q02, Q05, Q13, Q14, Q15 |

### EQ01 — Excluded Backend Memory-Context Logging (formerly Q16)

### Purpose

Meminta PostgreSQL menulis memory-context tree milik satu target backend ke
PostgreSQL log.

Ini adalah forensic mechanism untuk:

- suspected backend memory leak;
- satu long-running query/session yang dicurigai menahan memory besar;
- memory yang terus meningkat saat load konstan, tetapi temp spill, cache, connection count, dan I/O tidak menjelaskan kenaikan tersebut.

EQ01 is not an implemented collector query. It is retained only as retired forensic reference. It was formerly Q16 and is deliberately excluded from the standard Q-series for Azure Database for PostgreSQL Flexible Server.

The function produces one log message per memory context and can generate large log volume. The SQL below is reference-only and must not be included in the Azure Function collector.

### Reference-only triggering condition

Do not schedule this in the collector. Historical/manual use would only be considered if one of the following conditions occurs and DBA-controlled execution is explicitly approved:

- Azure `memory_percent ≥ 90%` selama 15 menit dan masih meningkat;
- Azure `memory_percent ≥ 95%` selama 5 menit;
- memory slope > 5 percentage-points/hour selama stable sustain;
- memory tidak plateau sampai akhir sustain dan Q02 menunjukkan backend yang sama tetap aktif/idle-in-transaction dalam waktu panjang.

Rate limit:

- maksimum 3 target PID per incident;
- maksimum satu invocation per PID setiap 15 menit;
- jangan invoke pada seluruh connection secara massal.

### PostgreSQL version target

PostgreSQL **18** only.

### Reference-only connection target

Salah satu application database. Target PID dapat berasal dari database mana pun
pada server, tetapi the executing role must have privilege to run the function. On Azure Flexible Server this is expected to be unavailable for customer-admin collector operation.

### SQL capability check

```sql
/* dms_metrics_collector:eq01_capability_reference_only */
SELECT
    clock_timestamp() AS collected_at,
    current_setting('server_version_num')::integer AS server_version_num,
    to_regprocedure(
        'pg_catalog.pg_log_backend_memory_contexts(integer)'
    ) IS NOT NULL AS function_exists,
    has_function_privilege(
        current_user,
        'pg_catalog.pg_log_backend_memory_contexts(integer)',
        'EXECUTE'
    ) AS can_execute;
```

### SQL invocation

Bind `$1` dengan target backend PID.

```sql
/* dms_metrics_collector:eq01_invoke_reference_only */
WITH target AS (
    SELECT
        pid,
        datname,
        usename,
        application_name,
        client_addr,
        backend_type,
        state,
        wait_event_type,
        wait_event,
        query_id,
        query_start,
        xact_start,
        LEFT(query, 2000) AS query_text
    FROM pg_stat_activity
    WHERE pid = $1::integer
      AND pid <> pg_backend_pid()
      AND application_name IS DISTINCT FROM 'dms_metrics_collector'
)
SELECT
    clock_timestamp() AS collected_at,
    t.pid,
    t.datname,
    t.usename,
    t.application_name,
    t.client_addr,
    t.backend_type,
    t.state,
    t.wait_event_type,
    t.wait_event,
    t.query_id,
    EXTRACT(EPOCH FROM (clock_timestamp() - t.query_start))
        AS query_age_seconds,
    EXTRACT(EPOCH FROM (clock_timestamp() - t.xact_start))
        AS transaction_age_seconds,
    t.query_text,
    pg_log_backend_memory_contexts(t.pid) AS log_request_accepted
FROM target t;
```

### Reference-only threshold and interpretation

Setelah PostgreSQL log diparse dan `used_bytes` seluruh context per backend
dijumlahkan:

- backend used memory > 2× same-query/same-role baseline: warning;
- backend used memory > 5% provisioned server RAM: warning;
- > 10% provisioned server RAM: high;
- > 20% provisioned server RAM atau terus meningkat pada PID yang sama selama stable workload: critical investigation trigger.

Additional interpretation:

- Memory-context snapshot adalah point-in-time forensic evidence, bukan continuous metric.
- Satu backend besar tidak selalu leak; sort, hash, maintenance, logical decoding, atau extension dapat menggunakan memory besar secara sah.
- Jika `can_execute = false`, tandai EQ01 unavailable. Jangan memberikan broad admin privilege hanya untuk collector. Gunakan controlled DBA execution bila forensic ini dibutuhkan.
- Capability harus diuji pada actual Azure PostgreSQL 18 server.
- PostgreSQLLogs Diagnostic Settings harus aktif agar output EQ01 dapat dikumpulkan. Hal ini tidak dapat divalidasi sepenuhnya melalui SQL.

---


### 14.2 Other Conditional-Only and Deliberately Excluded Items

| Object/query | Reason |
|---|---|
| `pg_backend_memory_contexts` | Only current collector backend; misleading as server memory usage |
| `EXPLAIN ANALYZE` | Executes the query and may change workload; use offline/controlled analysis only |
| `pg_stat_reset()` | Breaks delta continuity and can affect autovacuum-related counters |
| Repeated `query_store.qs_view` polling | Duplicates Q05; Query Store persists by window, not suitable for 15-second collection |
| Full lock inventory every 15 seconds | Q03 is conditional to avoid unnecessary overhead and data volume |
| Table/index size every 15–60 seconds | Object size changes too slowly; Q10 size at 15-minute cadence is sufficient |
| Azure Monitor Metrics API for exportable metrics | Metrics already exported to Storage Account; avoid duplicate collection path |
| PgBouncer `SHOW FDS` | Internal diagnostic command that can block the PgBouncer event loop; do not run during load test except controlled DBA investigation |


Reconciliation notes for version 3.10:

- The exclusion of the `pg_backend_memory_contexts` **view** still holds: it only shows the collector's own backend. The former Q16 function-based mechanism is now retired as EQ01 because it is not reliably executable by customer admin roles on Azure Flexible Server and writes to PostgreSQLLogs instead of normal SQL result rows.
- The "no 15-second polling of `query_store.qs_view`" exclusion still holds. Q11 (15-minute) and Q19 (phase-boundary/conditional) read Query Store only at low cadence from `azure_sys`, which is consistent with that exclusion.
- Q14, Q15, Q17, DICT01, Q19, PB04, PB05, and PB06 are **conditional or low-frequency**, not part of the core 15/30/60-second PostgreSQL path. PB02/PB03 are PgBouncer-specific and run only when PgBouncer is in scope.

---

## 15. Acceptance Checklist Before Official Test


- [ ] Q01 confirms expected PostgreSQL version and required views.
- [ ] Monitoring user can see all application sessions and query statistics.
- [ ] Q02 completes in less than 2 seconds at expected connection count.
- [ ] Q03 returns known blocking pair in a controlled smoke test.
- [ ] Q04/Q05/Q07/Q08 deltas are calculated correctly.
- [ ] Counter reset detection is tested.
- [ ] Q05 runs once server-wide, does not exclude `current_user`, does not filter `current_database()`, and preserves `(dbid, userid, queryid, toplevel)` as the delta key.
- [ ] Query text masking/retention policy is approved.
- [ ] Q11 watermark does not duplicate or skip Query Store windows.
- [ ] Storage Account summarizer preserves 1-minute timestamps.
- [ ] Phase markers line up with k6 timestamps.
- [ ] Azure Function does not overlap executions.
- [ ] Collector failure does not affect application traffic.
- [ ] Cool-down collection remains active for 30–60 minutes after ramp-down.

- [ ] Q01A–Q01D report capability, effective settings, database/role overrides, and routine-local overrides without duplicate M03/C01 standalone query execution.
- [ ] Q02 emits the parallel-worker-pool fields from a single `pg_stat_activity` scan (no standalone parallel-worker execution path).
- [ ] Q03 returns `actual_lock_wait_seconds` from `pg_locks.waitstart` for a known blocking pair.
- [ ] Q05 returns planning/JIT columns and PostgreSQL 18 parallel-worker-launch counters without compatibility fallback.
- [ ] Q13 uses the PostgreSQL 18 `pg_buffercache_summary()` / `pg_buffercache_usage_counts()` path and `pg_buffercache` is enabled.
- [ ] Q16 SLRU deltas are computed with reset detection.
- [ ] Q17 confirms `log_lock_waits = on` and an acceptable `deadlock_timeout`; PostgreSQLLogs export is verified out-of-band.
- [ ] DICT01 runs on PostgreSQL 18 and captures the wait-event dictionary.
- [ ] Q19 enablement (`pg_qs.store_query_plans = on`) is validated and it runs only at phase boundary/conditionally.
- [ ] EQ01 is explicitly recorded as retired/excluded from the implemented collector; official test readiness does not depend on `pg_log_backend_memory_contexts(integer)`.
- [ ] If PgBouncer is in scope, PB01 connects to database `pgbouncer` on port `6432` and captures version/config/databases/users.
- [ ] PB02 `SHOW POOLS` and PB03 `SHOW STATS` are captured at the planned cadence and correlated to Q02 backend connections.
- [ ] PB04/PB05 conditional diagnostics trigger when `cl_waiting`, `maxwait`, or pool saturation appears.
- [ ] Azure Monitor PgBouncer metrics and PgBouncer logs are exported or explicitly marked unavailable.
- [ ] Before-after PgBouncer comparison includes k6 latency/error, PgBouncer queue/wait, PostgreSQL backend connection reduction, and DB CPU/memory.

---

## 16. References

### Microsoft Azure

1. Monitor using metrics and logs in Azure Database for PostgreSQL Flexible Server — https://learn.microsoft.com/en-us/azure/postgresql/monitor/concepts-monitoring
2. Supported metrics for Microsoft.DBforPostgreSQL/flexibleServers — https://learn.microsoft.com/en-us/azure/azure-monitor/reference/supported-metrics/microsoft-dbforpostgresql-flexibleservers-metrics
3. Query Store in Azure Database for PostgreSQL Flexible Server — https://learn.microsoft.com/en-us/azure/postgresql/monitor/concepts-query-store
4. Usage scenarios for Query Store — https://learn.microsoft.com/en-us/azure/postgresql/monitor/concepts-query-store-scenarios
5. Configure and access logs in Azure Database for PostgreSQL — https://learn.microsoft.com/en-us/azure/postgresql/monitor/how-to-configure-and-access-logs
6. Troubleshoot high CPU utilization — https://learn.microsoft.com/en-us/azure/postgresql/troubleshoot/how-to-high-cpu-utilization
7. Troubleshoot high memory utilization — https://learn.microsoft.com/en-us/azure/postgresql/troubleshoot/how-to-high-memory-utilization
8. Troubleshoot high IOPS utilization — https://learn.microsoft.com/en-us/azure/postgresql/troubleshoot/how-to-high-io-utilization
9. Parameters in Azure Database for PostgreSQL — https://learn.microsoft.com/en-us/azure/postgresql/parameters/concepts-parameters
10. Planner cost constants — https://learn.microsoft.com/en-us/azure/postgresql/parameters/parameters-query-tuning-planner-cost-constants
11. Resource-usage / memory parameters — https://learn.microsoft.com/en-us/azure/postgresql/parameters/parameters-resource-usage-memory
12. Reporting and logging / what-to-log parameters — https://learn.microsoft.com/en-us/azure/postgresql/parameters/parameters-reporting-logging-what-log
13. Extensions: considerations and allow-list — https://learn.microsoft.com/en-us/azure/postgresql/extensions/concepts-extensions-considerations
14. Limits in Azure Database for PostgreSQL — https://learn.microsoft.com/en-us/azure/postgresql/configure-maintain/concepts-limits

### PostgreSQL

15. The Cumulative Statistics System (16 / current) — https://www.postgresql.org/docs/current/monitoring-stats.html
16. pg_stat_statements — https://www.postgresql.org/docs/current/pgstatstatements.html
17. pg_locks, including `waitstart` and `pg_blocking_pids()` — https://www.postgresql.org/docs/current/view-pg-locks.html
18. Lock management and `deadlock_timeout` — https://www.postgresql.org/docs/current/runtime-config-locks.html
19. Logging configuration (`log_lock_waits`, `log_temp_files`) — https://www.postgresql.org/docs/current/runtime-config-logging.html
20. pg_wait_events (PostgreSQL 17+) — https://www.postgresql.org/docs/current/view-pg-wait-events.html
21. pg_buffercache — https://www.postgresql.org/docs/current/pgbuffercache.html
22. pg_backend_memory_contexts / pg_log_backend_memory_contexts() — https://www.postgresql.org/docs/current/view-pg-backend-memory-contexts.html
23. pg_shmem_allocations — https://www.postgresql.org/docs/current/view-pg-shmem-allocations.html
24. Worker-process and parallel-query resource settings — https://www.postgresql.org/docs/current/runtime-config-resource.html
25. Planner cost, parallel scan, and JIT cost settings — https://www.postgresql.org/docs/current/runtime-config-query.html
26. How parallel query works — https://www.postgresql.org/docs/current/how-parallel-query-works.html
27. When to use JIT — https://www.postgresql.org/docs/current/jit-decision.html
28. Predefined roles (`pg_monitor`, `pg_read_all_stats`) — https://www.postgresql.org/docs/current/predefined-roles.html

### PgBouncer

29. PgBouncer usage and admin console SHOW commands — https://www.pgbouncer.org/usage.html
30. PgBouncer configuration reference — https://www.pgbouncer.org/config.html
31. PgBouncer in Azure Database for PostgreSQL Flexible Server — https://learn.microsoft.com/en-us/azure/postgresql/connectivity/concepts-pgbouncer

### Astra DMS project inputs

32. `DMS_Performance_Test_Specification_AzurePostgre (1).pdf`
33. `astra_dms_postgresql_metrics_query_spec.md` (v1.0)
34. `astra_dms_postgresql_wait_stats_addendum.md`
35. `astra_dms_postgresql_cpu_parallelism_metrics_addendum.md`
36. `astra_dms_postgresql_memory_metrics_addendum.md`
37. `01-astra_dms_database_testing_phases.md`
38. `02-astra_dms_database_observability_tools.md`
39. `03-astra_dms_database_risks_unknowns_clarifications.md`

---

## 17. Final Implementation Position

Recommended minimum implementation for the official run:

- Q01 startup configuration + capability + override inventory (memory, CPU/planner/JIT, role/database/routine overrides);
- 15-second Q02 unified activity/wait/connection/long-query/long-transaction snapshot with the parallel-worker-pool component folded into the same scan;
- conditional Q03 blocking detail with exact `waitstart`-based lock-wait duration and chain depth;
- 30-second Q04 and Q07 cumulative database/I/O counters;
- 60-second server-wide Q05 (query workload + planning/JIT/parallel counters) and Q08 (checkpoint/WAL) counters;
- 5-minute Q06 health, 5–15 minute Q09/Q10 table/index snapshots;
- 5-minute Q13 shared-buffer occupancy and 15-minute Q14 top-relation residency;
- 60-second Q16 SLRU counters;
- 15-minute Q11 Query Store wait windows enriched with runtime and query text;
- low-frequency Q15 shared-memory allocations and Q19 parallel-plan inventory at phase boundaries/conditionally;
- EQ01 is excluded from the implemented collector; if backend memory-context logging is ever needed, treat it as manual/out-of-band forensic evidence, not as normal Q-series collection;
- startup Q17/DICT01 logging-config validation and wait-event dictionary;
- PB01–PB06 PgBouncer admin-console collection when PgBouncer is part of the test architecture, including pool runtime, stats, client/server diagnostics, and before-after connection shielding metrics;
- Azure Monitor PgBouncer metrics and PgBouncer logs from Diagnostic Settings when PgBouncer is part of the evidence pack;
- 2-hour ingestion of raw 1-minute Azure Monitor metrics from Storage Account, with `cpu_percent` and `memory_percent` kept as the authoritative server CPU/memory sources;
- downstream CPU-demand/vCore, RPS/CPU efficiency, and parallelism-benefit metrics (Section 9);
- explicit phase and resilience-event markers.

This provides high-resolution root-cause evidence across waits, CPU/parallelism,
memory, and PgBouncer connection-pool behavior without making every object a
15-second polling workload and without duplicating the Azure Monitor metrics path
already stored in Storage Account. Exact per-query CPU time remains unavailable
from built-in statistics and Azure Query Store; CPU attribution stays
correlation-based. PgBouncer effectiveness is judged through before-after
connection shielding, pool wait, k6 latency/error, and PostgreSQL backend
connection reduction together, not from a single PgBouncer counter.


---

## Appendix — External implementation references

The PgBouncer and .NET notes in version 3.11 are based on these implementation references:

- Microsoft Learn, Azure Database for PostgreSQL Flexible Server built-in PgBouncer: port `6432`, internal database `pgbouncer`, `pgbouncer.stats_users`, admin console `SHOW` commands, metrics, logs, and application compatibility testing.  
  https://learn.microsoft.com/en-us/azure/postgresql/connectivity/concepts-pgbouncer
- PgBouncer official documentation, admin console and `SHOW` commands: admin users/stat users, `SHOW STATS`, `SHOW POOLS`, `SHOW CLIENTS`, `SHOW SERVERS`, and the note that the admin console currently supports only simple-query protocol.  
  https://www.pgbouncer.org/usage.html
- Npgsql official documentation, connection string parameters: `Host`, `Port`, `Database`, `Username`, `Password`, `SSL Mode`, timeout, and pooling-related parameters.  
  https://www.npgsql.org/doc/connection-string-parameters.html
