using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q03 — Blocking pair, lock detail, exact lock-wait duration, and blocking chain.
/// Conditional tier: triggered immediately when Q02 detects active_lock_waiting_sessions > 0,
/// then repeated every 15 s while blocking persists. Not driven by a periodic timer.
/// </summary>
public sealed class Q03BlockingChain : IMetricQuery
{
    private readonly string _serverId;

    public Q03BlockingChain(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q03";
    public CadenceTier Tier => CadenceTier.Conditional;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    private const string Sql = """
        /* dms_metrics_collector:q03 */
        WITH RECURSIVE waiting_lock AS (
            SELECT l.pid, l.locktype, l.database, l.relation, l.page, l.tuple,
                   l.virtualxid, l.transactionid, l.classid, l.objid, l.objsubid,
                   l.virtualtransaction, l.mode, l.waitstart
            FROM pg_locks l
            WHERE l.granted = false
        ),
        edges AS (
            SELECT w.pid AS blocked_pid, x.blocker_pid
            FROM waiting_lock w
            CROSS JOIN LATERAL unnest(pg_blocking_pids(w.pid)) AS x(blocker_pid)
        ),
        chain AS (
            SELECT e.blocked_pid AS root_blocked_pid, e.blocked_pid, e.blocker_pid,
                   1 AS depth, ARRAY[e.blocked_pid, e.blocker_pid]::integer[] AS path
            FROM edges e
            UNION ALL
            SELECT c.root_blocked_pid, e.blocked_pid, e.blocker_pid, c.depth + 1, c.path || e.blocker_pid
            FROM chain c
            JOIN edges e ON e.blocked_pid = c.blocker_pid
            WHERE c.depth < 32 AND NOT e.blocker_pid = ANY(c.path)
        ),
        chain_depth AS (
            SELECT root_blocked_pid, MAX(depth) AS maximum_chain_depth
            FROM chain GROUP BY root_blocked_pid
        ),
        blocker_summary AS (
            SELECT blocked_pid,
                   COUNT(*) AS direct_blocker_count,
                   ARRAY_AGG(blocker_pid ORDER BY blocker_pid) AS direct_blocker_pids
            FROM edges GROUP BY blocked_pid
        ),
        waiter_count AS (SELECT COUNT(*) AS waiting_lock_count FROM waiting_lock)
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
            EXTRACT(EPOCH FROM (clock_timestamp() - w.waitstart)) AS actual_lock_wait_seconds,
            EXTRACT(EPOCH FROM (clock_timestamp() - blocked.query_start)) AS blocked_query_age_seconds,
            EXTRACT(EPOCH FROM (clock_timestamp() - blocked.xact_start)) AS blocked_transaction_age_seconds,
            CASE
                WHEN w.relation IS NOT NULL
                 AND (w.database = (SELECT oid FROM pg_database WHERE datname = current_database()) OR w.database = 0)
                THEN format('%I.%I', n.nspname, c.relname)
            END AS locked_relation_name,
            w.relation AS locked_relation_oid,
            w.page, w.tuple, w.transactionid, w.virtualxid,
            bs.direct_blocker_count, bs.direct_blocker_pids,
            COALESCE(cd.maximum_chain_depth, 0) AS maximum_chain_depth,
            blocker.pid AS blocking_pid,
            blocker.usename AS blocking_user,
            blocker.application_name AS blocking_application_name,
            blocker.client_addr AS blocking_client_addr,
            blocker.state AS blocking_state,
            blocker.query_id AS blocking_query_id,
            EXTRACT(EPOCH FROM (clock_timestamp() - blocker.query_start)) AS blocking_query_age_seconds,
            EXTRACT(EPOCH FROM (clock_timestamp() - blocker.xact_start)) AS blocking_transaction_age_seconds,
            LEFT(blocked.query, 2000) AS blocked_query_text,
            LEFT(blocker.query, 2000) AS blocking_query_text
        FROM waiting_lock w
        JOIN pg_stat_activity blocked ON blocked.pid = w.pid
        LEFT JOIN blocker_summary bs ON bs.blocked_pid = w.pid
        LEFT JOIN chain_depth cd ON cd.root_blocked_pid = w.pid
        LEFT JOIN edges e ON e.blocked_pid = w.pid
        LEFT JOIN pg_stat_activity blocker ON blocker.pid = e.blocker_pid
        LEFT JOIN pg_class c ON c.oid = w.relation
        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN waiter_count wc
        ORDER BY actual_lock_wait_seconds DESC NULLS LAST, w.pid, blocker.pid;
        """;

    public async Task<CollectionResult> ExecuteAsync(
        NpgsqlConnection connection, DeltaCache deltas, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(Sql, connection);
        var rows = await RowReader.ReadAllAsync(cmd, ct).ConfigureAwait(false);

        var sourceTs = rows.Count > 0 && rows[0]["collected_at"] is DateTime dt
            ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            : DateTimeOffset.UtcNow;

        return new CollectionResult(Id, _serverId, sourceTs, DateTimeOffset.UtcNow, rows);
    }
}
