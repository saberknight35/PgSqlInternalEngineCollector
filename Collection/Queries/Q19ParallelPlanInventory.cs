using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q19 — Query Store parallel-plan inventory. PhaseBoundary tier, AzureSys database.
/// Triggered at each phase boundary, or conditionally when CPU >= 80% for 15 min.
/// Uses a rolling 15-minute analysis window on each invocation.
/// Requires pg_qs.store_query_plans = on.
/// </summary>
public sealed class Q19ParallelPlanInventory : IMetricQuery
{
    private readonly string _serverId;

    public Q19ParallelPlanInventory(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q19";
    public CadenceTier Tier => CadenceTier.PhaseBoundary;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.AzureSys;

    private const string Sql = """
        /* dms_metrics_collector:q19 */
        WITH runtime AS (
            SELECT
                q.db_id, q.query_id, q.plan_id,
                MIN(q.start_time) AS first_window_start,
                MAX(q.end_time) AS last_window_end,
                SUM(q.calls) AS query_store_calls,
                SUM(q.total_time) AS total_exec_time_ms,
                CASE WHEN SUM(q.calls) > 0 THEN SUM(q.total_time) / SUM(q.calls) END AS mean_time_ms,
                MAX(q.max_time) AS max_time_ms,
                MAX(q.query_sql_text) AS query_sql_text
            FROM query_store.qs_view q
            WHERE q.end_time > $1
              AND q.start_time < $2
              AND NOT q.is_system_query
            GROUP BY q.db_id, q.query_id, q.plan_id
        ),
        plans AS (
            SELECT
                p.plan_id, p.db_id, p.query_id, p.plan_text,
                p.plan_text LIKE '%Gather%' AS has_gather,
                p.plan_text LIKE '%Gather Merge%' AS has_gather_merge,
                p.plan_text LIKE '%Parallel Seq Scan%' AS has_parallel_seq_scan,
                p.plan_text LIKE '%Parallel Index Scan%' AS has_parallel_index_scan,
                p.plan_text LIKE '%Parallel Index Only Scan%' AS has_parallel_index_only_scan,
                p.plan_text LIKE '%Parallel Hash%' AS has_parallel_hash,
                p.plan_text LIKE '%Parallel Append%' AS has_parallel_append,
                NULLIF(substring(p.plan_text FROM 'Workers Planned: ([0-9]+)'), '')::integer
                    AS workers_planned_from_plan
            FROM query_store.query_plans_view p
        )
        SELECT
            clock_timestamp() AS collected_at,
            r.db_id, r.query_id, r.plan_id,
            r.first_window_start, r.last_window_end,
            r.query_store_calls, r.total_exec_time_ms, r.mean_time_ms, r.max_time_ms,
            p.has_gather, p.has_gather_merge,
            p.has_parallel_seq_scan, p.has_parallel_index_scan,
            p.has_parallel_index_only_scan, p.has_parallel_hash, p.has_parallel_append,
            p.workers_planned_from_plan,
            LEFT(r.query_sql_text, 6000) AS query_sql_text,
            LEFT(p.plan_text, 10000) AS plan_text
        FROM runtime r
        JOIN plans p ON p.db_id = r.db_id AND p.query_id = r.query_id AND p.plan_id = r.plan_id
        WHERE p.has_gather OR p.has_parallel_seq_scan OR p.has_parallel_index_scan
           OR p.has_parallel_index_only_scan OR p.has_parallel_hash OR p.has_parallel_append
        ORDER BY r.total_exec_time_ms DESC, r.query_store_calls DESC
        LIMIT 200;
        """;

    public async Task<CollectionResult> ExecuteAsync(
        NpgsqlConnection connection, DeltaCache deltas, CancellationToken ct)
    {
        var windowEnd = DateTimeOffset.UtcNow;
        var windowStart = windowEnd.AddMinutes(-15);

        await using var cmd = new NpgsqlCommand(Sql, connection);
        cmd.Parameters.AddWithValue("$1", NpgsqlDbType.TimestampTz, windowStart);
        cmd.Parameters.AddWithValue("$2", NpgsqlDbType.TimestampTz, windowEnd);

        var rows = await RowReader.ReadAllAsync(cmd, ct).ConfigureAwait(false);

        var sourceTs = rows.Count > 0 && rows[0]["collected_at"] is DateTime dt
            ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            : DateTimeOffset.UtcNow;

        return new CollectionResult(Id, _serverId, sourceTs, DateTimeOffset.UtcNow, rows.Cast<IReadOnlyDictionary<string, object?>>().ToList());
    }
}