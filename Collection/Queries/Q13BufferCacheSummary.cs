using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q13 — shared buffer cache summary and usage-count distribution. Health5m tier,
/// ServerWide. Uses PostgreSQL 18 functions (pg_buffercache_summary, pg_buffercache_usage_counts).
/// If capability is missing (PG < 18 and/or extension not available), this query
/// returns an empty result with a note instead of failing the collection tick.
/// </summary>
public sealed class Q13BufferCacheSummary : IMetricQuery
{
    private readonly string _serverId;

    public Q13BufferCacheSummary(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q13";
    public CadenceTier Tier => CadenceTier.Health5m;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    private const string CapabilitySql = """
        SELECT
            current_setting('server_version_num')::integer AS server_version_num,
            to_regprocedure('pg_buffercache_summary()') IS NOT NULL AS has_summary_fn,
            to_regprocedure('pg_buffercache_usage_counts()') IS NOT NULL AS has_usage_counts_fn;
        """;

    private const string Sql = """
        /* dms_metrics_collector:q13 */
        WITH summary AS (SELECT * FROM pg_buffercache_summary()),
        usage_distribution AS (
            SELECT COALESCE(
                jsonb_agg(
                    jsonb_build_object(
                        'usage_count', usage_count,
                        'buffers', buffers,
                        'dirty', dirty,
                        'pinned', pinned
                    ) ORDER BY usage_count
                ), '[]'::jsonb
            ) AS usage_counts
            FROM pg_buffercache_usage_counts()
        )
        SELECT
            clock_timestamp() AS collected_at,
            current_setting('server_version_num')::integer AS server_version_num,
            s.buffers_used, s.buffers_unused, s.buffers_dirty, s.buffers_pinned,
            s.usagecount_avg,
            ROUND(100.0 * s.buffers_used / NULLIF(s.buffers_used + s.buffers_unused, 0), 2)
                AS buffer_used_percent,
            ROUND(100.0 * s.buffers_dirty / NULLIF(s.buffers_used, 0), 2)
                AS dirty_buffer_percent_of_used,
            ROUND(100.0 * s.buffers_pinned / NULLIF(s.buffers_used, 0), 2)
                AS pinned_buffer_percent_of_used,
            u.usage_counts
        FROM summary s CROSS JOIN usage_distribution u;
        """;

    public async Task<CollectionResult> ExecuteAsync(
        NpgsqlConnection connection, DeltaCache deltas, CancellationToken ct)
    {
        await using (var capabilityCmd = new NpgsqlCommand(CapabilitySql, connection))
        await using (var reader = await capabilityCmd.ExecuteReaderAsync(ct).ConfigureAwait(false))
        {
            if (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var serverVersion = reader.GetInt32(reader.GetOrdinal("server_version_num"));
                var hasSummaryFn = reader.GetBoolean(reader.GetOrdinal("has_summary_fn"));
                var hasUsageCountsFn = reader.GetBoolean(reader.GetOrdinal("has_usage_counts_fn"));

                if (serverVersion < 180000 || !hasSummaryFn || !hasUsageCountsFn)
                {
                    var note = $"Skipped: pg_buffercache_summary/usage_counts unavailable (server_version_num={serverVersion}).";
                    return new CollectionResult(
                        Id,
                        _serverId,
                        DateTimeOffset.UtcNow,
                        DateTimeOffset.UtcNow,
                        Array.Empty<IReadOnlyDictionary<string, object?>>(),
                        note);
                }
            }
        }

        await using var cmd = new NpgsqlCommand(Sql, connection);
        var rows = await RowReader.ReadAllAsync(cmd, ct).ConfigureAwait(false);

        var sourceTs = rows.Count > 0 && rows[0]["collected_at"] is DateTime dt
            ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            : DateTimeOffset.UtcNow;

        return new CollectionResult(Id, _serverId, sourceTs, DateTimeOffset.UtcNow, rows.Cast<IReadOnlyDictionary<string, object?>>().ToList());
    }
}