using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q07 — cluster I/O by backend type, object, and context. Counter30 tier, ServerWide.
/// Requires PostgreSQL 18 pg_stat_io byte counters. On older versions the query will fail
/// and the CollectorWorker error handler will log and skip the tick.
/// </summary>
public sealed class Q07ClusterIo : IMetricQuery
{
    private readonly string _serverId;

    public Q07ClusterIo(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q07";
    public CadenceTier Tier => CadenceTier.Counter30;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    private const string Sql = """
        /* dms_metrics_collector:q07 */
        SELECT
            clock_timestamp() AS collected_at,
            backend_type, object, context,
            reads, read_bytes, read_time,
            writes, write_bytes, write_time,
            writebacks, writeback_time,
            extends, extend_bytes, extend_time,
            hits, evictions, reuses,
            fsyncs, fsync_time, stats_reset
        FROM pg_stat_io
        ORDER BY backend_type, object, context;
        """;

    public async Task<CollectionResult> ExecuteAsync(
        NpgsqlConnection connection, DeltaCache deltas, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(Sql, connection);
        var rows = await RowReader.ReadAllAsync(cmd, ct).ConfigureAwait(false);

        var now = DateTimeOffset.UtcNow;
        var enriched = new List<IReadOnlyDictionary<string, object?>>(rows.Count);

        foreach (var row in rows)
        {
            var statsReset = row["stats_reset"]?.ToString();
            var key = $"Q07:{row["backend_type"]}:{row["object"]}:{row["context"]}";
            var mutable = new Dictionary<string, object?>(row);

            ComputeDelta(deltas, mutable, row, $"{key}:reads", "reads", statsReset, now, "reads_delta", "reads_per_second");
            ComputeDelta(deltas, mutable, row, $"{key}:read_bytes", "read_bytes", statsReset, now, "read_bytes_delta", "read_bytes_per_second");
            ComputeDelta(deltas, mutable, row, $"{key}:writes", "writes", statsReset, now, "writes_delta", "writes_per_second");
            ComputeDelta(deltas, mutable, row, $"{key}:write_bytes", "write_bytes", statsReset, now, "write_bytes_delta", "write_bytes_per_second");
            ComputeDelta(deltas, mutable, row, $"{key}:extend_bytes", "extend_bytes", statsReset, now, "extend_bytes_delta", "extend_bytes_per_second");
            ComputeDelta(deltas, mutable, row, $"{key}:hits", "hits", statsReset, now, "hits_delta", "hits_per_second");
            ComputeDelta(deltas, mutable, row, $"{key}:evictions", "evictions", statsReset, now, "evictions_delta", "evictions_per_second");

            enriched.Add(mutable);
        }

        var sourceTs = rows.Count > 0 && rows[0]["collected_at"] is DateTime dt
            ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            : now;

        return new CollectionResult(Id, _serverId, sourceTs, now, enriched);
    }

    private static void ComputeDelta(
        DeltaCache deltas,
        Dictionary<string, object?> mutable,
        IReadOnlyDictionary<string, object?> row,
        string cacheKey, string column,
        string? statsReset,
        DateTimeOffset now,
        string deltaColumn,
        string rateColumn)
    {
        if (!row.TryGetValue(column, out var v) || v is null) return;
        var outcome = deltas.Compute(cacheKey, Convert.ToInt64(v), statsReset, now);
        mutable[deltaColumn] = outcome.HasDelta ? outcome.Delta : null;
        mutable[rateColumn] = outcome.HasDelta ? outcome.RatePerSecond : null;
    }
}
