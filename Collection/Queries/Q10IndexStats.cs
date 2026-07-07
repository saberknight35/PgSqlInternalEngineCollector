using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q10 — index usage, index I/O, size, and validity. Object15m tier,
/// PerApplicationDatabase (index stats are current-database-scoped).
/// </summary>
public sealed class Q10IndexStats : IMetricQuery
{
    private readonly string _serverId;

    public Q10IndexStats(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q10";
    public CadenceTier Tier => CadenceTier.Object15m;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.PerApplicationDatabase;

    private const string Sql = """
        /* dms_metrics_collector:q10 */
        SELECT
            clock_timestamp() AS collected_at,
            current_database() AS database_name,
            s.relid, s.indexrelid, s.schemaname, s.relname, s.indexrelname,
            s.idx_scan, s.idx_tup_read, s.idx_tup_fetch,
            io.idx_blks_read, io.idx_blks_hit,
            pg_relation_size(s.indexrelid) AS index_size_bytes,
            i.indisprimary, i.indisunique, i.indisvalid, i.indisready
        FROM pg_stat_user_indexes s
        LEFT JOIN pg_statio_user_indexes io ON io.indexrelid = s.indexrelid
        JOIN pg_index i ON i.indexrelid = s.indexrelid
        ORDER BY s.indexrelid;
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
            var db = row["database_name"]?.ToString();
            var indexrelid = row["indexrelid"]?.ToString();
            var key = $"Q10:{db}:{indexrelid}";
            var mutable = new Dictionary<string, object?>(row);

            if (row.TryGetValue("idx_scan", out var v) && v is not null)
            {
                var outcome = deltas.Compute($"{key}:idx_scan", Convert.ToInt64(v), null, now);
                mutable["idx_scan_delta"] = outcome.HasDelta ? outcome.Delta : null;
            }

            enriched.Add(mutable);
        }

        var sourceTs = rows.Count > 0 && rows[0]["collected_at"] is DateTime dt
            ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            : now;

        return new CollectionResult(Id, _serverId, sourceTs, now, enriched);
    }
}
