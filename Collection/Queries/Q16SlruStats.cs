using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q16 — SLRU I/O statistics. Counter60 tier, server-wide.
/// Cumulative counters; key delta fields enriched via DeltaCache.
/// </summary>
public sealed class Q16SlruStats : IMetricQuery
{
    private readonly string _serverId;

    public Q16SlruStats(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q16";
    public CadenceTier Tier => CadenceTier.Counter60;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    private const string Sql = """
        /* dms_metrics_collector:q16 */
        SELECT
            clock_timestamp() AS collected_at,
            name, blks_zeroed, blks_hit, blks_read, blks_written,
            blks_exists, flushes, truncates, stats_reset
        FROM pg_stat_slru
        ORDER BY name;
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
            var name = row["name"]?.ToString();
            var statsReset = row["stats_reset"]?.ToString();
            var key = $"Q16:{name}";
            var mutable = new Dictionary<string, object?>(row);

            foreach (var col in new[] { "blks_hit", "blks_read", "blks_written", "flushes" })
            {
                if (row.TryGetValue(col, out var v) && v is not null)
                {
                    var outcome = deltas.Compute($"{key}:{col}", Convert.ToInt64(v), statsReset, now);
                    mutable[$"{col}_delta"] = outcome.HasDelta ? outcome.Delta : null;
                    mutable[$"{col}_per_second"] = outcome.HasDelta ? outcome.RatePerSecond : null;
                }
            }

            enriched.Add(mutable);
        }

        var sourceTs = rows.Count > 0 && rows[0]["collected_at"] is DateTime dt
            ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            : now;

        return new CollectionResult(Id, _serverId, sourceTs, now, enriched);
    }
}
