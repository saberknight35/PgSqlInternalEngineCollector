using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// PB03 — PgBouncer statistics snapshot. Counter60 tier.
/// Issues SHOW STATS, SHOW STATS_TOTALS, SHOW STATS_AVERAGES, SHOW TOTALS.
/// Cumulative columns are enriched with delta values using DeltaCache.
/// </summary>
public sealed class PB03PoolerStats : IMetricQuery
{
    private readonly string _serverId;

    public PB03PoolerStats(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "PB03";
    public CadenceTier Tier => CadenceTier.Counter60;
    public QuerySource Source => QuerySource.PgBouncerAdmin;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    public async Task<CollectionResult> ExecuteAsync(
        NpgsqlConnection connection, DeltaCache deltas, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;
        var all = new List<IReadOnlyDictionary<string, object?>>();

        foreach (var showCmd in new[] { "SHOW STATS", "SHOW STATS_TOTALS", "SHOW STATS_AVERAGES", "SHOW TOTALS" })
        {
            var rows = await PgBouncerConnectionFactory.ShowAsync(connection, showCmd, ct).ConfigureAwait(false);
            foreach (var row in rows)
            {
                var database = row.TryGetValue("database", out var db) ? db?.ToString() : "all";
                var mutable = new Dictionary<string, object?>(row)
                {
                    ["collected_at"] = now,
                    ["pb_command"] = showCmd
                };

                if (showCmd == "SHOW STATS" || showCmd == "SHOW STATS_TOTALS")
                {
                    foreach (var col in new[] { "total_xact_count", "total_query_count",
                                                "total_received", "total_sent",
                                                "total_wait_time", "total_xact_time",
                                                "total_query_time", "total_server_assignment_count" })
                    {
                        if (!row.TryGetValue(col, out var v) || v is null) continue;
                        var outcome = deltas.Compute($"PB03:{database}:{col}", Convert.ToInt64(v), null, now);
                        mutable[$"{col}_delta"] = outcome.HasDelta ? outcome.Delta : null;
                        mutable[$"{col}_per_second"] = outcome.HasDelta ? outcome.RatePerSecond : null;
                    }
                }

                all.Add(mutable);
            }
        }

        return new CollectionResult(Id, _serverId, now, now, all);
    }
}
