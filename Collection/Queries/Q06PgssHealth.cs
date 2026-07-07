using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q06 — pg_stat_statements capacity and reset health. Health5m tier, ServerWide.
/// Detects deallocation (fingerprint overflow) and stats resets that break Q05 deltas.
/// </summary>
public sealed class Q06PgssHealth : IMetricQuery
{
    private readonly string _serverId;

    public Q06PgssHealth(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q06";
    public CadenceTier Tier => CadenceTier.Health5m;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    private const string Sql = """
        /* dms_metrics_collector:q06 */
        SELECT
            clock_timestamp() AS collected_at,
            dealloc,
            stats_reset
        FROM pg_stat_statements_info;
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
            var mutable = new Dictionary<string, object?>(row);

            if (row["dealloc"] is { } deallocObj)
            {
                var outcome = deltas.Compute("Q06:dealloc", Convert.ToInt64(deallocObj), statsReset, now);
                mutable["dealloc_delta"] = outcome.HasDelta ? outcome.Delta : null;
            }

            enriched.Add(mutable);
        }

        var sourceTs = rows.Count > 0 && rows[0]["collected_at"] is DateTime dt
            ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            : now;

        return new CollectionResult(Id, _serverId, sourceTs, now, enriched);
    }
}
