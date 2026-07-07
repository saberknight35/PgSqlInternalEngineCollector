using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// PB06 — PgBouncer internal lists, memory, and state snapshot. Health5m tier.
/// Issues SHOW LISTS, SHOW STATE, SHOW MEM on the PgBouncer admin console.
/// </summary>
public sealed class PB06PoolerLists : IMetricQuery
{
    private readonly string _serverId;

    public PB06PoolerLists(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "PB06";
    public CadenceTier Tier => CadenceTier.Health5m;
    public QuerySource Source => QuerySource.PgBouncerAdmin;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    public async Task<CollectionResult> ExecuteAsync(
        NpgsqlConnection connection, DeltaCache deltas, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;
        var all = new List<IReadOnlyDictionary<string, object?>>();

        foreach (var showCmd in new[] { "SHOW LISTS", "SHOW STATE", "SHOW MEM" })
        {
            var rows = await PgBouncerConnectionFactory.ShowAsync(connection, showCmd, ct).ConfigureAwait(false);
            foreach (var row in rows)
            {
                var m = new Dictionary<string, object?>(row)
                {
                    ["collected_at"] = now,
                    ["pb_command"] = showCmd
                };
                all.Add(m);
            }
        }

        return new CollectionResult(Id, _serverId, now, now, all);
    }
}
