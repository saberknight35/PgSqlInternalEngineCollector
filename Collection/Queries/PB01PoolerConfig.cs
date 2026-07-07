using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// PB01 — PgBouncer capability and configuration snapshot. Config6h tier.
/// Issues SHOW VERSION, SHOW CONFIG, SHOW DATABASES, SHOW USERS on the
/// PgBouncer admin console and combines them as rows with a 'command' discriminator.
/// </summary>
public sealed class PB01PoolerConfig : IMetricQuery
{
    private readonly string _serverId;

    public PB01PoolerConfig(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "PB01";
    public CadenceTier Tier => CadenceTier.Config6h;
    public QuerySource Source => QuerySource.PgBouncerAdmin;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    public async Task<CollectionResult> ExecuteAsync(
        NpgsqlConnection connection, DeltaCache deltas, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;
        var all = new List<IReadOnlyDictionary<string, object?>>();

        foreach (var cmd in new[] { "SHOW VERSION", "SHOW CONFIG", "SHOW DATABASES", "SHOW USERS" })
        {
            var rows = await PgBouncerConnectionFactory.ShowAsync(connection, cmd, ct).ConfigureAwait(false);
            foreach (var row in rows)
            {
                var enriched = new Dictionary<string, object?>(row)
                {
                    ["collected_at"] = now,
                    ["pb_command"] = cmd
                };
                all.Add(enriched);
            }
        }

        return new CollectionResult(Id, _serverId, now, now, all);
    }
}
