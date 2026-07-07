using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// PB02 — PgBouncer pool runtime snapshot (15 s tier, PgBouncer admin source).
/// Runs "SHOW POOLS" on the pgbouncer virtual database via the simple query
/// protocol. The cl_waiting / maxwait columns here are what trigger the conditional
/// PB04/PB05 diagnostics in the spec.
/// </summary>
public sealed class PB02PoolSnapshot : IMetricQuery
{
    private readonly string _serverId;

    public PB02PoolSnapshot(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "PB02";
    public CadenceTier Tier => CadenceTier.Fast;
    public QuerySource Source => QuerySource.PgBouncerAdmin;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    public async Task<CollectionResult> ExecuteAsync(
        NpgsqlConnection connection, DeltaCache deltas, CancellationToken ct)
    {
        var raw = await PgBouncerConnectionFactory
            .ShowAsync(connection, "SHOW POOLS", ct)
            .ConfigureAwait(false);

        var now = DateTimeOffset.UtcNow;
        var rows = raw.Select(r => (IReadOnlyDictionary<string, object?>)r).ToList();
        return new CollectionResult(Id, _serverId, now, now, rows);
    }
}
