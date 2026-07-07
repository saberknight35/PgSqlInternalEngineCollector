using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// PB04 — PgBouncer client diagnostic snapshot. Conditional tier.
/// Triggered when PB02 reports cl_waiting > 0 or maxwait > 0.
/// Issues SHOW CLIENTS on the PgBouncer admin console.
/// </summary>
public sealed class PB04ClientDiagnostic : IMetricQuery
{
    private readonly string _serverId;

    public PB04ClientDiagnostic(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "PB04";
    public CadenceTier Tier => CadenceTier.Conditional;
    public QuerySource Source => QuerySource.PgBouncerAdmin;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    public async Task<CollectionResult> ExecuteAsync(
        NpgsqlConnection connection, DeltaCache deltas, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;
        var rows = await PgBouncerConnectionFactory.ShowAsync(connection, "SHOW CLIENTS", ct).ConfigureAwait(false);

        var result = rows.Select(r =>
        {
            var m = new Dictionary<string, object?>(r) { ["collected_at"] = now };
            return (IReadOnlyDictionary<string, object?>)m;
        }).ToList();

        return new CollectionResult(Id, _serverId, now, now, result);
    }
}
