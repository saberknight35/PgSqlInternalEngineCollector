using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// PB05 — PgBouncer server diagnostic snapshot. Conditional tier.
/// Triggered when sv_idle = 0 and cl_waiting > 0, or pool saturation events.
/// Issues SHOW SERVERS on the PgBouncer admin console.
/// </summary>
public sealed class PB05ServerDiagnostic : IMetricQuery
{
    private readonly string _serverId;

    public PB05ServerDiagnostic(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "PB05";
    public CadenceTier Tier => CadenceTier.Conditional;
    public QuerySource Source => QuerySource.PgBouncerAdmin;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    public async Task<CollectionResult> ExecuteAsync(
        NpgsqlConnection connection, DeltaCache deltas, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;
        var rows = await PgBouncerConnectionFactory.ShowAsync(connection, "SHOW SERVERS", ct).ConfigureAwait(false);

        var result = rows.Select(r =>
        {
            var m = new Dictionary<string, object?>(r) { ["collected_at"] = now };
            return (IReadOnlyDictionary<string, object?>)m;
        }).ToList();

        return new CollectionResult(Id, _serverId, now, now, result);
    }
}
