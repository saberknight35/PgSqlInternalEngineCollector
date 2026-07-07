using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// DICT01 — wait event dictionary snapshot. Config6h tier, server-wide.
/// Requires PostgreSQL 17+ (pg_wait_events).
/// </summary>
public sealed class Dict01WaitEventDictionary : IMetricQuery
{
    private readonly string _serverId;

    public Dict01WaitEventDictionary(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "DICT01";
    public CadenceTier Tier => CadenceTier.Config6h;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    private const string Sql = """
        /* dms_metrics_collector:dict01 */
        SELECT
            clock_timestamp() AS collected_at,
            current_setting('server_version_num')::integer AS server_version_num,
            type AS wait_event_type,
            name AS wait_event,
            description
        FROM pg_wait_events
        ORDER BY type, name;
        """;

    public async Task<CollectionResult> ExecuteAsync(
        NpgsqlConnection connection, DeltaCache deltas, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(Sql, connection);
        var rows = await RowReader.ReadAllAsync(cmd, ct).ConfigureAwait(false);

        var sourceTs = rows.Count > 0 && rows[0]["collected_at"] is DateTime dt
            ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            : DateTimeOffset.UtcNow;

        return new CollectionResult(Id, _serverId, sourceTs, DateTimeOffset.UtcNow, rows.Cast<IReadOnlyDictionary<string, object?>>().ToList());
    }
}
