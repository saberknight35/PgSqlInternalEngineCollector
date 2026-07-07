using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q17 — lock and deadlock logging configuration validation.
/// Config6h tier, server-wide.
/// </summary>
public sealed class Q17LockLoggingConfig : IMetricQuery
{
    private readonly string _serverId;

    public Q17LockLoggingConfig(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q17";
    public CadenceTier Tier => CadenceTier.Config6h;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    private const string Sql = """
        /* dms_metrics_collector:q17 */
        SELECT
            clock_timestamp() AS collected_at,
            name, setting, unit, source, pending_restart
        FROM pg_settings
        WHERE name IN (
            'deadlock_timeout',
            'log_lock_waits',
            'log_min_messages',
            'log_min_error_statement',
            'log_error_verbosity',
            'log_line_prefix'
        )
        ORDER BY name;
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
