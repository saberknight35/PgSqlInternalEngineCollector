using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q12 — active vacuum and analyze progress. Conditional tier, ServerWide.
/// Triggered when Q02 sees autovacuum workers active, or Q09 shows dead-tuple
/// pressure, or disk I/O is high. Not on a periodic timer.
/// v3.12 scope correction: ExecutionScope changed to ServerWide — pg_stat_progress_*
/// views already expose database attribution (datname) for all active maintenance
/// backends across the server; per-database fan-out was redundant.
/// </summary>
public sealed class Q12VacuumProgress : IMetricQuery
{
    private readonly string _serverId;

    public Q12VacuumProgress(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q12";
    public CadenceTier Tier => CadenceTier.Conditional;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    private const string Sql = """
        /* dms_metrics_collector:q12 */
        SELECT
            clock_timestamp() AS collected_at,
            'vacuum'::text AS operation_type,
            to_jsonb(v) AS progress,
            a.datname,
            a.usename,
            a.application_name,
            a.wait_event_type,
            a.wait_event,
            EXTRACT(EPOCH FROM (clock_timestamp() - a.query_start)) AS operation_age_seconds
        FROM pg_stat_progress_vacuum v
        LEFT JOIN pg_stat_activity a ON a.pid = v.pid

        UNION ALL

        SELECT
            clock_timestamp() AS collected_at,
            'analyze'::text AS operation_type,
            to_jsonb(an) AS progress,
            a.datname,
            a.usename,
            a.application_name,
            a.wait_event_type,
            a.wait_event,
            EXTRACT(EPOCH FROM (clock_timestamp() - a.query_start)) AS operation_age_seconds
        FROM pg_stat_progress_analyze an
        LEFT JOIN pg_stat_activity a ON a.pid = an.pid;
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
