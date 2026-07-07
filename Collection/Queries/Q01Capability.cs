using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q01A — capability, version, and extension snapshot.
/// Config6h tier, server-wide.
/// </summary>
public sealed class Q01Capability : IMetricQuery
{
    private readonly string _serverId;

    public Q01Capability(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q01A";
    public CadenceTier Tier => CadenceTier.Config6h;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    private const string Sql = """
        /* dms_metrics_collector:q01a_capability */
        SELECT
            clock_timestamp() AS collected_at,
            current_database() AS database_name,
            current_user AS collector_user,
            current_setting('server_version_num')::integer AS server_version_num,
            version() AS server_version,
            EXISTS (
                SELECT 1
                FROM pg_extension
                WHERE extname = 'pg_stat_statements'
            ) AS has_pg_stat_statements_extension,
            to_regclass('public.pg_stat_statements') IS NOT NULL
                AS has_pg_stat_statements_view,
            to_regclass('public.pg_stat_statements_info') IS NOT NULL
                AS has_pg_stat_statements_info_view,
            to_regclass('pg_catalog.pg_stat_io') IS NOT NULL
                AS has_pg_stat_io,
            to_regclass('pg_catalog.pg_stat_checkpointer') IS NOT NULL
                AS has_pg_stat_checkpointer;
        """;

    public async Task<CollectionResult> ExecuteAsync(
        NpgsqlConnection connection, DeltaCache deltas, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(Sql, connection);
        var rows = await RowReader.ReadAllAsync(cmd, ct).ConfigureAwait(false);

        var sourceTs = rows.Count > 0 && rows[0]["collected_at"] is DateTime dt
            ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            : DateTimeOffset.UtcNow;

        return new CollectionResult(Id, _serverId, sourceTs, DateTimeOffset.UtcNow, rows);
    }
}
