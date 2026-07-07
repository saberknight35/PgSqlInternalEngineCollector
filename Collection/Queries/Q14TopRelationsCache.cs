using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q14 — top relations occupying shared buffers. Object15m tier,
/// PerApplicationDatabase (pg_buffercache relation mapping is current-database-scoped).
/// Requires PostgreSQL 18 and pg_buffercache extension. Top 100 relations by cached buffer count.
/// </summary>
public sealed class Q14TopRelationsCache : IMetricQuery
{
    private readonly string _serverId;

    public Q14TopRelationsCache(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q14";
    public CadenceTier Tier => CadenceTier.Object15m;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.PerApplicationDatabase;

    private const string CapabilitySql = """
        SELECT
            clock_timestamp() AS collected_at,
            current_database() AS database_name,
            current_setting('server_version_num')::integer AS server_version_num,
            to_regclass('pg_buffercache') IS NOT NULL AS has_pg_buffercache;
        """;

    private const string Sql = """
        /* dms_metrics_collector:q14 */
        WITH current_db AS (
            SELECT oid AS database_oid, dattablespace AS database_default_tablespace_oid
            FROM pg_database WHERE datname = current_database()
        ),
        cache AS MATERIALIZED (
            SELECT b.reldatabase, b.reltablespace, b.relfilenode, b.relforknumber,
                   COUNT(*) AS cached_buffers,
                   COUNT(*) FILTER (WHERE b.isdirty) AS dirty_buffers,
                   COUNT(*) FILTER (WHERE b.pinning_backends > 0) AS pinned_buffers,
                   AVG(b.usagecount) AS avg_usagecount
            FROM pg_buffercache b
            CROSS JOIN current_db d
            WHERE b.relfilenode IS NOT NULL
              AND b.relforknumber = 0
              AND b.reldatabase = d.database_oid
            GROUP BY b.reldatabase, b.reltablespace, b.relfilenode, b.relforknumber
        ),
        mapped AS (
            SELECT
                c.oid AS relation_oid,
                n.nspname AS schemaname, c.relname, c.relkind,
                cache.cached_buffers, cache.dirty_buffers, cache.pinned_buffers,
                cache.avg_usagecount,
                cache.cached_buffers * current_setting('block_size')::bigint AS cached_bytes,
                pg_relation_size(c.oid) AS relation_main_fork_size_bytes
            FROM cache
            CROSS JOIN current_db d
            JOIN pg_class c
              ON pg_relation_filenode(c.oid) = cache.relfilenode
             AND COALESCE(NULLIF(c.reltablespace, 0), d.database_default_tablespace_oid) = cache.reltablespace
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname NOT LIKE 'pg_temp_%'
        )
        SELECT
            clock_timestamp() AS collected_at,
            current_database() AS database_name,
            relation_oid, schemaname, relname, relkind,
            cached_buffers, cached_bytes, relation_main_fork_size_bytes,
            ROUND(100.0 * cached_bytes / NULLIF(relation_main_fork_size_bytes, 0), 2)
                AS relation_cached_percent,
            ROUND(100.0 * cached_buffers / NULLIF(SUM(cached_buffers) OVER (), 0), 2)
                AS share_of_current_database_cached_buffers_percent,
            dirty_buffers, pinned_buffers, avg_usagecount
        FROM mapped
        ORDER BY cached_buffers DESC
        LIMIT 100;
        """;

    public async Task<CollectionResult> ExecuteAsync(
        NpgsqlConnection connection, DeltaCache deltas, CancellationToken ct)
    {
        await using (var capabilityCmd = new NpgsqlCommand(CapabilitySql, connection))
        await using (var reader = await capabilityCmd.ExecuteReaderAsync(ct).ConfigureAwait(false))
        {
            if (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var serverVersion = reader.GetInt32(reader.GetOrdinal("server_version_num"));
                var hasPgBuffercache = reader.GetBoolean(reader.GetOrdinal("has_pg_buffercache"));

                if (serverVersion < 180000 || !hasPgBuffercache)
                {
                    var note = $"Skipped: pg_buffercache unavailable for PostgreSQL 18-only query (server_version_num={serverVersion}).";
                    return new CollectionResult(
                        Id,
                        _serverId,
                        DateTimeOffset.UtcNow,
                        DateTimeOffset.UtcNow,
                        Array.Empty<IReadOnlyDictionary<string, object?>>(),
                        note);
                }
            }
        }

        await using var cmd = new NpgsqlCommand(Sql, connection);
        var rows = await RowReader.ReadAllAsync(cmd, ct).ConfigureAwait(false);

        var sourceTs = rows.Count > 0 && rows[0]["collected_at"] is DateTime dt
            ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            : DateTimeOffset.UtcNow;

        return new CollectionResult(Id, _serverId, sourceTs, DateTimeOffset.UtcNow, rows.Cast<IReadOnlyDictionary<string, object?>>().ToList());
    }
}