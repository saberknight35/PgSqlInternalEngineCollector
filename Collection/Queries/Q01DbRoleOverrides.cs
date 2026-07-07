using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q01C — database, role, and role-database overrides.
/// Config6h tier, server-wide. Zero-row result is valid.
/// </summary>
public sealed class Q01DbRoleOverrides : IMetricQuery
{
    private readonly string _serverId;

    public Q01DbRoleOverrides(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q01C";
    public CadenceTier Tier => CadenceTier.Config6h;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    private const string Sql = """
        /* dms_metrics_collector:q01c_db_role_overrides */
        SELECT
            clock_timestamp() AS collected_at,
            CASE
                WHEN drs.setdatabase = 0 AND drs.setrole = 0
                    THEN 'all_database_all_role'
                WHEN drs.setdatabase = 0
                    THEN 'all_database_specific_role'
                WHEN drs.setrole = 0
                    THEN 'specific_database_all_role'
                ELSE 'specific_database_specific_role'
            END AS setting_scope,
            CASE
                WHEN drs.setdatabase = 0 THEN 'ALL DATABASES'
                ELSE d.datname::text
            END AS database_name,
            CASE
                WHEN drs.setrole = 0 THEN 'ALL ROLES'
                ELSE r.rolname::text
            END AS role_name,
            NULL::text AS object_identity,
            split_part(x.cfg, '=', 1) AS parameter_name,
            substring(x.cfg FROM position('=' IN x.cfg) + 1) AS parameter_value,
            ps.unit,
            'pg_db_role_setting'::text AS source,
            ps.context,
            NULL::boolean AS pending_restart
        FROM pg_db_role_setting drs
        LEFT JOIN pg_database d
          ON d.oid = drs.setdatabase
        LEFT JOIN pg_roles r
          ON r.oid = drs.setrole
        CROSS JOIN LATERAL unnest(drs.setconfig) AS x(cfg)
        LEFT JOIN pg_settings ps
          ON ps.name = split_part(x.cfg, '=', 1)
        WHERE position('=' IN x.cfg) > 0
          AND split_part(x.cfg, '=', 1) IN (
              'track_activities',
              'track_counts',
              'track_io_timing',
              'track_wal_io_timing',
              'track_activity_query_size',
              'compute_query_id',
              'pg_stat_statements.max',
              'pg_stat_statements.track',
              'pg_stat_statements.track_planning',
              'pg_stat_statements.save',
              'pg_qs.query_capture_mode',
              'pg_qs.interval_length_minutes',
              'pg_qs.max_captured_queries',
              'pgms_wait_sampling.query_capture_mode',
              'pgms_wait_sampling.history_period',
              'metrics.collector_database_activity',
              'metrics.autovacuum_diagnostics',
              'max_connections',
              'shared_buffers',
              'effective_cache_size',
              'work_mem',
              'hash_mem_multiplier',
              'temp_buffers',
              'maintenance_work_mem',
              'autovacuum_work_mem',
              'logical_decoding_work_mem',
              'temp_file_limit',
              'log_temp_files',
              'max_worker_processes',
              'max_parallel_workers',
              'max_parallel_workers_per_gather',
              'max_parallel_maintenance_workers',
              'autovacuum_max_workers',
              'max_prepared_transactions',
              'huge_pages',
              'huge_page_size',
              'parallel_leader_participation',
              'parallel_setup_cost',
              'parallel_tuple_cost',
              'min_parallel_table_scan_size',
              'min_parallel_index_scan_size',
              'cpu_tuple_cost',
              'cpu_index_tuple_cost',
              'cpu_operator_cost',
              'seq_page_cost',
              'random_page_cost',
              'jit',
              'jit_above_cost',
              'jit_inline_above_cost',
              'jit_optimize_above_cost',
              'enable_gathermerge',
              'enable_parallel_append',
              'enable_parallel_hash'
          )
        ORDER BY
            parameter_name,
            setting_scope,
            database_name,
            role_name;
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
