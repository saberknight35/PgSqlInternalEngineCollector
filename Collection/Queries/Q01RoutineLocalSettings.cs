using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q01D — routine-local function/procedure settings.
/// Config6h tier, per-application-database. Zero-row result is valid.
/// </summary>
public sealed class Q01RoutineLocalSettings : IMetricQuery
{
    private readonly string _serverId;

    public Q01RoutineLocalSettings(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q01D";
    public CadenceTier Tier => CadenceTier.Config6h;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.PerApplicationDatabase;

    private const string Sql = """
        /* dms_metrics_collector:q01d_routine_local_settings */
        SELECT
            clock_timestamp() AS collected_at,
            'routine_local_setting'::text AS setting_scope,
            current_database()::text AS database_name,
            owner_role.rolname::text AS role_name,
            pr.oid::regprocedure::text AS object_identity,
            split_part(x.cfg, '=', 1) AS parameter_name,
            substring(x.cfg FROM position('=' IN x.cfg) + 1) AS parameter_value,
            ps.unit,
            'pg_proc.proconfig'::text AS source,
            ps.context,
            NULL::boolean AS pending_restart
        FROM pg_proc pr
        JOIN pg_namespace n
          ON n.oid = pr.pronamespace
        JOIN pg_roles owner_role
          ON owner_role.oid = pr.proowner
        CROSS JOIN LATERAL unnest(pr.proconfig) AS x(cfg)
        LEFT JOIN pg_settings ps
          ON ps.name = split_part(x.cfg, '=', 1)
        WHERE pr.proconfig IS NOT NULL
          AND position('=' IN x.cfg) > 0
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND n.nspname NOT LIKE 'pg_toast%'
          AND n.nspname NOT LIKE 'pg_temp_%'
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
            role_name,
            object_identity;
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
