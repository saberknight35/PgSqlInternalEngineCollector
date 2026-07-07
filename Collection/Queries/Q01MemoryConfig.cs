using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q01B — effective server/session settings using the unified parameter list.
/// Config6h tier, server-wide.
/// </summary>
public sealed class Q01MemoryConfig : IMetricQuery
{
    private readonly string _serverId;

    public Q01MemoryConfig(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q01B";
    public CadenceTier Tier => CadenceTier.Config6h;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    private const string Sql = """
        /* dms_metrics_collector:q01b_effective_settings */
        SELECT
            clock_timestamp() AS collected_at,
            'server_effective_for_collector'::text AS setting_scope,
            current_database()::text AS database_name,
            current_user::text AS role_name,
            NULL::text AS object_identity,
            s.name AS parameter_name,
            s.setting AS parameter_value,
            s.unit,
            s.source,
            s.context,
            s.pending_restart
        FROM pg_settings s
        WHERE s.name IN (
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
        ORDER BY parameter_name;
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
