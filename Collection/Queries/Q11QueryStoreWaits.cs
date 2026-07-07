using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q11 — Query Store historical waits enriched with runtime and query text.
/// Object15m tier, AzureSys database. Requires Azure PostgreSQL Query Store
/// (pg_qs.query_capture_mode != none). Tracks last fetched end_time to avoid
/// re-reading old windows and honours the 2-minute persistence lag.
/// </summary>
public sealed class Q11QueryStoreWaits : IMetricQuery
{
    private readonly string _serverId;
    private DateTimeOffset _lastEndTime = DateTimeOffset.MinValue;

    public Q11QueryStoreWaits(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q11";
    public CadenceTier Tier => CadenceTier.Object15m;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.AzureSys;

    private const string Sql = """
        /* dms_metrics_collector:q11 */
        WITH settings AS (
            SELECT current_setting('pgms_wait_sampling.history_period')::numeric AS history_period_ms
        ),
        waits AS (
            SELECT w.start_time, w.end_time, w.user_id, w.db_id, w.query_id,
                   w.event_type, w.event,
                   w.calls::bigint AS wait_sample_count,
                   w.calls::numeric * s.history_period_ms AS estimated_sampled_wait_ms
            FROM query_store.pgms_wait_sampling_view w
            CROSS JOIN settings s
            WHERE w.end_time > @last_end_time
              AND w.end_time <= (clock_timestamp() AT TIME ZONE 'UTC') - INTERVAL '2 minutes'
        ),
        runtime AS (
            SELECT
                q.start_time, q.end_time, q.user_id, q.db_id, q.query_id,
                MAX(q.query_sql_text) AS query_sql_text,
                BOOL_OR(q.is_system_query) AS is_system_query,
                SUM(q.calls) AS runtime_calls,
                SUM(q.total_time) AS total_exec_time_ms,
                CASE WHEN SUM(q.calls) > 0 THEN SUM(q.total_time) / SUM(q.calls) END AS mean_exec_time_ms,
                MAX(q.max_time) AS max_exec_time_ms,
                SUM(q.shared_blks_hit) AS shared_blks_hit,
                SUM(q.shared_blks_read) AS shared_blks_read,
                SUM(q.shared_blks_dirtied) AS shared_blks_dirtied,
                SUM(q.shared_blks_written) AS shared_blks_written,
                SUM(q.temp_blks_read) AS temp_blks_read,
                SUM(q.temp_blks_written) AS temp_blks_written,
                SUM(q.blk_read_time) AS blk_read_time_ms,
                SUM(q.blk_write_time) AS blk_write_time_ms
            FROM query_store.qs_view q
                        WHERE q.end_time > @last_end_time
              AND q.end_time <= (clock_timestamp() AT TIME ZONE 'UTC') - INTERVAL '2 minutes'
            GROUP BY q.start_time, q.end_time, q.user_id, q.db_id, q.query_id
        ),
        joined AS (
            SELECT w.*,
                   r.query_sql_text, r.is_system_query, r.runtime_calls,
                   r.total_exec_time_ms, r.mean_exec_time_ms, r.max_exec_time_ms,
                   r.shared_blks_hit, r.shared_blks_read, r.shared_blks_dirtied,
                   r.shared_blks_written, r.temp_blks_read, r.temp_blks_written,
                   r.blk_read_time_ms, r.blk_write_time_ms,
                   SUM(w.wait_sample_count) OVER (PARTITION BY w.start_time, w.end_time)
                       AS all_window_wait_samples,
                   SUM(w.wait_sample_count) OVER (
                       PARTITION BY w.start_time, w.end_time, w.user_id, w.db_id, w.query_id
                   ) AS query_window_wait_samples
            FROM waits w
            LEFT JOIN runtime r
              ON r.start_time = w.start_time AND r.end_time = w.end_time
             AND r.user_id = w.user_id AND r.db_id = w.db_id AND r.query_id = w.query_id
        )
        SELECT
            clock_timestamp() AS collected_at,
            start_time, end_time, user_id, db_id, query_id, event_type, event,
            CASE
                WHEN event_type = 'Activity' THEN 'background_or_idle_activity'
                WHEN event_type = 'Client' AND event = 'ClientRead' THEN 'client_idle_or_think_time'
                WHEN event_type = 'Client' THEN 'client_or_network_backpressure'
                ELSE 'database_resource_wait'
            END AS wait_classification,
            wait_sample_count, estimated_sampled_wait_ms,
            ROUND(100.0 * wait_sample_count / NULLIF(all_window_wait_samples, 0), 2)
                AS event_share_of_all_window_wait_percent,
            ROUND(100.0 * wait_sample_count / NULLIF(query_window_wait_samples, 0), 2)
                AS event_share_within_query_percent,
            CASE WHEN runtime_calls > 0 THEN ROUND(wait_sample_count::numeric / runtime_calls, 4) END
                AS wait_samples_per_runtime_call,
            CASE WHEN runtime_calls > 0 THEN ROUND(estimated_sampled_wait_ms / runtime_calls, 2) END
                AS estimated_wait_ms_per_runtime_call,
            runtime_calls, total_exec_time_ms, mean_exec_time_ms, max_exec_time_ms,
            shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written,
            temp_blks_read, temp_blks_written, blk_read_time_ms, blk_write_time_ms,
            is_system_query, LEFT(query_sql_text, 6000) AS query_sql_text
        FROM joined
        ORDER BY end_time, wait_sample_count DESC, db_id, query_id, event_type, event;
        """;

    public async Task<CollectionResult> ExecuteAsync(
        NpgsqlConnection connection, DeltaCache deltas, CancellationToken ct)
    {
        // Initialise the window start to 17 minutes ago on the very first run so we
        // cover the most recent closed Query Store window (which closes every 1 minute
        // and has a 2-minute persistence lag).
        if (_lastEndTime == DateTimeOffset.MinValue)
            _lastEndTime = DateTimeOffset.UtcNow.AddMinutes(-17);

        await using var cmd = new NpgsqlCommand(Sql, connection);
    cmd.Parameters.AddWithValue("last_end_time", NpgsqlTypes.NpgsqlDbType.TimestampTz, _lastEndTime);

        var rows = await RowReader.ReadAllAsync(cmd, ct).ConfigureAwait(false);

        // Advance the watermark to the latest end_time seen so the next run doesn't re-read.
        foreach (var row in rows)
        {
            if (row.TryGetValue("end_time", out var et) && et is DateTime endDt)
            {
                var eto = new DateTimeOffset(DateTime.SpecifyKind(endDt, DateTimeKind.Utc));
                if (eto > _lastEndTime)
                    _lastEndTime = eto;
            }
        }

        var now = DateTimeOffset.UtcNow;
        var sourceTs = rows.Count > 0 && rows[0]["collected_at"] is DateTime dt
            ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            : now;

        return new CollectionResult(Id, _serverId, sourceTs, now, rows.Cast<IReadOnlyDictionary<string, object?>>().ToList());
    }
}
