using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q02 — unified activity / wait / connection / long query / long transaction
/// / parallel-worker snapshot. Fast tier (15 seconds).
/// </summary>
public sealed class Q02ActivitySnapshot : IMetricQuery
{
    private readonly string _serverId;

    public Q02ActivitySnapshot(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q02";
    public CadenceTier Tier => CadenceTier.Fast;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    private const string Sql = """
        /* dms_metrics_collector:q02 */
        WITH a AS MATERIALIZED (
            SELECT
                datid,
                datname,
                pid,
                leader_pid,
                usesysid,
                usename,
                application_name,
                client_addr,
                backend_start,
                xact_start,
                query_start,
                state_change,
                wait_event_type,
                wait_event,
                state,
                backend_xid,
                backend_xmin,
                query_id,
                backend_type,
                CASE
                    WHEN state = 'active' AND query_start IS NOT NULL
                    THEN EXTRACT(EPOCH FROM (clock_timestamp() - query_start))
                END AS active_query_age_seconds,
                CASE
                    WHEN xact_start IS NOT NULL
                    THEN EXTRACT(EPOCH FROM (clock_timestamp() - xact_start))
                END AS transaction_age_seconds,
                CASE
                    WHEN state LIKE 'idle in transaction%' AND state_change IS NOT NULL
                    THEN EXTRACT(EPOCH FROM (clock_timestamp() - state_change))
                END AS idle_in_transaction_age_seconds,
                LEFT(query, 2000) AS query_text
            FROM pg_stat_activity
            WHERE pid <> pg_backend_pid()
              AND application_name IS DISTINCT FROM 'dms_metrics_collector'
        ),
        settings AS (
            SELECT
                current_setting('max_connections')::integer AS max_connections,
                current_setting('max_worker_processes')::integer AS max_worker_processes,
                current_setting('max_parallel_workers')::integer AS max_parallel_workers,
                current_setting('max_parallel_workers_per_gather')::integer
                    AS max_parallel_workers_per_gather
        ),
        state_counts AS (
            SELECT COALESCE(state, 'unknown') AS key, COUNT(*) AS value
            FROM a
            WHERE backend_type = 'client backend'
            GROUP BY COALESCE(state, 'unknown')
        ),
        wait_type_counts AS (
            SELECT
                COALESCE(wait_event_type, 'CPU_or_not_waiting') AS wait_event_type,
                COUNT(*) AS session_count
            FROM a
            WHERE backend_type = 'client backend'
              AND state = 'active'
            GROUP BY COALESCE(wait_event_type, 'CPU_or_not_waiting')
        ),
        wait_event_counts AS (
            SELECT
                wait_event_type,
                wait_event,
                COUNT(*) AS session_count,
                MAX(active_query_age_seconds) AS longest_query_age_seconds
            FROM a
            WHERE backend_type = 'client backend'
              AND state = 'active'
              AND wait_event_type IS NOT NULL
            GROUP BY wait_event_type, wait_event
        ),
        wait_query_counts AS (
            SELECT
                datid,
                datname,
                application_name,
                query_id,
                wait_event_type,
                wait_event,
                COUNT(*) AS session_count,
                MAX(active_query_age_seconds) AS longest_query_age_seconds
            FROM a
            WHERE backend_type = 'client backend'
              AND state = 'active'
              AND wait_event_type IS NOT NULL
            GROUP BY
                datid,
                datname,
                application_name,
                query_id,
                wait_event_type,
                wait_event
        ),
        backend_counts AS (
            SELECT COALESCE(backend_type, 'unknown') AS key, COUNT(*) AS value
            FROM a
            GROUP BY COALESCE(backend_type, 'unknown')
        ),
        system_backend_wait_counts AS (
            SELECT
                backend_type,
                wait_event_type,
                wait_event,
                COUNT(*) AS process_count
            FROM a
            WHERE backend_type <> 'client backend'
              AND wait_event_type IS NOT NULL
            GROUP BY backend_type, wait_event_type, wait_event
        ),
        worker_groups AS (
            SELECT
                leader_pid,
                COUNT(*) AS launched_worker_count,
                COUNT(*) FILTER (WHERE wait_event_type IS NULL)
                    AS workers_running_or_not_waiting,
                COUNT(*) FILTER (WHERE wait_event_type IS NOT NULL)
                    AS workers_waiting,
                COUNT(*) FILTER (WHERE wait_event_type = 'IO')
                    AS workers_waiting_io,
                COUNT(*) FILTER (WHERE wait_event_type = 'LWLock')
                    AS workers_waiting_lwlock,
                COUNT(*) FILTER (WHERE wait_event_type = 'Lock')
                    AS workers_waiting_lock,
                COUNT(*) FILTER (WHERE wait_event_type = 'IPC')
                    AS workers_waiting_ipc,
                COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'worker_pid', pid,
                            'state', state,
                            'wait_event_type', wait_event_type,
                            'wait_event', wait_event
                        )
                        ORDER BY pid
                    ),
                    '[]'::jsonb
                ) AS workers
            FROM a
            WHERE backend_type = 'parallel worker'
              AND leader_pid IS NOT NULL
            GROUP BY leader_pid
        ),
        leader_groups AS (
            SELECT
                l.pid AS leader_pid,
                l.datid,
                l.datname,
                l.usename,
                l.application_name,
                l.state AS leader_state,
                l.wait_event_type AS leader_wait_event_type,
                l.wait_event AS leader_wait_event,
                l.query_id,
                EXTRACT(EPOCH FROM (clock_timestamp() - l.query_start))
                    AS query_age_seconds,
                COALESCE(w.launched_worker_count, 0) AS launched_worker_count,
                COALESCE(w.workers_running_or_not_waiting, 0)
                    AS workers_running_or_not_waiting,
                COALESCE(w.workers_waiting, 0) AS workers_waiting,
                COALESCE(w.workers_waiting_io, 0) AS workers_waiting_io,
                COALESCE(w.workers_waiting_lwlock, 0) AS workers_waiting_lwlock,
                COALESCE(w.workers_waiting_lock, 0) AS workers_waiting_lock,
                COALESCE(w.workers_waiting_ipc, 0) AS workers_waiting_ipc,
                COALESCE(w.workers, '[]'::jsonb) AS workers,
                l.query_text
            FROM worker_groups w
            JOIN a l
              ON l.pid = w.leader_pid
        )
        SELECT
            clock_timestamp() AS source_collected_at,
            s.max_connections,
            COUNT(*) FILTER (WHERE a.backend_type = 'client backend')
                AS client_connections,
            COUNT(*) FILTER (
                WHERE a.backend_type = 'client backend' AND a.state = 'active'
            ) AS active_client_connections,
            COUNT(*) FILTER (
                WHERE a.backend_type = 'client backend' AND a.state = 'idle'
            ) AS idle_client_connections,
            COUNT(*) FILTER (
                WHERE a.backend_type = 'client backend'
                  AND a.state LIKE 'idle in transaction%'
            ) AS idle_in_transaction_connections,
            COUNT(*) FILTER (
                WHERE a.backend_type = 'client backend'
                  AND a.state = 'active'
                  AND a.wait_event_type IS NOT NULL
            ) AS active_waiting_sessions,
            COUNT(*) FILTER (
                WHERE a.backend_type = 'client backend'
                  AND a.state = 'active'
                  AND a.wait_event_type = 'Lock'
            ) AS active_lock_waiting_sessions,
            COUNT(*) FILTER (
                WHERE a.backend_type = 'client backend'
                  AND a.state = 'active'
                  AND a.wait_event_type = 'IO'
            ) AS active_io_waiting_sessions,
            COUNT(*) FILTER (
                WHERE a.backend_type = 'client backend'
                  AND a.state = 'active'
                  AND a.wait_event_type = 'LWLock'
            ) AS active_lwlock_waiting_sessions,
            COUNT(*) FILTER (
                WHERE a.backend_type = 'client backend'
                  AND a.state = 'active'
                  AND a.wait_event_type = 'BufferPin'
            ) AS active_bufferpin_waiting_sessions,
            COUNT(*) FILTER (
                WHERE a.backend_type = 'client backend'
                  AND a.state = 'active'
                  AND a.wait_event_type IS NULL
            ) AS active_cpu_or_running_sessions,
            MAX(a.active_query_age_seconds) AS longest_active_query_seconds,
            MAX(a.transaction_age_seconds) AS longest_transaction_seconds,
            MAX(a.idle_in_transaction_age_seconds)
                AS longest_idle_in_transaction_seconds,
            ROUND(
                100.0 * COUNT(*) FILTER (WHERE a.backend_type = 'client backend')
                / NULLIF(s.max_connections::numeric, 0),
                2
            ) AS connection_utilization_percent,
            (SELECT COUNT(*) FROM wait_query_counts) AS wait_query_group_count,
            COALESCE(
                (SELECT jsonb_object_agg(key, value) FROM state_counts),
                '{}'::jsonb
            ) AS sessions_by_state,
            COALESCE(
                (
                    SELECT jsonb_object_agg(wait_event_type, session_count)
                    FROM wait_type_counts
                ),
                '{}'::jsonb
            ) AS active_sessions_by_wait_type,
            COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'wait_event_type', wait_event_type,
                            'wait_event', wait_event,
                            'session_count', session_count,
                            'longest_query_age_seconds', longest_query_age_seconds
                        )
                        ORDER BY session_count DESC, wait_event_type, wait_event
                    )
                    FROM wait_event_counts
                ),
                '[]'::jsonb
            ) AS active_sessions_by_wait_event,
            COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'datid', q.datid,
                            'datname', q.datname,
                            'application_name', q.application_name,
                            'query_id', q.query_id,
                            'wait_event_type', q.wait_event_type,
                            'wait_event', q.wait_event,
                            'session_count', q.session_count,
                            'longest_query_age_seconds', q.longest_query_age_seconds
                        )
                        ORDER BY q.session_count DESC,
                                 q.longest_query_age_seconds DESC NULLS LAST
                    )
                    FROM (
                        SELECT *
                        FROM wait_query_counts
                        ORDER BY session_count DESC,
                                 longest_query_age_seconds DESC NULLS LAST
                        LIMIT 500
                    ) q
                ),
                '[]'::jsonb
            ) AS active_waits_by_query_event,
            COALESCE(
                (SELECT jsonb_object_agg(key, value) FROM backend_counts),
                '{}'::jsonb
            ) AS sessions_by_backend_type,
            COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'backend_type', backend_type,
                            'wait_event_type', wait_event_type,
                            'wait_event', wait_event,
                            'process_count', process_count
                        )
                        ORDER BY backend_type, wait_event_type, wait_event
                    )
                    FROM system_backend_wait_counts
                ),
                '[]'::jsonb
            ) AS system_backend_waits,
            COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'datname', d.datname,
                            'pid', d.pid,
                            'leader_pid', d.leader_pid,
                            'usename', d.usename,
                            'application_name', d.application_name,
                            'client_addr', d.client_addr,
                            'backend_type', d.backend_type,
                            'state', d.state,
                            'wait_event_type', d.wait_event_type,
                            'wait_event', d.wait_event,
                            'query_id', d.query_id,
                            'active_query_age_seconds', d.active_query_age_seconds,
                            'transaction_age_seconds', d.transaction_age_seconds,
                            'idle_in_transaction_age_seconds',
                                d.idle_in_transaction_age_seconds,
                            'query_text', d.query_text
                        )
                        ORDER BY
                            COALESCE(
                                d.active_query_age_seconds,
                                d.transaction_age_seconds,
                                0
                            ) DESC
                    )
                    FROM (
                        SELECT *
                        FROM a
                        WHERE state LIKE 'idle in transaction%'
                           OR (
                                backend_type = 'client backend'
                                AND state = 'active'
                                AND (
                                    wait_event_type IS NOT NULL
                                    OR active_query_age_seconds >= 2
                                )
                           )
                           OR backend_type IN (
                                'autovacuum worker',
                                'parallel worker',
                                'checkpointer',
                                'background writer',
                                'walwriter',
                                'startup'
                           )
                        ORDER BY
                            COALESCE(
                                active_query_age_seconds,
                                transaction_age_seconds,
                                0
                            ) DESC
                        LIMIT 200
                    ) d
                ),
                '[]'::jsonb
            ) AS interesting_sessions,
            s.max_worker_processes,
            s.max_parallel_workers,
            s.max_parallel_workers_per_gather,
            COUNT(*) FILTER (WHERE a.backend_type = 'parallel worker')
                AS active_parallel_workers,
            COUNT(*) FILTER (
                WHERE a.backend_type = 'parallel worker'
                  AND a.wait_event_type IS NULL
            ) AS parallel_workers_running_or_not_waiting,
            COUNT(*) FILTER (
                WHERE a.backend_type = 'parallel worker'
                  AND a.wait_event_type IS NOT NULL
            ) AS parallel_workers_waiting,
            COUNT(DISTINCT a.leader_pid) FILTER (
                WHERE a.backend_type = 'parallel worker'
                  AND a.leader_pid IS NOT NULL
            ) AS active_parallel_query_groups,
            ROUND(
                100.0 * COUNT(*) FILTER (WHERE a.backend_type = 'parallel worker')
                / NULLIF(s.max_parallel_workers::numeric, 0),
                2
            ) AS parallel_worker_pool_utilization_percent,
            COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'leader_pid', g.leader_pid,
                            'datid', g.datid,
                            'datname', g.datname,
                            'usename', g.usename,
                            'application_name', g.application_name,
                            'query_id', g.query_id,
                            'query_age_seconds', g.query_age_seconds,
                            'leader_state', g.leader_state,
                            'leader_wait_event_type', g.leader_wait_event_type,
                            'leader_wait_event', g.leader_wait_event,
                            'launched_worker_count', g.launched_worker_count,
                            'workers_running_or_not_waiting',
                                g.workers_running_or_not_waiting,
                            'workers_waiting', g.workers_waiting,
                            'workers_waiting_io', g.workers_waiting_io,
                            'workers_waiting_lwlock', g.workers_waiting_lwlock,
                            'workers_waiting_lock', g.workers_waiting_lock,
                            'workers_waiting_ipc', g.workers_waiting_ipc,
                            'workers', g.workers,
                            'query_text', g.query_text
                        )
                        ORDER BY
                            g.launched_worker_count DESC,
                            g.query_age_seconds DESC NULLS LAST
                    )
                    FROM leader_groups g
                ),
                '[]'::jsonb
            ) AS parallel_query_groups
        FROM settings s
        LEFT JOIN a ON TRUE
        GROUP BY
            s.max_connections,
            s.max_worker_processes,
            s.max_parallel_workers,
            s.max_parallel_workers_per_gather;
        """;

    public async Task<CollectionResult> ExecuteAsync(
        NpgsqlConnection connection, DeltaCache deltas, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(Sql, connection);
        var rows = await RowReader.ReadAllAsync(cmd, ct).ConfigureAwait(false);

        var sourceTs = rows.Count > 0 && rows[0]["source_collected_at"] is DateTime dt
            ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            : DateTimeOffset.UtcNow;

        return new CollectionResult(Id, _serverId, sourceTs, DateTimeOffset.UtcNow, rows);
    }
}
