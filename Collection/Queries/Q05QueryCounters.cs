using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q05 — query-level workload, CPU candidate, planning, JIT, parallel, I/O,
/// and temp spill. Server-wide pg_stat_statements collection; run once from
/// the database where the extension view is available. Counter60 tier.
/// Cumulative counters; key delta fields enriched via DeltaCache.
///
/// v3.12 scope correction: ExecutionScope changed to ServerWide. Q05 must not
/// be fanned out per application database — pg_stat_statements already spans the
/// entire server. current_user exclusion removed; collector self-noise is handled
/// downstream. pg_database and pg_roles joins added for labeling. Stable counter
/// identity remains (dbid, userid, queryid, toplevel).
/// Phase-boundary full snapshot: parameterize LIMIT to NULL (no limit) when
/// phase-boundary triggering is wired (spec §Q05).
/// </summary>
public sealed class Q05QueryCounters : IMetricQuery
{
    private readonly string _serverId;

    public Q05QueryCounters(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q05";
    public CadenceTier Tier => CadenceTier.Counter60;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    private const string Sql = """
        /* dms_metrics_collector:q05 */
        WITH raw AS MATERIALIZED (
            SELECT
                s.*,
                d.datname AS database_name,
                r.rolname AS role_name,
                to_jsonb(s) AS j
            FROM pg_stat_statements s
            LEFT JOIN pg_database d
              ON d.oid = s.dbid
            LEFT JOIN pg_roles r
              ON r.oid = s.userid
        ),
        normalized AS (
            SELECT
                dbid,
                database_name,
                userid,
                role_name,
                queryid,
                toplevel,
                plans,
                total_plan_time,
                min_plan_time,
                max_plan_time,
                mean_plan_time,
                stddev_plan_time,
                calls,
                total_exec_time,
                min_exec_time,
                max_exec_time,
                mean_exec_time,
                stddev_exec_time,
                rows,
                shared_blks_hit,
                shared_blks_read,
                shared_blks_dirtied,
                shared_blks_written,
                local_blks_hit,
                local_blks_read,
                local_blks_dirtied,
                local_blks_written,
                temp_blks_read,
                temp_blks_written,
                COALESCE(
                    NULLIF(j ->> 'blk_read_time', '')::double precision,
                    NULLIF(j ->> 'shared_blk_read_time', '')::double precision,
                    0
                ) AS shared_or_legacy_blk_read_time,
                COALESCE(
                    NULLIF(j ->> 'blk_write_time', '')::double precision,
                    NULLIF(j ->> 'shared_blk_write_time', '')::double precision,
                    0
                ) AS shared_or_legacy_blk_write_time,
                COALESCE(
                    NULLIF(j ->> 'temp_blk_read_time', '')::double precision,
                    0
                ) AS temp_blk_read_time,
                COALESCE(
                    NULLIF(j ->> 'temp_blk_write_time', '')::double precision,
                    0
                ) AS temp_blk_write_time,
                wal_records,
                wal_fpi,
                wal_bytes,
                COALESCE(NULLIF(j ->> 'wal_buffers_full', '')::bigint, 0)
                    AS wal_buffers_full,
                COALESCE(NULLIF(j ->> 'jit_functions', '')::bigint, 0)
                    AS jit_functions,
                COALESCE(NULLIF(j ->> 'jit_generation_time', '')::double precision, 0)
                    AS jit_generation_time,
                COALESCE(NULLIF(j ->> 'jit_inlining_count', '')::bigint, 0)
                    AS jit_inlining_count,
                COALESCE(NULLIF(j ->> 'jit_inlining_time', '')::double precision, 0)
                    AS jit_inlining_time,
                COALESCE(NULLIF(j ->> 'jit_optimization_count', '')::bigint, 0)
                    AS jit_optimization_count,
                COALESCE(NULLIF(j ->> 'jit_optimization_time', '')::double precision, 0)
                    AS jit_optimization_time,
                COALESCE(NULLIF(j ->> 'jit_emission_count', '')::bigint, 0)
                    AS jit_emission_count,
                COALESCE(NULLIF(j ->> 'jit_emission_time', '')::double precision, 0)
                    AS jit_emission_time,
                COALESCE(NULLIF(j ->> 'jit_deform_count', '')::bigint, 0)
                    AS jit_deform_count,
                COALESCE(NULLIF(j ->> 'jit_deform_time', '')::double precision, 0)
                    AS jit_deform_time,
                NULLIF(j ->> 'parallel_workers_to_launch', '')::bigint
                    AS parallel_workers_to_launch,
                NULLIF(j ->> 'parallel_workers_launched', '')::bigint
                    AS parallel_workers_launched,
                NULLIF(j ->> 'stats_since', '')::timestamptz AS stats_since,
                NULLIF(j ->> 'minmax_stats_since', '')::timestamptz
                    AS minmax_stats_since,
                LEFT(query, 6000) AS normalized_query_text
            FROM raw
        )
        SELECT
            clock_timestamp() AS collected_at,
            current_setting('server_version_num')::integer AS server_version_num,
            n.*,
            (
                n.jit_generation_time
                + n.jit_inlining_time
                + n.jit_optimization_time
                + n.jit_emission_time
                + n.jit_deform_time
            ) AS total_jit_time
        FROM normalized n
        ORDER BY
            total_exec_time DESC,
            shared_blks_read DESC,
            temp_blks_written DESC
        LIMIT 500;
        """;

    public async Task<CollectionResult> ExecuteAsync(
        NpgsqlConnection connection, DeltaCache deltas, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(Sql, connection);
        var rows = await RowReader.ReadAllAsync(cmd, ct).ConfigureAwait(false);

        var now = DateTimeOffset.UtcNow;
        var enriched = new List<IReadOnlyDictionary<string, object?>>(rows.Count);

        foreach (var row in rows)
        {
            var key = $"Q05:{row["dbid"]}:{row["userid"]}:{row["queryid"]}:{row["toplevel"]}";
            var statsReset = row.TryGetValue("stats_since", out var sr) ? sr?.ToString() : null;
            var mutable = new Dictionary<string, object?>(row);

            if (row["calls"] is { } callsObj)
            {
                var calls = Convert.ToInt64(callsObj);
                var outcome = deltas.Compute($"{key}:calls", calls, statsReset, now);
                mutable["calls_delta"] = outcome.HasDelta ? outcome.Delta : null;
                mutable["calls_per_second"] = outcome.HasDelta ? outcome.RatePerSecond : null;
            }

            if (row["total_exec_time"] is { } execObj)
            {
                var execUs = (long)(Convert.ToDouble(execObj) * 1000);
                var outcome = deltas.Compute($"{key}:total_exec_us", execUs, statsReset, now);
                mutable["total_exec_time_delta_ms"] = outcome.HasDelta ? outcome.Delta / 1000.0 : null;
            }

            enriched.Add(mutable);
        }

        var sourceTs = rows.Count > 0 && rows[0]["collected_at"] is DateTime dt
            ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            : now;

        return new CollectionResult(Id, _serverId, sourceTs, now, enriched);
    }
}
