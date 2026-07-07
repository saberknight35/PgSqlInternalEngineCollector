using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q04 — database-level cumulative counters (30 s tier).
/// Raw cumulative values are preserved; representative deltas are attached.
/// </summary>
public sealed class Q04DatabaseCounters : IMetricQuery
{
    private readonly string _serverId;

    public Q04DatabaseCounters(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q04";
    public CadenceTier Tier => CadenceTier.Counter30;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    private const string Sql = """
        /* dms_metrics_collector:q04 */
        SELECT
                        clock_timestamp() AS source_collected_at,
            datid,
            datname,
                        numbackends,
            xact_commit,
            xact_rollback,
            blks_read,
            blks_hit,
                        tup_returned,
                        tup_fetched,
                        tup_inserted,
                        tup_updated,
                        tup_deleted,
                        conflicts,
            temp_files,
            temp_bytes,
            deadlocks,
                        blk_read_time,
                        blk_write_time,
                        session_time,
                        active_time,
                        idle_in_transaction_time,
                        sessions,
                        sessions_abandoned,
                        sessions_fatal,
                        sessions_killed,
            stats_reset
        FROM pg_stat_database
                WHERE datname IS NOT NULL
                    AND datname NOT IN ('template0', 'template1')
                ORDER BY datid;
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
            var datid = Convert.ToString(row["datid"]);
            var statsReset = row["stats_reset"]?.ToString();
            var mutable = new Dictionary<string, object?>(row);

            // Derive a delta for one representative counter; repeat per counter as needed.
            if (row["xact_commit"] is { } commitObj)
            {
                var commit = Convert.ToInt64(commitObj);
                var outcome = deltas.Compute($"Q04:{datid}:xact_commit", commit, statsReset, now);
                mutable["xact_commit_delta"] = outcome.HasDelta ? outcome.Delta : null;
                mutable["xact_commit_rate_per_sec"] = outcome.HasDelta ? outcome.RatePerSecond : null;
                mutable["counter_reset_boundary"] = outcome.IsResetBoundary;
            }

            enriched.Add(mutable);
        }

        var sourceTs = rows.Count > 0 && rows[0]["source_collected_at"] is DateTime dt
            ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            : now;

        return new CollectionResult(Id, _serverId, sourceTs, now, enriched);
    }
}
