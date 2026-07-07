using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q08 — checkpoint, background writer, and WAL snapshot. Counter60 tier, ServerWide.
/// PostgreSQL 18 shape using pg_stat_bgwriter + pg_stat_checkpointer + pg_stat_wal.
/// </summary>
public sealed class Q08CheckpointWal : IMetricQuery
{
    private readonly string _serverId;

    public Q08CheckpointWal(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q08";
    public CadenceTier Tier => CadenceTier.Counter60;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    private const string Sql = """
        /* dms_metrics_collector:q08 */
        SELECT clock_timestamp() AS collected_at, 'pg_stat_bgwriter'::text AS source_view, to_jsonb(bg) AS payload
        FROM pg_stat_bgwriter bg
        UNION ALL
        SELECT clock_timestamp() AS collected_at, 'pg_stat_checkpointer'::text AS source_view, to_jsonb(cp) AS payload
        FROM pg_stat_checkpointer cp
        UNION ALL
        SELECT clock_timestamp() AS collected_at, 'pg_stat_wal'::text AS source_view, to_jsonb(wal) AS payload
        FROM pg_stat_wal wal;
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
            var sourceView = row["source_view"]?.ToString();
            var mutable = new Dictionary<string, object?>(row);

            // Parse the jsonb payload to extract stats_reset for delta keying
            var payloadStr = row["payload"]?.ToString();
            string? statsReset = null;
            if (payloadStr != null)
            {
                using var doc = System.Text.Json.JsonDocument.Parse(payloadStr);
                if (doc.RootElement.TryGetProperty("stats_reset", out var sr))
                    statsReset = sr.ValueKind != System.Text.Json.JsonValueKind.Null ? sr.GetString() : null;

                if (sourceView == "pg_stat_bgwriter")
                {
                    AddJsonDelta(deltas, mutable, doc, $"Q08:bgwriter:buffers_clean", "buffers_clean", statsReset, now);
                    AddJsonDelta(deltas, mutable, doc, $"Q08:bgwriter:buffers_backend", "buffers_backend", statsReset, now);
                }
                else if (sourceView == "pg_stat_checkpointer")
                {
                    AddJsonDelta(deltas, mutable, doc, $"Q08:checkpointer:num_timed", "num_timed", statsReset, now);
                    AddJsonDelta(deltas, mutable, doc, $"Q08:checkpointer:num_requested", "num_requested", statsReset, now);
                    AddJsonDelta(deltas, mutable, doc, $"Q08:checkpointer:write_time", "write_time", statsReset, now);
                    AddJsonDelta(deltas, mutable, doc, $"Q08:checkpointer:sync_time", "sync_time", statsReset, now);
                }
                else if (sourceView == "pg_stat_wal")
                {
                    AddJsonDelta(deltas, mutable, doc, $"Q08:wal:wal_bytes", "wal_bytes", statsReset, now);
                    AddJsonDelta(deltas, mutable, doc, $"Q08:wal:wal_buffers_full", "wal_buffers_full", statsReset, now);
                }
            }

            enriched.Add(mutable);
        }

        var sourceTs = rows.Count > 0 && rows[0]["collected_at"] is DateTime dt
            ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            : now;

        return new CollectionResult(Id, _serverId, sourceTs, now, enriched);
    }

    private static void AddJsonDelta(
        DeltaCache deltas,
        Dictionary<string, object?> mutable,
        System.Text.Json.JsonDocument doc,
        string cacheKey, string jsonProp,
        string? statsReset, DateTimeOffset now)
    {
        if (!doc.RootElement.TryGetProperty(jsonProp, out var el)) return;
        long value;
        if (!el.TryGetInt64(out value))
        {
            if (!el.TryGetDouble(out var dv)) return;
            value = (long)dv;
        }
        var outcome = deltas.Compute(cacheKey, value, statsReset, now);
        mutable[$"{jsonProp}_delta"] = outcome.HasDelta ? outcome.Delta : null;
        mutable[$"{jsonProp}_per_second"] = outcome.HasDelta ? outcome.RatePerSecond : null;
    }
}
