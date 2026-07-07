using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q09 — table access, DML, dead tuples, vacuum, and table I/O. Health5m tier,
/// PerApplicationDatabase (pg_stat_user_tables is current-database-scoped).
/// </summary>
public sealed class Q09TableStats : IMetricQuery
{
    private readonly string _serverId;

    public Q09TableStats(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q09";
    public CadenceTier Tier => CadenceTier.Health5m;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.PerApplicationDatabase;

    private const string Sql = """
        /* dms_metrics_collector:q09 */
        SELECT
            clock_timestamp() AS collected_at,
            current_database() AS database_name,
            st.relid, st.schemaname, st.relname,
            st.seq_scan, st.seq_tup_read, st.idx_scan, st.idx_tup_fetch,
            st.n_tup_ins, st.n_tup_upd, st.n_tup_del, st.n_tup_hot_upd,
            st.n_live_tup, st.n_dead_tup, st.n_mod_since_analyze,
            st.last_vacuum, st.last_autovacuum, st.last_analyze, st.last_autoanalyze,
            st.vacuum_count, st.autovacuum_count, st.analyze_count, st.autoanalyze_count,
            io.heap_blks_read, io.heap_blks_hit,
            io.idx_blks_read, io.idx_blks_hit,
            io.toast_blks_read, io.toast_blks_hit,
            io.tidx_blks_read, io.tidx_blks_hit
        FROM pg_stat_user_tables st
        LEFT JOIN pg_statio_user_tables io ON io.relid = st.relid
        ORDER BY st.relid;
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
            var db = row["database_name"]?.ToString();
            var relid = row["relid"]?.ToString();
            var key = $"Q09:{db}:{relid}";
            var mutable = new Dictionary<string, object?>(row);

            AddDelta(deltas, mutable, row, $"{key}:seq_scan", "seq_scan", null, now);
            AddDelta(deltas, mutable, row, $"{key}:n_tup_ins", "n_tup_ins", null, now);
            AddDelta(deltas, mutable, row, $"{key}:n_tup_upd", "n_tup_upd", null, now);
            AddDelta(deltas, mutable, row, $"{key}:n_tup_del", "n_tup_del", null, now);
            AddDelta(deltas, mutable, row, $"{key}:n_tup_hot_upd", "n_tup_hot_upd", null, now);
            AddDelta(deltas, mutable, row, $"{key}:heap_blks_read", "heap_blks_read", null, now);
            AddDelta(deltas, mutable, row, $"{key}:heap_blks_hit", "heap_blks_hit", null, now);

            enriched.Add(mutable);
        }

        var sourceTs = rows.Count > 0 && rows[0]["collected_at"] is DateTime dt
            ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            : now;

        return new CollectionResult(Id, _serverId, sourceTs, now, enriched);
    }

    private static void AddDelta(
        DeltaCache deltas, Dictionary<string, object?> mutable,
        IReadOnlyDictionary<string, object?> row,
        string cacheKey, string column, string? statsReset, DateTimeOffset now)
    {
        if (!row.TryGetValue(column, out var v) || v is null) return;
        var outcome = deltas.Compute(cacheKey, Convert.ToInt64(v), statsReset, now);
        mutable[$"{column}_delta"] = outcome.HasDelta ? outcome.Delta : null;
    }
}
