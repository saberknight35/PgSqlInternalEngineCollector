using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection.Queries;

/// <summary>
/// Q15 — main shared-memory allocation snapshot. Config6h tier, ServerWide.
/// Runs at startup and every 6 hours. Data is stable between restart/extension events.
/// </summary>
public sealed class Q15SharedMemory : IMetricQuery
{
    private readonly string _serverId;

    public Q15SharedMemory(IOptions<CollectorOptions> options)
        => _serverId = options.Value.ServerId;

    public string Id => "Q15";
    public CadenceTier Tier => CadenceTier.Config6h;
    public QuerySource Source => QuerySource.Postgres;
    public QueryExecutionScope ExecutionScope => QueryExecutionScope.ServerWide;

    private const string Sql = """
        /* dms_metrics_collector:q15 */
        WITH a AS (
            SELECT COALESCE(name, '<unused>') AS allocation_name, off, size, allocated_size
            FROM pg_shmem_allocations
        ),
        summary AS (
            SELECT
                SUM(allocated_size) AS total_main_shared_memory_bytes,
                SUM(allocated_size) FILTER (WHERE allocation_name <> '<unused>')
                    AS allocated_named_and_anonymous_bytes,
                SUM(allocated_size) FILTER (WHERE allocation_name = '<unused>')
                    AS unused_main_shared_memory_bytes
            FROM a
        )
        SELECT
            clock_timestamp() AS collected_at,
            a.allocation_name, a.off, a.size, a.allocated_size,
            s.total_main_shared_memory_bytes,
            s.allocated_named_and_anonymous_bytes,
            s.unused_main_shared_memory_bytes,
            ROUND(100.0 * s.allocated_named_and_anonymous_bytes
                / NULLIF(s.total_main_shared_memory_bytes, 0), 2)
                AS main_shared_memory_allocated_percent,
            ROUND(100.0 * a.allocated_size
                / NULLIF(s.total_main_shared_memory_bytes, 0), 4)
                AS allocation_share_percent
        FROM a CROSS JOIN summary s
        ORDER BY a.allocated_size DESC, a.allocation_name;
        """;

    public async Task<CollectionResult> ExecuteAsync(
        NpgsqlConnection connection, DeltaCache deltas, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(Sql, connection);
        var rows = await RowReader.ReadAllAsync(cmd, ct).ConfigureAwait(false);

        var sourceTs = rows.Count > 0 && rows[0]["collected_at"] is DateTime dt
            ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            : DateTimeOffset.UtcNow;

        return new CollectionResult(Id, _serverId, sourceTs, DateTimeOffset.UtcNow, rows.Cast<IReadOnlyDictionary<string, object?>>().ToList());
    }
}