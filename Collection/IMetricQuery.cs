using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection;

/// <summary>
/// A single collectable metric query. Implementations declare which cadence tier
/// drives them and which connection source they need. Add a new query by adding
/// one class and registering it in Program.cs.
/// Both Postgres and the PgBouncer admin console are reached through Npgsql, so a
/// single connection type is used; the factory configures protocol/compatibility.
/// </summary>
public interface IMetricQuery
{
    /// <summary>Stable id, e.g. "Q02", "Q04", "PB02". Used as the overlap-guard key.</summary>
    string Id { get; }

    /// <summary>Cadence tier that schedules this query.</summary>
    CadenceTier Tier { get; }

    /// <summary>Which open connection this query expects.</summary>
    QuerySource Source { get; }

    /// <summary>
    /// How PostgreSQL queries are targeted across multiple databases.
    /// Ignored for PgBouncer admin queries.
    /// </summary>
    QueryExecutionScope ExecutionScope { get; }

    /// <summary>
    /// Executes the query against an already-open connection and returns the rows.
    /// Cumulative-counter queries should pass their values through <paramref name="deltas"/>.
    /// </summary>
    Task<CollectionResult> ExecuteAsync(
        NpgsqlConnection connection,
        DeltaCache deltas,
        CancellationToken cancellationToken);
}

/// <summary>
/// Result of one collection run. Preserves raw values; deltas/rates are attached
/// as extra columns by the query when relevant. The sink persists this.
/// </summary>
public sealed record CollectionResult(
    string QueryId,
    string ServerId,
    DateTimeOffset SourceCollectedAt,   // clock_timestamp() from PostgreSQL
    DateTimeOffset CollectorReceivedAt, // wall clock on the VM when rows arrived
    IReadOnlyList<IReadOnlyDictionary<string, object?>> Rows,
    string? Note = null)
{
    public int RowCount => Rows.Count;
}

public enum QueryExecutionScope
{
    ServerWide,
    PerApplicationDatabase,
    AzureSys
}
