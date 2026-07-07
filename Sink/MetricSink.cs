using System.Text.Json;
using System.Globalization;
using PgSqlInternalEngineCollector.Service.Collection;
using PgSqlInternalEngineCollector.Service.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;

namespace PgSqlInternalEngineCollector.Service.Sink;

public interface IMetricSink
{
    Task WriteAsync(CollectionResult result, CancellationToken ct);
}

/// <summary>
/// Tries the consolidation DB first; on any failure it spills the sample to local
/// disk so a transient DB outage never blocks the 15-second path or loses samples.
/// A background drain (not shown) would later replay buffered files into the DB.
/// </summary>
public sealed class BufferedSink : IMetricSink
{
    private readonly ConsolidationDbSink _primary;
    private readonly LocalBufferSink _buffer;
    private readonly ILogger<BufferedSink> _log;

    public BufferedSink(ConsolidationDbSink primary, LocalBufferSink buffer, ILogger<BufferedSink> log)
    {
        _primary = primary;
        _buffer = buffer;
        _log = log;
    }

    public async Task WriteAsync(CollectionResult result, CancellationToken ct)
    {
        try
        {
            await _primary.WriteAsync(result, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex,
                "Consolidation write failed for {QueryId}; buffering to disk.", result.QueryId);
            await _buffer.WriteAsync(result, ct).ConfigureAwait(false);
        }
    }
}

/// <summary>
/// Writes a result into the consolidation SQL DB. Schema follows the spec §12
/// minimum keys (collector_run + per-query tables). Left as a stub: wire up the
/// real INSERTs / table-valued parameters per your consolidation schema.
/// </summary>
public sealed class ConsolidationDbSink : IMetricSink
{
    private readonly ConsolidationOptions _options;
    private readonly ILogger<ConsolidationDbSink> _log;
    private readonly SemaphoreSlim _schemaGate = new(1, 1);
    private readonly SemaphoreSlim _queryTableGate = new(1, 1);
    private readonly HashSet<string> _readyQueryTables = new(StringComparer.OrdinalIgnoreCase);
    private volatile bool _schemaReady;

    public ConsolidationDbSink(IOptions<CollectorOptions> options, ILogger<ConsolidationDbSink> log)
    {
        _options = options.Value.Consolidation;
        _log = log;
    }

    public async Task WriteAsync(CollectionResult result, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(_options.ConnectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);
        await EnsureSchemaAsync(conn, ct).ConfigureAwait(false);
        var queryTableName = BuildQueryTableName(result.QueryId);
        var columnMap = BuildColumnMap(result.Rows);
        await EnsureQueryTableAsync(conn, queryTableName, columnMap.Values.ToArray(), ct).ConfigureAwait(false);

        await using var tx = await conn.BeginTransactionAsync(ct).ConfigureAwait(false);

        var runId = Guid.NewGuid();
        var durationMs = Math.Max(0L, (long)(result.CollectorReceivedAt - result.SourceCollectedAt).TotalMilliseconds);

        const string insertRunSql = """
            INSERT INTO collector_run (
                run_id,
                server_id,
                query_id,
                scheduled_at,
                started_at,
                completed_at,
                status,
                row_count,
                duration_ms,
                error,
                source_collected_at,
                collector_received_at,
                note
            ) VALUES (
                @run_id,
                @server_id,
                @query_id,
                @scheduled_at,
                @started_at,
                @completed_at,
                @status,
                @row_count,
                @duration_ms,
                @error,
                @source_collected_at,
                @collector_received_at,
                @note
            );
            """;

        await using (var cmd = new NpgsqlCommand(insertRunSql, conn, tx))
        {
            cmd.Parameters.AddWithValue("run_id", NpgsqlDbType.Uuid, runId);
            cmd.Parameters.AddWithValue("server_id", NpgsqlDbType.Text, result.ServerId);
            cmd.Parameters.AddWithValue("query_id", NpgsqlDbType.Text, result.QueryId);
            cmd.Parameters.AddWithValue("scheduled_at", NpgsqlDbType.TimestampTz, result.SourceCollectedAt);
            cmd.Parameters.AddWithValue("started_at", NpgsqlDbType.TimestampTz, result.SourceCollectedAt);
            cmd.Parameters.AddWithValue("completed_at", NpgsqlDbType.TimestampTz, result.CollectorReceivedAt);
            cmd.Parameters.AddWithValue("status", NpgsqlDbType.Text, "ok");
            cmd.Parameters.AddWithValue("row_count", NpgsqlDbType.Integer, result.RowCount);
            cmd.Parameters.AddWithValue("duration_ms", NpgsqlDbType.Bigint, durationMs);
            cmd.Parameters.AddWithValue("error", NpgsqlDbType.Text, DBNull.Value);
            cmd.Parameters.AddWithValue("source_collected_at", NpgsqlDbType.TimestampTz, result.SourceCollectedAt);
            cmd.Parameters.AddWithValue("collector_received_at", NpgsqlDbType.TimestampTz, result.CollectorReceivedAt);
            cmd.Parameters.AddWithValue("note", NpgsqlDbType.Text, (object?)result.Note ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        var quotedQueryTableName = QuoteIdentifier(queryTableName);
        var dynamicColumnPairs = columnMap.ToArray();
        var dynamicColumns = dynamicColumnPairs.Select(x => x.Value).ToArray();
        var sourceKeys = dynamicColumnPairs.Select(x => x.Key).ToArray();
        var dynamicInsertColumns = dynamicColumns.Select(QuoteIdentifier).ToArray();
        var dynamicInsertParams = dynamicColumns.Select((_, idx) => $"@f_{idx}").ToArray();

        var insertColumns = new List<string>
        {
            "run_id",
            "row_ordinal",
            "server_id",
            "source_collected_at",
            "collector_received_at"
        };
        insertColumns.AddRange(dynamicInsertColumns);

        var insertParams = new List<string>
        {
            "@run_id",
            "@row_ordinal",
            "@server_id",
            "@source_collected_at",
            "@collector_received_at"
        };
        insertParams.AddRange(dynamicInsertParams);

        var insertRowSql = $"""
            INSERT INTO {quotedQueryTableName} ({string.Join(", ", insertColumns)})
            VALUES ({string.Join(", ", insertParams)});
            """;

        for (var i = 0; i < result.Rows.Count; i++)
        {
            var row = result.Rows[i];
            await using var rowCmd = new NpgsqlCommand(insertRowSql, conn, tx);
            rowCmd.Parameters.AddWithValue("run_id", NpgsqlDbType.Uuid, runId);
            rowCmd.Parameters.AddWithValue("row_ordinal", NpgsqlDbType.Integer, i);
            rowCmd.Parameters.AddWithValue("server_id", NpgsqlDbType.Text, result.ServerId);
            rowCmd.Parameters.AddWithValue("source_collected_at", NpgsqlDbType.TimestampTz, result.SourceCollectedAt);
            rowCmd.Parameters.AddWithValue("collector_received_at", NpgsqlDbType.TimestampTz, result.CollectorReceivedAt);

            for (var j = 0; j < dynamicColumns.Length; j++)
            {
                row.TryGetValue(sourceKeys[j], out var value);
                rowCmd.Parameters.AddWithValue($"f_{j}", NpgsqlDbType.Text, (object?)ToFlatString(value) ?? DBNull.Value);
            }

            await rowCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await tx.CommitAsync(ct).ConfigureAwait(false);

        _log.LogInformation(
            "Persisted {QueryId} ({RowCount} rows) to consolidation DB table {QueryTableName}.",
            result.QueryId,
            result.RowCount,
            queryTableName);
    }

    /// <summary>
    /// Pre-creates collector_run and one query_result_* table per query id so
    /// event-driven queries have tables even before their first trigger.
    /// </summary>
    public async Task PrecreateQueryTablesAsync(IEnumerable<string> queryIds, CancellationToken ct)
    {
        var ids = queryIds
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (ids.Length == 0)
            return;

        await using var conn = new NpgsqlConnection(_options.ConnectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);
        await EnsureSchemaAsync(conn, ct).ConfigureAwait(false);

        foreach (var id in ids)
        {
            var queryTableName = BuildQueryTableName(id);
            await EnsureQueryTableAsync(conn, queryTableName, Array.Empty<string>(), ct)
                .ConfigureAwait(false);
        }

        _log.LogInformation(
            "Schema warmup created/verified {Count} query_result tables.",
            ids.Length);
    }

    private async Task EnsureSchemaAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        if (_schemaReady)
            return;

        await _schemaGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_schemaReady)
                return;

            const string ddl = """
                CREATE TABLE IF NOT EXISTS collector_run (
                    run_id UUID PRIMARY KEY,
                    server_id TEXT NOT NULL,
                    query_id TEXT NOT NULL,
                    scheduled_at TIMESTAMPTZ NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL,
                    completed_at TIMESTAMPTZ NOT NULL,
                    status TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    duration_ms BIGINT NOT NULL,
                    error TEXT NULL,
                    source_collected_at TIMESTAMPTZ NOT NULL,
                    collector_received_at TIMESTAMPTZ NOT NULL,
                    note TEXT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );

                CREATE INDEX IF NOT EXISTS ix_collector_run_query_time
                    ON collector_run (server_id, query_id, source_collected_at DESC);
                """;

            await using var cmd = new NpgsqlCommand(ddl, conn);
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            _schemaReady = true;
        }
        finally
        {
            _schemaGate.Release();
        }
    }

    private async Task EnsureQueryTableAsync(
        NpgsqlConnection conn,
        string queryTableName,
        IReadOnlyCollection<string> dynamicColumns,
        CancellationToken ct)
    {
        await _queryTableGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var quotedTableName = QuoteIdentifier(queryTableName);
            var indexName = $"ix_{queryTableName}_time";
            var quotedIndexName = QuoteIdentifier(indexName);
            if (!_readyQueryTables.Contains(queryTableName))
            {
                var ddl = $"""
                    CREATE TABLE IF NOT EXISTS {quotedTableName} (
                        run_id UUID NOT NULL REFERENCES collector_run(run_id) ON DELETE CASCADE,
                        row_ordinal INTEGER NOT NULL,
                        server_id TEXT NOT NULL,
                        source_collected_at TIMESTAMPTZ NOT NULL,
                        collector_received_at TIMESTAMPTZ NOT NULL,
                        PRIMARY KEY (run_id, row_ordinal)
                    );

                    CREATE INDEX IF NOT EXISTS {quotedIndexName}
                        ON {quotedTableName} (source_collected_at DESC, collector_received_at DESC);
                    """;

                await using var cmd = new NpgsqlCommand(ddl, conn);
                await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                _readyQueryTables.Add(queryTableName);
            }

            if (dynamicColumns.Count > 0)
            {
                var addCols = string.Join(",\n", dynamicColumns.Select(c =>
                    $"ADD COLUMN IF NOT EXISTS {QuoteIdentifier(c)} TEXT NULL"));
                var alterSql = $"""
                    ALTER TABLE {quotedTableName}
                    {addCols};
                    """;
                await using var alterCmd = new NpgsqlCommand(alterSql, conn);
                await alterCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
        }
        finally
        {
            _queryTableGate.Release();
        }
    }

    private static Dictionary<string, string> BuildColumnMap(IReadOnlyList<IReadOnlyDictionary<string, object?>> rows)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        var used = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "run_id",
            "row_ordinal",
            "server_id",
            "source_collected_at",
            "collector_received_at"
        };

        foreach (var row in rows)
        {
            foreach (var key in row.Keys)
            {
                if (map.ContainsKey(key))
                    continue;

                var candidate = BuildSafeColumnName(key);
                if (candidate.Length == 0)
                    candidate = "f_unknown";

                if (char.IsDigit(candidate[0]))
                    candidate = $"f_{candidate}";

                var unique = candidate;
                var suffix = 2;
                while (used.Contains(unique))
                    unique = $"{candidate}_{suffix++}";

                used.Add(unique);
                map[key] = unique;
            }
        }

        return map;
    }

    private static string BuildSafeColumnName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return "f_unknown";

        Span<char> chars = stackalloc char[name.Length];
        var len = 0;
        foreach (var ch in name)
        {
            if (char.IsLetterOrDigit(ch) || ch == '_')
                chars[len++] = char.ToLowerInvariant(ch);
            else
                chars[len++] = '_';
        }

        return new string(chars.Slice(0, len)).Trim('_');
    }

    private static string? ToFlatString(object? value)
    {
        if (value is null)
            return null;

        if (value is string s)
            return s;

        if (value is DateTimeOffset dto)
            return dto.ToString("O", CultureInfo.InvariantCulture);

        if (value is DateTime dt)
            return dt.ToString("O", CultureInfo.InvariantCulture);

        if (value is IFormattable formattable)
            return formattable.ToString(null, CultureInfo.InvariantCulture);

        return JsonSerializer.Serialize(value);
    }

    private static string BuildQueryTableName(string queryId)
    {
        if (string.IsNullOrWhiteSpace(queryId))
            return "query_result_unknown";

        Span<char> tmp = stackalloc char[queryId.Length];
        var len = 0;
        foreach (var ch in queryId)
        {
            if (char.IsLetterOrDigit(ch) || ch == '_')
                tmp[len++] = char.ToLowerInvariant(ch);
            else
                tmp[len++] = '_';
        }

        var safeId = new string(tmp.Slice(0, len)).Trim('_');
        if (safeId.Length == 0)
            safeId = "unknown";

        return $"query_result_{safeId}";
    }

    private static string QuoteIdentifier(string identifier)
        => $"\"{identifier.Replace("\"", "\"\"")}\"";
}

/// <summary>Appends results as JSON lines to disk, partitioned by query id and day.</summary>
public sealed class LocalBufferSink : IMetricSink
{
    private readonly string _root;

    public LocalBufferSink(IOptions<CollectorOptions> options)
    {
        _root = options.Value.Consolidation.LocalBufferPath;
        Directory.CreateDirectory(_root);
    }

    public async Task WriteAsync(CollectionResult result, CancellationToken ct)
    {
        var file = Path.Combine(
            _root, $"{result.QueryId}_{DateTimeOffset.UtcNow:yyyyMMdd}.jsonl");
        var line = JsonSerializer.Serialize(result) + Environment.NewLine;
        await File.AppendAllTextAsync(file, line, ct).ConfigureAwait(false);
    }
}
