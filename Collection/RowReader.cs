using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection;

internal static class RowReader
{
    public static async Task<List<IReadOnlyDictionary<string, object?>>> ReadAllAsync(
        NpgsqlCommand cmd, CancellationToken ct)
    {
        var rows = new List<IReadOnlyDictionary<string, object?>>();
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var row = new Dictionary<string, object?>(reader.FieldCount);
            for (var i = 0; i < reader.FieldCount; i++)
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            rows.Add(row);
        }
        return rows;
    }
}
