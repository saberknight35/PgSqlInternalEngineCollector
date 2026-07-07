using PgSqlInternalEngineCollector.Service.Configuration;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection;

/// <summary>
/// Opens a connection to the PgBouncer admin console (db "pgbouncer", port 6432).
///
/// IMPORTANT (spec §3.3 #2): the admin console only understands the *simple* query
/// protocol and does not support the type-loading / extended-protocol round-trips
/// Npgsql normally performs. The connection string therefore sets:
///   - Server Compatibility Mode=NoTypeLoading  (skip catalog type queries)
///   - No Reset On Close=true                   (PgBouncer admin has no DISCARD ALL)
/// and admin commands are issued without parameters so Npgsql uses simple query.
/// This MUST be confirmed in smoke test: if the driver still uses extended protocol
/// the admin connection can fail even when normal DB connections succeed.
/// </summary>
public sealed class PgBouncerConnectionFactory
{
    private readonly PgBouncerOptions _options;

    public PgBouncerConnectionFactory(IOptions<CollectorOptions> options)
        => _options = options.Value.PgBouncer;

    public bool Enabled => _options.Enabled
        && !string.IsNullOrWhiteSpace(_options.ConnectionString);

    public async Task<NpgsqlConnection> OpenAsync(CancellationToken ct)
    {
        if (!Enabled)
            throw new InvalidOperationException("PgBouncer collection is disabled.");

        var conn = new NpgsqlConnection(_options.ConnectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);
        return conn;
    }

    /// <summary>
    /// Issues a PgBouncer admin SHOW command via the simple query protocol and
    /// returns the rows. SHOW commands take no parameters, which keeps Npgsql on
    /// the simple protocol path the admin console requires.
    /// </summary>
    public static async Task<List<Dictionary<string, object?>>> ShowAsync(
        NpgsqlConnection conn, string showCommand, CancellationToken ct)
    {
        var rows = new List<Dictionary<string, object?>>();
        await using var cmd = new NpgsqlCommand(showCommand, conn);
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
