using PgSqlInternalEngineCollector.Service.Configuration;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PgSqlInternalEngineCollector.Service.Collection;

/// <summary>
/// Opens pooled Npgsql connections to the application database as the monitoring
/// role, and applies the spec's recommended session settings (§3.2): read-only,
/// short statement/lock/idle timeouts, and the collector application_name.
/// Npgsql pooling is keyed by the connection string, so a small Maximum Pool Size
/// keeps the collector from inflating the connection-pressure metric it measures.
/// </summary>
public sealed class PostgresConnectionFactory
{
    private readonly PostgresOptions _options;
    private readonly IReadOnlyList<string> _applicationDatabases;
    private readonly string _serverWideDatabase;

    public PostgresConnectionFactory(IOptions<CollectorOptions> options)
    {
        _options = options.Value.Postgres;
        _applicationDatabases = ResolveApplicationDatabases(_options);
        _serverWideDatabase = _applicationDatabases[0];
    }

    public IReadOnlyList<string> ApplicationDatabases => _applicationDatabases;
    public string AzureSysDatabase => _options.AzureSysDatabase;
    public string ServerWideDatabase => _serverWideDatabase;

    public async Task<NpgsqlConnection> OpenAsync(CancellationToken ct)
        => await OpenServerWideAsync(ct).ConfigureAwait(false);

    public async Task<NpgsqlConnection> OpenServerWideAsync(CancellationToken ct)
        => await OpenForDatabaseAsync(_serverWideDatabase, ct).ConfigureAwait(false);

    public async Task<NpgsqlConnection> OpenAzureSysAsync(CancellationToken ct)
        => await OpenForDatabaseAsync(_options.AzureSysDatabase, ct).ConfigureAwait(false);

    public async Task<NpgsqlConnection> OpenForDatabaseAsync(string databaseName, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
            throw new InvalidOperationException("Database name must be provided.");

        var csb = new NpgsqlConnectionStringBuilder(_options.ConnectionString)
        {
            Database = databaseName
        };

        var conn = new NpgsqlConnection(csb.ConnectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);

        // Applied per session. application_name is also in the connection string so
        // the collector is identifiable in pg_stat_activity (Q02). Collector self-noise
        // in Q05 is handled downstream — do not add an in-query role exclusion (spec §Q05).
        var setup =
            "SET application_name = 'dms_metrics_collector'; " +
            "SET default_transaction_read_only = on; " +
            $"SET statement_timeout = '{_options.StatementTimeoutSeconds}s'; " +
            $"SET lock_timeout = '{_options.LockTimeoutSeconds}s'; " +
            $"SET idle_in_transaction_session_timeout = '{_options.IdleInTransactionTimeoutSeconds}s';";

        await using var cmd = new NpgsqlCommand(setup, conn);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

        return conn;
    }

    private static IReadOnlyList<string> ResolveApplicationDatabases(PostgresOptions options)
    {
        var configured = options.ApplicationDatabases
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (configured.Count > 0)
            return configured;

        var fromConnectionString = new NpgsqlConnectionStringBuilder(options.ConnectionString).Database;
        if (!string.IsNullOrWhiteSpace(fromConnectionString))
            return new List<string> { fromConnectionString };

        throw new InvalidOperationException(
            "Collector.Postgres.ApplicationDatabases must contain at least one database " +
            "or ConnectionString must include Database=<name>.");
    }
}
