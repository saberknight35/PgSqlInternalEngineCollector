namespace PgSqlInternalEngineCollector.Service.Configuration;

/// <summary>
/// Strongly typed view of the "Collector" section of appsettings.json.
/// Bound in Program.cs via Configure&lt;CollectorOptions&gt;.
/// </summary>
public sealed class CollectorOptions
{
    public string ServerId { get; set; } = "unknown-server";
    public PostgresOptions Postgres { get; set; } = new();
    public PgBouncerOptions PgBouncer { get; set; } = new();
    public ConsolidationOptions Consolidation { get; set; } = new();
    public IntervalOptions Intervals { get; set; } = new();
}

public sealed class PostgresOptions
{
    public string ConnectionString { get; set; } = "";
    public List<string> ApplicationDatabases { get; set; } = new();
    public string AzureSysDatabase { get; set; } = "azure_sys";
    public int StatementTimeoutSeconds { get; set; } = 10;
    public int LockTimeoutSeconds { get; set; } = 1;
    public int IdleInTransactionTimeoutSeconds { get; set; } = 15;
}

public sealed class PgBouncerOptions
{
    public bool Enabled { get; set; }
    public string ConnectionString { get; set; } = "";
}

public sealed class ConsolidationOptions
{
    public string ConnectionString { get; set; } = "";
    public string LocalBufferPath { get; set; } = "buffer";
}

/// <summary>
/// One interval per cadence tier. Sub-minute values are honoured because the
/// service drives them with PeriodicTimer, not Windows Task Scheduler (whose
/// repetition floor is 1 minute).
/// </summary>
public sealed class IntervalOptions
{
    public TimeSpan Fast { get; set; } = TimeSpan.FromSeconds(15);
    public TimeSpan Counter30 { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan Counter60 { get; set; } = TimeSpan.FromSeconds(60);
    public TimeSpan Health5m { get; set; } = TimeSpan.FromMinutes(5);
    public TimeSpan Object15m { get; set; } = TimeSpan.FromMinutes(15);
    public TimeSpan Config6h { get; set; } = TimeSpan.FromHours(6);
}
