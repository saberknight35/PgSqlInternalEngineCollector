using PgSqlInternalEngineCollector.Service.Collection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace PgSqlInternalEngineCollector.Service.Sink;

/// <summary>
/// Ensures consolidation tables exist for all registered query ids at startup,
/// including event-driven queries that may not run immediately.
/// </summary>
public sealed class SchemaWarmupHostedService : IHostedService
{
    private readonly ConsolidationDbSink _sink;
    private readonly IEnumerable<IMetricQuery> _queries;
    private readonly ILogger<SchemaWarmupHostedService> _log;

    public SchemaWarmupHostedService(
        ConsolidationDbSink sink,
        IEnumerable<IMetricQuery> queries,
        ILogger<SchemaWarmupHostedService> log)
    {
        _sink = sink;
        _queries = queries;
        _log = log;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var ids = _queries.Select(q => q.Id);

        try
        {
            await _sink.PrecreateQueryTablesAsync(ids, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Startup should continue even if consolidation DB is temporarily unavailable.
            _log.LogWarning(ex, "Schema warmup failed; query tables will be created lazily on first write.");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
