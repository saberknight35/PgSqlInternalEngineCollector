using PgSqlInternalEngineCollector.Service.Collection;
using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Scheduling;
using PgSqlInternalEngineCollector.Service.Sink;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using System.Collections.Concurrent;
using System.Globalization;

namespace PgSqlInternalEngineCollector.Service;

/// <summary>
/// The single long-running service. Builds one PeriodicTimer loop per periodic
/// cadence tier and dispatches the queries registered to that tier on each tick,
/// each behind its own overlap guard. Event-driven tiers (Conditional,
/// PhaseBoundary) are not started here; wire their triggers from fast-path results.
/// </summary>
public sealed class CollectorWorker : BackgroundService
{
    private readonly IReadOnlyList<IMetricQuery> _queries;
    private readonly PostgresConnectionFactory _pg;
    private readonly PgBouncerConnectionFactory _pgb;
    private readonly DeltaCache _deltas;
    private readonly IMetricSink _sink;
    private readonly IntervalOptions _intervals;
    private readonly ILogger<CollectorWorker> _log;
    private readonly ConcurrentDictionary<string, OverlapGuard> _guards;
    private readonly Dictionary<string, IMetricQuery> _queriesById;
    private readonly ConcurrentDictionary<string, DateTimeOffset> _eventLastTriggered;

    public CollectorWorker(
        IEnumerable<IMetricQuery> queries,
        PostgresConnectionFactory pg,
        PgBouncerConnectionFactory pgb,
        DeltaCache deltas,
        IMetricSink sink,
        IOptions<CollectorOptions> options,
        ILogger<CollectorWorker> log)
    {
        _queries = queries.ToList();
        _pg = pg;
        _pgb = pgb;
        _deltas = deltas;
        _sink = sink;
        _intervals = options.Value.Intervals;
        _log = log;
        _guards = new ConcurrentDictionary<string, OverlapGuard>(StringComparer.OrdinalIgnoreCase);
        _queriesById = _queries
            .GroupBy(q => q.Id, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);
        _eventLastTriggered = new ConcurrentDictionary<string, DateTimeOffset>(StringComparer.OrdinalIgnoreCase);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _log.LogInformation("DMS metrics collector starting with {Count} queries.", _queries.Count);

        // Run startup/config queries once immediately so Q01 capability check happens at boot.
        foreach (var q in _queries.Where(q => q.Tier == CadenceTier.Config6h))
            await RunSafeAsync(q, stoppingToken).ConfigureAwait(false);

        // One loop per periodic tier that actually has queries.
        var loops = _queries
            .Where(q => q.Tier.IsPeriodic())
            .GroupBy(q => q.Tier)
            .Select(g => RunTierLoopAsync(g.Key, g.ToList(), stoppingToken));

        try
        {
            await Task.WhenAll(loops).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            _log.LogInformation("Collector shutdown requested.");
        }
    }

    private async Task RunTierLoopAsync(
        CadenceTier tier, IReadOnlyList<IMetricQuery> queries, CancellationToken ct)
    {
        var interval = tier.ToInterval(_intervals);
        _log.LogInformation("Tier {Tier} every {Interval} → {Queries}",
            tier, interval, string.Join(", ", queries.Select(q => q.Id)));

        using var timer = new PeriodicTimer(interval);
        while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
        {
            // Queries in a tier are independent; run concurrently, each guarded.
            var ticks = queries.Select(q => RunSafeAsync(q, ct));
            await Task.WhenAll(ticks).ConfigureAwait(false);
        }
    }

    private async Task RunSafeAsync(IMetricQuery query, CancellationToken ct)
    {
        var executions = BuildExecutions(query);
        var runs = executions.Select(exec => RunOneExecutionSafeAsync(query, exec, ct));
        await Task.WhenAll(runs).ConfigureAwait(false);
    }

    private async Task RunOneExecutionSafeAsync(
        IMetricQuery query,
        QueryExecution exec,
        CancellationToken ct)
    {
        if (query.Source == QuerySource.PgBouncerAdmin && !_pgb.Enabled)
            return;

        var guard = _guards.GetOrAdd(exec.GuardKey, _ => new OverlapGuard());
        var ran = await guard.TryRunAsync(async () =>
        {
            try
            {
                NpgsqlConnection conn = await OpenConnectionForExecutionAsync(query, exec, ct)
                    .ConfigureAwait(false);

                await using (conn)
                {
                    var result = await query.ExecuteAsync(conn, _deltas, ct).ConfigureAwait(false);
                    await _sink.WriteAsync(result, ct).ConfigureAwait(false);
                    await TriggerEventQueriesAsync(query, result, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { /* shutting down */ }
            catch (Exception ex)
            {
                // A collector failure must never affect application traffic; log and move on.
                _log.LogError(ex, "Query {QueryId} failed for target {TargetDb}.", query.Id, exec.TargetDatabase ?? "<none>");
            }
        }).ConfigureAwait(false);

        if (!ran)
            _log.LogWarning(
                "Query {QueryId} skipped for target {TargetDb} — previous run still in progress (overlap guard).",
                query.Id,
                exec.TargetDatabase ?? "<none>");
    }

    private async Task<NpgsqlConnection> OpenConnectionForExecutionAsync(
        IMetricQuery query,
        QueryExecution exec,
        CancellationToken ct)
    {
        if (query.Source == QuerySource.PgBouncerAdmin)
            return await _pgb.OpenAsync(ct).ConfigureAwait(false);

        return query.ExecutionScope switch
        {
            QueryExecutionScope.ServerWide => await _pg.OpenServerWideAsync(ct).ConfigureAwait(false),
            QueryExecutionScope.AzureSys => await _pg.OpenAzureSysAsync(ct).ConfigureAwait(false),
            QueryExecutionScope.PerApplicationDatabase when !string.IsNullOrWhiteSpace(exec.TargetDatabase)
                => await _pg.OpenForDatabaseAsync(exec.TargetDatabase, ct).ConfigureAwait(false),
            _ => throw new InvalidOperationException(
                $"Unsupported execution mapping for query {query.Id}. Scope={query.ExecutionScope}")
        };
    }

    private IEnumerable<QueryExecution> BuildExecutions(IMetricQuery query)
    {
        if (query.Source == QuerySource.PgBouncerAdmin)
            return new[] { new QueryExecution(query.Id, null) };

        return query.ExecutionScope switch
        {
            QueryExecutionScope.ServerWide => new[]
            {
                new QueryExecution(query.Id, _pg.ServerWideDatabase)
            },
            QueryExecutionScope.AzureSys => new[]
            {
                new QueryExecution(query.Id, _pg.AzureSysDatabase)
            },
            QueryExecutionScope.PerApplicationDatabase => _pg.ApplicationDatabases
                .Select(db => new QueryExecution($"{query.Id}:{db}", db))
                .ToArray(),
            _ => throw new InvalidOperationException(
                $"Unsupported execution scope for query {query.Id}: {query.ExecutionScope}")
        };
    }

    private sealed record QueryExecution(string GuardKey, string? TargetDatabase);

    private async Task TriggerEventQueriesAsync(
        IMetricQuery sourceQuery,
        CollectionResult sourceResult,
        CancellationToken ct)
    {
        if (sourceResult.Rows.Count == 0)
            return;

        if (sourceQuery.Id.Equals("Q02", StringComparison.OrdinalIgnoreCase))
        {
            var row = sourceResult.Rows[0];

            var lockWaiting = GetLong(row, "active_lock_waiting_sessions")
                              ?? GetLong(row, "waiting_backends")
                              ?? 0;

            if (lockWaiting > 0)
                await TriggerByIdAsync("Q03", TimeSpan.FromSeconds(15), "Q02 lock wait detected", ct)
                    .ConfigureAwait(false);

            var autovacuumWorkers = GetLong(row, "autovacuum_workers") ?? 0;
            if (autovacuumWorkers > 0)
                await TriggerByIdAsync("Q12", TimeSpan.FromSeconds(30), "Q02 autovacuum worker detected", ct)
                    .ConfigureAwait(false);

            var parallelWorkers = GetLong(row, "parallel_workers") ?? 0;
            if (parallelWorkers > 0)
                await TriggerByIdAsync("Q19", TimeSpan.FromMinutes(15), "Q02 parallel workers active", ct)
                    .ConfigureAwait(false);
        }

        if (sourceQuery.Id.Equals("PB02", StringComparison.OrdinalIgnoreCase))
        {
            var anyClientWaiting = sourceResult.Rows.Any(r => (GetLong(r, "cl_waiting") ?? 0) > 0);
            var anyMaxWait = sourceResult.Rows.Any(r => (GetDouble(r, "maxwait") ?? 0d) > 0d);
            var anyServerIdleZeroWithWaiting = sourceResult.Rows.Any(r =>
                (GetLong(r, "sv_idle") ?? 0) == 0 && (GetLong(r, "cl_waiting") ?? 0) > 0);

            if (anyClientWaiting || anyMaxWait)
                await TriggerByIdAsync("PB04", TimeSpan.FromSeconds(15), "PB02 client queue/maxwait detected", ct)
                    .ConfigureAwait(false);

            if (anyServerIdleZeroWithWaiting)
                await TriggerByIdAsync("PB05", TimeSpan.FromSeconds(15), "PB02 pool saturation detected", ct)
                    .ConfigureAwait(false);

            // Pool churn: connections being health-checked (sv_tested) or being established
            // (sv_login) indicate PgBouncer is actively cycling its server-side pool.
            var anyServerChurn = sourceResult.Rows.Any(r =>
                (GetLong(r, "sv_tested") ?? 0) > 0 || (GetLong(r, "sv_login") ?? 0) > 0);

            if (anyServerChurn)
                await TriggerByIdAsync("PB06", TimeSpan.FromMinutes(5), "PB02 pool churn detected (sv_tested/sv_login)", ct)
                    .ConfigureAwait(false);
        }

        if (sourceQuery.Id.Equals("Q08", StringComparison.OrdinalIgnoreCase))
        {
            // Phase boundary: any timed/requested checkpoint in the current 60-second
            // interval triggers boundary snapshots without waiting for periodic ticks.
            var checkpointerRow = sourceResult.Rows.FirstOrDefault(r =>
                "pg_stat_checkpointer".Equals(r.TryGetValue("source_view", out var sv) ? sv?.ToString() : null,
                    StringComparison.OrdinalIgnoreCase));

            if (checkpointerRow is not null)
            {
                var timedDelta = GetDouble(checkpointerRow, "num_timed_delta") ?? 0d;
                var requestedDelta = GetDouble(checkpointerRow, "num_requested_delta") ?? 0d;
                if (timedDelta > 0 || requestedDelta > 0)
                {
                    await TriggerByIdAsync("Q05", TimeSpan.FromMinutes(1),  "Q08 checkpoint phase boundary", ct).ConfigureAwait(false);
                    await TriggerByIdAsync("Q09", TimeSpan.FromMinutes(5),  "Q08 checkpoint phase boundary", ct).ConfigureAwait(false);
                    await TriggerByIdAsync("Q10", TimeSpan.FromMinutes(15), "Q08 checkpoint phase boundary", ct).ConfigureAwait(false);
                    await TriggerByIdAsync("Q13", TimeSpan.FromMinutes(5),  "Q08 checkpoint phase boundary", ct).ConfigureAwait(false);
                    await TriggerByIdAsync("Q14", TimeSpan.FromMinutes(15), "Q08 checkpoint phase boundary", ct).ConfigureAwait(false);
                }
            }
        }
    }

    private async Task TriggerByIdAsync(string queryId, TimeSpan cooldown, string reason, CancellationToken ct)
    {
        if (!_queriesById.TryGetValue(queryId, out var query))
            return;

        var now = DateTimeOffset.UtcNow;
        if (_eventLastTriggered.TryGetValue(queryId, out var last) && now - last < cooldown)
            return;

        _eventLastTriggered[queryId] = now;
        _log.LogInformation("Event trigger: running {QueryId} ({Reason}).", queryId, reason);
        await RunSafeAsync(query, ct).ConfigureAwait(false);
    }

    private static long? GetLong(IReadOnlyDictionary<string, object?> row, string key)
    {
        if (!row.TryGetValue(key, out var value) || value is null)
            return null;

        return value switch
        {
            byte v => v,
            short v => v,
            int v => v,
            long v => v,
            float v => (long)v,
            double v => (long)v,
            decimal v => (long)v,
            string s when long.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed)
                => parsed,
            _ => null
        };
    }

    private static double? GetDouble(IReadOnlyDictionary<string, object?> row, string key)
    {
        if (!row.TryGetValue(key, out var value) || value is null)
            return null;

        return value switch
        {
            byte v => v,
            short v => v,
            int v => v,
            long v => v,
            float v => v,
            double v => v,
            decimal v => (double)v,
            string s when double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed)
                => parsed,
            _ => null
        };
    }
}
