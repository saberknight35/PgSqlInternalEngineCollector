namespace PgSqlInternalEngineCollector.Service.Scheduling;

/// <summary>
/// Per-query concurrency guard. If a tick fires while the previous run of the
/// same query is still in flight, the new run is skipped (not queued). This is
/// the implementation of the spec acceptance item "does not overlap executions".
/// One instance per query id.
/// </summary>
public sealed class OverlapGuard
{
    private readonly SemaphoreSlim _gate = new(1, 1);

    /// <summary>
    /// Runs <paramref name="action"/> if no run is in progress.
    /// Returns false (and does nothing) if a run is already active.
    /// </summary>
    public async Task<bool> TryRunAsync(Func<Task> action)
    {
        // WaitAsync(0): acquire only if immediately available; never block/queue.
        if (!await _gate.WaitAsync(0).ConfigureAwait(false))
            return false;

        try
        {
            await action().ConfigureAwait(false);
            return true;
        }
        finally
        {
            _gate.Release();
        }
    }
}
