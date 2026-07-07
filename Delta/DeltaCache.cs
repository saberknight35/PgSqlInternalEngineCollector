using System.Collections.Concurrent;

namespace PgSqlInternalEngineCollector.Service.Delta;

/// <summary>
/// Holds the previous cumulative value per key so the collector can compute
/// delta and rate between samples (spec §3.1). A reset boundary is flagged when
/// stats_reset changes or the counter goes backwards; across a reset, no delta is
/// produced and the new value becomes the baseline. Raw values are still persisted
/// by the caller — the cache only derives the delta.
///
/// Key convention: include everything that identifies the counter row, e.g.
///   "Q04:datid=16384:xact_commit" or "PB03:db=appdb:total_query_count".
/// </summary>
public sealed class DeltaCache
{
    private readonly ConcurrentDictionary<string, Snapshot> _last = new();

    public DeltaOutcome Compute(string key, long current, string? statsReset, DateTimeOffset now)
    {
        if (_last.TryGetValue(key, out var prev))
        {
            var resetDetected = prev.StatsReset != statsReset || current < prev.Value;
            _last[key] = new Snapshot(current, statsReset, now);

            if (resetDetected)
                return DeltaOutcome.ResetBoundary();

            var elapsedSeconds = (now - prev.At).TotalSeconds;
            var delta = current - prev.Value;
            var rate = elapsedSeconds > 0 ? delta / elapsedSeconds : 0d;
            return DeltaOutcome.WithDelta(delta, elapsedSeconds, rate);
        }

        _last[key] = new Snapshot(current, statsReset, now);
        return DeltaOutcome.FirstSample();
    }

    private readonly record struct Snapshot(long Value, string? StatsReset, DateTimeOffset At);
}

/// <summary>Outcome of a delta computation. HasDelta is false for the first sample and across resets.</summary>
public readonly record struct DeltaOutcome(
    bool HasDelta,
    bool IsResetBoundary,
    long Delta,
    double ElapsedSeconds,
    double RatePerSecond)
{
    public static DeltaOutcome FirstSample() => new(false, false, 0, 0, 0);
    public static DeltaOutcome ResetBoundary() => new(false, true, 0, 0, 0);
    public static DeltaOutcome WithDelta(long delta, double elapsed, double rate) =>
        new(true, false, delta, elapsed, rate);
}
