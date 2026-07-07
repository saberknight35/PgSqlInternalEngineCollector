using PgSqlInternalEngineCollector.Service.Configuration;

namespace PgSqlInternalEngineCollector.Service.Scheduling;

/// <summary>
/// Cadence classes from the v3 scheduler table (§3.1). Conditional and
/// PhaseBoundary are event-driven, not periodic — they are not on a timer.
/// </summary>
public enum CadenceTier
{
    Fast,           // 15 s  — Q02, PB02
    Counter30,      // 30 s  — Q04, Q07
    Counter60,      // 60 s  — Q05, Q08, W04, PB03
    Health5m,       // 5 m   — Q06, Q09, Q13(PG18), PB06
    Object15m,      // 15 m  — Q10, Q11, Q14
    Config6h,       // 6 h   — Q01, W05, PB01
    PhaseBoundary,  // event — Q05 full, Q09, Q10, Q13, Q14, Q19
    Conditional     // event — Q03, Q12, PB04, PB05
}

public enum QuerySource
{
    Postgres,
    PgBouncerAdmin
}

public static class CadenceTierExtensions
{
    /// <summary>Maps a periodic tier to its configured interval. Throws for event tiers.</summary>
    public static TimeSpan ToInterval(this CadenceTier tier, IntervalOptions intervals) => tier switch
    {
        CadenceTier.Fast => intervals.Fast,
        CadenceTier.Counter30 => intervals.Counter30,
        CadenceTier.Counter60 => intervals.Counter60,
        CadenceTier.Health5m => intervals.Health5m,
        CadenceTier.Object15m => intervals.Object15m,
        CadenceTier.Config6h => intervals.Config6h,
        _ => throw new InvalidOperationException(
            $"Tier {tier} is event-driven and has no fixed interval.")
    };

    public static bool IsPeriodic(this CadenceTier tier) =>
        tier is not (CadenceTier.PhaseBoundary or CadenceTier.Conditional);
}
