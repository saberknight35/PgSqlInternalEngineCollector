# PgSql Internal Engine Collector

A long-running Windows Service implementing the
`astra_dms_postgresql_metrics_query_spec_v3_12.md` collector, deployed on a
**Windows VM** instead of an Azure Function. The query specification document still
names Azure Function as the primary consumer; this code runs the same query
catalogue from a VM.

See `Docs/component-architecture.md` for the component diagram and rationale.

## Why this shape

- **One long-running Windows Service** runs the whole live-SQL path (15 s through
  15 min, plus conditional and config tiers) using an internal `PeriodicTimer` per
  cadence. It does **not** use Windows Task Scheduler for these, because Task
  Scheduler's repetition floor is 1 minute (Microsoft schema: min `PT1M`), which
  the 15 s and 30 s tiers fall below — and because a fresh process per tick would
  pay connection cold-start every 15 s and make overlap more likely.
- **One scheduled task** runs the 2-hour Storage Account ingestion. That cadence is
  above the 1-minute floor and is a separate failure domain, so Task Scheduler is
  appropriate there.

## Layout

```
PgSqlInternalEngineCollector.sln
PgSqlInternalEngineCollector.csproj
Program.cs                           host + DI + query registration
appsettings.json                     connection strings + per-tier intervals
Configuration/CollectorOptions.cs
Scheduling/                          tiers, overlap guard, CollectorWorker
Collection/                          connection factories + IMetricQuery + queries
Delta/DeltaCache.cs                  previous-value cache + reset detection
Sink/MetricSink.cs                   consolidation write + local disk buffer
Docs/component-architecture.md
scripts/install-service.ps1
```

## Build and run locally

```powershell
dotnet build
dotnet run
```

It runs as a console app when launched directly (handy for debugging) and as a
Windows Service when installed. Fill in `appsettings.json` first.

## Install on the VM

```powershell
cd scripts
./install-service.ps1   # publish + create service + restart-on-failure recovery
```

## Adding a query

1. Add a class implementing `IMetricQuery` under `Collection/Queries/`, declaring
  its `Id`, `Tier`, and `Source`. Paste the full SQL from the v3.12 spec (the three
  bundled queries are trimmed examples).
2. Register it in `Program.cs`: `AddSingleton<IMetricQuery, YourQuery>();`
3. For cumulative-counter queries, route values through `DeltaCache` (see
   `Q04DatabaseCounters`).

## What is stubbed and must be wired before a real run

- `ConsolidationDbSink.WriteAsync` — currently logs only; implement the real INSERTs
  against your consolidation schema (spec §12 minimum keys).
- PgBouncer admin connection — confirm in smoke test that `SHOW` actually returns
  rows through the configured Npgsql mode (spec §3.3 #2).
- The full query set — only Q02, Q04, PB02 are included as patterns.
- Conditional triggering (Q03 on blocking, PB04/PB05 on pool wait) — add a trigger
  from fast-path results into the conditional queries.
- Multi-database support — `CollectorOptions.PostgresOptions.ApplicationDatabases`
  configures per-database queries (Q09, Q10, Q14, Q01D) that fan out once per entry.
  Server-wide queries (Q02, Q04, Q05, Q07, Q08, Q12, etc.) run once per tick.
  Use `OverlapGuard` keys scoped per database (e.g. `Q09:appdb1`). See spec
  §3.4 for the full scope table and implementation rules.
- VM time sync (w32time/NTP) so `collector_received_at` aligns with k6 timestamps.

## Known single point of failure

One VM, one service collects everything for the 8-hour run. Service recovery
(restart on failure) plus local disk buffering on consolidation-DB outage are the
mitigations. Acceptable for a one-off test; document the risk.
