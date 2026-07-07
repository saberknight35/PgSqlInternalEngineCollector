using PgSqlInternalEngineCollector.Service;
using PgSqlInternalEngineCollector.Service.Collection;
using PgSqlInternalEngineCollector.Service.Collection.Queries;
using PgSqlInternalEngineCollector.Service.Configuration;
using PgSqlInternalEngineCollector.Service.Delta;
using PgSqlInternalEngineCollector.Service.Sink;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

// Bind configuration.
builder.Services.Configure<CollectorOptions>(builder.Configuration.GetSection("Collector"));

// Infrastructure singletons.
builder.Services.AddSingleton<PostgresConnectionFactory>();
builder.Services.AddSingleton<PgBouncerConnectionFactory>();
builder.Services.AddSingleton<DeltaCache>();

// Sink chain: BufferedSink wraps the consolidation writer and the local disk buffer.
builder.Services.AddSingleton<ConsolidationDbSink>();
builder.Services.AddSingleton<LocalBufferSink>();
builder.Services.AddSingleton<IMetricSink, BufferedSink>();

// Query registry.
// --- Config6h: run once at startup then every 6 hours ---
builder.Services.AddSingleton<IMetricQuery, Q01Capability>();
builder.Services.AddSingleton<IMetricQuery, Q01MemoryConfig>();
builder.Services.AddSingleton<IMetricQuery, Q01DbRoleOverrides>();
builder.Services.AddSingleton<IMetricQuery, Q01RoutineLocalSettings>();
builder.Services.AddSingleton<IMetricQuery, Q15SharedMemory>();
builder.Services.AddSingleton<IMetricQuery, Q17LockLoggingConfig>();
builder.Services.AddSingleton<IMetricQuery, Dict01WaitEventDictionary>(); // DICT01, PG17+ only
builder.Services.AddSingleton<IMetricQuery, PB01PoolerConfig>();  // skipped when PgBouncer disabled

// --- Fast (15 s) ---
builder.Services.AddSingleton<IMetricQuery, Q02ActivitySnapshot>();
builder.Services.AddSingleton<IMetricQuery, PB02PoolSnapshot>();  // skipped when PgBouncer disabled

// --- Counter30 (30 s) ---
builder.Services.AddSingleton<IMetricQuery, Q04DatabaseCounters>();
builder.Services.AddSingleton<IMetricQuery, Q07ClusterIo>();      // PG16+ only; skipped gracefully on older versions

// --- Counter60 (60 s) ---
builder.Services.AddSingleton<IMetricQuery, Q05QueryCounters>();
builder.Services.AddSingleton<IMetricQuery, Q08CheckpointWal>();
builder.Services.AddSingleton<IMetricQuery, Q16SlruStats>();
builder.Services.AddSingleton<IMetricQuery, PB03PoolerStats>();   // skipped when PgBouncer disabled

// --- Health5m (5 min) ---
builder.Services.AddSingleton<IMetricQuery, Q06PgssHealth>();
builder.Services.AddSingleton<IMetricQuery, Q09TableStats>();
builder.Services.AddSingleton<IMetricQuery, Q13BufferCacheSummary>(); // PG18 only; skipped gracefully on older versions
builder.Services.AddSingleton<IMetricQuery, PB06PoolerLists>();   // skipped when PgBouncer disabled

// --- Object15m (15 min) ---
builder.Services.AddSingleton<IMetricQuery, Q10IndexStats>();
builder.Services.AddSingleton<IMetricQuery, Q11QueryStoreWaits>(); // requires azure_sys / Query Store
builder.Services.AddSingleton<IMetricQuery, Q14TopRelationsCache>();

// --- Conditional (event-driven, not on a periodic timer) ---
// These are registered so they can be invoked programmatically; the periodic
// timer loop skips them because CadenceTier.Conditional is not IsPeriodic().
builder.Services.AddSingleton<IMetricQuery, Q03BlockingChain>();   // trigger: Q02 active_lock_waiting_sessions > 0
builder.Services.AddSingleton<IMetricQuery, Q12VacuumProgress>();  // trigger: Q02 autovacuum workers active
builder.Services.AddSingleton<IMetricQuery, PB04ClientDiagnostic>(); // trigger: PB02 cl_waiting > 0
builder.Services.AddSingleton<IMetricQuery, PB05ServerDiagnostic>(); // trigger: PB02 sv_idle = 0 with cl_waiting > 0

// --- PhaseBoundary (event-driven) ---
builder.Services.AddSingleton<IMetricQuery, Q19ParallelPlanInventory>(); // Q19 trigger: phase boundary / CPU >= 80%

// The single long-running collector.
builder.Services.AddHostedService<SchemaWarmupHostedService>();
builder.Services.AddHostedService<CollectorWorker>();

// Run as a Windows Service when installed; still runs as console for local debugging.
builder.Services.AddWindowsService(o => o.ServiceName = "PgSqlInternalEngineCollector");

var host = builder.Build();
host.Run();
