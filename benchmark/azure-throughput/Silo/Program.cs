// Azure throughput benchmark harness - single-silo lattice host.
//
// Listens on a TCP port for newline-delimited JSON `VehicleTelemetryEvent` records, batches
// them, and writes each batch into a single lattice tree backed by the Azure Table WAL
// storage provider (managed identity to the configured storage account).
//
// Reports "Entries written per second" to stdout once per second so the ACI log is the
// canonical result surface.
//
// Environment variables:
//   BENCH_STORAGE_URI       https://{account}.table.core.windows.net  (required for managed identity)
//   BENCH_STORAGE_CONN      connection string fallback (optional, overrides BENCH_STORAGE_URI when set)
//   BENCH_WAL_TABLE         WAL table name (default OrleansLatticeWal)
//   BENCH_TREE_ID           lattice tree id. Default is "azure-throughput-{utc-yyyyMMdd-HHmmss}"
//                           which rotates per silo restart - the Azure Tables WAL keeps
//                           every run's rows under their own partition-key namespace
//                           (`_m_|{treeId}|{shardIndex}`), so a previous run's offsets
//                           never bias the next run's WalShardGrain activation cost.
//                           Set explicitly to pin a stable id (e.g. for cross-run replay
//                           testing) - that re-uses the existing rows.
//   BENCH_TCP_PORT          TCP listen port (default 7000)
//   BENCH_BATCH_SIZE        SetManyAsync batch size (default 4096 - sized so the 64-way
//                           shard/leaf/WAL fan-out still leaves ~64 entries per WAL
//                           partition transaction, giving phase-2 coalescing real
//                           work to do)
//   BENCH_FLUSH_MS          max flush latency in ms (default 50)
//   BENCH_FLUSH_CONCURRENCY max in-flight SetManyAsync calls (default 8 - matches the
//                           WalPartitions/WalMaxPendingBatches window so the pipelined
//                           phase-2 path always has a batch N+1 in flight to overlap
//                           with batch N's phase 2. Drop to 1 for diagnostic A/B runs
//                           that isolate leaf-mailbox queueing from per-leaf-turn
//                           Azure Tables RTT cost.)
//   BENCH_WAL_PARTITIONS    WAL partitions per tree (default 8 - matches flush concurrency so
//                           parallel SetManyAsync flushes fan out across distinct WAL grains
//                           and therefore distinct Azure Tables manifest partitions)
//   BENCH_WAL_MAX_PENDING_BATCHES
//                           Per-WalShardGrain pipeline depth (default 8). Library default is
//                           1 (single in-flight append per partition) for wire-compat; raising
//                           it lets each partition's flushes overlap against Azure Tables
//                           rather than strictly serialising.
//   BENCH_SHARD_COUNT       Override the tree's physical shard count at startup via
//                           ILattice.ReshardAsync. 0 = keep the library default (64).
//                           Notes: (a) ReshardAsync is grow-only against a populated
//                           tree (target must be > current shard count); (b) against a
//                           freshly-registered/empty tree any target works via the
//                           empty-tree fast-path and returns synchronously; (c) the
//                           harness polls IsReshardCompleteAsync before opening the TCP
//                           listener so writes never race a still-running migration.
//   BENCH_PIPELINE_PHASE2   Set to 0 to disable AzureTableWalStorageOptions.
//                           PipelinePhaseTwoCommits, which overlaps phase 2 of batch N
//                           with phase 0+1 of batch N+1 on the same shard. Halves the
//                           steady-state request-path latency under WalMaxPendingBatches=1
//                           and lets the PhaseTwoWorker's coalescing window actually
//                           collapse multiple commits into one Azure Tables transaction.
//                           Default 1 (on) for the bench; the library default remains
//                           off to preserve the existing wire-compat shape where every
//                           AppendBatchAsync awaits its own phase 2.
//   BENCH_WAL_PHASE2_COALESCING_WINDOW_MS
//                           AzureTableWalStorageOptions.PhaseTwoCoalescingWindow in ms
//                           (default 0 - drain-on-first-signal, matches the library
//                           default). Set to a small positive value (e.g. 5-10 ms,
//                           below the observed phase-2 commit duration p50) to let the
//                           per-shard PhaseTwoWorker wait briefly after the first arrival
//                           so additional commits coalesce into the same Azure Tables
//                           transaction. Probe lever for U9c.
//   BENCH_REPORT_SEC        stdout report interval in seconds (default 1)
//   BENCH_PHASEA_REPORT_SEC stdout cadence for the Phase A diagnostic
//                           reporter (default 10). Set to 0 to disable
//                           the reporter entirely. Emits one
//                           [phaseA] line per (instrument, tree, shard,
//                           phase, status) tuple per cadence tick,
//                           carrying p50/p90/p99/count/min/max over the
//                           preceding window. The ladder script
//                           (40-ladder.ps1) scrapes these lines to
//                           attribute caller-visible append latency to
//                           grain-side queueing vs storage-provider
//                           commit time.
//   BENCH_TOTAL_DURATION_SEC
//                           Server-side watchdog. After this many seconds the silo
//                           triggers a graceful host shutdown so the ACI container
//                           group transitions to Terminated even if the local deploy
//                           shell that orchestrated the run has died. 0 disables the
//                           watchdog. Default 600 (10 minutes) - well above the
//                           harness's nominal 120s run so a normal client-driven stop
//                           still wins the race, while a runaway run cannot burn paid
//                           Azure compute indefinitely.
//   BENCH_RESPONSE_TIMEOUT_SEC
//                           Orleans Silo+Client ResponseTimeout in seconds (default 30,
//                           matches the Orleans default). U9p step 8c-b-i probe lever:
//                           lifts the caller-side timeout on ILattice.SetManyAsync so a
//                           slow worst-partition flush no longer triggers Orleans's
//                           TimeoutException and the producer's reconnect/retransmit
//                           storm. Disambiguates whether the post-timeout retry storm
//                           is itself a throughput multiplier on top of provider-tail
//                           latency. Applied to both SiloMessagingOptions and
//                           ClientMessagingOptions so in-silo TcpIngestService callers
//                           see the same lift.
//   BENCH_LEAF_STORAGE_KIND IGrainStorage implementation used for the lattice
//                           leaf/internal/atomic grain checkpoints. Allowed values:
//                             "azure" (default) - production-shape Azure Table grain
//                                                 storage (Microsoft.Orleans.Persistence
//                                                 .AzureStorage). Reuses BENCH_STORAGE_URI
//                                                 / BENCH_STORAGE_CONN; writes to the
//                                                 table named by BENCH_LEAF_STORAGE_TABLE
//                                                 (default "OrleansLatticeGrainState").
//                                                 This is what a real production host
//                                                 would wire; the benchmark uses it as
//                                                 the baseline so durable-storage cost
//                                                 stays on the critical path.
//                             "memory"          - Orleans.Persistence.Memory. Kept as a
//                                                 diagnostic-only lever; ships with
//                                                 NumStorageGrains=10 by default, which
//                                                 became the chokepoint in step 8c-c-i
//                                                 (2074 "Unable to create local
//                                                 activation" rejections). Useful for
//                                                 isolating in-process latency from
//                                                 durable-IO latency in a controlled A/B.
//                             "null"            - benchmark-only NullGrainStorage that
//                                                 no-ops every WriteStateAsync /
//                                                 ReadStateAsync. Diagnostic lever from
//                                                 step 8c-c-ii Run B; removes persistence
//                                                 entirely so the WAL's true ceiling
//                                                 becomes visible. NOT production-shape.
//   BENCH_LEAF_STORAGE_TABLE Azure Table name for the leaf/internal/atomic grain
//                           checkpoints (default "OrleansLatticeGrainState"). Only
//                           consulted when BENCH_LEAF_STORAGE_KIND=azure.
//   BENCH_LEAF_STORAGE_NUM_GRAINS
//                           Memory storage NumStorageGrains override (default 0 = keep
//                           the Orleans library default of 10). Only consulted when
//                           BENCH_LEAF_STORAGE_KIND=memory.

using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Azure.Data.Tables;
using Azure.Identity;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Storage.AzureTable;
using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.AzureThroughput.Silo;

// Force autoflush on stdout/stderr. When the process is running inside
// a Linux container (ACI, Docker) and stdout is redirected, .NET's
// default `Console.Out` is a buffered StreamWriter that does NOT flush
// on every WriteLine. The buffer is ~4 KiB, so periodic single-line
// progress output (one line/sec from the throughput drainer) sits in
// the buffer for tens of seconds before the container log driver sees
// it - which looks exactly like a hung silo. Wrapping the existing
// stream in a new StreamWriter with AutoFlush=true is the canonical
// fix and is harmless on Windows/dev runs.
Console.SetOut(new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true });
Console.SetError(new StreamWriter(Console.OpenStandardError()) { AutoFlush = true });

var storageUri  = Environment.GetEnvironmentVariable("BENCH_STORAGE_URI");
var storageConn = Environment.GetEnvironmentVariable("BENCH_STORAGE_CONN");
var walTable    = Environment.GetEnvironmentVariable("BENCH_WAL_TABLE") ?? "OrleansLatticeWal";
// Auto-rotate the tree id per silo restart so each run gets a fresh
// manifest-key namespace in the persisted WAL table. Lattice grain state
// is memory-backed and resets on restart anyway, but the WAL table is
// Azure-Tables-backed and keeps every previous run's offsets - cross-run
// activation cost would otherwise bias the first ~10s of each new
// benchmark. Operator can pin BENCH_TREE_ID explicitly to opt out.
var treeId      = Environment.GetEnvironmentVariable("BENCH_TREE_ID")
                  ?? $"azure-throughput-{DateTime.UtcNow:yyyyMMdd-HHmmss}";
var tcpPort     = ReadInt("BENCH_TCP_PORT", 7000);
var batchSize   = ReadInt("BENCH_BATCH_SIZE", 4096);
var flushMs     = ReadInt("BENCH_FLUSH_MS", 50);
var flushConcurrency = ReadInt("BENCH_FLUSH_CONCURRENCY", 8);
var walPartitions = ReadInt("BENCH_WAL_PARTITIONS", 8);
var walMaxPending = ReadInt("BENCH_WAL_MAX_PENDING_BATCHES", 8);
var shardCountOverride = ReadIntAllowZero("BENCH_SHARD_COUNT", 0);
var pipelinePhase2 = ReadBool("BENCH_PIPELINE_PHASE2", true);
var eliminateCandidateRow = ReadBool("BENCH_WAL_ELIMINATE_CANDIDATE_ROW", false);
var phaseTwoCoalescingMs = ReadIntAllowZero("BENCH_WAL_PHASE2_COALESCING_WINDOW_MS", 0);
var digestCoalescingMs = ReadIntAllowZero("BENCH_DIGEST_COALESCING_WINDOW_MS", 5);
var reportSec   = ReadInt("BENCH_REPORT_SEC", 1);
var totalDurationSec = ReadIntAllowZero("BENCH_TOTAL_DURATION_SEC", 600);
var responseTimeoutSec = ReadInt("BENCH_RESPONSE_TIMEOUT_SEC", 30);
var leafStorageKind = (Environment.GetEnvironmentVariable("BENCH_LEAF_STORAGE_KIND") ?? "azure").Trim().ToLowerInvariant();
if (leafStorageKind is not ("azure" or "memory" or "null"))
{
    Console.Error.WriteLine($"[silo] FATAL: BENCH_LEAF_STORAGE_KIND='{leafStorageKind}' is invalid; expected 'azure', 'memory', or 'null'.");
    Environment.Exit(2);
    return;
}
var leafStorageTable = Environment.GetEnvironmentVariable("BENCH_LEAF_STORAGE_TABLE") ?? "OrleansLatticeGrainState";
var leafStorageNumGrains = ReadIntAllowZero("BENCH_LEAF_STORAGE_NUM_GRAINS", 0);
// Throughput-capture (throughput-capture-plan.md step 2): selects which
// ILattice operation the silo dispatches per producer batch. Default is
// `set-many` which preserves the existing harness behaviour. The other
// four modes drive `ILattice.SetManyAtomicAsync`, `ILattice.SetAsync`
// (fan-out point write), `ILattice.GetAsync` (fan-out point read), and
// `ILattice.GetManyAsync` so a single rung can produce headline numbers
// for every public ILattice op against the c2-iii operating point. The
// `get-*` modes pre-seed the keyspace via `ILattice.BulkLoadAsync` at
// silo startup before the TCP listener opens (step 5 wires this).
var workloadMode = ParseWorkloadMode(Environment.GetEnvironmentVariable("BENCH_WORKLOAD_MODE"));
// Per-saga batch size used only when `workloadMode == SetManyAtomic`.
// A 4096-key atomic saga is not a realistic shape; 64 reflects audience-
// relevant atomic-write usage. Falls back to `batchSize` (4096) when the
// env-var is unset, which is the legacy bench shape so the operator can
// opt back to it.
var atomicBatchSize = ReadInt("BENCH_ATOMIC_BATCH_SIZE", 64);
// Read-mode pre-seed size. The producer's BENCH_VEHICLE_COUNT env-var
// determines the keyspace the producer's events touch; the silo
// mirrors that same env-var so `workloadMode in { GetPoint, GetMany }`
// can pre-seed the exact set of keys the producer will subsequently
// drive. Default 0 means "no pre-seed" (the read modes will then read
// keys that may not exist - useful only when paired with a
// previously-populated tree, e.g. against a pinned BENCH_TREE_ID).
var preseedKeyCount = ReadIntAllowZero("BENCH_VEHICLE_COUNT", 0);

if (string.IsNullOrWhiteSpace(storageUri) && string.IsNullOrWhiteSpace(storageConn))
{
    Console.Error.WriteLine("[silo] FATAL: set BENCH_STORAGE_URI (managed identity) or BENCH_STORAGE_CONN (connection string).");
    Environment.Exit(2);
    return;
}

Console.WriteLine($"[silo] treeId={treeId} walTable={walTable} tcpPort={tcpPort} batch={batchSize} flushMs={flushMs} flushConcurrency={flushConcurrency} walPartitions={walPartitions} walMaxPending={walMaxPending} shardCountOverride={shardCountOverride} pipelinePhase2={pipelinePhase2} eliminateCandidateRow={eliminateCandidateRow} phase2CoalescingMs={phaseTwoCoalescingMs} totalDurationSec={totalDurationSec} responseTimeoutSec={responseTimeoutSec} leafStorageKind={leafStorageKind} leafStorageTable={leafStorageTable} leafStorageNumGrains={leafStorageNumGrains} workloadMode={BenchWorkloadMetadata.FormatWorkloadMode(workloadMode)} atomicBatchSize={atomicBatchSize} preseedKeyCount={preseedKeyCount}");
Console.WriteLine($"[silo] auth={(string.IsNullOrEmpty(storageConn) ? $"managed-identity {storageUri}" : "connection-string")}");

var builder = Host.CreateApplicationBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss "; });
builder.Logging.SetMinimumLevel(LogLevel.Warning);
// Diagnostic verbosity for the surfaces most likely to reveal a WAL
// replay / activation fault. The bench's normal output is one progress
// line per second, so a handful of additional Information-level lines
// from these categories costs nothing and is the only way to capture
// an OnActivateAsync exception stack from inside the silo (Orleans
// wraps the original exception in "Unable to create local activation"
// at the rejection seam, and the underlying cause only appears as an
// Information / Warning line from the runtime's activation directory).
builder.Logging.AddFilter("Orleans.Lattice.Storage.AzureTable", LogLevel.Information);
builder.Logging.AddFilter("Orleans.Runtime.Catalog", LogLevel.Information);
builder.Logging.AddFilter("Orleans.Runtime.ActivationData", LogLevel.Information);
// Suppress two categories that emit a Warning per in-flight grain call
// during the post-FINAL drain window (the host is stopping, activations
// are being destroyed, the placement directory has been torn down). The
// underlying behaviour is expected shutdown back-pressure; suppressing
// these makes the bench log readable without hiding a real fault - any
// pre-shutdown forwarding/placement issue would still surface elsewhere
// in the log (e.g. as an exception from TcpIngestService's own handler).
builder.Logging.AddFilter("Orleans.Messaging", LogLevel.Error);
builder.Logging.AddFilter("Orleans.Runtime.Placement.PlacementService", LogLevel.Error);

builder.Services.AddHostedService<TcpIngestService>();
builder.Services.AddHostedService<VehicleFleetSimulator.AzureThroughput.Silo.PhaseADiagnosticReporter>();
builder.Services.AddSingleton(new IngestSettings(treeId, tcpPort, batchSize, TimeSpan.FromMilliseconds(flushMs), TimeSpan.FromSeconds(reportSec), flushConcurrency, shardCountOverride, workloadMode, atomicBatchSize, preseedKeyCount));

builder.UseOrleans(silo =>
{
    silo.Configure<ClusterOptions>(o =>
    {
        o.ClusterId = "azure-throughput";
        o.ServiceId = "azure-throughput";
    });

    // U9p step 8c-b-i probe lever. The Orleans default ResponseTimeout is 30 s,
    // which is the immediate cause of the step-8b/8c-a-i caller-side TimeoutExceptions
    // on ILattice.SetManyAsync when a worst-partition WAL flush stalls behind Azure
    // Tables tail latency. Lifting it converts caller-side *timeouts* (which trigger
    // a producer reconnect/retransmit storm) into caller-side *wall-clock slowdown*
    // with no retry cost, isolating provider tail from retry-storm amplification.
    // Both Silo and Client (StatelessWorker grains can call inward) get the same value.
    silo.Configure<SiloMessagingOptions>(o =>
    {
        o.ResponseTimeout = TimeSpan.FromSeconds(responseTimeoutSec);
    });
    silo.Configure<ClientMessagingOptions>(o =>
    {
        o.ResponseTimeout = TimeSpan.FromSeconds(responseTimeoutSec);
    });

    // In-memory single-silo clustering: no Azure Storage clustering table, no peer discovery.
    silo.UseLocalhostClustering();

    // Reminders: LatticeGrain.EnsureCompactionReminderAsync() registers a reminder on the
    // first write, so a reminder service must be wired even on a single-silo benchmark.
    // The in-memory reminder table is fine here - the harness is short-lived and the
    // compaction reminder is purely opportunistic.
    silo.UseInMemoryReminderService();

    // Leaf/internal/atomic grain checkpoint storage. The benchmark defaults to
    // a production-shape Azure Table provider (Microsoft.Orleans.Persistence
    // .AzureStorage) so the measured throughput keeps durable-IO latency on
    // the critical path; "memory" and "null" remain as diagnostic-only A/B
    // levers (see the BENCH_LEAF_STORAGE_KIND comment block at the top of
    // this file for the rationale).
    switch (leafStorageKind)
    {
        case "null":
            silo.AddNullGrainStorageAsDefault();
            silo.AddLattice((s, name) => s.AddNullGrainStorage(name));
            break;

        case "memory":
            if (leafStorageNumGrains > 0)
            {
                silo.AddMemoryGrainStorageAsDefault(o => o.NumStorageGrains = leafStorageNumGrains);
                silo.AddLattice((s, name) => s.AddMemoryGrainStorage(name, o => o.NumStorageGrains = leafStorageNumGrains));
            }
            else
            {
                silo.AddMemoryGrainStorageAsDefault();
                silo.AddLattice((s, name) => s.AddMemoryGrainStorage(name));
            }
            break;

        case "azure":
        default:
            // Reuse the same storage account and credential that the WAL
            // provider uses below so a single ACI managed-identity grant
            // covers both the WAL table and the grain-state table.
            void ConfigureAzure(Orleans.Configuration.AzureTableStorageOptions o)
            {
                if (!string.IsNullOrWhiteSpace(storageConn))
                {
                    o.TableServiceClient = new TableServiceClient(storageConn);
                }
                else
                {
                    o.TableServiceClient = new TableServiceClient(new Uri(storageUri!), new DefaultAzureCredential());
                }
                o.TableName = leafStorageTable;
            }
            silo.AddAzureTableGrainStorageAsDefault(ConfigureAzure);
            silo.AddLattice((s, name) => s.AddAzureTableGrainStorage(name, ConfigureAzure));
            break;
    }

    // Fan WAL throughput across N independent per-shard WalShardGrain
    // activations - each one hits its own Azure Tables manifest
    // partition (`_m_|{treeId}|{shardIndex}`) and gets its own
    // PhaseTwoWorker, so the foreground SetManyAsync fan-out's flush
    // concurrency actually maps to N parallel Azure-side commits
    // instead of serialising behind a single WAL grain's turn.
    //
    // WalMaxPendingBatches also raises the per-WalShardGrain pipeline
    // depth from the library's wire-compat default of 1 so each
    // partition can have multiple appends in flight against Azure
    // Tables (offset assignment is still serialised under the grain
    // turn; only the AppendBatchAsync RPCs overlap).
    silo.ConfigureLattice(treeId, o =>
    {
        o.WalPartitions = walPartitions;
        o.WalMaxPendingBatches = walMaxPending;
        // c2-xxviii: opt the bench into the leaf-side digest coalescing
        // window so the bulk-write hot path collapses N per-call
        // OnChildDigestPublishedAsync hops into one per window. Library
        // default is 0 (wire-compat synchronous publish, preserves the
        // read-your-own-digest-after-write invariant integration tests
        // pin); the bench has no such consumer.
        o.DigestCoalescingWindowMs = digestCoalescingMs;
    });

    silo.AddAzureTableWalStorage(o =>
    {
        if (!string.IsNullOrWhiteSpace(storageConn))
        {
            o.ConnectionString = storageConn;
        }
        else
        {
            o.ServiceUri = new Uri(storageUri!);
            o.TokenCredential = new DefaultAzureCredential();
        }
        o.TableName = walTable;
        o.PipelinePhaseTwoCommits = pipelinePhase2;
        // Opt-in to the phase-0 candidate-row elision optimisation. The library
        // default is off for wire-compat; enabling here lets the benchmark A/B
        // the hot-path candidate-row write against real Azure Tables.
        o.EliminateCandidateRowOnHotPath = eliminateCandidateRow;
        // U9c probe lever. Default 0 = drain-on-first-signal (library default).
        // A small positive window lets the per-shard PhaseTwoWorker wait briefly
        // after the first arrival so additional commits coalesce into the same
        // Azure Tables transaction; without this, provider.phase2.batch_size
        // stays pinned at 1.00 whenever per-partition arrival inter-spacing
        // exceeds the commit's own duration.
        o.PhaseTwoCoalescingWindow = TimeSpan.FromMilliseconds(phaseTwoCoalescingMs);
    });
});

var host = builder.Build();

// Server-side watchdog: if BENCH_TOTAL_DURATION_SEC > 0, schedule a graceful
// IHostApplicationLifetime.StopApplication() once that wall-clock window
// elapses. This is the only stop signal that survives a local deploy-shell
// crash; the ACI container group is configured with restartPolicy: Never,
// so a clean host exit transitions the group to Terminated and stops
// billing for the bench compute.
if (totalDurationSec > 0)
{
    var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
    _ = Task.Run(async () =>
    {
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(totalDurationSec), lifetime.ApplicationStopping);
            Console.WriteLine($"[silo] watchdog: BENCH_TOTAL_DURATION_SEC={totalDurationSec}s elapsed; requesting graceful shutdown.");
            lifetime.StopApplication();
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown raced the watchdog - nothing to do.
        }
    });
}

await host.RunAsync();

static int ReadInt(string name, int @default)
{
    var raw = Environment.GetEnvironmentVariable(name);
    return int.TryParse(raw, out var v) && v > 0 ? v : @default;
}

static int ReadIntAllowZero(string name, int @default)
{
    var raw = Environment.GetEnvironmentVariable(name);
    return int.TryParse(raw, out var v) && v >= 0 ? v : @default;
}

static bool ReadBool(string name, bool @default)
{
    var raw = Environment.GetEnvironmentVariable(name);
    if (string.IsNullOrWhiteSpace(raw)) return @default;
    if (bool.TryParse(raw, out var b)) return b;
    // Accept 1/0 / yes/no shorthand for convenience in deployment scripts.
    return raw.Trim() switch
    {
        "1" => true,
        "0" => false,
        "yes" or "YES" or "Yes" or "y" or "Y" => true,
        "no" or "NO" or "No" or "n" or "N" => false,
        _ => @default,
    };
}

// Throughput-capture (step 2): parse the BENCH_WORKLOAD_MODE env-var.
// Accepts case-insensitive kebab-case (set-many, set-many-atomic,
// set-point, get-point, get-many). Null/empty/unknown falls back to
// SetMany so a missing env-var preserves the legacy bench shape.
static BenchWorkloadMode ParseWorkloadMode(string? raw) =>
    string.IsNullOrWhiteSpace(raw) ? BenchWorkloadMode.SetMany : raw.Trim().ToLowerInvariant() switch
    {
        "set-many" or "setmany" => BenchWorkloadMode.SetMany,
        "set-many-atomic" or "setmanyatomic" => BenchWorkloadMode.SetManyAtomic,
        "set-point" or "setpoint" or "set" => BenchWorkloadMode.SetPoint,
        "get-point" or "getpoint" or "get" => BenchWorkloadMode.GetPoint,
        "get-many" or "getmany" => BenchWorkloadMode.GetMany,
        _ => BenchWorkloadMode.SetMany,
    };

// Throughput-capture (step 2): kebab-case rendering for the startup
// echo line and any future diagnostic surfaces lives on
// BenchWorkloadMetadata.FormatWorkloadMode (a static class) so it is
// reachable from both the top-level startup section AND from the
// TcpIngestService class methods. Top-level local functions cannot
// be referenced from non-top-level types per CS8801.

internal sealed record IngestSettings(string TreeId, int TcpPort, int BatchSize, TimeSpan FlushInterval, TimeSpan ReportInterval, int FlushConcurrency, int ShardCountOverride, BenchWorkloadMode WorkloadMode, int AtomicBatchSize, int PreseedKeyCount);

/// <summary>
/// Selects which <c>ILattice</c> operation the benchmark silo dispatches
/// per producer batch. Used by <see cref="BenchWorkloadDispatcher"/> in
/// <c>TcpIngestService.FlushAsync</c>. The default <see cref="SetMany"/>
/// preserves the harness's legacy behaviour (one <c>SetManyAsync</c> per
/// producer batch); the other four modes exist so a single rung can
/// produce headline numbers for every public <c>ILattice</c> op against
/// the c2-iii operating point. See throughput-capture-plan.md.
/// </summary>
public enum BenchWorkloadMode
{
    SetMany,
    SetManyAtomic,
    SetPoint,
    GetPoint,
    GetMany,
}

internal sealed class TcpIngestService(
    IGrainFactory grainFactory,
    IngestSettings settings,
    IHostApplicationLifetime lifetime,
    ILogger<TcpIngestService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Diagnostic: print what the HOSTED SERVICE actually received,
        // not just what Program.cs parsed at boot. If lat01 is running a
        // stale binary this line will surface it - the values here are
        // the ones that govern every flush dispatch, so they must match
        // the Program.cs "[silo]" startup line. The build stamp is the
        // assembly's BuiltAt UTC so we can prove which binary is running
        // even if the env-var defaults haven't changed across builds.
        var asm = typeof(TcpIngestService).Assembly;
        var asmLoc = asm.Location;
        var builtAtUtc = string.IsNullOrEmpty(asmLoc) ? "unknown" : File.GetLastWriteTimeUtc(asmLoc).ToString("yyyy-MM-ddTHH:mm:ssZ");
        Console.WriteLine($"[silo:ingest] settings.BatchSize={settings.BatchSize} settings.FlushConcurrency={settings.FlushConcurrency} settings.FlushInterval={settings.FlushInterval.TotalMilliseconds:F0}ms settings.ShardCountOverride={settings.ShardCountOverride} treeId={settings.TreeId} asm={Path.GetFileName(asmLoc)} builtAtUtc={builtAtUtc}");

        var lattice = grainFactory.GetGrain<ILattice>(settings.TreeId);

        // Optional one-shot reshard at silo startup.
        //
        // ShardCount is not a LatticeOptions field - it's pinned in the tree
        // registry at first-use - so the only way to push the bench above
        // the library default (64) is to call ReshardAsync once. A 0
        // override means "keep whatever is pinned".
        //
        // Important: ReshardAsync returns as soon as the coordinator has
        // accepted the request. For a non-empty tree the actual slot
        // migration then runs in the background driven by reminders. We
        // MUST poll IsReshardCompleteAsync before opening the TCP listener -
        // otherwise the benchmark's first writes race the migration and
        // get swallowed by StaleTreeRoutingException retries, masking the
        // very throughput number we're trying to measure.
        //
        // For a freshly-registered/empty tree (the typical bench start
        // state) ReshardAsync takes the empty-tree fast-path and returns
        // synchronously already complete, so the poll is a no-op.
        if (settings.ShardCountOverride > 0)
        {
            // The very first call into a freshly-activated LatticeGrain
            // races the Orleans client directory cache and routinely fails
            // with OrleansMessageRejectionException ("Unable to create
            // local activation" / "to invalid activation. Rejecting
            // now."). The directory recovers on its own within a few
            // hundred milliseconds, but a single un-retried call here
            // silently leaves the tree pinned at the library default
            // shard count (64) instead of the configured override - the
            // bench then measures the wrong configuration. Retry the
            // submit a few times on rejection and emit a loud, greppable
            // ERROR line if every attempt fails.
            const int MaxReshardAttempts = 4;
            var attempt = 0;
            var reshardSubmitted = false;
            Exception? lastReshardException = null;
            while (attempt < MaxReshardAttempts && !reshardSubmitted && !stoppingToken.IsCancellationRequested)
            {
                attempt++;
                try
                {
                    Console.WriteLine($"[silo] reshard treeId={settings.TreeId} -> shardCount={settings.ShardCountOverride} (submit attempt={attempt}/{MaxReshardAttempts})");
                    await lattice.ReshardAsync(settings.ShardCountOverride, stoppingToken).ConfigureAwait(false);
                    reshardSubmitted = true;
                }
                catch (ArgumentOutOfRangeException ex)
                {
                    // Grow-only violation (target <= current shard count on a
                    // populated tree) or above the virtual-shard-space ceiling.
                    // Not retriable - log and continue with the existing pinned
                    // shard count.
                    Console.WriteLine($"[silo] reshard treeId={settings.TreeId} rejected: {ex.Message}");
                    lastReshardException = ex;
                    break;
                }
                catch (OperationCanceledException) { throw; }
                catch (Exception ex) when (IsOrleansMessageRejection(ex))
                {
                    lastReshardException = ex;
                    var backoffMs = 100 * (1 << (attempt - 1));
                    Console.WriteLine($"[silo] reshard treeId={settings.TreeId} attempt={attempt} REJECTED ({ex.GetType().Name}: {Truncate(ex.Message, 160)}); backing off {backoffMs}ms before retry");
                    try
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(backoffMs), stoppingToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) { throw; }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[silo] reshard treeId={settings.TreeId} FAILED: {ex.GetType().Name}: {ex.Message}");
                    lastReshardException = ex;
                    break;
                }
            }

            if (!reshardSubmitted)
            {
                // Loud, greppable failure line. The harness must see this
                // and treat the run as misconfigured rather than silently
                // measuring the default shard count.
                var detail = lastReshardException is null
                    ? "no exception captured"
                    : $"{lastReshardException.GetType().Name}: {Truncate(lastReshardException.Message, 240)}";
                Console.WriteLine($"[silo] ERROR reshard treeId={settings.TreeId} ABORTED after {attempt} attempt(s): {detail}. Tree remains at its previously-pinned shard count (likely the library default, NOT shardCount={settings.ShardCountOverride}).");
            }
            else
            {
                // Bound the wait so a stuck reshard logs and continues
                // rather than wedging the silo permanently. 5 min is
                // generous for the bench's tree sizes; production callers
                // would size this against their data volume.
                var deadline = DateTime.UtcNow.AddMinutes(5);
                while (true)
                {
                    bool complete;
                    try
                    {
                        complete = await lattice.IsReshardCompleteAsync(stoppingToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) { throw; }
                    catch (Exception ex) when (IsOrleansMessageRejection(ex))
                    {
                        // Same directory-cache race can hit the very first
                        // IsReshardCompleteAsync. Treat as "not yet
                        // complete", wait, and try again on the next loop.
                        Console.WriteLine($"[silo] reshard treeId={settings.TreeId} IsReshardCompleteAsync rejected ({ex.GetType().Name}); retrying");
                        complete = false;
                    }
                    if (complete)
                    {
                        Console.WriteLine($"[silo] reshard treeId={settings.TreeId} complete");
                        break;
                    }
                    if (DateTime.UtcNow >= deadline)
                    {
                        Console.WriteLine($"[silo] reshard treeId={settings.TreeId} TIMEOUT - migration still in progress, continuing anyway");
                        break;
                    }
                    Console.WriteLine($"[silo] reshard treeId={settings.TreeId} in progress, waiting...");
                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken).ConfigureAwait(false);
                }
            }
        }

        // Proactive warm-up. Pre-activate every physical shard root before
        // we open the TCP listener so producers never see the placement-
        // directory + grain-storage first-touch storm under traffic. The
        // very first warm-up attempt can still race the Orleans client
        // directory cache the same way ReshardAsync above does, so we
        // wrap it in the same bounded retry loop and emit a single loud
        // ERROR line if every attempt fails (degraded mode: traffic
        // still starts, but the warm-start kink will be visible in the
        // per-second timeline).
        //
        // Retry budget is intentionally wider than the reshard submit
        // loop's: reshard ran INSIDE a subsequent 5-min
        // IsReshardCompleteAsync poll loop (2 s ticks) that effectively
        // gave the directory cache extra time to settle before warm-up
        // ran. Warm-up has no such follow-up loop, so we need to absorb
        // that slack here. 8 attempts with exponential backoff capped
        // at 4 s totals ~25 s worst-case - comfortably under the rung
        // duration and matches the empirically-observed time for a
        // fresh silo's local client directory to converge.
        const int MaxWarmUpAttempts = 8;
        const int MaxWarmUpBackoffMs = 4000;
        var warmUpAttempt = 0;
        var warmUpCompleted = false;
        Exception? lastWarmUpException = null;
        var warmUpSw = System.Diagnostics.Stopwatch.StartNew();
        while (warmUpAttempt < MaxWarmUpAttempts && !warmUpCompleted && !stoppingToken.IsCancellationRequested)
        {
            warmUpAttempt++;
            try
            {
                Console.WriteLine($"[silo] warmup treeId={settings.TreeId} (attempt={warmUpAttempt}/{MaxWarmUpAttempts})");
                await lattice.WarmUpAsync(stoppingToken).ConfigureAwait(false);
                warmUpCompleted = true;
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex) when (IsOrleansMessageRejection(ex))
            {
                lastWarmUpException = ex;
                var backoffMs = Math.Min(100 * (1 << (warmUpAttempt - 1)), MaxWarmUpBackoffMs);
                Console.WriteLine($"[silo] warmup treeId={settings.TreeId} attempt={warmUpAttempt} REJECTED ({ex.GetType().Name}: {Truncate(ex.Message, 160)}); backing off {backoffMs}ms before retry");
                try
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(backoffMs), stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) { throw; }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[silo] warmup treeId={settings.TreeId} FAILED: {ex.GetType().Name}: {ex.Message}");
                lastWarmUpException = ex;
                break;
            }
        }
        warmUpSw.Stop();
        if (warmUpCompleted)
        {
            Console.WriteLine($"[silo] warmup treeId={settings.TreeId} complete elapsedMs={warmUpSw.Elapsed.TotalMilliseconds:F0}");
        }
        else
        {
            var detail = lastWarmUpException is null
                ? "no exception captured"
                : $"{lastWarmUpException.GetType().Name}: {Truncate(lastWarmUpException.Message, 240)}";
            Console.WriteLine($"[silo] ERROR warmup treeId={settings.TreeId} ABORTED after {warmUpAttempt} attempt(s) elapsedMs={warmUpSw.Elapsed.TotalMilliseconds:F0}: {detail}. Continuing in degraded mode - first writes may stall on cold-shard activation.");
        }

        // Throughput-capture (step 5): read-mode pre-seed. When the silo
        // is configured to drive ILattice.GetAsync or GetManyAsync per
        // batch, populate the keyspace with PreseedKeyCount entries
        // BEFORE the TCP listener opens so the read modes hit existing
        // rows. Keys mirror the producer's vehicle-id derivation
        // (`new Guid(i, 0xC0FFEE, 0xDEADBEEF, 0xCAFEBABE).ToString("N")`)
        // so the producer's later "write" events touch exactly the same
        // 32-char hex keys the pre-seed populated. Payload is 245 bytes
        // -- matches the producer's measured JSON payload p50 in c2-vii
        // silo logs -- so per-key read latency compares apples-to-apples
        // against per-key write latency in the SetMany mode. Skipped
        // when PreseedKeyCount == 0 or the workload mode does not need
        // pre-seeded reads.
        if ((settings.WorkloadMode == BenchWorkloadMode.GetPoint || settings.WorkloadMode == BenchWorkloadMode.GetMany)
            && settings.PreseedKeyCount > 0)
        {
            var preseedSw = System.Diagnostics.Stopwatch.StartNew();
            const int PreseedPayloadBytes = 245;
            var seedEntries = new List<KeyValuePair<string, byte[]>>(settings.PreseedKeyCount);
            Span<byte> idBytes = stackalloc byte[16];
            for (var i = 0; i < settings.PreseedKeyCount; i++)
            {
                // Mirror Producer/Program.cs vehicle-id construction.
                BitConverter.TryWriteBytes(idBytes[..4], i);
                BitConverter.TryWriteBytes(idBytes.Slice(4, 4), 0xC0FFEE);
                BitConverter.TryWriteBytes(idBytes.Slice(8, 4), unchecked((int)0xDEADBEEF));
                BitConverter.TryWriteBytes(idBytes.Slice(12, 4), unchecked((int)0xCAFEBABE));
                var vehicleId = new Guid(idBytes).ToString("N");
                // Deterministic 245-byte payload so two re-runs over the
                // same keyspace produce bit-identical rows in the WAL
                // (cleanest cross-run diff). i mod 256 fill is enough to
                // tell the rows apart on a hex-dump if anything is ever
                // off.
                var payload = new byte[PreseedPayloadBytes];
                for (var b = 0; b < PreseedPayloadBytes; b++) payload[b] = (byte)((i + b) & 0xFF);
                seedEntries.Add(new KeyValuePair<string, byte[]>(vehicleId, payload));
            }
            try
            {
                // Use SetManyAsync (not BulkLoadAsync) for the pre-seed:
                // BulkLoadAsync requires an empty shard, but the silo's
                // warm-up step already materialises the root leaf, and
                // any prior probe run against the same Azure Tables
                // grain-state table leaves rows behind. SetManyAsync
                // handles a populated tree fine - that's what every
                // foreground commit path runs - and produces the
                // identical end-state (every key -> 245-byte payload)
                // for the subsequent read modes.
                await lattice.SetManyAsync(seedEntries, stoppingToken).ConfigureAwait(false);
                preseedSw.Stop();
                Console.WriteLine($"[silo] preseed treeId={settings.TreeId} entries={settings.PreseedKeyCount} payloadBytes={PreseedPayloadBytes} elapsedMs={preseedSw.Elapsed.TotalMilliseconds:F0}");
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                preseedSw.Stop();
                // Fail loud: the read modes report meaningless numbers
                // against an empty keyspace, so an aborted pre-seed must
                // surface as an obvious bench-harness fault, not silently
                // proceed.
                Console.Error.WriteLine($"[silo] ERROR preseed treeId={settings.TreeId} entries={settings.PreseedKeyCount} elapsedMs={preseedSw.Elapsed.TotalMilliseconds:F0} FAILED: {ex.GetType().Name}: {Truncate(ex.Message, 240)}");
                throw;
            }
        }

        // Drain channel: each connection writes into the same shared channel; a single drain
        // task batches and pushes into ILattice.SetManyAsync. One reader keeps the rate
        // reporter and the flush cadence trivially monotonic.
        var channel = Channel.CreateBounded<KeyValuePair<string, byte[]>>(new BoundedChannelOptions(capacity: 1 << 16)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
        });

        var drainTask = Task.Run(() => DrainAsync(lattice, channel.Reader, stoppingToken), CancellationToken.None);

        var listener = new TcpListener(IPAddress.Any, settings.TcpPort);
        listener.Start();
        logger.LogInformation("[silo] tcp listener on :{Port}", settings.TcpPort);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                TcpClient client;
                try
                {
                    client = await listener.AcceptTcpClientAsync(stoppingToken);
                }
                catch (OperationCanceledException) { break; }

                _ = Task.Run(() => HandleConnectionAsync(client, channel.Writer, stoppingToken), CancellationToken.None);
            }
        }
        finally
        {
            listener.Stop();
            channel.Writer.TryComplete();
            try { await drainTask; } catch { /* swallow on shutdown */ }
        }
    }

    private async Task HandleConnectionAsync(TcpClient client, ChannelWriter<KeyValuePair<string, byte[]>> writer, CancellationToken ct)
    {
        var remote = client.Client.RemoteEndPoint?.ToString() ?? "?";
        Console.WriteLine($"[silo] accepted {remote}");
        var treeTag = new KeyValuePair<string, object?>("tree", settings.TreeId);
        try
        {
            using (client)
            await using (var stream = client.GetStream())
            using (var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, bufferSize: 64 * 1024))
            {
                string? line;
                while ((line = await reader.ReadLineAsync(ct)) is not null)
                {
                    if (line.Length == 0) continue;

                    VehicleTelemetryEvent ev;
                    try
                    {
                        ev = JsonSerializer.Deserialize<VehicleTelemetryEvent>(line);
                    }
                    catch (JsonException)
                    {
                        continue;
                    }

                    var key = ev.VehicleId.ToString("N");
                    var value = Encoding.UTF8.GetBytes(line);

                    BenchMetrics.TcpReadLineBytes.Record(value.Length, treeTag);

                    // Time the ChannelWriter.WriteAsync separately so we
                    // can distinguish "TCP read loop is the bottleneck"
                    // (write completes immediately) from "drain is the
                    // bottleneck" (write blocks because the bounded
                    // channel is full). This is the U9o step-2 probe:
                    // it sits exactly between the TCP socket and the
                    // lattice flush dispatcher.
                    var startTs = Stopwatch.GetTimestamp();
                    await writer.WriteAsync(new KeyValuePair<string, byte[]>(key, value), ct);
                    var waitMs = Stopwatch.GetElapsedTime(startTs).TotalMilliseconds;
                    BenchMetrics.TcpReadChannelWriteWaitMs.Record(waitMs, treeTag);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (IOException ex)
        {
            logger.LogWarning(ex, "[silo] connection {Remote} dropped", remote);
        }
        Console.WriteLine($"[silo] closed {remote}");
    }

    private async Task DrainAsync(ILattice lattice, ChannelReader<KeyValuePair<string, byte[]>> reader, CancellationToken ct)
    {
        // Concurrent flush model: the drain loop fills a working batch
        // and, when the batch is full or the flush deadline elapses,
        // hands the batch off to a background flush task and starts a
        // fresh batch immediately. A SemaphoreSlim caps the number of
        // in-flight SetManyAsync calls at `FlushConcurrency` so we
        // don't unboundedly queue against the silo / WAL. The previous
        // single-flusher shape serialised every batch behind one
        // outstanding SetManyAsync, which capped throughput at
        // (1 / per-call-latency) regardless of how cheap the per-key
        // work was. Lifting the in-flight cap exposes the WAL's
        // phase-2 worker to coalesce opportunities (up to 49 batches
        // per phase-2 transaction) and lets the leaf's batched
        // commit-log seam actually run in parallel against
        // independent shards / partitions.
        var startedAt = Stopwatch.GetTimestamp();
        long writtenTotal = 0;
        long writtenSinceReport = 0;
        long failedTotal = 0;
        long failedSinceReport = 0;
        long inFlight = 0;

        using var flushGate = new SemaphoreSlim(settings.FlushConcurrency, settings.FlushConcurrency);
        var flushTasks = new HashSet<Task>();
        var firstDispatchLogged = 0;

        // Local helper: awaits a flush slot, then schedules the
        // SetManyAsync call on the threadpool and returns. Returning
        // the Task lets the caller decide whether to await commit
        // ordering or just track it for the FINAL drain. Crucially,
        // we `await flushGate.WaitAsync` BEFORE scheduling the work
        // and BEFORE returning, so the drain loop naturally stalls
        // when `FlushConcurrency` flushes are already in flight. The
        // semaphore is released inside the running task once
        // SetManyAsync completes (or faults), so the next caller's
        // `WaitAsync` unblocks at the right moment.
        async Task<Task> DispatchFlushAsync(List<KeyValuePair<string, byte[]>> batchToFlush)
        {
            var gateWaitStart = Stopwatch.GetTimestamp();
            await flushGate.WaitAsync(ct).ConfigureAwait(false);
            var gateWaitMs = Stopwatch.GetElapsedTime(gateWaitStart).TotalMilliseconds;
            var treeTag = new KeyValuePair<string, object?>("tree", settings.TreeId);
            BenchMetrics.DrainFlushDispatchWaitMs.Record(gateWaitMs, treeTag);
            BenchMetrics.DrainFlushDispatchSize.Record(batchToFlush.Count, treeTag);
            Interlocked.Increment(ref inFlight);
            if (Interlocked.Exchange(ref firstDispatchLogged, 1) == 0)
            {
                // One-shot: prove what the very first SetManyAsync call
                // actually got. Configured batch size is the cap; the
                // first batch may be smaller if a flush deadline hit
                // before the batch filled, so we log both.
                Console.WriteLine($"[silo:ingest] first dispatch entries={batchToFlush.Count} (configured BatchSize={settings.BatchSize})");
            }
            return Task.Run(async () =>
            {
                try
                {
                    var committed = await FlushAsync(lattice, batchToFlush, ct).ConfigureAwait(false);
                    if (committed == ShutdownDiscarded)
                    {
                        // Neither accepted nor failed - shutdown back-pressure.
                    }
                    else
                    {
                        Interlocked.Add(ref writtenTotal, committed);
                        Interlocked.Add(ref writtenSinceReport, committed);
                        var failed = batchToFlush.Count - committed;
                        if (failed > 0)
                        {
                            Interlocked.Add(ref failedTotal, failed);
                            Interlocked.Add(ref failedSinceReport, failed);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Shutdown path - failures already accounted for.
                }
                finally
                {
                    Interlocked.Decrement(ref inFlight);
                    flushGate.Release();
                }
            }, CancellationToken.None);
        }

        // Reporter task: samples Interlocked counters on the report
        // cadence so a stalled drain loop (e.g. all flushers blocked
        // on the WAL) still produces a progress line. Cleanly exits
        // when `ct` is cancelled.
        var reporterCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var reporterTask = Task.Run(async () =>
        {
            var lastReport = Stopwatch.GetTimestamp();
            try
            {
                while (!reporterCts.IsCancellationRequested)
                {
                    await Task.Delay(settings.ReportInterval, reporterCts.Token).ConfigureAwait(false);
                    var now = Stopwatch.GetTimestamp();
                    var sinceLocal = now - lastReport;
                    var written = Interlocked.Exchange(ref writtenSinceReport, 0);
                    var failed = Interlocked.Exchange(ref failedSinceReport, 0);
                    var inFlightNow = Interlocked.Read(ref inFlight);
                    var totalNow = Interlocked.Read(ref writtenTotal);
                    var rate = written / Math.Max(0.001, sinceLocal / (double)Stopwatch.Frequency);
                    var elapsed = (now - startedAt) / (double)Stopwatch.Frequency;
                    var failedTag = failed > 0 ? $" failed={failed,8:N0}" : string.Empty;
                    Console.WriteLine($"[silo] t={elapsed,7:0.0}s written={totalNow,12:N0} Entries written per second={rate,10:N0} inFlight={inFlightNow,3}{failedTag}");
                    lastReport = now;
                }
            }
            catch (OperationCanceledException) { }
        }, CancellationToken.None);

        var batch = new List<KeyValuePair<string, byte[]>>(settings.BatchSize);
        var nextFlush = Stopwatch.GetTimestamp() + (long)(settings.FlushInterval.TotalSeconds * Stopwatch.Frequency);

        // Track in-flight flush tasks so the FINAL line can wait for
        // them all to drain. Add/Remove happen on different threads
        // (drain loop vs flush-completion continuations) so every
        // mutation is under the set's own lock.
        void TrackFlush(Task task)
        {
            lock (flushTasks) { flushTasks.Add(task); }
            _ = task.ContinueWith(t => { lock (flushTasks) { flushTasks.Remove(t); } }, TaskScheduler.Default);
        }

        try
        {
            while (await reader.WaitToReadAsync(ct))
            {
                while (reader.TryRead(out var entry))
                {
                    batch.Add(entry);
                    if (batch.Count >= settings.BatchSize)
                    {
                        var ready = batch;
                        batch = new List<KeyValuePair<string, byte[]>>(settings.BatchSize);
                        // `DispatchFlushAsync` awaits the semaphore so the
                        // drain loop pauses here until a flush slot is
                        // free. That propagates backpressure all the way
                        // up: the channel fills, the TCP reader's
                        // `WriteAsync` blocks, and the producer slows
                        // to the silo's actual write rate. The previous
                        // shape created a Task per batch unconditionally
                        // and only awaited the gate inside the Task,
                        // which let the threadpool accumulate thousands
                        // of pending flush tasks while the silo plodded
                        // along at its own rate.
                        var flushTask = await DispatchFlushAsync(ready);
                        TrackFlush(flushTask);
                        nextFlush = Stopwatch.GetTimestamp() + (long)(settings.FlushInterval.TotalSeconds * Stopwatch.Frequency);
                    }
                }

                if (batch.Count > 0 && Stopwatch.GetTimestamp() >= nextFlush)
                {
                    var ready = batch;
                    batch = new List<KeyValuePair<string, byte[]>>(settings.BatchSize);
                    var flushTask = await DispatchFlushAsync(ready);
                    TrackFlush(flushTask);
                    nextFlush = Stopwatch.GetTimestamp() + (long)(settings.FlushInterval.TotalSeconds * Stopwatch.Frequency);
                }
            }

            if (batch.Count > 0)
            {
                var ready = batch;
                batch = new List<KeyValuePair<string, byte[]>>(0);
                var flushTask = await DispatchFlushAsync(ready);
                TrackFlush(flushTask);
            }
        }
        catch (OperationCanceledException) { }

        // Drain in-flight flushes so the FINAL line reflects everything
        // that was accepted from the channel.
        Task[] outstanding;
        lock (flushTasks)
        {
            outstanding = new Task[flushTasks.Count];
            flushTasks.CopyTo(outstanding);
        }
        try { await Task.WhenAll(outstanding); } catch { /* per-task failures already accounted for */ }

        reporterCts.Cancel();
        try { await reporterTask; } catch { /* shutdown */ }
        reporterCts.Dispose();

        var totalElapsed = (Stopwatch.GetTimestamp() - startedAt) / (double)Stopwatch.Frequency;
        var writtenFinal = Interlocked.Read(ref writtenTotal);
        var failedFinal = Interlocked.Read(ref failedTotal);
        Console.WriteLine($"[silo] FINAL written={writtenFinal:N0} failed={failedFinal:N0} elapsed={totalElapsed:0.0}s Entries written per second (avg)={writtenFinal / Math.Max(0.001, totalElapsed):N0}");
    }

    // Sentinel returned by FlushAsync when a SetManyAsync was rejected
    // because the silo is draining at shutdown. The dispatcher treats it
    // as "neither accepted nor failed" - the in-flight batch raced the
    // drain after the producer closed the socket and is correctly not
    // counted in either the written or the failed total.
    private const int ShutdownDiscarded = -1;

    // Bounded retry policy for the silo-side SetManyAsync call. A
    // freshly-started silo's first thousands of leaf-grain activations
    // race the placement directory and surface as
    // OrleansMessageRejectionException("Unable to create local
    // activation" / "to invalid activation"); the directory recovers
    // within a few hundred ms. The startup-reshard path (line ~497)
    // already retries this exact class on the same rationale; the hot
    // path needs the same treatment so the cold-start storm does not
    // count an entire batch (up to FlushBatchSize) as failed.
    //
    // 5 attempts total, 50 ms base, exponential * 2 capped at 800 ms,
    // with +/-25% jitter. With FlushBatchSize=4096 and the cold-start
    // window measured at ~2 s on step 8c-c-iii, this gives each
    // rejected batch up to ~4 s of grace before falling through to the
    // failed-batch counter, which is comfortably inside the cold-start
    // window without leaking into steady-state recovery.
    private const int FlushMaxAttempts = 5;
    private const int FlushRetryBaseMs = 50;
    private const int FlushRetryMaxMs = 800;

    private async Task<int> FlushAsync(ILattice lattice, List<KeyValuePair<string, byte[]>> batch, CancellationToken ct)
    {
        var startTs = Stopwatch.GetTimestamp();
        Exception? lastRejection = null;
        var modeTag = new KeyValuePair<string, object?>("mode", BenchWorkloadMetadata.FormatWorkloadMode(settings.WorkloadMode));
        var treeTag = new KeyValuePair<string, object?>("tree", settings.TreeId);
        for (var attempt = 1; attempt <= FlushMaxAttempts; attempt++)
        {
            try
            {
                await BenchWorkloadDispatcher.DispatchAsync(
                    settings.WorkloadMode,
                    lattice,
                    batch,
                    settings.AtomicBatchSize,
                    settings.FlushConcurrency,
                    ct).ConfigureAwait(false);
                var elapsedMs = Stopwatch.GetElapsedTime(startTs).TotalMilliseconds;
                BenchMetrics.LatticeOpDurationMs.Record(elapsedMs, treeTag, modeTag);
                BenchMetrics.LatticeOpRetryAttempts.Record(attempt - 1, treeTag, modeTag);
                return batch.Count;
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex) when (lifetime.ApplicationStopping.IsCancellationRequested && IsShutdownRejection(ex))
            {
                // Expected: producer closed the socket, the silo emitted its
                // FINAL line, and the host is now draining grain activations.
                // Any in-flight SetManyAsync that races the drain gets an
                // OrleansMessageRejectionException ("Unable to create local
                // activation" / "silo is blocking application messages"). The
                // entries those batches carried were never accepted by the
                // lattice so they are correctly not in `written`; they should
                // also not be in `failed`, because they are not a real
                // ingestion failure - they are shutdown back-pressure. Return
                // the sentinel so the dispatcher skips both counters.
                BenchMetrics.LatticeOpRetryAttempts.Record(attempt - 1, treeTag, modeTag);
                return ShutdownDiscarded;
            }
            catch (Exception ex) when (!lifetime.ApplicationStopping.IsCancellationRequested && IsOrleansMessageRejection(ex))
            {
                // Transient: the placement directory rejected the forward
                // because the target activation has not landed yet. The
                // directory recovers on its own; back off and retry.
                lastRejection = ex;
                if (attempt >= FlushMaxAttempts)
                {
                    break;
                }
                var backoffMs = Math.Min(FlushRetryMaxMs, FlushRetryBaseMs * (1 << (attempt - 1)));
                // +/-25% jitter so concurrent flushGate slots do not
                // resynchronise on the same retry wave.
                var jitter = Random.Shared.NextDouble() * 0.5 - 0.25;
                var delayMs = (int)Math.Max(1, backoffMs * (1 + jitter));
                try
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(delayMs), ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) { throw; }
            }
            catch (Exception ex)
            {
                BenchMetrics.LatticeOpRetryAttempts.Record(attempt - 1, treeTag, modeTag);
                logger.LogWarning(ex, "[silo] flush of {Count} failed (mode={Mode})", batch.Count, BenchWorkloadMetadata.FormatWorkloadMode(settings.WorkloadMode));
                return 0;
            }
        }

        BenchMetrics.LatticeOpRetryAttempts.Record(FlushMaxAttempts - 1, treeTag, modeTag);
        logger.LogWarning(
            lastRejection,
            "[silo] flush of {Count} failed after {Attempts} retry attempts against transient OrleansMessageRejectionException (mode={Mode})",
            batch.Count,
            FlushMaxAttempts,
            BenchWorkloadMetadata.FormatWorkloadMode(settings.WorkloadMode));
        return 0;
    }

    private static bool IsShutdownRejection(Exception ex)
    {
        // Match the two messages Orleans emits when an activation cannot
        // be created because the silo is draining. Type-name match keeps
        // the check resilient to Orleans internalising the type.
        var typeName = ex.GetType().FullName ?? string.Empty;
        if (!typeName.Contains("OrleansMessageRejectionException", StringComparison.Ordinal))
        {
            return false;
        }
        var msg = ex.Message ?? string.Empty;
        return msg.Contains("Unable to create local activation", StringComparison.Ordinal)
            || msg.Contains("silo is blocking application messages", StringComparison.Ordinal)
            || msg.Contains("to invalid activation", StringComparison.Ordinal);
    }

    // Type-name match for any Orleans message-rejection exception. Used
    // by the startup reshard retry: a brand-new silo's first call to a
    // never-activated grain races the client directory cache and lands
    // here, but the directory recovers within a few hundred ms and a
    // retry succeeds. Keeping this separate from IsShutdownRejection
    // makes the retry safe to use before the silo is anywhere near
    // shutdown.
    private static bool IsOrleansMessageRejection(Exception ex)
    {
        var typeName = ex.GetType().FullName ?? string.Empty;
        return typeName.Contains("OrleansMessageRejectionException", StringComparison.Ordinal);
    }

    private static string Truncate(string? s, int max)
    {
        if (string.IsNullOrEmpty(s)) return string.Empty;
        return s.Length <= max ? s : s.Substring(0, max) + "...";
    }
}

/// <summary>
/// Shared kebab-case formatter for <see cref="BenchWorkloadMode"/>.
/// Lives on a static class (not as a top-level local function) so the
/// same mapping is reachable from both the program's startup section
/// AND from the <c>TcpIngestService</c> / <c>BenchWorkloadDispatcher</c>
/// class methods. Top-level local functions cannot be referenced from
/// non-top-level types (CS8801).
/// </summary>
public static class BenchWorkloadMetadata
{
    /// <summary>
    /// Renders <paramref name="mode"/> in the same kebab-case form
    /// <c>ParseWorkloadMode</c> accepts. Used for the silo's startup
    /// banner echo line, the per-call latency histogram's <c>mode</c>
    /// tag, and log-message context.
    /// </summary>
    public static string FormatWorkloadMode(BenchWorkloadMode mode) => mode switch
    {
        BenchWorkloadMode.SetMany => "set-many",
        BenchWorkloadMode.SetManyAtomic => "set-many-atomic",
        BenchWorkloadMode.SetPoint => "set-point",
        BenchWorkloadMode.GetPoint => "get-point",
        BenchWorkloadMode.GetMany => "get-many",
        _ => mode.ToString(),
    };
}

/// <summary>
/// Workload-mode dispatcher used by <c>TcpIngestService.FlushAsync</c> to
/// route each producer batch through the <c>ILattice</c> operation
/// selected by <see cref="BenchWorkloadMode"/>. Extracted as a static
/// helper so the per-mode dispatch logic is independently testable
/// (see throughput-capture-plan.md step 7) without exposing the
/// <c>TcpIngestService</c> internals via <c>[InternalsVisibleTo]</c>.
/// </summary>
public static class BenchWorkloadDispatcher
{
    /// <summary>
    /// Dispatches <paramref name="batch"/> through the <c>ILattice</c>
    /// operation selected by <paramref name="mode"/>. Returns the number
    /// of <c>ILattice</c>-visible entries the silo has issued
    /// (always <c>batch.Count</c>; the count is mode-independent so the
    /// existing per-second "Entries written per second" counter remains
    /// directly comparable across modes when the offered load is held
    /// constant).
    /// </summary>
    /// <param name="mode">Workload selector resolved from the
    /// <c>BENCH_WORKLOAD_MODE</c> env-var at silo startup.</param>
    /// <param name="lattice">The lattice instance, already warmed up
    /// and (for read modes) already pre-seeded.</param>
    /// <param name="batch">One producer batch's worth of
    /// key/value pairs. For read modes the values are ignored; for
    /// <see cref="BenchWorkloadMode.GetMany"/> the entire batch's key
    /// list becomes the single <c>ILattice.GetManyAsync</c> argument.</param>
    /// <param name="atomicBatchSize">Saga slice size for
    /// <see cref="BenchWorkloadMode.SetManyAtomic"/>; ignored
    /// otherwise.</param>
    /// <param name="parallelism">In-flight cap for the per-entry fan-out
    /// modes (<see cref="BenchWorkloadMode.SetPoint"/>,
    /// <see cref="BenchWorkloadMode.GetPoint"/>); ignored
    /// otherwise.</param>
    /// <param name="ct">Propagates shutdown / producer-disconnect.</param>
    public static async Task<int> DispatchAsync(
        BenchWorkloadMode mode,
        ILattice lattice,
        List<KeyValuePair<string, byte[]>> batch,
        int atomicBatchSize,
        int parallelism,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentNullException.ThrowIfNull(batch);
        if (batch.Count == 0) return 0;

        switch (mode)
        {
            case BenchWorkloadMode.SetMany:
                await lattice.SetManyAsync(batch, ct).ConfigureAwait(false);
                return batch.Count;

            case BenchWorkloadMode.SetManyAtomic:
                {
                    // Slice the producer batch into atomicBatchSize-sized
                    // sagas. Each saga is awaited under the same flush
                    // gate the caller already holds, so concurrent sagas
                    // are bounded by the outer FlushConcurrency cap.
                    var sliceSize = Math.Max(1, atomicBatchSize);
                    var i = 0;
                    while (i < batch.Count)
                    {
                        var len = Math.Min(sliceSize, batch.Count - i);
                        // GetRange returns a fresh List<KeyValuePair<,>>
                        // so the slice is a self-contained value the
                        // SetManyAtomicAsync seam can pin without
                        // worrying about aliasing the outer batch.
                        var slice = batch.GetRange(i, len);
                        await lattice.SetManyAtomicAsync(slice, ct).ConfigureAwait(false);
                        i += len;
                    }
                    return batch.Count;
                }

            case BenchWorkloadMode.SetPoint:
                await FanOutAsync(
                    batch,
                    parallelism,
                    (kvp, token) => lattice.SetAsync(kvp.Key, kvp.Value, token),
                    ct).ConfigureAwait(false);
                return batch.Count;

            case BenchWorkloadMode.GetPoint:
                await FanOutAsync(
                    batch,
                    parallelism,
                    async (kvp, token) =>
                    {
                        _ = await lattice.GetAsync(kvp.Key, token).ConfigureAwait(false);
                    },
                    ct).ConfigureAwait(false);
                return batch.Count;

            case BenchWorkloadMode.GetMany:
                {
                    // Project the producer batch to its key list. Capacity-
                    // hinted so the List grows zero times on the hot path.
                    var keys = new List<string>(batch.Count);
                    for (var i = 0; i < batch.Count; i++) keys.Add(batch[i].Key);
                    _ = await lattice.GetManyAsync(keys, ct).ConfigureAwait(false);
                    return batch.Count;
                }

            default:
                throw new InvalidOperationException($"Unhandled BenchWorkloadMode: {mode}");
        }
    }

    /// <summary>
    /// Bounded-parallelism fan-out over <paramref name="batch"/>: at
    /// most <paramref name="parallelism"/> calls to
    /// <paramref name="action"/> are in flight at any time. Used by the
    /// per-entry point-write and point-read modes so concurrency is
    /// capped at the caller-supplied flush concurrency rather than
    /// thrashing the threadpool with one Task per entry.
    /// </summary>
    private static async Task FanOutAsync(
        List<KeyValuePair<string, byte[]>> batch,
        int parallelism,
        Func<KeyValuePair<string, byte[]>, CancellationToken, Task> action,
        CancellationToken ct)
    {
        var maxInFlight = Math.Max(1, parallelism);
        using var gate = new SemaphoreSlim(maxInFlight, maxInFlight);
        // Track every issued task so a single failure surfaces via
        // Task.WhenAll rather than escaping into the threadpool. The
        // FlushAsync caller wraps DispatchAsync in retry/shutdown
        // handling that treats a thrown exception as a transient
        // failure or as a real fault; either way a fan-out task that
        // faulted must propagate.
        var tasks = new List<Task>(batch.Count);
        for (var i = 0; i < batch.Count; i++)
        {
            await gate.WaitAsync(ct).ConfigureAwait(false);
            var kvp = batch[i];
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    await action(kvp, ct).ConfigureAwait(false);
                }
                finally
                {
                    gate.Release();
                }
            }, ct));
        }
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }
}
