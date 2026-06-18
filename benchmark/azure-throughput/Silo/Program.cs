// Azure throughput benchmark harness - single-silo lattice host.
//
// Listens on a TCP port for newline-delimited JSON `VehicleTelemetryEvent` records, batches
// them, and writes each batch into a single lattice tree backed by the Azure Table WAL
// storage provider (managed identity to the configured storage account).
//
// Reports "ops/sec" to stdout once per second so the systemd-journald
// log is the canonical result surface.
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
//   BENCH_WAL_PARTITIONS    WAL partitions per tree (defaults to LatticeOptions
//                           .DefaultWalPartitions so the bench harness tracks the
//                           shipping default automatically). Matches flush concurrency
//                           so parallel SetManyAsync flushes fan out across distinct
//                           WAL grains and therefore distinct Azure Tables manifest
//                           partitions.
//   BENCH_WAL_MAX_PENDING_BATCHES
//                           Per-WalShardGrain pipeline depth (defaults to
//                           LatticeOptions.DefaultWalMaxPendingBatches so the bench
//                           harness tracks the shipping default automatically). Drop
//                           to 1 for the historical single-in-flight-per-partition
//                           shape (strict ordering against the provider; no pipeline
//                           depth). Raising in combination with a matching
//                           BENCH_FLUSH_CONCURRENCY lift can saturate a single Azure
//                           Tables Standard storage account (~2,500 ops/sec/account)
//                           and surface as 429 throttling - see
//                           docs/lattice/wal-tuning.md.
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
//                           Default inherits AzureTableWalStorageOptions
//                           .DefaultPipelinePhaseTwoCommits (on).
//   BENCH_WAL_PHASE2_COALESCING_WINDOW_MS
//                           AzureTableWalStorageOptions.PhaseTwoCoalescingWindow in ms.
//                           Default inherits AzureTableWalStorageOptions
//                           .DefaultPhaseTwoCoalescingWindow (5 ms). Set to 0 for
//                           drain-on-first-signal, or another small positive value
//                           (below the observed phase-2 commit duration p50) to let the
//                           per-shard PhaseTwoWorker wait briefly after the first arrival
//                           so additional commits coalesce into the same Azure Tables
//                           transaction.
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
//                           triggers a graceful host shutdown so the systemd unit
//                           transitions to inactive even if the local cohort runner
//                           that orchestrated the run has died. 0 disables the
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
//   BENCH_SATURATION_SAMPLE_MS
//                           F-085 WAL saturation sampler tick interval in ms.
//                           Defaults to LatticeOptions.DefaultWalSaturationSampleInterval
//                           (200 ms). Lower values reduce the worst-case
//                           transition latency the bench TCP reader sees
//                           when the silo crosses Saturated, at the cost
//                           of slightly more timer-driven sampler work.
//                           0 explicitly disables the sampler (signal pins
//                           to Healthy and the TCP-read gating in
//                           HandleConnectionAsync becomes a no-op).
//   BENCH_SATURATION_THROTTLED_RATIO
//                           F-085 admission-depth ratio at-or-above which
//                           the saturation signal raises the tree to
//                           Throttled. Defaults to
//                           LatticeOptions.DefaultWalSaturationThrottledRatio
//                           (0.75). Lower the ratio for an earlier-engaging
//                           throttled regime; raise to keep the bench
//                           dispatching at full rate until later in the
//                           saturation episode. Range [0.0, 1.0].
//   BENCH_SATURATION_DISPATCH_TIMEOUT_THRESHOLD
//                           F-085 minimum WalAppendDispatchTimeout trips
//                           per sample window that raise the tree to
//                           Saturated regardless of admission depth.
//                           Defaults to
//                           LatticeOptions.DefaultWalSaturationDispatchTimeoutThreshold
//                           (1). Raise for less aggressive failure-tail
//                           classification (e.g. a noisy storage account
//                           where occasional single trips are expected
//                           without operator concern).

using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using Azure.Core.Pipeline;
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

// Force autoflush on stdout/stderr. When the process is running under
// systemd (or any other process supervisor that redirects stdout to a
// pipe/journal, including Docker), .NET's default `Console.Out` is a
// buffered StreamWriter that does NOT flush on every WriteLine. The
// buffer is ~4 KiB, so periodic single-line progress output (one
// line/sec from the throughput drainer) sits in the buffer for tens of
// seconds before the journal sees it - which looks exactly like a hung
// silo. Wrapping the existing stream in a new StreamWriter with
// AutoFlush=true is the canonical fix and is harmless on Windows/dev runs.
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
var walPartitions = ReadInt("BENCH_WAL_PARTITIONS", LatticeOptions.DefaultWalPartitions);
var walMaxPending = ReadInt("BENCH_WAL_MAX_PENDING_BATCHES", LatticeOptions.DefaultWalMaxPendingBatches);
// Connection-REUSE transport (cloud-NAT socket hygiene; originally
// attributed to ACI but the same long-lived-connection failure mode applies
// to any cloud-side SNAT including a VM behind an Azure load balancer or
// outbound NAT gateway). NOTE: the original SNAT-socket-hang narrative for
// the 25k wedge was falsified by three post-mortems
// (wal-wedge-root-cause-2025-11-25-revised, wal-wedge-watchdog-tcpdump-2025-11-25,
// and wal-wedge-watchdog-confirmation), which attributed the wedge to an
// unbounded cross-grain await held under _ensureRootGate in
// ShardRootGrain.EnsureRootSlowAsync rather than to a hung Azure Tables
// socket. That await was bounded by PR #568 via
// LatticeOptions.ActivationReadyTimeout (15 s default), which closed the
// activation back-pressure deadlock tracked as the 25k wedge.
// This knob and the per-attempt timeout below are kept as correct
// long-lived-connection hygiene (reuse pooled connections with a FINITE
// pooled-connection lifetime + idle timeout so cloud-NAT-killed sockets are
// torn down rather than reused into a hang, paired with a bounded
// per-attempt timeout) - they are not the wedge fix. A residual wedge with
// the same inFlight=8 pinned signature still surfaces at the 4k-vehicle
// saturation rung after PR #568 (see benchmark/azure-throughput/throughput.md
// section 18); attribution of that residual is independent of this knob.
var walConnectionReuse = ReadBool("BENCH_WAL_CONNECTION_REUSE", false);
// Per-attempt network timeout for the WAL Azure Tables client. Default 0
// leaves the SDK default (100s, effectively unbounded). A finite value
// bounds every individual HTTP attempt so a hung request fails and
// releases its pending-batch slot - kept as long-lived-connection hygiene
// against any cloud SNAT path, not as the wedge fix (see the
// connection-reuse note above).
var walNetworkTimeoutSec = ReadIntAllowZero("BENCH_WAL_NETWORK_TIMEOUT_SEC", 0);
// Finite per-commit deadline (seconds) for the per-shard PhaseTwoWorker's
// manifest commit, mapped to AzureTableWalStorageOptions.PhaseTwoCommitTimeout.
// When the env var is ABSENT the option is left at the library default
// (AzureTableWalStorageOptions.DefaultPhaseTwoCommitTimeout, 3 s) - the deploy
// script only emits this var when the operator overrides it. When SUPPLIED the
// value is honoured verbatim: 0 explicitly disables the deadline (null - the
// historical unbounded behaviour), > 0 sets that finite deadline. A finite
// deadline converts a hung commit into a bounded TimeoutException the
// sticky-failure resync path recovers, and increments the
// orleans.lattice.provider.phase2.commit.timeouts counter once per abandoned
// commit so the wedge fix is directly observable pre/post.
int? walPhaseTwoCommitTimeoutSec =
    Environment.GetEnvironmentVariable("BENCH_WAL_PHASE2_COMMIT_TIMEOUT_SEC") is { } rawPhase2Timeout
    && int.TryParse(rawPhase2Timeout, out var parsedPhase2Timeout) && parsedPhase2Timeout >= 0
        ? parsedPhase2Timeout
        : null;
// Finite pooled-connection lifetime (seconds) for the reuse transport so
// cloud-NAT-killed sockets are recycled instead of reused into a hang.
var walConnLifetimeSec = ReadInt("BENCH_WAL_CONN_LIFETIME_SEC", 90);
var shardCountOverride = ReadIntAllowZero("BENCH_SHARD_COUNT", 0);
var pipelinePhase2 = ReadBool("BENCH_PIPELINE_PHASE2", AzureTableWalStorageOptions.DefaultPipelinePhaseTwoCommits);
var eliminateCandidateRow = ReadBool("BENCH_WAL_ELIMINATE_CANDIDATE_ROW", AzureTableWalStorageOptions.DefaultEliminateCandidateRowOnHotPath);
var phaseTwoCoalescingMs = ReadIntAllowZero("BENCH_WAL_PHASE2_COALESCING_WINDOW_MS", (int)AzureTableWalStorageOptions.DefaultPhaseTwoCoalescingWindow.TotalMilliseconds);
var digestCoalescingMs = ReadIntAllowZero("BENCH_DIGEST_COALESCING_WINDOW_MS", 5);
// F-086: BENCH_* knobs pinning the F-085 saturation sampler cadence
// and thresholds. Defaults match the library shipping defaults so
// removing the env-vars reproduces the out-of-the-box behaviour
// exactly; the bench can pin them for per-cohort A/B sweeps without
// re-deploying. ReadDouble allows zero so a bench operator can set
// WalSaturationThrottledRatio=0 (every depth at-or-above 0 raises
// Throttled - exercises the always-throttled regime) without the
// option being silently rejected by ReadInt's >0 guard. The library
// validator already rejects out-of-range / NaN values on first
// IOptionsMonitor resolution, so an invalid env-var crashes the silo
// at startup rather than producing a wrong-but-running configuration.
var saturationSampleMs = ReadIntAllowZero(
    "BENCH_SATURATION_SAMPLE_MS",
    (int)LatticeOptions.DefaultWalSaturationSampleInterval.TotalMilliseconds);
var saturationThrottledRatio = ReadDouble(
    "BENCH_SATURATION_THROTTLED_RATIO",
    LatticeOptions.DefaultWalSaturationThrottledRatio);
var saturationDispatchTimeoutThreshold = ReadInt(
    "BENCH_SATURATION_DISPATCH_TIMEOUT_THRESHOLD",
    LatticeOptions.DefaultWalSaturationDispatchTimeoutThreshold);
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
// modes drive `ILattice.SetManyAtomicAsync`, `ILattice.SetAsync`
// (fan-out point write), `ILattice.GetAsync` (fan-out point read), and
// `ILattice.GetManyAsync` so a single rung can produce headline numbers
// for every public ILattice op against the c2-iii operating point. The
// `get-*` modes pre-seed the keyspace via `ILattice.BulkLoadAsync` at
// silo startup before the TCP listener opens (step 5 wires this).
// The fixed-shape atomic modes (`set-many-atomic-2`, `cross-tree-atomic-2`,
// `cross-tree-atomic-64`) let one rung compare single-tree against
// multi-tree (cross-tree) atomic-write throughput at matched batch sizes;
// the cross-tree modes commit across a sibling `{treeId}-b` tree via
// `IGrainFactory.BeginAtomicWrite(...).CommitAsync()`.
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

// c2-xxix: a misleading prior header reported `preseedKeyCount=` as the
// raw env-var value even when the gate inside the IngestService skipped
// the seed (e.g. set-point inherits BENCH_VEHICLE_COUNT as
// preseedKeyCount but the gate only fires on read modes - write modes
// deliberately do not pre-seed because seeding the target keys would
// convert the bench from "write keys" to "update existing keys"). Report
// both the configured value and the effective fire-or-not state so a
// glance at the silo log line answers "did the seed actually run?"
// unambiguously.
var preseedWillFire = preseedKeyCount > 0
    && (workloadMode == BenchWorkloadMode.GetPoint
        || workloadMode == BenchWorkloadMode.GetMany);
// Banner descriptor for the phase-2 commit deadline: "default(3s)" when the
// operator left it unset (library DefaultPhaseTwoCommitTimeout applies),
// "off" when explicitly disabled (supplied 0), or the supplied second-count.
var walPhase2CommitTimeoutBanner = walPhaseTwoCommitTimeoutSec switch
{
    null => $"default({AzureTableWalStorageOptions.DefaultPhaseTwoCommitTimeout.TotalSeconds:0.##}s)",
    0 => "off",
    var s => $"{s}s",
};
// Deployment-verification tokens: the residual phase-1/activation WAL
// wedge diagnostic pack added two new bounded-deadline options on
// LatticeOptions whose default values are emitted verbatim in the banner.
// Their PRESENCE in the banner is the cheapest proof that the deployed
// silo binary contains the diagnostic-pack code path - the symbols
// referenced here do not exist on earlier binaries, so a stale image
// would fail to compile / start. The values themselves are the library
// defaults; the bench harness does not currently override them, but if
// it later does the override path must update these tokens too.
var walAppendDispatchTimeoutBanner = $"default({LatticeOptions.DefaultWalAppendDispatchTimeout.TotalSeconds:0.##}s)";
var walFlushPreflightTimeoutBanner = $"default({LatticeOptions.DefaultWalFlushPreflightTimeout.TotalSeconds:0.##}s)";
Console.WriteLine($"[silo] treeId={treeId} walTable={walTable} tcpPort={tcpPort} batch={batchSize} flushMs={flushMs} flushConcurrency={flushConcurrency} walPartitions={walPartitions} walMaxPending={walMaxPending} shardCountOverride={shardCountOverride} pipelinePhase2={pipelinePhase2} eliminateCandidateRow={eliminateCandidateRow} phase2CoalescingMs={phaseTwoCoalescingMs} walNetworkTimeoutSec={walNetworkTimeoutSec} walPhase2CommitTimeout={walPhase2CommitTimeoutBanner} walAppendDispatchTimeout={walAppendDispatchTimeoutBanner} walFlushPreflightTimeout={walFlushPreflightTimeoutBanner} totalDurationSec={totalDurationSec} responseTimeoutSec={responseTimeoutSec} leafStorageKind={leafStorageKind} leafStorageTable={leafStorageTable} leafStorageNumGrains={leafStorageNumGrains} workloadMode={BenchWorkloadMetadata.FormatWorkloadMode(workloadMode)} atomicBatchSize={atomicBatchSize} preseedKeyCount={preseedKeyCount} preseedWillFire={preseedWillFire}");
Console.WriteLine($"[silo] auth={(string.IsNullOrEmpty(storageConn) ? $"managed-identity {storageUri}" : "connection-string")}");
// F-086: echo the saturation knobs so the cohort log shows the exact
// values the TCP-read gating + the silo's sampler use. A "default"
// suffix on the sample interval is implicit when the env-var was not
// supplied; the actual value the silo will use is shown for clarity.
Console.WriteLine($"[silo] saturationSampleMs={saturationSampleMs} saturationThrottledRatio={saturationThrottledRatio:0.###} saturationDispatchTimeoutThreshold={saturationDispatchTimeoutThreshold}");

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
// F-086: register the per-silo saturation-transition logger so each
// transition lands a [silo:saturation] line on stdout. Pure
// observability: the TCP-read loop in TcpIngestService consumes the
// same F-085 signal via the polling getter on its hot path; this
// observer surfaces the transitions as log events so the cohort
// post-mortem can correlate the producer's slipMaxMs spike with the
// silo's recorded transition windows without scraping the meter.
//
// FX-029: the same logger also tracks per-tree "most-recently
// Saturated" wall-clock so TcpIngestService.DrainAsync can detect a
// recent saturation episode at the producer-stop boundary and
// abandon the residual ingest-channel batch (rather than dispatching
// it against a residually back-pressured storage account where it
// would trip WalAppendDispatchTimeout 30 s later and surface as
// failed=N on FINAL). Register the concrete type as a singleton, then
// forward the IWalSaturationObserver interface registration to the
// same instance so the saturation sampler's dispatcher and the bench's
// drain loop see consistent state.
builder.Services.AddSingleton<VehicleFleetSimulator.AzureThroughput.Silo.BenchSaturationLogger>();
builder.Services.AddSingleton<Orleans.Lattice.IWalSaturationObserver>(sp =>
    sp.GetRequiredService<VehicleFleetSimulator.AzureThroughput.Silo.BenchSaturationLogger>());
builder.Services.AddSingleton(new IngestSettings(treeId, tcpPort, batchSize, TimeSpan.FromMilliseconds(flushMs), TimeSpan.FromSeconds(reportSec), flushConcurrency, shardCountOverride, workloadMode, atomicBatchSize, preseedKeyCount, walMaxPending, responseTimeoutSec));

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
            // provider uses below so a single VM managed-identity grant
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
        // F-086: pin the F-085 saturation sampler cadence + thresholds
        // for this tree. Defaults are the library shipping defaults so
        // a cohort with no env-vars set reproduces the out-of-the-box
        // behaviour exactly; the env-vars exist for per-cohort A/B
        // sweeps. The signal is silo-scoped per F-085, so per-tree
        // overrides here only affect the sampler's classification of
        // *this* tree - aligned with the bench's single-tree topology.
        o.WalSaturationSampleInterval = TimeSpan.FromMilliseconds(saturationSampleMs);
        o.WalSaturationThrottledRatio = saturationThrottledRatio;
        o.WalSaturationDispatchTimeoutThreshold = saturationDispatchTimeoutThreshold;
    });

    // Storage-usage poller cadence is left at the library default (15s).
    // The previous override pinned it to TimeSpan.Zero to dodge the
    // leaf-walk-on-every-tick path that activated every shard's whole
    // leaf chain on each poll, monopolising the ShardRootGrain turn under
    // load. That path was rewritten: the poller now drives the leaf-free
    // ILatticeAdmin.PollWalUsageAsync (touches only WAL partition grains)
    // and the deep leaf/snapshot bytes are served in O(1) per shard from
    // an incrementally-maintained running total. The poll path no longer
    // competes with foreground ingest, so the override is not needed.
    //
    // BENCH_DISABLE_STORAGE_USAGE_POLLER (default empty) is a per-cohort
    // escape hatch: setting it to "1"/"true" reverts to the pre-cold-tree-fix
    // behaviour so a like-for-like A/B against the historic baseline can
    // be run without recompiling the silo image.
    var disablePoller = (Environment.GetEnvironmentVariable("BENCH_DISABLE_STORAGE_USAGE_POLLER") ?? string.Empty).Trim();
    if (disablePoller == "1" || string.Equals(disablePoller, "true", StringComparison.OrdinalIgnoreCase))
    {
        silo.ConfigureLattice(o =>
        {
            o.StorageUsagePollInterval = TimeSpan.Zero;
        });
        Console.WriteLine("[silo] BENCH_DISABLE_STORAGE_USAGE_POLLER=1 -> StorageUsagePollInterval=Zero (poller disabled)");
    }

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
        // Elide the phase-0 candidate-row write. Default inherits the
        // library default (AzureTableWalStorageOptions
        // .DefaultEliminateCandidateRowOnHotPath = true); the C-row
        // contends with the per-shard PhaseTwoWorker on the shared
        // manifest partition, so eliding it removes a server-side-
        // serialised round-trip from every batch's hot path.
        o.EliminateCandidateRowOnHotPath = eliminateCandidateRow;
        // Default inherits AzureTableWalStorageOptions
        // .DefaultPhaseTwoCoalescingWindow (5 ms). A small positive
        // window lets the per-shard PhaseTwoWorker wait briefly after
        // the first arrival so additional commits coalesce into the
        // same Azure Tables transaction; without it,
        // provider.phase2.batch_size stays pinned at 1.00 whenever
        // per-partition arrival inter-spacing exceeds the commit's own
        // duration.
        o.PhaseTwoCoalescingWindow = TimeSpan.FromMilliseconds(phaseTwoCoalescingMs);
        // Wedge fix part 1: bound every individual HTTP attempt. Without a
        // finite per-attempt timeout, a request dispatched onto a
        // cloud-NAT-killed socket hangs at TableClient.AddEntityAsync forever,
        // holds a WalMaxPendingBatches slot, and the back-pressure await in
        // WalShardGrain deadlocks the whole pipeline at inFlight=cap. A
        // finite RetryNetworkTimeout turns that hang into a per-attempt
        // failure the retry policy can recover from, releasing the slot.
        if (walNetworkTimeoutSec > 0)
        {
            o.RetryNetworkTimeout = TimeSpan.FromSeconds(walNetworkTimeoutSec);
        }
        // Wedge fix part 2: bound the per-shard PhaseTwoWorker's whole
        // manifest commit, not just each HTTP attempt. RetryNetworkTimeout
        // caps a single attempt, but the worker's background drain loop
        // commits phase-2 transactions one coalesced group at a time and
        // the next group cannot start until the current commit returns -
        // so a commit that keeps re-issuing past the network timeout (or
        // parks in a state the SDK never surfaces) still wedges every later
        // commit on the shard. PhaseTwoCommitTimeout is the finite deadline
        // covering the whole commit; on expiry the worker faults the batch
        // and every later pending commit (recovered by the sticky-failure
        // resync path) and increments
        // orleans.lattice.provider.phase2.commit.timeouts so the fix is
        // directly observable pre/post. Only overridden when the operator
        // supplied BENCH_WAL_PHASE2_COMMIT_TIMEOUT_SEC; absent leaves the
        // library default (DefaultPhaseTwoCommitTimeout). A supplied 0 maps
        // to null (explicitly unbounded); a supplied > 0 sets that deadline.
        if (walPhaseTwoCommitTimeoutSec is { } phase2TimeoutSec)
        {
            o.PhaseTwoCommitTimeout = phase2TimeoutSec > 0
                ? TimeSpan.FromSeconds(phase2TimeoutSec)
                : null;
        }
        // Connection-reuse transport. When BENCH_WAL_CONNECTION_REUSE
        // is set, replace the default Azure.Core transport with one that reuses
        // connections (dodging the SNAT establishment stall) but with a FINITE
        // lifetime/idle timeout so cloud-NAT-killed sockets are recycled rather
        // than reused into a hang. BuildServiceClient honours this callback only
        // in connection-string / credential mode (the path this silo uses); the
        // provider re-attaches RetryAttemptTrackingPolicy afterwards so
        // provider.retry.attempts{status=...} still records.
        if (walConnectionReuse)
        {
            o.ConfigureClientOptions = clientOptions =>
            {
                var handler = new SocketsHttpHandler
                {
                    // Wedge fix part 2: FINITE lifetime/idle so a cloud-NAT-
                    // killed socket is torn down and re-established on next use
                    // instead of being reused into a request that hangs forever.
                    PooledConnectionLifetime = TimeSpan.FromSeconds(walConnLifetimeSec),
                    PooledConnectionIdleTimeout = TimeSpan.FromSeconds(walConnLifetimeSec),
                    // Bound connection establishment too, so a wedged handshake
                    // fails fast rather than parking a request indefinitely.
                    ConnectTimeout = TimeSpan.FromSeconds(15),
                    // Multiplex concurrent requests over a single HTTP/2
                    // connection per server rather than fanning out onto a
                    // connection-per-request.
                    EnableMultipleHttp2Connections = false,
                };
                clientOptions.Transport = new HttpClientTransport(new HttpClient(handler));
            };
        }
    });

    // set-point-mv cohort only: attach an asynchronous materialised view to the
    // target tree. The view is a key-preserving passthrough (no filter, no
    // re-key) so it mirrors the source 1:1 and the maintainer performs real,
    // representative WAL-tailing work for every committed write - but entirely
    // off the foreground SetAsync hot path. This is the A/B partner of the
    // plain set-point cohort: if the materialised view is truly asynchronous,
    // the primary tree's point-write throughput/latency must be statistically
    // indistinguishable between the two cohorts. AddLatticeViews folds in the
    // WAL consumer-cursor registry; the commit-log reader the maintainer tails
    // comes from AddLattice above, so no replication package is involved (the
    // view is local-derive only).
    if (workloadMode == BenchWorkloadMode.SetPointMv)
    {
        silo.AddLatticeViews(views =>
            views.AddView("bench", treeId, new PredicateLatticeViewProjection()));
    }
});

var host = builder.Build();

// Server-side watchdog: if BENCH_TOTAL_DURATION_SEC > 0, schedule a graceful
// IHostApplicationLifetime.StopApplication() once that wall-clock window
// elapses. This is the only stop signal that survives a local cohort-runner
// crash; the lattice-silo systemd unit is configured to not auto-restart
// (cohort-driven lifecycle), so a clean host exit leaves the unit inactive
// and the VM-level DevTestLab auto-shutdown schedule (see
// benchmark/azure-throughput/README.md's Auto-shutdown safety net) puts the
// VM into deallocated state on a fixed daily window to bound paid compute.
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

static double ReadDouble(string name, double @default)
{
    var raw = Environment.GetEnvironmentVariable(name);
    return double.TryParse(raw, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var v)
        && !double.IsNaN(v) && v >= 0
        ? v
        : @default;
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
        "set-many-atomic-2" or "setmanyatomic2" => BenchWorkloadMode.SetManyAtomic2,
        "cross-tree-atomic-2" or "crosstreeatomic2" => BenchWorkloadMode.CrossTreeAtomic2,
        "cross-tree-atomic-64" or "crosstreeatomic64" => BenchWorkloadMode.CrossTreeAtomic64,
        "set-point-mv" or "setpointmv" => BenchWorkloadMode.SetPointMv,
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

internal sealed record IngestSettings(string TreeId, int TcpPort, int BatchSize, TimeSpan FlushInterval, TimeSpan ReportInterval, int FlushConcurrency, int ShardCountOverride, BenchWorkloadMode WorkloadMode, int AtomicBatchSize, int PreseedKeyCount, int WalMaxPendingBatches, int ResponseTimeoutSec);

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

    // Fixed-shape atomic-write modes added so a single rung can produce the
    // single-tree vs multi-tree (cross-tree) atomic-write comparison the
    // published single-silo perf doc wants at matched batch sizes. Unlike
    // SetManyAtomic (whose saga slice follows BENCH_ATOMIC_BATCH_SIZE), these
    // pin their batch shapes: SetManyAtomic2 slices the producer batch into
    // 2-key single-tree sagas; CrossTreeAtomic2 / CrossTreeAtomic64 commit
    // all-or-nothing across two trees ({treeId} and {treeId}-b) with 2 keys
    // (1 per tree) and 64 keys (32 per tree) per saga respectively, via
    // IGrainFactory.BeginAtomicWrite(...).CommitAsync().
    SetManyAtomic2,
    CrossTreeAtomic2,
    CrossTreeAtomic64,

    // set-point-mv: the exact same write path as SetPoint (one SetAsync per
    // key against the target tree), but the silo additionally attaches an
    // asynchronous materialised view derived from that tree (a key-preserving
    // passthrough view registered via AddLatticeViews). It exists only as the
    // A/B partner of set-point: the primary tree's foreground write path is
    // untouched, so comparing the two cohorts shows whether maintaining a
    // materialised view perturbs the source tree's point-write throughput and
    // latency. It should not - the view maintainer tails the WAL off the hot
    // path, so the asynchronous derivation must not appear on the writer's
    // critical path.
    SetPointMv,
}

internal sealed class TcpIngestService(
    IGrainFactory grainFactory,
    IngestSettings settings,
    IHostApplicationLifetime lifetime,
    IWalSaturationSignal saturationSignal,
    BenchSaturationLogger saturationLogger,
    IServiceProvider services,
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
            // ERROR line if every attempt fails. The bench then throws so
            // the silo container exits non-zero and the harness marks the
            // run as misconfigured rather than silently measuring the wrong
            // shard count.
            const int MaxReshardAttempts = 12;
            const int MaxReshardBackoffMs = 6000;
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
                    // Not retriable - and not silently survivable either:
                    // the bench would otherwise measure the previously-pinned
                    // shard count, which is exactly the misconfiguration the
                    // operator is trying to avoid by requesting the override.
                    Console.WriteLine($"[silo] reshard treeId={settings.TreeId} rejected: {ex.Message}");
                    lastReshardException = ex;
                    break;
                }
                catch (OperationCanceledException) { throw; }
                catch (Exception ex) when (IsOrleansMessageRejection(ex))
                {
                    lastReshardException = ex;
                    // Exponential backoff capped at MaxReshardBackoffMs:
                    // 100, 200, 400, 800, 1600, 3200, 6000, 6000, 6000,
                    // 6000, 6000, 6000 ms. Cumulative wait across 12
                    // attempts is ~48 s - well within the harness deploy
                    // timeout and long enough to absorb the cold-start
                    // Orleans client directory convergence observed in
                    // production runs (the prior 8-attempt / 25 s budget
                    // was empirically too tight: the 25000:5 c2-xxix
                    // probe saw 7+ consecutive rejections across two
                    // restarts before the directory cleared).
                    var backoffMs = Math.Min(100 * (1 << (attempt - 1)), MaxReshardBackoffMs);
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
                // Loud, greppable failure line - and a hard throw so the
                // silo container exits non-zero rather than silently
                // measuring the wrong shard count. Previously this only
                // warned and continued, but the operator's
                // ShardCountOverride is the entire reason the bench needs
                // a reshard in the first place; silently falling back to
                // the registry-pinned default invalidates the entire run
                // and pollutes any throughput cell that depended on it.
                var detail = lastReshardException is null
                    ? "no exception captured"
                    : $"{lastReshardException.GetType().Name}: {Truncate(lastReshardException.Message, 240)}";
                var msg = $"[silo] ERROR reshard treeId={settings.TreeId} ABORTED after {attempt} attempt(s): {detail}. Tree remains at its previously-pinned shard count (likely the library default, NOT shardCount={settings.ShardCountOverride}).";
                Console.WriteLine(msg);
                throw new InvalidOperationException(msg, lastReshardException);
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
        const int MaxWarmUpAttempts = 12;
        const int MaxWarmUpBackoffMs = 6000;
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
            // Loud, greppable failure line - and a hard throw so the silo
            // container exits non-zero rather than measuring a cold tree.
            // Previously this only warned and continued ("degraded mode -
            // first writes may stall on cold-shard activation"), but a
            // cold start materially distorts the first ~30 s of per-call
            // latency by paying the placement-directory + grain-storage
            // first-touch storm against the measurement window. The
            // throughput.md cells are quoted as steady-state numbers; a
            // silently-degraded warm-up invalidates them just as surely
            // as a silently-degraded reshard does.
            var detail = lastWarmUpException is null
                ? "no exception captured"
                : $"{lastWarmUpException.GetType().Name}: {Truncate(lastWarmUpException.Message, 240)}";
            var msg = $"[silo] ERROR warmup treeId={settings.TreeId} ABORTED after {warmUpAttempt} attempt(s) elapsedMs={warmUpSw.Elapsed.TotalMilliseconds:F0}: {detail}.";
            Console.WriteLine(msg);
            throw new InvalidOperationException(msg, lastWarmUpException);
        }

        // Cross-tree warm-up: the cross-tree atomic-write modes commit across
        // a sibling "{treeId}-b" tree as well as the primary tree. Warm that
        // second tree's shard roots before the listener opens so the first
        // cross-tree saga does not pay the second tree's placement-directory +
        // grain-storage first-touch storm against the measurement window
        // (same rationale as the primary-tree warm-up above). Best-effort: a
        // warm-up blip on the sibling tree logs but does not abort the run, so
        // a transient directory race cannot wedge the bench at startup.
        if (settings.WorkloadMode is BenchWorkloadMode.CrossTreeAtomic2 or BenchWorkloadMode.CrossTreeAtomic64)
        {
            var secondTreeId = settings.TreeId + "-b";
            try
            {
                var secondTree = grainFactory.GetGrain<ILattice>(secondTreeId);
                await secondTree.WarmUpAsync(stoppingToken).ConfigureAwait(false);
                Console.WriteLine($"[silo] warmup treeId={secondTreeId} (cross-tree sibling) complete");
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                Console.WriteLine($"[silo] warmup treeId={secondTreeId} (cross-tree sibling) degraded: {ex.GetType().Name}: {Truncate(ex.Message, 160)} (continuing; first cross-tree saga may stall on cold-shard activation)");
            }
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
        //
        // Write modes (SetPoint, SetMany, SetManyAtomic) deliberately do
        // NOT pre-seed: seeding the very keys the writes target would
        // convert the benchmark from "write keys to a tree" into "update
        // existing keys", which is a different latency profile. Tree
        // warm-up (proactive shard-root activation, library-level grain
        // cache population) happens for every mode via the
        // `lattice.WarmUpAsync` call earlier in startup; only the
        // tree-content pre-seed is gated on the read modes.
        var preseedEnabled = settings.PreseedKeyCount > 0
            && (settings.WorkloadMode == BenchWorkloadMode.GetPoint
                || settings.WorkloadMode == BenchWorkloadMode.GetMany);
        if (preseedEnabled)
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
        // Per-line back-pressure response uses the canonical library
        // helper IWalSaturationSignal.ApplyBackPressureAsync (no-op
        // on Healthy, honest per-line delay on Throttled, full
        // park-on-Saturated). The Throttled per-line delay is
        // tunable via BENCH_THROTTLED_LINE_DELAY_MICROS so operators
        // can dial back-pressure strength without recompiling; the
        // default (1 ms, matching the library helper's default)
        // slows a 10 k events/sec offered stream to ~1 k events/sec
        // during Throttled, the TCP receive buffer fills, the
        // producer's socket.SendAsync blocks, and the writer's
        // admission gate drains before the regime escalates to
        // Saturated. The original F-086 design (one Task.Yield per
        // line) was too soft - the reader still drained the socket
        // at near-full speed during Throttled, and operationally
        // surfaced as the 409-Conflict burst when the in-flight
        // saga count crossed the Azure-Tables single-account
        // ceiling because the bench kept reading at producer-rate
        // during the brief Throttled windows between Saturated
        // transitions.
        var throttledLineDelayMicrosRaw = Environment.GetEnvironmentVariable("BENCH_THROTTLED_LINE_DELAY_MICROS");
        var throttledLineDelayMicros = int.TryParse(throttledLineDelayMicrosRaw, out var v) && v >= 0
            ? v
            : (int)WalSaturationSignalExtensions.DefaultThrottledDelay.TotalMicroseconds;
        var throttledLineDelay = throttledLineDelayMicros > 0
            ? TimeSpan.FromMicroseconds(throttledLineDelayMicros)
            : TimeSpan.Zero;
        try
        {
            using (client)
            await using (var stream = client.GetStream())
            using (var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, bufferSize: 64 * 1024))
            {
                string? line;
                while (true)
                {
                    // F-086 adoption: gate the TCP-read loop on the
                    // per-tree saturation signal from F-085 using the
                    // canonical IWalSaturationSignal.ApplyBackPressureAsync
                    // helper. The helper translates the three-state
                    // signal into the right per-call action:
                    //
                    // - Healthy: no-op (synchronous fast path, one
                    //   ConcurrentDictionary lookup).
                    // - Throttled: per-call delay so the producer
                    //   observes a measurable slowdown that gives
                    //   the silo's writer admission gate time to
                    //   drain before the regime escalates to
                    //   Saturated.
                    // - Saturated: park the reader by awaiting
                    //   WaitForHealthyAsync - the kernel's per-
                    //   connection receive buffer fills, the TCP
                    //   window shrinks to zero, the producer's
                    //   socket.SendAsync blocks, and slipMaxMs rises
                    //   in the producer reporter window that overlaps.
                    //   No application-protocol back-pressure: the
                    //   kernel TCP window does all the work, so this
                    //   same pattern applies unchanged to any
                    //   TCP-fronted ingest path.
                    await saturationSignal.ApplyBackPressureAsync(settings.TreeId, throttledLineDelay, ct).ConfigureAwait(false);

                    line = await reader.ReadLineAsync(ct);
                    if (line is null) break;
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

                    // FX-033 Gap 3: flow-control-fence the channel.
                    // The per-line ApplyBackPressureAsync above gates
                    // the READ side of the loop, but a Healthy -> Saturated
                    // transition between sample ticks (default 200 ms)
                    // lets the reader queue thousands of lines into the
                    // bounded channel before the next sample tick
                    // observes Saturated and the reader parks. Pre-FX-033
                    // those queued lines drained into SetManyAsync
                    // against a still-saturated storage account and
                    // surfaced as failed=N on FINAL despite the producer
                    // having stopped. Re-applying back-pressure
                    // immediately before the WriteAsync call turns the
                    // channel into a flow-control fence rather than a
                    // burst-absorber: the second call observes Saturated
                    // synchronously after the next tick and parks the
                    // reader on WaitForHealthyAsync before the line
                    // crosses into the drain pipeline. Both calls share
                    // the same per-tree dictionary lookup so the per-
                    // line overhead under Healthy is two concurrent-
                    // dictionary reads (sub-microsecond).
                    await saturationSignal.ApplyBackPressureAsync(settings.TreeId, throttledLineDelay, ct).ConfigureAwait(false);

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
        // Timestamp of the first SetManyAsync dispatch - set inside the
        // dispatch loop the first time a batch is actually accepted from
        // the channel. The FINAL line uses this (when present) to compute
        // an "active" average that excludes the idle window before any
        // producer connected; `startedAt` is retained so the elapsed
        // field continues to mean "wall-clock since the worker started"
        // for any external parser that already depended on it.
        long firstAcceptedAt = 0;
        long writtenTotal = 0;
        long writtenSinceReport = 0;
        long failedTotal = 0;
        long failedSinceReport = 0;
        long inFlight = 0;
        // FX-029: count entries that were discarded from the channel
        // because the producer-stop boundary coincided with a Saturated
        // signal regime. These are neither `written` nor `failed` -
        // they were never dispatched to ILattice in the first place;
        // the bench deliberately abandons them rather than feeding them
        // into the in-flight queue against a residually back-pressured
        // storage account where they would trip
        // `WalAppendDispatchTimeout` 30 s later and surface as failed=N
        // on FINAL. The trade-off is documented in the FX-029 issue
        // body: a benchmark whose entire point is measuring steady-
        // state throughput correctly drops the post-producer backlog
        // when the silo is saturated.
        long discardedTotal = 0;

        // set-point-mv only: open a read handle over the asynchronous
        // materialised view attached to this tree at startup, so the reporter
        // can surface the view's apply lag (source WAL entries committed but not
        // yet applied to the view) on each progress line and at FINAL. The handle
        // resolves the maintainer by view name; GetLagAsync is a cheap
        // checkpoint-vs-head read that runs off the foreground write path, so
        // sampling it never perturbs the point-write numbers the set-point-mv
        // cohort exists to compare against the plain set-point cohort. A bounded
        // non-zero lag while writes flow, draining to zero after they stop, is
        // the operator-visible evidence that the view is maintained
        // asynchronously without taxing the primary tree.
        ILatticeView? mvView = null;
        if (settings.WorkloadMode == BenchWorkloadMode.SetPointMv)
        {
            var viewFactory = services.GetService<ILatticeViewFactory>();
            if (viewFactory is not null)
            {
                mvView = viewFactory.Create(
                    lattice,
                    "bench",
                    new LatticeViewDefinition("bench", new PredicateLatticeViewProjection()));
                Console.WriteLine($"[silo] mv: materialised view 'bench' (view-bench) attached to treeId={settings.TreeId}; apply lag reported as mvLag=N on each progress line and at MV-FINAL.");
            }
            else
            {
                Console.WriteLine("[silo] mv: WARNING workload=set-point-mv but no ILatticeViewFactory is registered; mvLag will not be reported.");
            }
        }

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
                Interlocked.Exchange(ref firstAcceptedAt, Stopwatch.GetTimestamp());
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
                    var mvLagTag = string.Empty;
                    if (mvView is not null)
                    {
                        try
                        {
                            // Bound the grain call so a momentarily-busy
                            // maintainer cannot stall the progress cadence; the
                            // lag is a best-effort observability signal, not a
                            // measured quantity on the hot path.
                            var lag = await mvView.GetLagAsync(reporterCts.Token)
                                .WaitAsync(TimeSpan.FromSeconds(2), reporterCts.Token)
                                .ConfigureAwait(false);
                            mvLagTag = $" mvLag={lag,8:N0}";
                        }
                        catch (OperationCanceledException) when (reporterCts.IsCancellationRequested) { throw; }
                        catch (Exception ex) { mvLagTag = $" mvLag=err({ex.GetType().Name})"; }
                    }
                    Console.WriteLine($"[silo] t={elapsed,7:0.0}s ops={totalNow,12:N0} ops/sec={rate,10:N0} inFlight={inFlightNow,3}{failedTag}{mvLagTag}");
                    lastReport = now;
                }
            }
            catch (OperationCanceledException) { }
        }, CancellationToken.None);

        // Stall watchdog: when the WAL write pipeline wedges (writtenTotal
        // frozen while inFlight stays non-zero, or while a sustained
        // provider-failure stream accumulates without inFlight
        // advancement), self-snapshot with ClrMD and print the parked async
        // state-machine chain + thread stacks to stdout - the in-process
        // equivalent of `dumpasync` / `dotnet-stack`, exfiltrated through
        // the systemd-journald-captured silo log. Three prior
        // `TimeoutException`-based fixes each fired zero times on the
        // wedge, so this captures the actually-parked await instead of
        // bounding another guessed one. The dual-arm shape (inFlight > 0
        // OR sustained failures) is the dual-arm generalisation: Shape A
        // (inFlight=N<cap parked on a saturating account) and Shape B
        // (inFlight=0 because batches faulted, but FINAL never emits)
        // both promote to a wedge now. Shares the reporter's cancellation
        // so it stops cleanly at end of run.
        var stallWatchdog = new StallWatchdog(
            writtenTotalSnapshot: () => Interlocked.Read(ref writtenTotal),
            inFlightSnapshot: () => Interlocked.Read(ref inFlight),
            failedTotalSnapshot: () => Interlocked.Read(ref failedTotal),
            // One full WAL batch (default 100 entries) of failures per
            // poll interval is the noise floor we use to promote a
            // frozen written-total to a wedge. Below that, a single
            // straggler batch failing late in the run can still be a
            // healthy tail.
            failedDeltaThreshold: 100L,
            stallWindow: TimeSpan.FromSeconds(20),
            pollInterval: TimeSpan.FromSeconds(1));
        var stallWatchdogTask = Task.Run(() => stallWatchdog.RunAsync(reporterCts.Token), CancellationToken.None);

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
                // FX-029: at the producer-stop boundary, if the silo
                // has been observed Saturated at any point within the
                // recent-saturation window, do NOT dispatch the residual
                // batch as a new SetManyAsync. Dispatching it would add
                // another batch to the in-flight queue against a
                // storage account that is back-pressured; the dispatch
                // would trip WalAppendDispatchTimeout 30 s later and
                // surface as failed=N on FINAL. The entries are
                // discarded instead - counted under `discardedTotal` so
                // the FINAL accounting is honest (neither `written` nor
                // `failed`). The previous failure mode is documented in
                // benchmark/azure-throughput/throughput.md section 33.4
                // and was the root cause of the WEDGE verdicts in 2/3
                // set-many cohorts of the F-086 closeout run.
                //
                // The recency check (vs. consulting GetCurrentState
                // directly) defends against the F-085 classifier's known
                // Healthy<->Saturated flap (FX-030): a tree that flapped
                // Saturated within RecentSaturationWindow is treated as
                // still-saturated for the drain decision even if the
                // current sampler tick reads Healthy. The window is
                // sized to the WalAppendDispatchTimeout the in-flight
                // batches sit on so a recently-Saturated tree is
                // assumed to have storage-side back-pressure that
                // persists at least that long.
                //
                // The check is intentionally narrow: only the FINAL
                // residual batch at producer-stop is guarded. In-flight
                // batches already dispatched through DispatchFlushAsync
                // are allowed to settle through the existing
                // Task.WhenAll(outstanding) path below; if they trip
                // WalAppendDispatchTimeout they still count as failed
                // (bounded by FlushConcurrency, so worst-case 8 batches
                // = 32k entries, vs. the unbounded post-stop channel
                // backlog that was the dominant contributor).
                var lastSat = saturationLogger.LastSaturatedUtc(settings.TreeId);
                var recentlySaturated = lastSat.HasValue
                    && (DateTimeOffset.UtcNow - lastSat.Value) < RecentSaturationWindow;
                if (recentlySaturated)
                {
                    Interlocked.Add(ref discardedTotal, ready.Count);
                    Console.WriteLine($"[silo:ingest] FX-029 abandon residual batch entries={ready.Count} (last Saturated at {lastSat:O})");
                }
                else
                {
                    var flushTask = await DispatchFlushAsync(ready);
                    TrackFlush(flushTask);
                }
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

        // FX-032 Symptom 2: in-flight-tail quiesce. The FX-029 gate
        // above guards only the residual ingest-channel batch (the
        // last batch the producer assembled but had not yet
        // dispatched). Batches that DispatchFlushAsync already
        // accepted into `flushTasks` before the producer-stop are
        // still racing the storage account at this point; under the
        // single-account 409-Conflict regime they sit parked on the
        // writer-side admission cap for up to ~30 s (WalAppendDispatchTimeout)
        // and surface as failed=N on FINAL even though the producer
        // exited cleanly. Mirror the FX-029 recency check here:
        // when the silo was Saturated within RecentSaturationWindow,
        // park on IWalSaturationSignal.WaitForHealthyAsync (bounded
        // by InFlightTailQuiesceBudget so the wait cannot consume the
        // systemd stop window before FINAL is emitted - see FX-038) so
        // the in-flight tail
        // gets a chance to settle against a recovered storage account
        // instead of bleeding through the deadline. The wait short-
        // circuits on signal recovery, on the budget timeout, or on
        // cancellation - on every exit path the existing
        // Task.WhenAll(outstanding) below releases the tail. This
        // gate is best-effort accounting (the failed=N count is
        // smaller when the storage account cools off in time) and
        // not a correctness guarantee.
        var lastSatTail = saturationLogger.LastSaturatedUtc(settings.TreeId);
        var recentlySaturatedTail = lastSatTail.HasValue
            && (DateTimeOffset.UtcNow - lastSatTail.Value) < RecentSaturationWindow;
        if (recentlySaturatedTail && outstanding.Length > 0)
        {
            Console.WriteLine($"[silo:ingest] in-flight-tail quiesce: awaiting WaitForHealthyAsync for up to {InFlightTailQuiesceBudget} before releasing {outstanding.Length} in-flight flushes (last Saturated at {lastSatTail:O}).");
            using var quiesceCts = new CancellationTokenSource(InFlightTailQuiesceBudget);
            var quiesceStartTicks = Stopwatch.GetTimestamp();
            try
            {
                await saturationSignal.WaitForHealthyAsync(settings.TreeId, quiesceCts.Token);
                var quiesceElapsed = Stopwatch.GetElapsedTime(quiesceStartTicks);
                Console.WriteLine($"[silo:ingest] in-flight-tail quiesce: signal recovered after {quiesceElapsed.TotalSeconds:0.0}s; releasing {outstanding.Length} in-flight flushes.");
            }
            catch (OperationCanceledException) when (quiesceCts.IsCancellationRequested)
            {
                Console.WriteLine($"[silo:ingest] in-flight-tail quiesce: budget {InFlightTailQuiesceBudget} expired without recovery; falling through to in-flight WhenAll (in-flight batches will settle through their dispatch deadlines).");
            }
        }

        // FX-038: bound the in-flight-tail release so a tail still parked
        // on a saturated account cannot consume the systemd stop window
        // and starve FINAL. Outstanding flushes left unsettled at the
        // deadline keep running detached and account themselves as
        // failed=N through their own dispatch deadlines; FINAL is emitted
        // immediately so the cohort reports HEALTHY-with-failures rather
        // than WEDGE.
        if (outstanding.Length > 0)
        {
            var whenAll = Task.WhenAll(outstanding);
            var completed = await Task.WhenAny(whenAll, Task.Delay(InFlightTailWhenAllBudget)).ConfigureAwait(false);
            if (completed == whenAll)
            {
                try { await whenAll; } catch { /* per-task failures already accounted for */ }
            }
            else
            {
                Console.WriteLine($"[silo:ingest] in-flight-tail release: budget {InFlightTailWhenAllBudget} expired with {outstanding.Length} flush(es) still settling; emitting FINAL now (unsettled batches account as failed=N through their dispatch deadlines).");
                _ = whenAll.ContinueWith(static t => { _ = t.Exception; }, TaskScheduler.Default);
            }
        }

        reporterCts.Cancel();
        try { await reporterTask; } catch { /* shutdown */ }
        try { await stallWatchdogTask; } catch { /* shutdown */ }
        reporterCts.Dispose();

        var endedAt = Stopwatch.GetTimestamp();
        var totalElapsed = (endedAt - startedAt) / (double)Stopwatch.Frequency;
        var opsFinal = Interlocked.Read(ref writtenTotal);
        var failedFinal = Interlocked.Read(ref failedTotal);
        var discardedFinal = Interlocked.Read(ref discardedTotal);
        // "Active" window: from first accepted batch to last drained flush.
        // Excludes the idle pre-connect window and is the most accurate
        // measure of sustained ingest throughput. Falls back to total
        // when nothing was accepted (silo started but no producer ever
        // connected).
        var firstAccept = Interlocked.Read(ref firstAcceptedAt);
        var activeElapsed = firstAccept != 0
            ? (endedAt - firstAccept) / (double)Stopwatch.Frequency
            : totalElapsed;
        var avgTotal = opsFinal / Math.Max(0.001, totalElapsed);
        var avgActive = opsFinal / Math.Max(0.001, activeElapsed);
        // FX-029: include `discarded=N` on the FINAL line so the cohort
        // runner and any external parser can attribute the at-shutdown
        // abandon-on-Saturated path independently of `failed=N` (which
        // counts genuine dispatch-deadline trips against the storage
        // account). A non-zero `discarded` count is the operational
        // signal that the producer's natural-stop window coincided with
        // a Saturated regime and the bench correctly chose to drop the
        // residual backlog. The token is suffix-appended and does not
        // affect the existing `ops=` / `failed=` regex parses in
        // run-cohort.ps1.
        // set-point-mv: a final apply-lag reading after the producer has
        // stopped and the foreground backlog has drained. With writes quiesced
        // the asynchronous view maintainer should catch up to the source head,
        // so a lag trending to zero here is the closeout evidence that the view
        // is eventually-consistent off the hot path rather than blocking it.
        // Best-effort and bounded: the host is on its way down, so a failed or
        // slow read is logged but never blocks FINAL.
        if (mvView is not null)
        {
            try
            {
                using var lagCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                var lagAtStop = await mvView.GetLagAsync(lagCts.Token).ConfigureAwait(false);
                Console.WriteLine($"[silo] MV-FINAL view=bench lagAtStop={lagAtStop:N0} (0 = the asynchronous materialised view has fully caught up to the source head)");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[silo] MV-FINAL view=bench lag read failed: {ex.GetType().Name}: {Truncate(ex.Message, 160)}");
            }
        }

        Console.WriteLine($"[silo] FINAL ops={opsFinal:N0} failed={failedFinal:N0} discarded={discardedFinal:N0} elapsed={totalElapsed:0.0}s active={activeElapsed:0.0}s ops/sec (avg)={avgTotal:N0} (active avg)={avgActive:N0}");
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

    // FX-029: time window after the most-recently observed Saturated
    // transition during which the bench's drain loop treats the silo
    // as still-saturated for the purposes of the residual-batch
    // dispatch decision. Sized to the WalAppendDispatchTimeout the
    // in-flight batches sit on so a recently-Saturated tree is
    // assumed to have storage-side back-pressure that persists at
    // least that long; a producer-stop that lands within 30 s of the
    // last Saturated transition abandons the residual batch rather
    // than dispatching it into a queue that would trip the deadline.
    // Matches LatticeOptions.DefaultWalAppendDispatchTimeout (the
    // bench inherits the library default unless an operator overrides
    // it via BENCH_WAL_APPEND_DISPATCH_TIMEOUT_SEC, in which case the
    // bench's behaviour here may slightly over- or under-shoot the
    // optimal window - acceptable for a benchmark whose entire point
    // is measuring steady-state throughput, not residual-batch
    // accounting precision).
    private static readonly TimeSpan RecentSaturationWindow = TimeSpan.FromSeconds(30);

    // FX-032 Symptom 2 / FX-038: hard ceiling on the in-flight-tail
    // quiesce wait at drain entry. After abandoning the residual
    // ingest-channel batch (FX-029) and before releasing the in-flight
    // tail via the bounded WhenAll below, the drain awaits
    // IWalSaturationSignal.WaitForHealthyAsync for at most this
    // duration when the silo was recently Saturated.
    //
    // FX-038: the binding constraint on this budget is the bench host's
    // systemd stop deadline (`lattice-silo.service` TimeoutStopSec=30s
    // and the host ShutdownTimeout, default 30s) - NOT the in-process
    // LatticeOptions.WalDrainBudget (75s). On SIGTERM the systemd unit
    // SIGKILLs the dotnet process 30s later regardless of WalDrainBudget,
    // so the FINAL line must be emitted well inside that 30s window.
    // The prior 30-second quiesce budget was sized against WalDrainBudget
    // and so was equal to TimeoutStopSec: when the tree was still
    // Saturated at the producer-stop boundary (the normal case for the
    // slow set-many-atomic saga path), WaitForHealthyAsync burned the
    // entire stop window and the process was SIGKILL'd before FINAL was
    // ever written - the WEDGE phenotype in 2/3 set-many-atomic cohorts
    // of the F-086 closeout run.
    //
    // The 10-second cap here, paired with the InFlightTailWhenAllBudget
    // bound on the subsequent Task.WhenAll, keeps the worst-case post-
    // stop drain (10s quiesce + 12s WhenAll + reporter shutdown + FINAL
    // write) comfortably under the 30s SIGKILL deadline, so FINAL always
    // emits. The wait short-circuits the moment the signal observes
    // recovery, so the common case (the account cools off within a few
    // seconds of the producer stop) returns well under the cap. On an
    // unrecovered tree the wait times out and the drain falls through to
    // the bounded WhenAll - the in-flight tail still settles, accounted
    // as failed=N through the normal dispatch-deadline path. A FINAL with
    // failed=N is strictly more useful than a WEDGE (no data): this gate
    // is best-effort accounting, not a correctness guarantee.
    private static readonly TimeSpan InFlightTailQuiesceBudget = TimeSpan.FromSeconds(10);

    // FX-038: hard ceiling on the post-quiesce Task.WhenAll(outstanding)
    // so an in-flight tail that stays parked on a still-saturated storage
    // account (each in-flight flush can sit on the writer-side admission
    // cap for up to WalAppendDispatchTimeout, default 30s) cannot itself
    // consume the systemd stop window and starve FINAL emission. When the
    // budget expires the outstanding flushes are left to settle/abandon
    // through their own dispatch deadlines and already account themselves
    // as failed=N; FINAL is emitted immediately so the cohort is reported
    // as HEALTHY-with-failures rather than wedged. Sized together with
    // InFlightTailQuiesceBudget to leave margin below TimeoutStopSec=30s.
    private static readonly TimeSpan InFlightTailWhenAllBudget = TimeSpan.FromSeconds(12);

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
                    ct,
                    grainFactory,
                    settings.TreeId).ConfigureAwait(false);
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
                // Surface the most common saturation-vs-bug class explicitly. A bare
                // TimeoutException out of Orleans means the SILO's own grain RPC deadline
                // (Silo+Client `ResponseTimeout`) fired before SetManyAsync returned. This
                // is NOT a wedge - it's the bench harness's outer call hitting its 30s
                // (default) ceiling because the configured offered rate exceeds the
                // sustainable Tables drain rate at this rung. The G-026 writer admission
                // cap is doing exactly what it's designed to do (queueing); the deadline
                // just happens to be shorter than the realistic worst-case admission
                // wait. Knobs: raise BENCH_RESPONSE_TIMEOUT_SEC, reduce BENCH_TICK_HZ or
                // BENCH_VEHICLE_COUNT, raise BENCH_WAL_PARTITIONS / WalMaxPendingBatches.
                if (ex is TimeoutException)
                {
                    logger.LogWarning(
                        "[silo] grain-rpc-deadline: SetManyAsync of {Count} did not return within ResponseTimeout " +
                        "(BENCH_RESPONSE_TIMEOUT_SEC={ResponseTimeoutSec}s). Offered rate exceeds sustained Tables " +
                        "drain rate at this rung; raise BENCH_RESPONSE_TIMEOUT_SEC, drop tickHz/vehicles, or tune WAL " +
                        "fan-out (BENCH_WAL_PARTITIONS / BENCH_WAL_MAX_PENDING_BATCHES). mode={Mode}",
                        batch.Count, settings.ResponseTimeoutSec, BenchWorkloadMetadata.FormatWorkloadMode(settings.WorkloadMode));
                }
                else
                {
                    logger.LogWarning(ex, "[silo] flush of {Count} failed (mode={Mode})", batch.Count, BenchWorkloadMetadata.FormatWorkloadMode(settings.WorkloadMode));
                }
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
        // The library surfaces shutdown-refused failures (from the
        // saga coordinator AND from direct SetAsync / SetManyAsync
        // calls that race the writer drain) as the typed public
        // LatticeShuttingDownException. The bench treats this as
        // ShutdownDiscarded so the residual at-shutdown failures
        // attribute to discarded=N rather than failed=N on FINAL,
        // mirroring the existing residual-channel-abandon contract.
        // This gate runs only when ApplicationStopping is requested,
        // so it cannot mask a steady-state error.
        if (ex is LatticeShuttingDownException)
        {
            return true;
        }

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
        BenchWorkloadMode.SetManyAtomic2 => "set-many-atomic-2",
        BenchWorkloadMode.CrossTreeAtomic2 => "cross-tree-atomic-2",
        BenchWorkloadMode.CrossTreeAtomic64 => "cross-tree-atomic-64",
        BenchWorkloadMode.SetPoint => "set-point",
        BenchWorkloadMode.SetPointMv => "set-point-mv",
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
    /// <param name="grainFactory">Grain factory used by the cross-tree
    /// modes (<see cref="BenchWorkloadMode.CrossTreeAtomic2"/>,
    /// <see cref="BenchWorkloadMode.CrossTreeAtomic64"/>) to open the
    /// cross-tree atomic-write builder; ignored by the single-tree
    /// modes.</param>
    /// <param name="treeId">Primary tree id; the cross-tree modes derive the
    /// sibling tree id (<c>{treeId}-b</c>) from it and split each saga's keys
    /// across the two trees. Ignored by the single-tree modes.</param>
    public static async Task<int> DispatchAsync(
        BenchWorkloadMode mode,
        ILattice lattice,
        List<KeyValuePair<string, byte[]>> batch,
        int atomicBatchSize,
        int parallelism,
        CancellationToken ct,
        IGrainFactory? grainFactory = null,
        string? treeId = null)
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

            case BenchWorkloadMode.SetManyAtomic2:
                {
                    // Fixed 2-key single-tree sagas (the single-tree
                    // counterpart to CrossTreeAtomic2 at a matched batch
                    // size). Pinned to 2 keys regardless of atomicBatchSize.
                    var i = 0;
                    while (i < batch.Count)
                    {
                        var len = Math.Min(2, batch.Count - i);
                        var slice = batch.GetRange(i, len);
                        await lattice.SetManyAtomicAsync(slice, ct).ConfigureAwait(false);
                        i += len;
                    }
                    return batch.Count;
                }

            case BenchWorkloadMode.CrossTreeAtomic2:
                await DispatchCrossTreeAsync(grainFactory, treeId, batch, keysPerSaga: 2, ct).ConfigureAwait(false);
                return batch.Count;

            case BenchWorkloadMode.CrossTreeAtomic64:
                await DispatchCrossTreeAsync(grainFactory, treeId, batch, keysPerSaga: 64, ct).ConfigureAwait(false);
                return batch.Count;

            // set-point-mv shares the identical foreground write path as
            // set-point (one SetAsync per key); the only difference is the
            // silo-side materialised view attached at startup, which the
            // maintainer derives asynchronously off this hot path. Keeping the
            // dispatch identical is what makes the two cohorts a clean A/B.
            case BenchWorkloadMode.SetPoint:
            case BenchWorkloadMode.SetPointMv:
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

    /// <summary>
    /// Commits the producer <paramref name="batch"/> as a sequence of
    /// cross-tree atomic writes, each spanning two trees
    /// (<paramref name="treeId"/> and <c>{treeId}-b</c>) and committed
    /// all-or-nothing through
    /// <see cref="LatticeCrossTreeAtomicWriteExtensions.BeginAtomicWrite(IGrainFactory, string)"/>.
    /// The batch is sliced into <paramref name="keysPerSaga"/>-key sagas; within
    /// each saga the first half of the keys target the primary tree and the
    /// second half target the sibling <c>-b</c> tree, so a 2-key saga writes
    /// 1 key per tree and a 64-key saga writes 32 keys per tree. Each saga mints
    /// a fresh operationId (a stable idempotency key is mandatory for a
    /// multi-registry cross-tree saga). Bounded by the outer FlushConcurrency
    /// gate the caller already holds.
    /// </summary>
    private static async Task DispatchCrossTreeAsync(
        IGrainFactory? grainFactory,
        string? treeId,
        List<KeyValuePair<string, byte[]>> batch,
        int keysPerSaga,
        CancellationToken ct)
    {
        if (grainFactory is null || string.IsNullOrEmpty(treeId))
        {
            throw new InvalidOperationException(
                "Cross-tree workload modes require an IGrainFactory and a tree id; "
                + "wire them through BenchWorkloadDispatcher.DispatchAsync.");
        }

        var secondTreeId = treeId + "-b";
        var i = 0;
        while (i < batch.Count)
        {
            var len = Math.Min(keysPerSaga, batch.Count - i);
            // Split the saga's keys evenly across the two trees: the first
            // ceil(len/2) keys go to the primary tree, the rest to the
            // sibling -b tree. A trailing odd-length tail saga puts its lone
            // last key on the primary tree (a single-tree cross-tree commit
            // is valid); steady-state batches divide evenly.
            var half = (len + 1) / 2;
            var operationId = Guid.NewGuid().ToString("N");
            var builder = grainFactory.BeginAtomicWrite(operationId).ForTree(treeId);
            for (var j = 0; j < len; j++)
            {
                if (j == half)
                {
                    builder.ForTree(secondTreeId);
                }
                var entry = batch[i + j];
                builder.Set(entry.Key, entry.Value);
            }
            await builder.CommitAsync(ct).ConfigureAwait(false);
            i += len;
        }
    }
}
