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
//   BENCH_SET_MANY_FANOUT_BUDGET_SEC
//                           Seconds LatticeGrain.SetManyAsync may spend awaiting its
//                           per-shard fan-out before refusing the call with
//                           LatticeSaturatedException (SetManyFanOut). Defaults to 30,
//                           which deliberately does NOT track the library default of
//                           Timeout.InfiniteTimeSpan: an unbounded fan-out is the
//                           #3348 collapse itself, so the rig opts in to the finite
//                           budget to exercise the seam. Set 0 for infinite.
//   BENCH_WAL_ADMISSION_CALL_BUDGET_SEC
//                           Seconds one top-level call may spend waiting at the WAL
//                           admission saturation gate, summed across every append and
//                           every retry layer (WalAdmissionSaturationCallBudget).
//                           Defaults to 15, which likewise does NOT track the library
//                           default of Timeout.InfiniteTimeSpan: left infinite the
//                           three nested retry layers each open a fresh per-append
//                           budget, which is the #3348 multiplication. Set 0 for
//                           infinite.
//   BENCH_WAL_APPEND_COALESCING_IN_FLIGHT_THRESHOLD
//                           In-flight flush depth at or above which an arriving
//                           batch's final entry stops kicking its own flush, so
//                           small fanned-out slices accumulate into the next
//                           flush window instead of each paying a round trip
//                           (defaults to
//                           LatticeOptions.DefaultWalAppendCoalescingInFlightThreshold
//                           so the harness tracks the shipping default). 0
//                           disables coalescing and restores the historical
//                           unconditional kick - use it as the control arm of a
//                           sweep. The shipping default was chosen on the
//                           fan-out arithmetic (#3396) and needs measuring.
//   BENCH_WAL_BATCHED_SINGLE_ENTRY_APPENDS
//                           Routes a bulk WAL append carrying exactly one entry
//                           through the interleaving batched grain method rather
//                           than the exclusive-turn singular overload (defaults
//                           to LatticeOptions.DefaultWalBatchedSingleEntryAppends,
//                           i.e. false / historical behaviour). Under a wide
//                           fan-out the per-leaf slice is one entry, so the
//                           exclusive turn holds the partition for a whole
//                           provider round trip and pins batch occupancy at 1,
//                           which also makes the coalescing threshold above
//                           unreachable. Set to 1 for the fix arm of the #3408
//                           A/B; leave unset for the control arm.
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
using System.Net.NetworkInformation;
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
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Storage.AzureTable;
using Orleans.Serialization;
using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.AzureThroughput.Silo;
using VehicleFleetSimulator.AzureThroughput.Engine;
using static VehicleFleetSimulator.AzureThroughput.Engine.BenchExceptionHelpers;

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
var walAppendCoalescing = ReadInt("BENCH_WAL_APPEND_COALESCING_IN_FLIGHT_THRESHOLD", LatticeOptions.DefaultWalAppendCoalescingInFlightThreshold);
var walBatchedSingleEntryAppends = ReadBool("BENCH_WAL_BATCHED_SINGLE_ENTRY_APPENDS", LatticeOptions.DefaultWalBatchedSingleEntryAppends);
// BENCH_WAL_REPLAY_QUEUE_DEPTH: the multi-silo (Layer 3) cold start is exactly
// the shape the replay admission gate is sized to refuse, and refusing it here
// is a measurement artefact rather than a finding.
//
// The gate's bound is queueDepthPerPermit x ceiling, where the ceiling is a
// process-wide static derived from this silo's own CPU grant. An ACA silo gets
// 4 vCPU, so the ceiling is 4 permits and the default depth of 4 admits 16
// concurrent replaying activations (12 for a Bulk caller, since the last
// ceiling's worth of slots is reserved for Interactive work). A Layer 3 cohort
// opens against a cold 64-shard tree, so the warm-up needs every shard root
// live at once: 64 activations on one silo, or 32 each across two. Both exceed
// the bound, the warm-up is refused with LatticeSaturatedException, and the
// cohort dies before the measurement window opens.
//
// The reservation the gate is protecting exists to keep a foreground read from
// queueing behind an O(corpus) background walk. The bench has no foreground
// reader - the cold fan-out IS the work - so there is nothing to protect and
// the refusal buys nothing. The exception's own remediation text names this
// option, so raising it is the sanctioned response rather than a workaround.
//
// Default is the library default, so the Layer 1/2 single-silo path, which
// never sets this, reproduces out-of-the-box behaviour exactly. Only Layer 3
// raises it, and the multi-silo document records that it does.
var walReplayQueueDepth = ReadIntAllowZero(
    "BENCH_WAL_REPLAY_QUEUE_DEPTH", LatticeOptions.DefaultWalReplayPermitQueueDepthPerPermit);
// BENCH_SET_MANY_FANOUT_BUDGET_SEC: one of two knobs here that deliberately do
// NOT inherit the library default (the other is
// BENCH_WAL_ADMISSION_CALL_BUDGET_SEC below), and the deviation is the whole
// point. The
// library defaults LatticeOptions.SetManyFanOutBudget to
// Timeout.InfiniteTimeSpan so that enabling the bound is opt-in on the released
// 9.x line (see #3386). An infinite budget makes the seam this rig exists to
// measure inert: #3348 is a scatter-gather tail-amplification collapse, and an
// unbounded fan-out is precisely the behaviour that produces it. So the rig
// opts in to the recommended finite value by default, which is what "measured
// against the corrected configuration" means here. Set the env-var to 0 to
// restore the library default (infinite) and reproduce the pre-#3348 shape.
var setManyFanOutBudgetSec = ReadIntAllowZero("BENCH_SET_MANY_FANOUT_BUDGET_SEC", 30);
var setManyFanOutBudget = setManyFanOutBudgetSec <= 0
    ? Timeout.InfiniteTimeSpan
    : TimeSpan.FromSeconds(setManyFanOutBudgetSec);
// BENCH_WAL_ADMISSION_CALL_BUDGET_SEC: the second knob that deliberately does
// not inherit its library default, for exactly the reason above.
// LatticeOptions.WalAdmissionSaturationCallBudget defaults to
// Timeout.InfiniteTimeSpan so that bounding a call's total saturation back-off
// is opt-in on the released 9.x line (see #3390). Left infinite, only the
// per-append WalAdmissionSaturationWaitBudget applies, and the three nested
// retry layers each buy a fresh one - which is the multiplication #3348's own
// remedy 3 names, and which the previous cohort logs recorded directly
// ("10488ms of that was saturation back-off" against a 5s per-append budget).
// The rig therefore opts in to the recommended 3x-per-append value so the seam
// is actually exercised. Set the env-var to 0 to restore the library default
// (infinite) and reproduce the unbounded shape.
var walAdmissionCallBudgetSec = ReadIntAllowZero("BENCH_WAL_ADMISSION_CALL_BUDGET_SEC", 15);
var walAdmissionCallBudget = walAdmissionCallBudgetSec <= 0
    ? Timeout.InfiniteTimeSpan
    : TimeSpan.FromSeconds(walAdmissionCallBudgetSec);
// Multi-account WAL fan-out (experiment knobs). BENCH_WAL_EXTRA_ACCOUNT_URIS is
// a ';'-delimited list of additional storage-account table endpoints wired in
// by update.ps1 (accounts 1..N-1; account 0 is BENCH_STORAGE_URI). Each becomes
// a keyed WAL provider acct1, acct2, ... BENCH_WAL_ACCOUNTS selects how many
// accounts (including the default at index 0) the tree's WAL partitions are
// spread across before load. Clamped to the number actually provisioned, so an
// over-specified arm degrades to "use all available" rather than failing.
var walExtraAccountUris = (Environment.GetEnvironmentVariable("BENCH_WAL_EXTRA_ACCOUNT_URIS") ?? string.Empty)
    .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
var walAccountsRequested = ReadInt("BENCH_WAL_ACCOUNTS", 1);
var walAccounts = Math.Clamp(walAccountsRequested, 1, 1 + walExtraAccountUris.Length);
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
// BENCH_WAL_SATURATION_RECOVERY_RELEASE_BATCH: how many parked WAL-admission
// waiters a recovered partition admits per sampler tick (#3402). Zero is the
// documented "release every parked waiter at once" sentinel - the pre-#3402
// behaviour - so this must read through ReadIntAllowZero rather than ReadInt.
var saturationReleaseBatch = ReadIntAllowZero(
    "BENCH_WAL_SATURATION_RECOVERY_RELEASE_BATCH",
    LatticeOptions.DefaultWalSaturationRecoveryReleaseBatch);
var reportSec   = ReadInt("BENCH_REPORT_SEC", 1);
var totalDurationSec = ReadIntAllowZero("BENCH_TOTAL_DURATION_SEC", 600);
var responseTimeoutSec = ReadInt("BENCH_RESPONSE_TIMEOUT_SEC", 30);
var clusteringMode = (Environment.GetEnvironmentVariable("BENCH_CLUSTERING") ?? "localhost").Trim().ToLowerInvariant();
if (clusteringMode is not ("localhost" or "azuretable"))
{
    Console.Error.WriteLine($"[silo] FATAL: BENCH_CLUSTERING='{clusteringMode}' is invalid; expected 'localhost' or 'azuretable'.");
    Environment.Exit(2);
    return;
}
var ingestMode = (Environment.GetEnvironmentVariable("BENCH_INGEST_MODE") ?? "tcp").Trim().ToLowerInvariant();
if (ingestMode is not ("tcp" or "cluster"))
{
    Console.Error.WriteLine($"[silo] FATAL: BENCH_INGEST_MODE='{ingestMode}' is invalid; expected 'tcp' or 'cluster'.");
    Environment.Exit(2);
    return;
}
var clusteringConn = Environment.GetEnvironmentVariable("BENCH_CLUSTERING_CONNECTION_STRING");
var clusteringTableServiceUri = Environment.GetEnvironmentVariable("BENCH_CLUSTERING_TABLE_SERVICE_URI");
var clusteringTable = Environment.GetEnvironmentVariable("BENCH_CLUSTERING_TABLE") ?? "OrleansSiloInstances";
// Azure Table membership partitions by ClusterId, and Orleans only reaps defunct
// rows after ClusterMembershipOptions.DefunctSiloExpiration (7 days by default).
// A rig that reuses one ClusterId therefore carries every prior cohort's dead silo
// rows into the next cluster, which must probe them before membership settles; that
// is what stalls warm-up when a cohort starts while the previous ACA revision is
// still retiring. Rotating ClusterId per cohort hands each one an empty partition.
// ServiceId stays fixed: it keys persistence, not membership.
var clusterId = Environment.GetEnvironmentVariable("BENCH_CLUSTER_ID") ?? "azure-throughput";
var siloClusterPort = ReadInt("BENCH_SILO_CLUSTER_PORT", 11111);
var gatewayPort = ReadInt("BENCH_GATEWAY_PORT", 30000);
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
var workloadMode = BenchWorkloadMetadata.ParseWorkloadMode(Environment.GetEnvironmentVariable("BENCH_WORKLOAD_MODE"));
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
if (clusteringMode == "azuretable"
    && string.IsNullOrWhiteSpace(clusteringConn)
    && string.IsNullOrWhiteSpace(clusteringTableServiceUri))
{
    Console.Error.WriteLine("[silo] FATAL: BENCH_CLUSTERING=azuretable requires BENCH_CLUSTERING_CONNECTION_STRING or BENCH_CLUSTERING_TABLE_SERVICE_URI.");
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
Console.WriteLine($"[silo] treeId={treeId} walTable={walTable} tcpPort={tcpPort} batch={batchSize} flushMs={flushMs} flushConcurrency={flushConcurrency} walPartitions={walPartitions} walMaxPending={walMaxPending} walAppendCoalescing={walAppendCoalescing} walReplayQueueDepth={walReplayQueueDepth} shardCountOverride={shardCountOverride} pipelinePhase2={pipelinePhase2} eliminateCandidateRow={eliminateCandidateRow} phase2CoalescingMs={phaseTwoCoalescingMs} walNetworkTimeoutSec={walNetworkTimeoutSec} walPhase2CommitTimeout={walPhase2CommitTimeoutBanner} walAppendDispatchTimeout={walAppendDispatchTimeoutBanner} walFlushPreflightTimeout={walFlushPreflightTimeoutBanner} totalDurationSec={totalDurationSec} responseTimeoutSec={responseTimeoutSec} clustering={clusteringMode} ingestMode={ingestMode} siloClusterPort={siloClusterPort} gatewayPort={gatewayPort} leafStorageKind={leafStorageKind} leafStorageTable={leafStorageTable} leafStorageNumGrains={leafStorageNumGrains} workloadMode={BenchWorkloadMetadata.FormatWorkloadMode(workloadMode)} atomicBatchSize={atomicBatchSize} preseedKeyCount={preseedKeyCount} preseedWillFire={preseedWillFire} walAccounts={walAccounts} walAccountsRequested={walAccountsRequested} walExtraAccounts={walExtraAccountUris.Length}");
Console.WriteLine($"[silo] auth={(string.IsNullOrEmpty(storageConn) ? $"managed-identity {storageUri}" : "connection-string")}");
// F-086: echo the saturation knobs so the cohort log shows the exact
// values the TCP-read gating + the silo's sampler use. A "default"
// suffix on the sample interval is implicit when the env-var was not
// supplied; the actual value the silo will use is shown for clarity.
Console.WriteLine($"[silo] saturationSampleMs={saturationSampleMs} saturationThrottledRatio={saturationThrottledRatio:0.###} saturationDispatchTimeoutThreshold={saturationDispatchTimeoutThreshold} saturationReleaseBatch={(saturationReleaseBatch == 0 ? "all" : $"{saturationReleaseBatch}")} setManyFanOutBudget={(setManyFanOutBudget == Timeout.InfiniteTimeSpan ? "infinite" : $"{setManyFanOutBudget.TotalSeconds:0.##}s")} walAdmissionCallBudget={(walAdmissionCallBudget == Timeout.InfiniteTimeSpan ? "infinite" : $"{walAdmissionCallBudget.TotalSeconds:0.##}s")} walBatchedSingleEntryAppends={walBatchedSingleEntryAppends}");

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
builder.Services.AddSingleton(new IngestSettings(treeId, tcpPort, batchSize, TimeSpan.FromMilliseconds(flushMs), TimeSpan.FromSeconds(reportSec), flushConcurrency, shardCountOverride, workloadMode, atomicBatchSize, preseedKeyCount, walMaxPending, responseTimeoutSec, walPartitions, walAccounts, ingestMode));

builder.UseOrleans(silo =>
{
    silo.Configure<ClusterOptions>(o =>
    {
        o.ClusterId = clusterId;
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

    if (clusteringMode == "azuretable")
    {
        silo.UseAzureStorageClustering(o =>
        {
            o.TableName = clusteringTable;
            o.TableServiceClient = !string.IsNullOrWhiteSpace(clusteringConn)
                ? new TableServiceClient(clusteringConn)
                : new TableServiceClient(new Uri(clusteringTableServiceUri!), new DefaultAzureCredential());
        });
        var advertisedAddress = ResolveContainerIPv4Address();
        silo.ConfigureEndpoints(advertisedAddress, siloClusterPort, gatewayPort, listenOnAnyHostAddress: true);
        Console.WriteLine($"[silo] clustering=azuretable table={clusteringTable} advertisedAddress={advertisedAddress} siloPort={siloClusterPort} gatewayPort={gatewayPort}");
    }
    else
    {
        // In-memory single-silo clustering: no Azure Storage clustering table, no peer discovery.
        silo.UseLocalhostClustering();
    }

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
        // #3396: in-flight depth at or above which an arriving batch's
        // final entry stops kicking its own flush. Exposed as a bench
        // knob (defaulting to the library value) so a cohort sweep can
        // vary it without a redeploy - the default itself was chosen on
        // the fan-out arithmetic and needs measuring, not asserting.
        o.WalAppendCoalescingInFlightThreshold = walAppendCoalescing;
        // Routes a one-entry bulk append onto the interleaving batched
        // grain method instead of the exclusive-turn singular one, so
        // a wide fan-out of single-entry leaf slices stops serialising
        // the partition behind one provider round trip (#3408).
        o.WalBatchedSingleEntryAppends = walBatchedSingleEntryAppends;
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
        o.WalSaturationRecoveryReleaseBatch = saturationReleaseBatch;
        // See the BENCH_WAL_REPLAY_QUEUE_DEPTH block above. Assigned
        // unconditionally because the default IS the library default, so
        // the single-silo path is byte-for-byte unchanged.
        o.WalReplayPermitQueueDepthPerPermit = walReplayQueueDepth;
        // See the BENCH_SET_MANY_FANOUT_BUDGET_SEC block above. Unlike the
        // knobs around it this one does NOT track the library default, which
        // is Timeout.InfiniteTimeSpan; the rig opts in to the finite budget so
        // the #3348 fan-out seam is actually exercised.
        o.SetManyFanOutBudget = setManyFanOutBudget;
        // See the BENCH_WAL_ADMISSION_CALL_BUDGET_SEC block above. Also
        // deliberately off the library default (Timeout.InfiniteTimeSpan) so
        // the per-call saturation bound from #3348 remedy 3 is exercised.
        o.WalAdmissionSaturationCallBudget = walAdmissionCallBudget;
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

    // Shared per-provider WAL tuning, applied identically to the default
    // account and to every keyed multi-account provider below, so a
    // multi-account experiment arm differs from the single-account baseline
    // only in the number of backing storage accounts - never in provider
    // config.
    void ApplyCommonWalTuning(AzureTableWalStorageOptions walOptions)
    {
        walOptions.TableName = walTable;
        walOptions.PipelinePhaseTwoCommits = pipelinePhase2;
        walOptions.EliminateCandidateRowOnHotPath = eliminateCandidateRow;
        walOptions.PhaseTwoCoalescingWindow = TimeSpan.FromMilliseconds(phaseTwoCoalescingMs);
        if (walNetworkTimeoutSec > 0)
        {
            walOptions.RetryNetworkTimeout = TimeSpan.FromSeconds(walNetworkTimeoutSec);
        }
        if (walPhaseTwoCommitTimeoutSec is { } phase2TimeoutSec)
        {
            walOptions.PhaseTwoCommitTimeout = phase2TimeoutSec > 0
                ? TimeSpan.FromSeconds(phase2TimeoutSec)
                : null;
        }
        if (walConnectionReuse)
        {
            walOptions.ConfigureClientOptions = clientOptions =>
            {
                var handler = new SocketsHttpHandler
                {
                    PooledConnectionLifetime = TimeSpan.FromSeconds(walConnLifetimeSec),
                    PooledConnectionIdleTimeout = TimeSpan.FromSeconds(walConnLifetimeSec),
                    ConnectTimeout = TimeSpan.FromSeconds(15),
                    EnableMultipleHttp2Connections = false,
                };
                clientOptions.Transport = new HttpClientTransport(new HttpClient(handler));
            };
        }
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
        ApplyCommonWalTuning(o);
    });

    // Multi-account WAL fan-out. Register each additional storage account
    // (BENCH_WAL_EXTRA_ACCOUNT_URIS, semicolon-delimited) as a keyed WAL
    // provider 'acct1', 'acct2', ... so the placement-spread step in
    // TcpIngestService can route WAL partitions to them when
    // BENCH_WAL_ACCOUNTS > 1. Every account uses managed-identity auth and the
    // exact same per-provider tuning as the default account, so the only
    // variable across the multi-account experiment arms is how many backing
    // accounts the partitions are spread over. Registration is lazy - the
    // catalog only constructs a provider the first time a partition pinned to
    // its key activates - so registering every available account is free for
    // arms that use fewer.
    for (var accountIndex = 0; accountIndex < walExtraAccountUris.Length; accountIndex++)
    {
        var accountUri = walExtraAccountUris[accountIndex];
        var providerKey = $"acct{accountIndex + 1}";
        silo.AddLatticeWalStorageProvider(providerKey, sp =>
        {
            var keyedOptions = new AzureTableWalStorageOptions
            {
                ServiceUri = new Uri(accountUri),
                TokenCredential = new DefaultAzureCredential(),
            };
            ApplyCommonWalTuning(keyedOptions);
            return new AzureTableWalStorageProvider(
                Options.Create(keyedOptions),
                sp.GetRequiredService<Serializer<WalRecord>>(),
                sp.GetService<IWalSaturationSignal>(),
                sp.GetServices<ILatticeCompressor>());
        });
    }


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

static IPAddress ResolveContainerIPv4Address()
{
    foreach (var nic in NetworkInterface.GetAllNetworkInterfaces())
    {
        if (nic.OperationalStatus != OperationalStatus.Up)
        {
            continue;
        }

        var properties = nic.GetIPProperties();
        foreach (var address in properties.UnicastAddresses)
        {
            var ip = address.Address;
            if (ip.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(ip))
            {
                return ip;
            }
        }
    }

    throw new InvalidOperationException("No non-loopback IPv4 address is available for Orleans endpoint advertisement.");
}

// Throughput-capture (step 2): parse the BENCH_WORKLOAD_MODE env-var.
// Accepts case-insensitive kebab-case (set-many, set-many-atomic,
// set-point, get-point, get-many). Null/empty/unknown falls back to
// SetMany so a missing env-var preserves the legacy bench shape.
// Throughput-capture (step 2): the BENCH_WORKLOAD_MODE parser lives on
// BenchWorkloadMetadata.ParseWorkloadMode alongside its formatter, so the
// silo and the Orleans-client producer cannot drift apart on what a given
// env-var value means.

// Throughput-capture (step 2): kebab-case rendering for the startup
// echo line and any future diagnostic surfaces lives on
// BenchWorkloadMetadata.FormatWorkloadMode (a static class) so it is
// reachable from both the top-level startup section AND from the
// TcpIngestService class methods. Top-level local functions cannot
// be referenced from non-top-level types per CS8801.

internal sealed class TcpIngestService(
    IGrainFactory grainFactory,
    IngestSettings settings,
    IHostApplicationLifetime lifetime,
    IWalSaturationSignal saturationSignal,
    BenchSaturationLogger saturationLogger,
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
            // Layer 3 silos must run with BENCH_SHARD_COUNT=0. In a
            // multi-replica cluster, racing this block from every silo
            // makes the first replica grow the tree and the rest hit the
            // grow-only ArgumentOutOfRangeException path below, crashlooping
            // otherwise healthy replicas. The Orleans-client producer owns
            // the one-shot reshard in that topology.
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
                catch (Exception ex) when (IsOrleansMessageRejection(ex)
                    || WarmUpRetryClassifier.IsTransientPlacementConvergence(ex))
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
                    var kind = IsOrleansMessageRejection(ex) ? "REJECTED" : "PLACEMENT-CONVERGING";
                    Console.WriteLine($"[silo] reshard treeId={settings.TreeId} attempt={attempt} {kind} ({ex.GetType().Name}: {Truncate(ex.Message, 160)}); backing off {backoffMs}ms before retry");
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
        // We retry two transient classes here: Orleans message rejections
        // (directory-cache race) and transient activation cancellations -
        // a shard warm-up probe that activates a grain whose state read
        // times out against a transiently-throttled storage account
        // surfaces as a bare TaskCanceledException. Without this, that
        // cancellation escaped the loop and (because the silo runs with
        // BackgroundServiceExceptionBehavior=StopHost) killed the host,
        // producing a spurious WEDGE on pinned re-runs (issue #821). The
        // genuine host-shutdown cancellation is still honoured immediately
        // via the stopping-token guard below.
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
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { throw; }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested
                && (IsOrleansMessageRejection(ex)
                    || WarmUpRetryClassifier.IsTransientActivationCancellation(ex)
                    || WarmUpRetryClassifier.IsTransientPlacementConvergence(ex)))
            {
                lastWarmUpException = ex;
                var backoffMs = Math.Min(100 * (1 << (warmUpAttempt - 1)), MaxWarmUpBackoffMs);
                var kind = IsOrleansMessageRejection(ex) ? "REJECTED"
                    : WarmUpRetryClassifier.IsTransientPlacementConvergence(ex) ? "PLACEMENT-CONVERGING"
                    : "TRANSIENT-CANCEL";
                Console.WriteLine($"[silo] warmup treeId={settings.TreeId} attempt={warmUpAttempt} {kind} ({ex.GetType().Name}: {Truncate(ex.Message, 160)}); backing off {backoffMs}ms before retry");
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

        if (settings.IngestMode == "cluster")
        {
            Console.WriteLine("[silo] ingest mode=cluster; TCP listener disabled, producer drives the cluster as an Orleans client");
            return;
        }

        // Multi-account WAL placement spread (experiment independent variable).
        // When BENCH_WAL_ACCOUNTS > 1, redistribute the tree's WAL partitions
        // across the keyed providers acct1..acct{N-1} - partition p routes to
        // acct(p % N), with p % N == 0 staying on the default account - before
        // the TCP listener opens. The tree is freshly warmed and empty here, so
        // each move is a cheap placement-pin flip with no retained tail to copy.
        // Arms differ only in how many accounts back the same WAL partitions.
        if (settings.WalAccounts > 1)
        {
            // Source of truth: Orleans.Lattice LatticeConstants.AdminGrainKey
            // (internal to the library; hardcoded here because the bench silo
            // assembly has no InternalsVisibleTo grant).
            const string adminGrainKey = "_lattice_admin";
            var admin = grainFactory.GetGrain<ILatticeAdmin>(adminGrainKey);
            var moves = new List<(int Partition, string TargetProviderKey)>();
            for (var p = 0; p < settings.WalPartitions; p++)
            {
                var accountIndex = p % settings.WalAccounts;
                if (accountIndex != 0)
                {
                    moves.Add((p, $"acct{accountIndex}"));
                }
            }
            if (moves.Count > 0)
            {
                try
                {
                    await admin.ExecuteWalMoveAsync(settings.TreeId, moves, null, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) { throw; }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"[silo] ERROR wal-placement-spread treeId={settings.TreeId} accounts={settings.WalAccounts} partitions={settings.WalPartitions} moves={moves.Count} FAILED: {ex.GetType().Name}: {Truncate(ex.Message, 240)}");
                    throw;
                }
            }
            try
            {
                var placement = await admin.GetWalPlacementAsync(settings.TreeId, stoppingToken).ConfigureAwait(false);
                var summary = string.Join(",", placement.Partitions.Select(pp => $"{pp.Partition}:{pp.ProviderKey}"));
                Console.WriteLine($"[silo] wal-placement treeId={settings.TreeId} accounts={settings.WalAccounts} partitions={settings.WalPartitions} version={placement.Version} -> {summary}");
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                Console.WriteLine($"[silo] WARN wal-placement read treeId={settings.TreeId} failed: {ex.GetType().Name}: {Truncate(ex.Message, 160)}");
            }
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

        // The drain, flush, dispatch and reporting engine lives in the
        // shared Engine assembly so the multi-silo rig's Orleans-client
        // producer runs the identical code against IClusterClient. Here
        // the silo supplies its own in-process saturation observations;
        // a client host supplies NoOpBenchSaturationGate instead.
        var engine = new BenchIngestEngine(
            grainFactory,
            settings,
            lifetime,
            new SiloBenchSaturationGate(saturationSignal, saturationLogger),
            logger);

        var drainTask = Task.Run(() => engine.DrainAsync(lattice, channel.Reader, stoppingToken), CancellationToken.None);

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

}
