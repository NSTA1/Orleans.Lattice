namespace Orleans.Lattice;

/// <summary>
/// Configuration options for a Lattice tree. Register a named instance to
/// override settings for a specific tree:
/// <code>
/// siloBuilder.Services.Configure&lt;LatticeOptions&gt;("my-tree", o => o.CacheTtl = TimeSpan.FromMilliseconds(100));
/// </code>
/// The unnamed (default) instance applies to all trees that do not have a
/// named override.
/// <para>
/// Structural sizing - the number of keys per leaf, the number of children
/// per internal node, and the shard count - is <em>not</em> configured here.
/// Those values are pinned in the internal tree registry at first-use
/// (seeded from the canonical defaults in
/// <see cref="Orleans.Lattice.BPlusTree.LatticeConstants"/>) and are mutable
/// only through <see cref="ILattice.ResizeAsync"/> (leaf / internal capacity)
/// or <see cref="ILattice.ReshardAsync"/> (shard count). This prevents
/// accidental divergence between the layout a tree was built with and a
/// later configuration change.
/// </para>
/// <para>
/// The virtual shard space is also not configurable here. It is a compile-time
/// constant,
/// <see cref="Orleans.Lattice.BPlusTree.LatticeConstants.DefaultVirtualShardCount"/>
/// (4096), because persisted shard maps reference virtual slots by integer
/// index and changing the constant would invalidate every stored map.
/// </para>
/// </summary>
public class LatticeOptions
{
    /// <summary>Number of keys per page returned by <see cref="IShardRootGrain.GetSortedKeysBatchAsync"/>.</summary>
    public int KeysPageSize { get; set; } = DefaultKeysPageSize;

    /// <summary>
    /// How long a tombstone must exist before it is eligible for compaction.
    /// A grain reminder fires at this interval; tombstones older than this
    /// grace period are permanently removed. Set to <see cref="Timeout.InfiniteTimeSpan"/>
    /// to disable compaction entirely. Per-tree overrides follow the same
    /// named-options pattern as other properties.
    /// </summary>
    public TimeSpan TombstoneGracePeriod { get; set; } = DefaultTombstoneGracePeriod;

    /// <summary>
    /// How long a soft-deleted tree is retained before its grains are permanently
    /// purged. During this window the tree is inaccessible (reads and writes throw
    /// <see cref="InvalidOperationException"/>), but its data still exists in storage
    /// and could theoretically be recovered by clearing the <c>IsDeleted</c> flag.
    /// After the duration elapses, a grain reminder triggers a full purge that
    /// walks every shard and clears all leaf and internal node state.
    /// Set to <see cref="TimeSpan.Zero"/> for immediate purge on the next reminder tick.
    /// </summary>
    public TimeSpan SoftDeleteDuration { get; set; } = DefaultSoftDeleteDuration;

    /// <summary>
    /// Minimum time between consecutive delta refreshes from the primary leaf
    /// in the <c>LeafCacheGrain</c>. When set to <see cref="TimeSpan.Zero"/>
    /// (the default), every read triggers a delta refresh - the version-vector
    /// comparison on the primary is cheap but the RPC overhead remains. Setting
    /// a non-zero value (e.g. 100 ms) allows the cache to serve reads from its
    /// local dictionary without contacting the primary, trading freshness for
    /// lower read latency. This option can be changed freely at any time.
    /// </summary>
    public TimeSpan CacheTtl { get; set; } = DefaultCacheTtl;

    /// <summary>Default value for <see cref="SoftDeleteDuration"/> (72 hours).</summary>
    public static readonly TimeSpan DefaultSoftDeleteDuration = TimeSpan.FromHours(72);

    /// <summary>Default value for <see cref="TombstoneGracePeriod"/> (24 hours).</summary>
    public static readonly TimeSpan DefaultTombstoneGracePeriod = TimeSpan.FromHours(24);

    /// <summary>Default value for <see cref="KeysPageSize"/>.</summary>
    public const int DefaultKeysPageSize = 512;

    /// <summary>Default value for <see cref="CacheTtl"/> (zero - refresh on every read).</summary>
    public static readonly TimeSpan DefaultCacheTtl = TimeSpan.Zero;

    /// <summary>
    /// When <c>true</c>, <see cref="ILattice.KeysAsync"/> pre-fetches the next page
    /// from each shard in parallel while the current page is being consumed,
    /// hiding per-shard grain-call latency during ordered scans. Because pre-fetched
    /// pages are held in memory until consumed, callers that abort iteration early
    /// (e.g. <c>Take(n)</c>) pay for pages they never read. Disabled by default.
    /// This option can also be overridden per-call via the <c>prefetch</c> parameter
    /// on <see cref="ILattice.KeysAsync"/>.
    /// </summary>
    public bool PrefetchKeysScan { get; set; } = DefaultPrefetchKeysScan;

    /// <summary>Default value for <see cref="PrefetchKeysScan"/> (<c>false</c>).</summary>
    public const bool DefaultPrefetchKeysScan = false;

    /// <summary>
    /// When <c>true</c>, <see cref="ILattice.EntriesAsync"/> pre-fetches the next page
    /// from each shard in parallel while the current page is being consumed,
    /// hiding per-shard grain-call latency during ordered entry scans. Because
    /// entries carry <c>byte[]</c> values, pre-fetched pages increase in-flight memory
    /// proportionally to <c>shardCount × KeysPageSize × avgValueSize</c>, so this is
    /// gated separately from <see cref="PrefetchKeysScan"/>. Callers that abort
    /// iteration early (e.g. <c>Take(n)</c>) pay for pages they never read.
    /// Disabled by default. This option can also be overridden per-call via the
    /// <c>prefetch</c> parameter on <see cref="ILattice.EntriesAsync"/>.
    /// </summary>
    public bool PrefetchEntriesScan { get; set; } = DefaultPrefetchEntriesScan;

    /// <summary>Default value for <see cref="PrefetchEntriesScan"/> (<c>false</c>).</summary>
    public const bool DefaultPrefetchEntriesScan = false;

    /// <summary>
    /// When <c>true</c>, the autonomic <c>HotShardMonitorGrain</c> periodically
    /// polls each physical shard's hotness counters (<see cref="IShardRootGrain.GetHotnessAsync"/>)
    /// and triggers an online adaptive split when the observed
    /// operations-per-second exceeds <see cref="HotShardOpsPerSecondThreshold"/>.
    /// Splits happen fully online via shadow-writing - no shard is ever taken
    /// offline. Set to <c>false</c> to disable autonomic splitting entirely.
    /// </summary>
    public bool AutoSplitEnabled { get; set; } = DefaultAutoSplitEnabled;

    /// <summary>Default value for <see cref="AutoSplitEnabled"/> (<c>true</c>).</summary>
    public const bool DefaultAutoSplitEnabled = true;

    /// <summary>
    /// Operations-per-second threshold above which a shard is considered hot
    /// and eligible for an autonomic split. Computed as
    /// <c>(reads + writes) / window.TotalSeconds</c> over the period reported
    /// by <see cref="ShardHotness.Window"/>. Lower values trigger splits more
    /// aggressively; the default of 200 ops/s is intentionally low so splits
    /// occur well before throughput degrades.
    /// </summary>
    public int HotShardOpsPerSecondThreshold { get; set; } = DefaultHotShardOpsPerSecondThreshold;

    /// <summary>Default value for <see cref="HotShardOpsPerSecondThreshold"/> (200).</summary>
    public const int DefaultHotShardOpsPerSecondThreshold = 200;

    /// <summary>
    /// How often the autonomic monitor polls shard hotness counters.
    /// Shorter intervals detect hot shards faster at a small CPU cost.
    /// </summary>
    public TimeSpan HotShardSampleInterval { get; set; } = DefaultHotShardSampleInterval;

    /// <summary>Default value for <see cref="HotShardSampleInterval"/> (30 seconds).</summary>
    public static readonly TimeSpan DefaultHotShardSampleInterval = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Minimum interval between consecutive autonomic splits of the same
    /// physical shard. Prevents thrashing when a single hot virtual slot
    /// dominates traffic (the slot will be split once, then need to wait this
    /// long before the new shard can be split again).
    /// </summary>
    public TimeSpan HotShardSplitCooldown { get; set; } = DefaultHotShardSplitCooldown;

    /// <summary>Default value for <see cref="HotShardSplitCooldown"/> (2 minutes).</summary>
    public static readonly TimeSpan DefaultHotShardSplitCooldown = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Maximum number of autonomic splits that can be in flight concurrently
    /// for a single tree. The monitor refuses to start a new split while this
    /// many are already active. Defaults to 2 - splits are I/O-bounded by the
    /// drain phase but small enough that two parallel splits typically saturate
    /// neither storage nor the coordinator silo. Set to <c>1</c> for the most
    /// conservative behavior, or higher when many shards are simultaneously
    /// hot and target storage can absorb the extra drain traffic.
    /// </summary>
    public int MaxConcurrentAutoSplits { get; set; } = DefaultMaxConcurrentAutoSplits;

    /// <summary>Default value for <see cref="MaxConcurrentAutoSplits"/> (2).</summary>
    public const int DefaultMaxConcurrentAutoSplits = 2;

    /// <summary>
    /// Maximum number of parallel <see cref="ITreeShardSplitGrain"/> splits
    /// that an online reshard (<see cref="ILattice.ReshardAsync"/>) may drive
    /// concurrently. Each split drains one physical shard's upper-half
    /// virtual slots into a newly allocated target shard; running several
    /// in parallel shortens total reshard time at the cost of proportionally
    /// higher background drain I/O. Splits dispatched by the reshard
    /// coordinator operate independently of those triggered autonomically
    /// by <c>HotShardMonitorGrain</c> - the two caps compose additively.
    /// </summary>
    public int MaxConcurrentMigrations { get; set; } = DefaultMaxConcurrentMigrations;

    /// <summary>Default value for <see cref="MaxConcurrentMigrations"/> (4).</summary>
    public const int DefaultMaxConcurrentMigrations = 4;

    /// <summary>
    /// Maximum number of parallel per-shard drains that an online snapshot
    /// (<see cref="ILattice.SnapshotAsync"/> in <see cref="SnapshotMode.Online"/>)
    /// may dispatch concurrently. Each drain reads one source shard's leaf
    /// chain and bulk-loads it into the corresponding destination shard while
    /// live writes continue to mirror via the shadow-forwarding primitive.
    /// Higher values shorten total snapshot duration at the cost of
    /// proportionally higher background drain I/O and memory on the
    /// coordinator silo. The snapshot remains crash-safe and idempotent
    /// under any cap - re-running converges via CRDT LWW.
    /// </summary>
    public int MaxConcurrentDrains { get; set; } = DefaultMaxConcurrentDrains;

    /// <summary>Default value for <see cref="MaxConcurrentDrains"/> (4).</summary>
    public const int DefaultMaxConcurrentDrains = 4;

    /// <summary>
    /// Maximum number of moved-slot entries the split coordinator accumulates
    /// in a single <see cref="IShardRootGrain.MergeManyAsync"/> call to the
    /// target shard during drain. Larger values reduce per-call overhead;
    /// smaller values bound peak memory on the coordinator silo and the size
    /// of the Orleans grain message. The drain phase remains idempotent under
    /// any chunking - re-running converges via CRDT LWW.
    /// </summary>
    public int SplitDrainBatchSize { get; set; } = DefaultSplitDrainBatchSize;

    /// <summary>Default value for <see cref="SplitDrainBatchSize"/> (1024 entries).</summary>
    public const int DefaultSplitDrainBatchSize = 1024;

    /// <summary>
    /// Minimum age of a tree (since the monitor first activated) before
    /// autonomic splits are allowed. Prevents premature splits during
    /// startup bursts before the workload has stabilised.
    /// </summary>
    public TimeSpan AutoSplitMinTreeAge { get; set; } = DefaultAutoSplitMinTreeAge;

    /// <summary>Default value for <see cref="AutoSplitMinTreeAge"/> (60 seconds).</summary>
    public static readonly TimeSpan DefaultAutoSplitMinTreeAge = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Maximum number of times a strongly-consistent scan
    /// (<c>ILattice.CountAsync</c>, <c>KeysAsync</c>, <c>EntriesAsync</c>)
    /// will reconcile against newly-discovered shard-map changes before giving
    /// up. Each retry only re-fetches data for the slots that actually moved
    /// during the scan, so the cost is bounded by the number of in-flight
    /// splits, not the size of the tree. Set to <c>1</c> to disable
    /// reconciliation entirely (fall back to eventually-consistent scans).
    /// </summary>
    public int MaxScanRetries { get; set; } = DefaultMaxScanRetries;

    /// <summary>Default value for <see cref="MaxScanRetries"/> (3).</summary>
    public const int DefaultMaxScanRetries = 3;

    /// <summary>
    /// How long an open stateful cursor may remain idle before it is
    /// automatically cleaned up. On every <c>Open</c> / <c>Next</c> /
    /// <c>DeleteRangeStep</c> call, the cursor grain refreshes a grain
    /// reminder set to fire after this interval; if the reminder ever fires
    /// (no activity for the full window) the grain clears its persisted
    /// state, unregisters the reminder, and deactivates. Protects against
    /// leaked cursor state from clients that open a cursor and never call
    /// <c>CloseCursorAsync</c>. Set to <see cref="Timeout.InfiniteTimeSpan"/>
    /// to disable automatic cleanup. Minimum effective interval is
    /// <c>1 minute</c> (Orleans reminder granularity).
    /// </summary>
    public TimeSpan CursorIdleTtl { get; set; } = DefaultCursorIdleTtl;

    /// <summary>Default value for <see cref="CursorIdleTtl"/> (48 hours).</summary>
    public static readonly TimeSpan DefaultCursorIdleTtl = TimeSpan.FromHours(48);

    /// <summary>
    /// How long a completed atomic-write saga retains its persisted
    /// state for idempotent re-invocation by the client. After the retention
    /// window elapses, a grain reminder fires, the saga's state is cleared,
    /// and the coordinator grain deactivates. A client that re-issues a
    /// <c>SetManyAtomicAsync</c> call with the same <c>operationId</c>
    /// within this window will see the original result (success or the
    /// original failure exception); after the window expires, a re-issue
    /// starts a new saga. Set to <see cref="Timeout.InfiniteTimeSpan"/> to
    /// disable automatic cleanup and retain saga state indefinitely.
    /// Minimum effective interval is <c>1 minute</c> (Orleans reminder
    /// granularity).
    /// </summary>
    public TimeSpan AtomicWriteRetention { get; set; } = DefaultAtomicWriteRetention;

    /// <summary>Default value for <see cref="AtomicWriteRetention"/> (48 hours).</summary>
    public static readonly TimeSpan DefaultAtomicWriteRetention = TimeSpan.FromHours(48);

    /// <summary>
    /// How long a completed saga's commit/abort decision persists in the
    /// per-tree <see cref="Grains.TxRegistryGrain"/> as a tombstone after
    /// the saga calls <c>ForgetAsync</c>. Covers the race window where a
    /// concurrent <c>TreeShardSplitGrain.RetroactiveSweepPreparedMutationsAsync</c>
    /// installs a pending bucket on a destination shard <i>after</i> the
    /// saga's terminal fan-out (and after the late-pickup fetch-loop in
    /// <c>AtomicWriteGrain.BroadcastTerminalsAsync</c>) but <i>before</i>
    /// the saga's <c>ForgetAsync</c> call: with a non-zero retention, the
    /// sweep's post-sweep cleanup pass can still resolve the saga's
    /// outcome via <c>GetStatusAsync</c> and apply the terminal directly,
    /// draining the orphan pending bucket. Default is 60 seconds - long
    /// enough to absorb typical sweep durations while bounding the
    /// registry's persisted footprint. Set to <see cref="TimeSpan.Zero"/>
    /// to disable tombstoning entirely (legacy behaviour: <c>ForgetAsync</c>
    /// removes the decision immediately, restoring the original semantic
    /// from before the tombstone feature shipped). Increase for
    /// environments where sweep completion can exceed 60 seconds (very
    /// large shards or cascading split storms under sustained write load).
    /// </summary>
    public TimeSpan TxDecisionRetention { get; set; } = DefaultTxDecisionRetention;

    /// <summary>Default value for <see cref="TxDecisionRetention"/> (60 seconds).</summary>
    public static readonly TimeSpan DefaultTxDecisionRetention = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Hard cap on how long the per-tree <see cref="Grains.TxRegistryGrain"/>
    /// will retain a point-in-time snapshot pin recorded for a
    /// <see cref="LatticeCursorSpec.PointInTime"/> cursor. The cursor grain
    /// refreshes its pin on every step (<c>Next*Async</c> /
    /// <c>DeleteRangeStepAsync</c>); a cursor that misses a refresh window
    /// past this TTL has its pin expired by the registry's own prune pass,
    /// and the next step throws
    /// <see cref="LatticeCursorSnapshotExpiredException"/>. Acts as a
    /// defence-in-depth bound against reminder-service degradation or
    /// <c>CursorIdleTtl = Timeout.InfiniteTimeSpan</c> configurations that
    /// would otherwise leave a forgotten cursor pinning registry decisions
    /// forever. Default 7 days. The minimum effective interval is
    /// <see cref="TxDecisionRetention"/> - a pin shorter than the tombstone
    /// retention is silently floored, because the registry's own
    /// tombstone-prune pass already covers anything shorter.
    /// </summary>
    public TimeSpan MaxCursorSnapshotPinTtl { get; set; } = DefaultMaxCursorSnapshotPinTtl;

    /// <summary>Default value for <see cref="MaxCursorSnapshotPinTtl"/> (7 days).</summary>
    public static readonly TimeSpan DefaultMaxCursorSnapshotPinTtl = TimeSpan.FromDays(7);

    /// <summary>
    /// Absolute footprint cap on the union of saga decisions pinned by all
    /// active <see cref="LatticeCursorSpec.PointInTime"/> cursors against
    /// the per-tree <see cref="Grains.TxRegistryGrain"/>. A new
    /// <c>OpenAsync(PointInTime: true)</c> whose snapshot would push the
    /// total pinned-decision footprint over this cap throws
    /// <see cref="LatticeCursorRegistryPinExhaustedException"/> rather than
    /// silently degrading or growing unbounded. <c>Next*Async</c> on an
    /// already-open cursor never throws for pin-exhaustion reasons.
    /// Default 100 000 decisions.
    /// </summary>
    public int MaxPinnedSagaDecisions { get; set; } = DefaultMaxPinnedSagaDecisions;

    /// <summary>Default value for <see cref="MaxPinnedSagaDecisions"/> (100 000).</summary>
    public const int DefaultMaxPinnedSagaDecisions = 100_000;

    /// <summary>
    /// Per-shard cap on the projected WAL-replay record count consulted
    /// at <c>OpenSnapshot*Async</c> time. The open fan-out reads each
    /// touched shard's <c>GetMaterialiserLagAsync</c> and fails fast
    /// with <see cref="LatticeSnapshotReplayBudgetExceededException"/>
    /// when any shard's projected lag exceeds this cap, so a snapshot
    /// cursor cannot be opened against a tree whose materialiser is
    /// far enough behind that replay would dominate the open call.
    /// Operators tune this against the steady-state apply rate and
    /// the materialiser-checkpoint cadence
    /// (<see cref="MaterialiserCheckpointInterval"/>,
    /// <see cref="MaterialiserCheckpointEntries"/>): a healthy
    /// materialiser sits well below the cap; a sustained excursion
    /// surfaces as the open-time fail-fast.
    /// <para>
    /// The cap is the snapshot analogue of
    /// <see cref="MaxLeafReplayEntries"/>, which bounds activation-
    /// time replay on a single leaf. Default 10 000 000 records,
    /// matching the conservative-but-non-degenerate sizing typical
    /// production WAL retention windows admit at the per-shard scale.
    /// </para>
    /// </summary>
    public long MaxSnapshotReplayEntries { get; set; } = DefaultMaxSnapshotReplayEntries;

    /// <summary>Default value for <see cref="MaxSnapshotReplayEntries"/> (10 000 000).</summary>
    public const long DefaultMaxSnapshotReplayEntries = 10_000_000L;

    /// <summary>
    /// Idle-eviction window for transient per-shard snapshot leaf
    /// grains materialised by a zero-observable-writes snapshot
    /// cursor. A snapshot leaf rebuilds the shard's projection on
    /// first read by replaying the captured WAL prefix, then stays
    /// activated for further pages against the same shard. After
    /// this window elapses without activity the grain self-evicts;
    /// the next <c>Next*Async</c> transparently rebuilds it on
    /// demand (the underlying WAL prefix is held alive by the
    /// snapshot's <see cref="MaxCursorSnapshotPinTtl"/> pin, so the
    /// rebuild is always feasible until the cursor is closed or
    /// expires).
    /// <para>
    /// Decoupled from <see cref="CursorIdleTtl"/> because the
    /// snapshot-leaf state is purely a replay-cost cache, not part
    /// of the cursor's correctness boundary - evicting it earlier
    /// only trades a replay re-run for memory. Default 30 minutes.
    /// </para>
    /// </summary>
    public TimeSpan SnapshotLeafIdleTtl { get; set; } = DefaultSnapshotLeafIdleTtl;

    /// <summary>Default value for <see cref="SnapshotLeafIdleTtl"/> (30 minutes).</summary>
    public static readonly TimeSpan DefaultSnapshotLeafIdleTtl = TimeSpan.FromMinutes(30);

    /// <summary>
    /// Optional retention window for <see cref="Primitives.VersionVector"/>
    /// entries.
    /// <see cref="Primitives.VersionVector.PruneOlderThan(long)"/> with
    /// <c>UtcNow - VersionVectorRetention</c>, replica entries whose
    /// wall-clock tick falls before the cutoff are dropped to bound the
    /// vector's memory footprint.
    /// <para>
    /// Defaults to <see cref="Timeout.InfiniteTimeSpan"/> (no pruning) to
    /// preserve wire and state compatibility. Pruning must be applied
    /// consistently across replicas that merge against each other,
    /// otherwise a short-retention replica will keep reinstating entries
    /// from a long-retention peer. Values below
    /// <see cref="DefaultMinVersionVectorRetention"/> are typically
    /// unsafe on networks where clock skew exceeds the window.
    /// </para>
    /// </summary>
    public TimeSpan VersionVectorRetention { get; set; } = DefaultVersionVectorRetention;

    /// <summary>Default value for <see cref="VersionVectorRetention"/> (disabled).</summary>
    public static readonly TimeSpan DefaultVersionVectorRetention = Timeout.InfiniteTimeSpan;

    /// <summary>
    /// Practical lower bound for <see cref="VersionVectorRetention"/> below
    /// which pruning may drop entries that are still causally relevant. Not
    /// enforced - provided as a reference constant.
    /// </summary>
    public static readonly TimeSpan DefaultMinVersionVectorRetention = TimeSpan.FromHours(1);

    /// <summary>
    /// How long an assembled <see cref="TreeDiagnosticReport"/> is held in-memory
    /// by the diagnostics grain before a fresh fan-out is performed. Short
    /// windows deliver near-live data at the cost of extra shard traffic under
    /// dashboard polling; longer windows smooth out diagnostics load.
    /// Shallow and deep reports are cached independently. Set to
    /// <see cref="TimeSpan.Zero"/> to disable caching entirely (every call
    /// fans out). The cache is automatically invalidated whenever the
    /// diagnostics grain observes a new split event via
    /// <c>ILatticeStats.RecordSplitAsync</c>.
    /// </summary>
    public TimeSpan DiagnosticsCacheTtl { get; set; } = DefaultDiagnosticsCacheTtl;

    /// <summary>Default value for <see cref="DiagnosticsCacheTtl"/> (5 seconds).</summary>
    public static readonly TimeSpan DefaultDiagnosticsCacheTtl = TimeSpan.FromSeconds(5);

    /// <summary>
    /// When <c>true</c>, Lattice publishes <see cref="LatticeTreeEvent"/> notifications
    /// on an Orleans stream (namespace <see cref="LatticeEventConstants.StreamNamespace"/>)
    /// covering per-key writes, atomic-write completions, splits, compactions,
    /// snapshots, resizes, reshards, and tree-lifecycle transitions. Consumers
    /// subscribe via <c>LatticeExtensions.SubscribeToEventsAsync</c>; publication
    /// is fire-and-forget and log-and-swallow, so a missing or misconfigured
    /// provider never breaks the write path. Opt in per tree.
    /// </summary>
    public bool PublishEvents { get; set; } = DefaultPublishEvents;
    /// <summary>Default value for <see cref="PublishEvents"/> (<c>false</c>).</summary>
    public const bool DefaultPublishEvents = false;

    /// <summary>
    /// Name of the Orleans stream provider used to publish and subscribe to
    /// <see cref="LatticeTreeEvent"/>. Defaults to <c>"Default"</c>, matching
    /// the conventional name used by <c>siloBuilder.AddMemoryStreams("Default")</c>.
    /// The same name must be used on the client (subscribers) and on every
    /// silo (publishers).
    /// </summary>
    public string EventStreamProviderName { get; set; } = DefaultEventStreamProviderName;

    /// <summary>Default value for <see cref="EventStreamProviderName"/> (<c>"Default"</c>).</summary>
    public const string DefaultEventStreamProviderName = "Default";

    /// <summary>
    /// Maximum number of entries a leaf grain will replay through its
    /// projection rebuild seam (<c>ILeafProjection.Apply</c>) at activation
    /// time before falling back to a full projection rebuild from the
    /// authoritative source. Bounds the worst-case replay cost when a leaf
    /// reactivates after an extended outage and the gap between its
    /// persisted projection checkpoint and the current write-ahead-log
    /// head exceeds this budget.
    /// <para>
    /// The seam itself ships dormant - the leaf grain still writes through
    /// its existing storage provider on every commit. This budget becomes
    /// observable when the WAL-as-sole-commit-point promotion lands and
    /// the activation path begins consulting the persisted checkpoint.
    /// </para>
    /// </summary>
    public int MaxLeafReplayEntries { get; set; } = DefaultMaxLeafReplayEntries;

    /// <summary>Default value for <see cref="MaxLeafReplayEntries"/> (10 000).</summary>
    public const int DefaultMaxLeafReplayEntries = 10_000;

    /// <summary>
    /// Maximum interval between durable persistences of a leaf grain's
    /// projection-checkpoint offset (the
    /// <c>ILeafProjection.SetCheckpointOffsetAsync</c> seam introduced
    /// for the WAL-as-sole-commit-point promotion). Checkpoint advances
    /// are coalesced in memory and durably written when whichever of
    /// this interval or <see cref="MaterialiserCheckpointEntries"/>
    /// elapses first; a graceful deactivation flushes any pending
    /// advance synchronously.
    /// <para>
    /// Set to <see cref="TimeSpan.Zero"/> for an every-entry checkpoint
    /// (one extra storage write per applied mutation, but zero replay
    /// on restart) when strict RTO budgets warrant the cost.
    /// <see cref="Timeout.InfiniteTimeSpan"/> disables the time-driven
    /// flush so checkpoints persist only when
    /// <see cref="MaterialiserCheckpointEntries"/> is reached or the
    /// grain deactivates.
    /// </para>
    /// <para>
    /// Crash-recovery cost on a worst-case unflushed checkpoint is
    /// bounded by this interval times the steady-state apply rate.
    /// The seam itself ships dormant - the leaf grain still writes
    /// through its existing storage provider on every commit. This
    /// option becomes observable when the WAL-as-sole-commit-point
    /// promotion lands and the activation path begins consulting the
    /// persisted checkpoint.
    /// </para>
    /// </summary>
    public TimeSpan MaterialiserCheckpointInterval { get; set; } = DefaultMaterialiserCheckpointInterval;

    /// <summary>Default value for <see cref="MaterialiserCheckpointInterval"/> (1 second).</summary>
    public static readonly TimeSpan DefaultMaterialiserCheckpointInterval = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Maximum number of pending checkpoint advances (measured as the
    /// difference between the last requested offset and the last
    /// durably-persisted offset) before the leaf grain forces a
    /// durable persist of the projection checkpoint, even if
    /// <see cref="MaterialiserCheckpointInterval"/> has not yet
    /// elapsed. Bounds the worst-case replay cost on crash recovery
    /// when the steady-state apply rate is high enough that the
    /// time-driven flush has not fired.
    /// </summary>
    public int MaterialiserCheckpointEntries { get; set; } = DefaultMaterialiserCheckpointEntries;

    /// <summary>Default value for <see cref="MaterialiserCheckpointEntries"/> (1 000).</summary>
    public const int DefaultMaterialiserCheckpointEntries = 1_000;

    /// <summary>
    /// Maximum age beyond which a leaf grain's persisted projection
    /// checkpoint is considered stale and triggers a fall-off-log
    /// recovery on the next activation. Compared against the wall-clock
    /// age of the persisted checkpoint at activation time. Long enough
    /// that even a healthy WAL has likely been trimmed past the
    /// checkpoint, so a tail replay would fail to converge - the leaf
    /// must take the rebuild path indicated by
    /// <see cref="ProjectionRebuildPolicy"/>.
    /// <para>
    /// Set to <see cref="Timeout.InfiniteTimeSpan"/> to disable the
    /// age-based trigger; the offset-gap trigger
    /// (<see cref="MaxLeafReplayEntries"/>) and the WAL-trim trigger
    /// continue to apply.
    /// </para>
    /// </summary>
    public TimeSpan LeafProjectionRetention { get; set; } = DefaultLeafProjectionRetention;

    /// <summary>Default value for <see cref="LeafProjectionRetention"/> (7 days).</summary>
    public static readonly TimeSpan DefaultLeafProjectionRetention = TimeSpan.FromDays(7);

    /// <summary>
    /// Selects the recovery strategy a leaf grain takes when one of
    /// the fall-off-log triggers fires at activation time
    /// (WAL trimmed past checkpoint, replay budget exceeded, projection
    /// older than <see cref="LeafProjectionRetention"/>).
    /// </summary>
    public ProjectionRebuildPolicy ProjectionRebuildPolicy { get; set; } = ProjectionRebuildPolicy.SnapshotThenWal;

    /// <summary>
    /// When <c>true</c> (the default), every leaf mutation incrementally
    /// updates the per-leaf <c>ProjectionHash</c> and publishes a
    /// <c>ChildDigestSnapshot</c> upward so each internal node maintains
    /// a pre-folded <c>SubtreeProjectionHash</c> aggregate. The pre-folded
    /// aggregate is what makes
    /// <see cref="ILattice.GetLeafProjectionDigestAsync(int, CancellationToken)"/>
    /// cost <c>O(shardCount)</c> grain hops regardless of tree size, so it
    /// must be left enabled for any deployment that polls the digest as a
    /// cross-silo drift-detection signal.
    /// <para>
    /// Set to <c>false</c> to skip both halves of the maintenance work on
    /// every mutation - the per-entry XOR fold on the leaf <i>and</i> the
    /// cross-grain hop that updates each ancestor internal node's
    /// aggregate. The trade-off is that
    /// <see cref="ILattice.GetLeafProjectionDigestAsync(int, CancellationToken)"/>
    /// will throw <see cref="InvalidOperationException"/> on every call
    /// against the affected tree: the persisted aggregates are no longer
    /// the source of truth and recomputing them on demand would defeat the
    /// purpose of opting out.
    /// </para>
    /// <para>
    /// Appropriate when the deployment has no cross-silo drift-detection
    /// requirement (single-replica or single-cluster shapes), when an
    /// external mechanism reconciles state independently (e.g. comparing
    /// WAL offsets directly), or when profiling shows the per-mutation
    /// publish chain is on the critical path of an otherwise
    /// digest-indifferent workload. The leaf's existing
    /// <c>ProjectionHash</c> column is preserved verbatim across the
    /// toggle flip, but see the one-way-disable note below: in practice
    /// the value is only safe to re-engage if no writes have landed under
    /// the disabled setting.
    /// </para>
    /// <para>
    /// <b>Disabling is a one-way operation per tree.</b> The first mutation
    /// that lands while maintenance is disabled stamps an irreversible
    /// registry latch (<c>TreeRegistryEntry.ProjectionDigestPermanentlyDisabled</c>)
    /// on the tree's registry entry. Once stamped, the latch supersedes any
    /// later attempt to flip this option, the per-tree override, or the
    /// silo-wide default back to <c>true</c>: every subsequent activation
    /// resolves <see cref="MaintainProjectionDigest"/> as <c>false</c> and
    /// every <see cref="ILattice.GetLeafProjectionDigestAsync(int, CancellationToken)"/>
    /// call against the tree keeps throwing. The latch exists because the
    /// digest is an XOR-fold aggregate: any mutation accepted while
    /// maintenance is off permanently invalidates the persisted aggregate,
    /// and silently re-enabling maintenance would publish a known-stale
    /// digest as if it were authoritative. The only way to re-engage
    /// digest maintenance for a tree that has been latched off is to
    /// rebuild the tree (or its leaf range) from scratch under a fresh
    /// registry entry.
    /// </para>
    /// <para>
    /// Per-tree overrides are honoured via the registry entry's
    /// <c>MaintainProjectionDigest</c> column - operators can opt an
    /// individual tree out (or, while no writes have yet landed under the
    /// disabled setting, back in) without flipping the silo-wide default.
    /// System trees (those whose id begins with <c>_lattice_</c>) are
    /// always resolved as <c>false</c> regardless of configuration,
    /// because they are not replicated and have no drift-detection
    /// consumer.
    /// </para>
    /// <para>
    /// Storage and wire format are unchanged: the persisted
    /// <c>ProjectionHash</c> / <c>SubtreeProjectionHash</c> / per-child
    /// snapshot tables remain in the leaf and internal-node state schemas
    /// regardless of this setting, so trees may move freely between
    /// digest-maintaining silos and digest-quiescent silos without
    /// reformatting.
    /// </para>
    /// </summary>
    public bool MaintainProjectionDigest { get; set; } = DefaultMaintainProjectionDigest;

    /// <summary>Default value for <see cref="MaintainProjectionDigest"/> (<c>true</c>).</summary>
    public const bool DefaultMaintainProjectionDigest = true;

    /// <summary>
    /// Number of WAL partitions per tree. Each partition is an independent
    /// per-shard append-only log; the foreground commit-log writer hashes
    /// the mutation key modulo this value to pick the partition. Defaults
    /// to <see cref="DefaultWalPartitions"/> (1) - the dominant
    /// single-cluster shape. Increase to fan out WAL throughput across
    /// independent grains when a single partition becomes the bottleneck.
    /// </summary>
    public int WalPartitions { get; set; } = DefaultWalPartitions;

    /// <summary>Default value for <see cref="WalPartitions"/> (1).</summary>
    public const int DefaultWalPartitions = 1;

    /// <summary>
    /// Maximum number of entries the WAL grain will batch into a single
    /// storage flush. Defaults to <see cref="DefaultWalMaxBatchEntries"/>
    /// (100). Lower values reduce flush latency at the cost of throughput.
    /// </summary>
    public int WalMaxBatchEntries { get; set; } = DefaultWalMaxBatchEntries;

    /// <summary>Default value for <see cref="WalMaxBatchEntries"/> (100).</summary>
    public const int DefaultWalMaxBatchEntries = 100;

    /// <summary>
    /// Maximum byte budget the WAL grain will accumulate into a single
    /// storage flush. Defaults to <see cref="DefaultWalMaxBatchBytes"/>
    /// (4 MiB). Reached whichever-first with <see cref="WalMaxBatchEntries"/>.
    /// <para>
    /// Measured against the <i>exact</i> serialised size of each
    /// captured <see cref="WalRecord"/> under the WAL grain's wire
    /// format - the per-entry encoder
    /// (<c>IWalMutationEncoder</c>) walks every field of the record
    /// through the same Orleans-binary codec that the storage
    /// provider will see, and the bytes it produces are handed
    /// straight to <see cref="IWalStorageProvider.AppendEncodedBatchAsync"/>
    /// on flush so the grain pays exactly one encode per append.
    /// Earlier releases approximated the per-entry cost with
    /// <c>key.Length * 2 + value.Length + 128</c>, which under-counted
    /// records carrying a populated
    /// <see cref="WalRecord.VectorClock"/> and over-counted small-key
    /// records with no vector clock; the budget is now an exact
    /// ceiling, suitable for sizing against the Azure Table Storage
    /// 4 MB transactional-batch limit which has zero tolerance for
    /// under-counts.
    /// </para>
    /// </summary>
    public long WalMaxBatchBytes { get; set; } = DefaultWalMaxBatchBytes;

    /// <summary>Default value for <see cref="WalMaxBatchBytes"/> (4 MiB).</summary>
    public const long DefaultWalMaxBatchBytes = 4L * 1024 * 1024;

    /// <summary>
    /// Maximum number of in-flight + pending batches the per-shard WAL
    /// grain will hold before applying back-pressure to new
    /// <c>Append</c> callers. The grain serialises offset assignment
    /// under the grain turn but lets each batch's
    /// <see cref="IWalStorageProvider.AppendBatchAsync"/> call proceed
    /// independently, so up to this many batches can be "in the system"
    /// at once. New appends beyond the cap await the oldest in-flight
    /// flush before being enqueued; this provides natural back-pressure
    /// under sustained burst load without changing the dense-offset
    /// invariant. Defaults to <see cref="DefaultWalMaxPendingBatches"/>
    /// (1) - bit-identical to the original single-in-flight behaviour.
    /// Raise to pipeline writer-side burst absorption against a
    /// higher-latency durable provider. Must be at least <c>1</c>; the
    /// registered options validator rejects non-positive values at
    /// first-resolve time.
    /// </summary>
    public int WalMaxPendingBatches { get; set; } = DefaultWalMaxPendingBatches;

    /// <summary>Default value for <see cref="WalMaxPendingBatches"/> (1, wire-compat).</summary>
    public const int DefaultWalMaxPendingBatches = 1;

    /// <summary>
    /// Optional per-tree <see cref="IWalStorageProvider"/> resolver. When
    /// supplied, takes precedence over the DI-registered default for the
    /// matching tree id. <c>null</c> falls back to the singleton default
    /// (<see cref="InMemoryWalStorageProvider"/> unless replaced by the
    /// host).
    /// </summary>
    public Func<string, IWalStorageProvider>? WalStorageProvider { get; set; }

    /// <summary>
    /// Optional wall-clock hard ceiling for WAL retention. When set, the
    /// WAL garbage collector (<see cref="ILatticeWalGc"/>) trims entries
    /// whose <see cref="Orleans.Lattice.Primitives.HybridLogicalClock.WallClockTicks"/>
    /// is older than <c>now - WalRetention</c> regardless of consumer
    /// cursor position - bounding worst-case disk usage even when a
    /// registered consumer is hopelessly behind. The lagging consumer
    /// then "falls off the log" on its next read, surfacing the gap to
    /// the auto-bootstrap trigger (replication-side concern).
    /// <para>
    /// <see langword="null"/> (the default) disables the ceiling: the GC
    /// predicate is purely <c>min(consumer cursors)</c>, and a lagging
    /// consumer pins the WAL until it catches up. When set, the value
    /// must be strictly greater than <see cref="TimeSpan.Zero"/>.
    /// </para>
    /// </summary>
    public TimeSpan? WalRetention { get; set; }

    /// <summary>
    /// Optional caller-controlled retry policy applied at the boundary
    /// of every public <see cref="ILattice"/> mutating call. When
    /// <c>null</c> (the default), the library preserves today's
    /// throw-and-revert contract: a failed grain write surfaces
    /// verbatim to the caller and the grain's in-memory state is
    /// reverted to match disk. When set, the policy re-runs the
    /// caller's mutation under the same ambient
    /// <see cref="LatticeIdempotencyContext"/> scope - which the
    /// caller must have entered explicitly - for the policy's
    /// budget; on exhaustion the original failure is surfaced
    /// verbatim. Retry is therefore strictly opt-in at two layers
    /// (policy registration + idempotency-key scope) so the library's
    /// default behaviour is bit-identical to the pre-feature shape.
    /// </summary>
    public ILatticeRetryPolicy? RetryPolicy { get; set; }

    /// <summary>
    /// The name of the Orleans grain storage provider used by Lattice grains.
    /// Used internally by <see cref="LatticeServiceCollectionExtensions.AddLattice"/>
    /// and exposed for advanced scenarios where callers register storage directly.
    /// </summary>
    public const string StorageProviderName = "lattice";
}
