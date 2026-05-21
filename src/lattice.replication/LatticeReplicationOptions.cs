using Orleans.Lattice.BPlusTree.Grains;
namespace Orleans.Lattice.Replication;

/// <summary>
/// Configuration options for <c>Orleans.Lattice.Replication</c>. Register a named
/// instance to override settings for a specific tree:
/// <code>
/// siloBuilder.Services.Configure&lt;LatticeReplicationOptions&gt;("my-tree", o => o.KeyFilter = k => k.StartsWith("repl/"));
/// </code>
/// The unnamed (default) instance applies cluster-wide. Per-tree overrides
/// follow the same named-options pattern as <c>LatticeOptions</c>; the
/// commit-time observer resolves the per-tree instance via
/// <c>IOptionsMonitor&lt;LatticeReplicationOptions&gt;.Get(treeId)</c>.
/// </summary>
public class LatticeReplicationOptions
{
    /// <summary>
    /// Stable identifier for the local Orleans cluster. Stamped on every
    /// replicated mutation so receivers can attribute the origin and break
    /// replication cycles. Must be globally unique across every cluster that
    /// participates in replication, and must be set to a non-empty value -
    /// the registered <c>IValidateOptions&lt;LatticeReplicationOptions&gt;</c>
    /// rejects an empty or whitespace cluster id at first-resolve time.
    /// </summary>
    public string ClusterId { get; set; } = DefaultClusterId;

    /// <summary>
    /// Per-tree opt-in map. Each entry declares both that the named tree
    /// participates in replication and which <see cref="LatticeMergeMode"/>
    /// receivers should use to apply its captured entries. <c>null</c>
    /// (the default) and an empty map both mean "no trees are replicated";
    /// there is no implicit "all trees" wildcard. The producer cannot
    /// recognise CRDT primitives by inspection (the core library stores
    /// every value as opaque <c>byte[]</c>), so explicit mode declaration
    /// is the only way the observer knows how a remote receiver will merge
    /// the value - opting trees in implicitly would silently fall back to
    /// last-writer-wins and risk concurrent-update data loss.
    /// <para>
    /// Membership is checked at commit time on the producer side, so a
    /// mutation against a tree outside this map never reaches the WAL.
    /// </para>
    /// </summary>
    public IReadOnlyDictionary<string, LatticeMergeMode>? ReplicatedTrees { get; set; }

    /// <summary>
    /// Optional per-key filter evaluated on the producer side at commit
    /// time. When non-<c>null</c>, only mutations whose key satisfies the
    /// predicate are forwarded to the WAL; rejected mutations never touch
    /// replication state. Combines with <see cref="KeyPrefixes"/> as a
    /// logical AND - both filters must accept the key for it to replicate.
    /// <para>
    /// For <see cref="MutationKind.DeleteRange"/> mutations the predicate
    /// is evaluated against the inclusive start key only; replicating a
    /// range with mixed-prefix keys is the responsibility of the caller.
    /// </para>
    /// </summary>
    public Func<string, bool>? KeyFilter { get; set; }

    /// <summary>
    /// Optional declarative prefix allowlist evaluated on the producer
    /// side at commit time. <c>null</c> or empty means "no prefix
    /// restriction"; a non-empty collection restricts replication to keys
    /// that start with at least one of the listed prefixes. Combines with
    /// <see cref="KeyFilter"/> as a logical AND - both filters must
    /// accept the key for it to replicate.
    /// </summary>
    public IReadOnlyCollection<string>? KeyPrefixes { get; set; }

    /// <summary>
    /// Number of write-ahead-log partitions per replicated tree. Each
    /// captured <see cref="WalRecord"/> is routed to a single
    /// <c>IWalShardGrain</c> activation keyed by
    /// <c>{treeId}/{partition}</c>, where <c>partition</c> is a stable hash
    /// of the entry's key modulo this value. Defaults to <see cref="DefaultReplogPartitions"/>
    /// (a single per-tree WAL, sufficient for low-fan-in workloads); raise
    /// to fan WAL writes across multiple grain activations on hot trees.
    /// Must be at least <c>1</c>; the registered options validator rejects
    /// non-positive values at first-resolve time.
    /// </summary>
    public int ReplogPartitions { get; set; } = DefaultReplogPartitions;

    /// <summary>
    /// Optional per-tree <see cref="IWalStorageProvider"/> resolver. When
    /// set, the WAL durability backend for the named tree is the value
    /// returned by invoking the delegate with the tree id; when
    /// <see langword="null"/> (the default), the DI-registered singleton
    /// <see cref="IWalStorageProvider"/> is used (see
    /// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>,

    /// which registers <see cref="InMemoryWalStorageProvider"/> as the
    /// fallback).
    /// <para>
    /// Per-tree configurability lets a host pick different durability /
    /// cost trade-offs per tree - for example, an Azure Table Storage
    /// backend for a hot, high-fan-in tree alongside the in-memory
    /// default for an ephemeral test tree - without having to register
    /// multiple competing singletons.
    /// </para>
    /// </summary>
    public Func<string, IWalStorageProvider>? WalStorageProvider { get; set; }

    /// <summary>
    /// Maximum number of <see cref="WalEntry"/> records the per-shard WAL
    /// grain will batch into a single <see cref="IWalStorageProvider.AppendBatchAsync"/>
    /// call. When the in-memory pending batch reaches this count, the
    /// next <c>Append</c> triggers a flush of the current batch before
    /// enqueueing the new entry. Defaults to
    /// <see cref="DefaultWalMaxBatchEntries"/>, matching the Azure Table
    /// Storage 100-entity batch limit. Must be at least <c>1</c>.
    /// </summary>
    public int WalMaxBatchEntries { get; set; } = DefaultWalMaxBatchEntries;

    /// <summary>
    /// Maximum estimated serialised size (in bytes) of a single batch
    /// supplied to <see cref="IWalStorageProvider.AppendBatchAsync"/>.
    /// Defaults to <see cref="DefaultWalMaxBatchBytes"/>, matching the
    /// Azure Table Storage 4 MB batch payload ceiling. The size estimate
    /// is computed from the key length, value length, and a small
    /// constant overhead per entry; it is a soft limit and may
    /// under-/over-estimate the exact serialised bytes by a few percent.
    /// Must be at least <c>1</c>.
    /// </summary>
    public long WalMaxBatchBytes { get; set; } = DefaultWalMaxBatchBytes;

    /// <summary>
    /// Maximum number of in-flight + pending batches the per-shard WAL
    /// grain will hold before applying back-pressure to new
    /// <c>Append</c> callers. The single-in-flight-flush model in v1
    /// treats this as <c>(in-flight=1) + (pending=N-1)</c>; new
    /// <c>Append</c> calls beyond the cap await the in-flight flush
    /// before being enqueued. Defaults to
    /// <see cref="DefaultWalMaxPendingBatches"/>. Must be at least
    /// <c>1</c>.
    /// </summary>
    public int WalMaxPendingBatches { get; set; } = DefaultWalMaxPendingBatches;

    /// <summary>
    /// Maximum number of consecutive failed apply attempts the inbound
    /// pipeline tolerates for the same
    /// <c>(treeId, originClusterId, timestamp, key, op)</c> tuple before
    /// the entry is parked on the per-tree dead-letter queue and the
    /// origin high-water-mark is advanced past it. Defaults to
    /// <see cref="DefaultMaxApplyRetries"/>. Must be at least <c>1</c>;
    /// a value of <c>1</c> means a single failure parks the entry
    /// immediately.
    /// </summary>
    public int MaxApplyRetries { get; set; } = DefaultMaxApplyRetries;

    /// <summary>
    /// Maximum number of <see cref="DeadLetterEntry"/> records the
    /// per-tree dead-letter queue retains. When the queue is full a new
    /// enqueue evicts the oldest entry (FIFO). Defaults to
    /// <see cref="DefaultDeadLetterQueueCapacity"/>. Must be at least
    /// <c>1</c>.
    /// </summary>
    public int DeadLetterQueueCapacity { get; set; } = DefaultDeadLetterQueueCapacity;

    /// <summary>
    /// Maximum number of entries the per-tree causal-apply buffer
    /// retains while waiting on declared causal dependencies. When
    /// the buffer reaches this cap, parking a new entry evicts the
    /// oldest blocked entry (FIFO) and routes it to the per-tree
    /// dead-letter queue with reason
    /// <see cref="LatticeReplicationMetrics.ReasonHlcSkew"/>. Defaults
    /// to <see cref="DefaultCausalBufferMaxEntries"/>. Must be at
    /// least <c>1</c>.
    /// </summary>
    public int CausalBufferMaxEntries { get; set; } = DefaultCausalBufferMaxEntries;

    /// <summary>
    /// Maximum estimated cumulative byte size of every entry parked
    /// on the per-tree causal-apply buffer. Eviction follows the same
    /// FIFO + dead-letter routing as <see cref="CausalBufferMaxEntries"/>.
    /// The size estimate is computed from the key length, value
    /// length, and a small constant overhead per entry; it is a soft
    /// limit and may differ from the exact serialised payload by a
    /// few percent. Defaults to <see cref="DefaultCausalBufferMaxBytes"/>.
    /// Must be at least <c>65536</c> (64 KB) so a single typical
    /// entry can be parked without immediately overflowing the cap.
    /// </summary>
    public long CausalBufferMaxBytes { get; set; } = DefaultCausalBufferMaxBytes;

    /// <summary>
    /// Maximum number of recently-applied
    /// <c>(originClusterId, timestamp, key, op)</c> identity tuples
    /// the per-tree shadow-forward dedupe cache retains. The cache
    /// is a fast-path receiver-side seam that drops the duplicate
    /// emit pair structural rewrites (shard split / merge / saga
    /// compensate) generate when they shadow-forward a user write
    /// into a different shard: both emits ride the WAL with
    /// identical <c>(origin, hlc, key, op)</c>, and a concurrent
    /// inbound delivery can otherwise race past the per-origin
    /// high-water-mark check (both deliveries observe the same
    /// pre-advance HWM and both apply before either advances it).
    /// <para>
    /// Defaults to <see cref="DefaultShadowForwardDedupeCacheSize"/>.
    /// Must be at least <c>64</c>; the registered options validator
    /// rejects smaller values at first-resolve time so a single
    /// pathological burst cannot evict the cache faster than it
    /// fills. Cache eviction under sustained churn cannot cause a
    /// re-merge - the per-origin HWM check is the authoritative
    /// dedupe key and remains in place for any entry the cache has
    /// evicted.
    /// </para>
    /// </summary>
    public int ShadowForwardDedupeCacheSize { get; set; } = DefaultShadowForwardDedupeCacheSize;

    /// <summary>
    /// Optional wall-clock hard ceiling for WAL retention. When set,
    /// the WAL garbage collector
    /// (<see cref="ILatticeWalGc"/>) trims entries whose
    /// <see cref="HybridLogicalClock.WallClockTicks"/> is older than
    /// <c>now - WalRetention</c> regardless of consumer cursor
    /// position - bounding worst-case disk usage even when a
    /// registered consumer is hopelessly behind. The lagging consumer
    /// then "falls off the log" on its next read, surfacing the gap
    /// to the auto-bootstrap trigger (<see cref="ILatticeFallOffLogDetector"/>).
    /// <para>
    /// <see langword="null"/> (the default) disables the ceiling: the
    /// GC predicate is purely <c>min(consumer cursors)</c>, and a
    /// lagging consumer pins the WAL until it catches up. When set,
    /// the value must be strictly greater than <see cref="TimeSpan.Zero"/>.
    /// </para>
    /// </summary>
    public TimeSpan? WalRetention { get; set; }

    /// <summary>
    /// Whether the receiver-side fall-off-the-log detector
    /// (<see cref="ILatticeFallOffLogDetector"/>) automatically calls
    /// <see cref="ILatticeBootstrapCoordinator.BootstrapAsync"/> when
    /// it observes that the receiver's per-origin high-water-mark is
    /// strictly less than the sender's oldest still-available WAL
    /// entry. Defaults to <see langword="true"/>; set to
    /// <see langword="false"/> to surface the detection through the
    /// <see cref="LatticeReplicationMetrics.PeerFellOffLog"/> metric
    /// only, leaving the bootstrap kickoff to operator-driven flows
    /// (<see cref="ILatticeBootstrapCoordinator.BootstrapAsync"/>
    /// invoked explicitly).
    /// <para>
    /// The detector emits the metric regardless of this setting, so
    /// disabling auto-bootstrap does not silence the alert path; it
    /// simply decouples detection from the recovery action so a host
    /// can gate re-seeds on additional policy (rate limits,
    /// maintenance windows, manual approval).
    /// </para>
    /// </summary>
    public bool AutoBootstrapOnFallOffLog { get; set; } = DefaultAutoBootstrapOnFallOffLog;

    /// <summary>
    /// Minimum interval between honoured operator-driven re-seed
    /// requests for the same <c>(treeName, sourceClusterId)</c>
    /// pair, enforced by
    /// <see cref="ILatticeReplicationAdmin.RequestSnapshotAsync"/>.
    /// A request that arrives before this interval has elapsed
    /// since the last honoured request is rejected with
    /// <see cref="OperatorReseedDecision.Triggered"/> set to
    /// <see langword="false"/>; the underlying bootstrap
    /// coordinator is not invoked and no exception is thrown.
    /// Defaults to <see cref="DefaultOperatorReseedMinInterval"/>.
    /// Set to <see cref="TimeSpan.Zero"/> to disable rate limiting
    /// entirely (every request reaches the coordinator, whose own
    /// idempotency contract still absorbs concurrent kickoffs from
    /// the same source cluster as no-ops). Must not be negative; the
    /// registered options validator rejects negative values at
    /// first-resolve time.
    /// </summary>
    public TimeSpan OperatorReseedMinInterval { get; set; } = DefaultOperatorReseedMinInterval;

    /// <summary>
    /// Bounded retry policy applied to the receiver-side bootstrap
    /// drain
    /// (<c>LatticeBootstrapCoordinatorGrain.DrainSnapshotAsync</c>)
    /// when the snapshot source surfaces a transient transport fault.
    /// A failed export or apply call is matched against
    /// <see cref="BoundedExponentialRetryPolicyOptions.RetryableExceptionClassifier"/>;
    /// transient faults consume one retry budget slot and re-open the
    /// snapshot from the persisted
    /// <c>BootstrapCoordinatorState.LastAppliedHlc</c> cursor (so
    /// replay is bounded by the cursor-persist interval and the
    /// per-origin HWM dedupe makes the overlap a no-op). Non-transient
    /// faults pivot the bootstrap to
    /// <c>LatticeBootstrapState.Failed</c> on the first failure, as
    /// they did before the retry seam landed; budget exhaustion
    /// re-throws the captured transient and the catch block in
    /// <c>ProcessNextPhaseAsync</c> persists <c>Failed</c> as the
    /// terminal outcome.
    /// <para>
    /// When <see langword="null"/> (the default) the grain installs
    /// a built-in policy with <see cref="DefaultBootstrapMaxAttempts"/>
    /// attempts,
    /// <see cref="DefaultBootstrapInitialRetryDelay"/> initial backoff,
    /// <see cref="DefaultBootstrapMaxRetryDelay"/> ceiling, and
    /// <see cref="LatticeBootstrapTransientFaultClassifier.IsTransient(Exception)"/>
    /// as the classifier. Set the property to disable retries
    /// entirely (<c>MaxAttempts = 1</c>) or to plug in a host-specific
    /// classifier for non-default transports.
    /// </para>
    /// </summary>
    public BoundedExponentialRetryPolicyOptions? BootstrapTransientRetry { get; set; }

    /// <summary>
    /// Stable identifiers of the remote clusters this silo ships
    /// captured WAL entries to for the configured replicated trees.
    /// The transport-agnostic counterpart to per-transport endpoint
    /// maps (e.g. <c>GrpcPushTransportOptions.PeerEndpoints</c>): the
    /// production replication drivers (the per-<c>(tree, peer)</c>
    /// shipper grain and the per-tree maintenance grain) iterate
    /// this collection to know which peers to ship to and probe for
    /// fall-off-the-log conditions, while the transport implementation
    /// owns the resolution of cluster id to wire-level endpoint.
    /// <para>
    /// <see langword="null"/> (the default) and an empty collection
    /// both mean "no peers configured" - the shipper grain stays
    /// dormant and the maintenance grain skips the fall-off-log
    /// probe (the WAL garbage collector still runs because it is
    /// peer-set independent). Hosts that ship to N peers populate
    /// this collection with the N peer cluster ids; the
    /// <see cref="ShardedReplogSink"/> writer-side doorbell then
    /// fires one no-op grain call per active peer per WAL append.
    /// </para>
    /// </summary>
    public IReadOnlyCollection<string>? ReplicationPeers { get; set; }

    /// <summary>
    /// Maximum number of <see cref="WalRecord"/> records the
    /// per-<c>(tree, peer)</c> shipper grain drains from
    /// <see cref="IChangeFeed.Subscribe"/> and submits to
    /// <see cref="IReplicationTransport.SendAsync"/> in a single
    /// batch. Larger values amortise the per-batch RPC overhead;
    /// smaller values bound memory and reduce the cursor advance
    /// granularity. Defaults to <see cref="DefaultShipBatchSize"/>.
    /// Must be at least <c>1</c>.
    /// </summary>
    public int ShipBatchSize { get; set; } = DefaultShipBatchSize;

    /// <summary>
    /// Maximum number of <see cref="WalRecord"/> records the
    /// per-<c>(tree, peer)</c> shipper grain reads per partition per
    /// pump tick when draining the WAL via the partition-resume
    /// hot path. The pump issues one
    /// <see cref="Grains.IWalShardGrain.ReadAsync"/> call per
    /// partition starting from each partition's saved resume cursor,
    /// merges the pages by <see cref="Primitives.HybridLogicalClock"/>
    /// ascending, and emits up to <see cref="ShipBatchSize"/> entries
    /// per outbound batch.
    /// <para>
    /// Distinct from <see cref="ShipBatchSize"/>: this value caps the
    /// per-partition page read; <see cref="ShipBatchSize"/> caps the
    /// merged output batch. With <c>P</c> partitions the worst-case
    /// in-memory drain footprint is <c>P × ShipPartitionPageSize</c>
    /// entries - <c>ShipBatchSize</c> bounds the wire batch but not
    /// the post-drain working set, so size <c>ShipPartitionPageSize</c>
    /// for one comfortable batch's worth of page-read work
    /// (default <see cref="DefaultShipPartitionPageSize"/>).
    /// </para>
    /// <para>
    /// Defaults to <see cref="DefaultShipPartitionPageSize"/>. Must be
    /// at least <c>1</c>.
    /// </para>
    /// </summary>
    public int ShipPartitionPageSize { get; set; } = DefaultShipPartitionPageSize;

    /// <summary>
    /// Number of successful acks the per-<c>(tree, peer)</c> shipper
    /// grain coalesces between calls to
    /// <see cref="IPersistentState{TState}.WriteStateAsync"/>. Setting
    /// this to <c>1</c> persists on every ack (the original behaviour
    /// before this option was introduced); higher values amortize the
    /// storage round-trip across multiple shipped batches at the cost
    /// of a bounded re-ship window after a silo crash.
    /// <para>
    /// <strong>Crash safety.</strong> Receiver-side apply is
    /// HLC-monotonic and dedupes on <c>(originClusterId, originHlc)</c>,
    /// so a crash inside the deferred-persist window replays at most
    /// <see cref="ShipCursorWriteInterval"/> &#xD7; <see cref="ShipBatchSize"/>
    /// entries on recovery - the receiver no-ops the duplicates. No
    /// data is lost.
    /// </para>
    /// <para>
    /// <strong>GC interaction.</strong> The WAL GC consumes the cursor
    /// reported via
    /// <see cref="IWalCursorRegistry.ReportCursorAsync"/>;
    /// the shipper calls that strictly <em>after</em> the durable
    /// <c>WriteStateAsync</c> completes, so the trim frontier never
    /// exceeds the durably-recoverable cursor regardless of this
    /// interval.
    /// </para>
    /// <para>
    /// Defaults to <see cref="DefaultShipCursorWriteInterval"/>. Must
    /// be at least <c>1</c>.
    /// </para>
    /// </summary>
    public int ShipCursorWriteInterval { get; set; } = DefaultShipCursorWriteInterval;

    /// <summary>
    /// Maximum number of in-flight <see cref="IReplicationTransport.SendAsync"/>
    /// calls per <c>(tree, peer)</c> shipper. The v1 implementation
    /// hard-codes a strict serial-send protocol so per-peer cursor
    /// advance stays simple; this option is reserved for a future
    /// relaxation that can pipeline multiple batches once the typed
    /// envelope shape lands. Defaults to
    /// <see cref="DefaultShipMaxInFlight"/>. Must be at least <c>1</c>.
    /// </summary>
    /// <remarks>
    /// <strong>v1-inert.</strong> The validator accepts any value
    /// <c>&gt;= 1</c>, but the shipper grain ignores values
    /// <c>&gt; 1</c> in this release; sends remain strictly serial
    /// per <c>(tree, peer)</c>. The relaxation is gated on the
    /// typed-envelope transport seam (eliminating the sender-side
    /// decode round-trip) and on multi-batch in-flight WAL flush
    /// landing first.
    /// </remarks>
    public int ShipMaxInFlight { get; set; } = DefaultShipMaxInFlight;

    /// <summary>
    /// Initial backoff delay applied to the per-peer shipper after a
    /// transient transport failure. The next ship attempt is skipped
    /// until this much wall-clock time has elapsed; subsequent
    /// failures double the delay (capped by
    /// <see cref="ShipBackoffMax"/>) and jitter the result by
    /// <see cref="ShipBackoffJitter"/>. Defaults to
    /// <see cref="DefaultShipBackoffInitial"/>. Must be strictly
    /// greater than <see cref="TimeSpan.Zero"/>.
    /// </summary>
    public TimeSpan ShipBackoffInitial { get; set; } = DefaultShipBackoffInitial;

    /// <summary>
    /// Period of the per-(tree, peer) shipper grain's phase timer
    /// - the wall-clock cadence at which the shipper polls the WAL
    /// for new entries to ship to the peer. A shorter period reduces
    /// the worst-case latency between a write being appended to the
    /// WAL and the next ship attempt picking it up; a longer period
    /// trades latency for cheaper steady-state load on the silo
    /// scheduler when the WAL is empty. The setting only matters when
    /// the doorbell signal (<see cref="ShipDoorbellEnabled"/>) is
    /// disabled or fails to ring (e.g. shipper grain not yet active);
    /// in normal operation the doorbell drives the next pump tick
    /// immediately on append. Defaults to
    /// <see cref="DefaultShipPhaseTimerPeriod"/>. Must be strictly
    /// greater than <see cref="TimeSpan.Zero"/>.
    /// </summary>
    public TimeSpan ShipPhaseTimerPeriod { get; set; } = DefaultShipPhaseTimerPeriod;

    /// <summary>
    /// Maximum backoff delay between failed ship attempts, capping
    /// the doubling sequence seeded by <see cref="ShipBackoffInitial"/>.
    /// Defaults to <see cref="DefaultShipBackoffMax"/>. Must be
    /// greater than or equal to <see cref="ShipBackoffInitial"/>.
    /// </summary>
    public TimeSpan ShipBackoffMax { get; set; } = DefaultShipBackoffMax;

    /// <summary>
    /// Multiplicative jitter applied to each backoff delay so a
    /// thundering herd of failed shippers re-attempts at slightly
    /// different times. Each delay is scaled by a random factor in
    /// <c>[1.0 - jitter, 1.0 + jitter]</c>. Defaults to
    /// <see cref="DefaultShipBackoffJitter"/> (20% spread). Must be
    /// in <c>[0.0, 1.0]</c>; <c>0.0</c> disables jitter entirely.
    /// </summary>
    public double ShipBackoffJitter { get; set; } = DefaultShipBackoffJitter;

    /// <summary>
    /// Cadence at which the per-tree maintenance grain calls
    /// <see cref="ILatticeWalGc.RunOnceAsync"/> to trim the
    /// WAL by min-acked cursor. Defaults to
    /// <see cref="DefaultMaintenanceGcInterval"/>. Must be strictly
    /// greater than <see cref="TimeSpan.Zero"/>.
    /// </summary>
    public TimeSpan MaintenanceGcInterval { get; set; } = DefaultMaintenanceGcInterval;

    /// <summary>
    /// Cadence at which the per-tree maintenance grain iterates the
    /// configured <see cref="ReplicationPeers"/> and invokes
    /// <see cref="ILatticeFallOffLogDetector.CheckAndTriggerAsync"/>
    /// on each peer. Defaults to
    /// <see cref="DefaultMaintenanceFallOffCheckInterval"/>. Must
    /// be strictly greater than <see cref="TimeSpan.Zero"/>.
    /// </summary>
    public TimeSpan MaintenanceFallOffCheckInterval { get; set; } = DefaultMaintenanceFallOffCheckInterval;

    /// <summary>
    /// Whether <see cref="ShardedReplogSink"/> rings the per-peer
    /// shipper grain after a successful WAL append, signalling that
    /// new entries are available. Disabling the doorbell falls back
    /// to the shipper's <see cref="DefaultShipDoorbellEnabled"/>
    /// timer-driven cadence (~200 ms). Defaults to
    /// <see langword="true"/>.
    /// <para>
    /// The doorbell is best-effort: a failure to reach a shipper
    /// activation (silo loss, transient network fault) is logged
    /// at <c>Trace</c> level and swallowed so the producer-side
    /// commit path never fails on a doorbell ring failure. A missed
    /// doorbell only delays the affected peer by one timer tick.
    /// </para>
    /// </summary>
    public bool ShipDoorbellEnabled { get; set; } = DefaultShipDoorbellEnabled;

    /// <summary>
    /// Compression algorithm applied to the framing tail (the
    /// variable-length bytes following the fixed 32-byte
    /// <see cref="EncodedBatchHeader"/>: <c>treeName</c>,
    /// <c>originClusterId</c>, and the length-prefixed entry segments)
    /// when the shipper builds an outbound batch. Defaults to
    /// <see cref="LatticeCompression.None"/>; setting this to
    /// <see cref="LatticeCompression.Zstd"/> opts in to Zstandard
    /// compression at the level configured by
    /// <see cref="FramingCompressionLevel"/>.
    /// <para>
    /// Compression is skipped (and <see cref="LatticeCompression.None"/>
    /// is stamped on the wire regardless of this setting) when the
    /// uncompressed tail is shorter than
    /// <see cref="FramingCompressionMinBatchBytes"/> - the per-batch
    /// fixed overhead of compression dominates the bandwidth saving on
    /// tiny batches.
    /// </para>
    /// <para>
    /// The receiver decompresses based on the on-wire algorithm value,
    /// not this option, so a coordinated rollout is required: every
    /// receiver in the topology must run a build that has the
    /// corresponding <see cref="ILatticeCompressor"/> registered before
    /// any sender flips this to a non-<see cref="LatticeCompression.None"/>
    /// value. An unsupported algorithm at the receiver surfaces as
    /// <see cref="NotSupportedException"/> from the framing decoder
    /// and routes through the existing transient-backoff +
    /// dead-letter classification path.
    /// </para>
    /// </summary>
    public LatticeCompression FramingCompression { get; set; } = DefaultFramingCompression;

    /// <summary>
    /// Compression level forwarded to the algorithm-specific
    /// <see cref="ILatticeCompressor"/> when
    /// <see cref="FramingCompression"/> is non-<see cref="LatticeCompression.None"/>.
    /// Interpreted by the algorithm: for <see cref="LatticeCompression.Zstd"/>
    /// the valid range is <c>1</c> (fastest, lowest ratio) to
    /// <c>22</c> (slowest, highest ratio); the canonical default is
    /// <c>3</c>. Defaults to <see cref="DefaultFramingCompressionLevel"/>.
    /// </summary>
    public int FramingCompressionLevel { get; set; } = DefaultFramingCompressionLevel;

    /// <summary>
    /// Minimum uncompressed-tail byte count below which the shipper
    /// stamps <see cref="LatticeCompression.None"/> regardless of the
    /// configured <see cref="FramingCompression"/> value. The
    /// per-batch fixed overhead of compression (algorithm-specific
    /// frame header plus the two 4-byte tail length prefixes the
    /// canonical encoder writes) dominates the bandwidth saving on
    /// tiny batches; the threshold lets a host enable compression
    /// for steady-state large batches without paying the overhead on
    /// heartbeats and small-bursty traffic. Defaults to
    /// <see cref="DefaultFramingCompressionMinBatchBytes"/>. Must be
    /// non-negative.
    /// </summary>
    public int FramingCompressionMinBatchBytes { get; set; } = DefaultFramingCompressionMinBatchBytes;


    /// <summary>
    /// Default value for <see cref="ClusterId"/>: an empty sentinel that
    /// represents "unset". This default is rejected by
    /// <c>LatticeReplicationOptionsValidator</c> so a host that calls
    /// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>
    /// without supplying a cluster id fails fast on first options resolution.
    /// </summary>
    public const string DefaultClusterId = "";

    /// <summary>
    /// Default value for <see cref="ReplogPartitions"/>: a single WAL
    /// partition per replicated tree. Adequate for low-fan-in workloads;
    /// raise for hot trees that benefit from parallel WAL append paths.
    /// </summary>
    public const int DefaultReplogPartitions = 1;

    /// <summary>
    /// Default value for <see cref="WalMaxBatchEntries"/>: matches the
    /// Azure Table Storage 100-entity batch insert limit.
    /// </summary>
    public const int DefaultWalMaxBatchEntries = 100;

    /// <summary>
    /// Default value for <see cref="WalMaxBatchBytes"/>: matches the
    /// Azure Table Storage 4 MB batch payload ceiling.
    /// </summary>
    public const long DefaultWalMaxBatchBytes = 4 * 1024 * 1024;

    /// <summary>
    /// Default value for <see cref="WalMaxPendingBatches"/>: caps the
    /// in-memory backlog at four full-sized batches before back-pressure
    /// engages.
    /// </summary>
    public const int DefaultWalMaxPendingBatches = 4;

    /// <summary>
    /// Default value for <see cref="MaxApplyRetries"/>: five consecutive
    /// failures park the entry. Chosen as a small bound that absorbs
    /// transient faults without dragging an origin cursor for hours.
    /// </summary>
    public const int DefaultMaxApplyRetries = 5;

    /// <summary>
    /// Default value for <see cref="DeadLetterQueueCapacity"/>: 1000
    /// parked entries per tree. Bounds the inspection-seam working set
    /// while leaving enough room to diagnose a sustained failure batch.
    /// </summary>
    public const int DefaultDeadLetterQueueCapacity = 1000;

    /// <summary>
    /// Default value for <see cref="CausalBufferMaxEntries"/>: 1024
    /// parked entries per tree. Sized to absorb a brief partition
    /// healing burst from a single peer without escalating to the
    /// dead-letter queue, while keeping the per-silo working set
    /// bounded.
    /// </summary>
    public const int DefaultCausalBufferMaxEntries = 1024;

    /// <summary>
    /// Default value for <see cref="CausalBufferMaxBytes"/>: 16 MB
    /// of cumulative parked-entry payload per tree. Sized so the
    /// buffer's worst-case memory footprint is one to two orders of
    /// magnitude below typical silo heap sizes.
    /// </summary>
    public const long DefaultCausalBufferMaxBytes = 16L * 1024L * 1024L;

    /// <summary>
    /// Default value for <see cref="ShadowForwardDedupeCacheSize"/>:
    /// 4096 retained identity tuples per tree. Sized to absorb a
    /// burst of shard-split / merge / saga-compensate shadow-forward
    /// activity without evicting the cache faster than concurrent
    /// duplicate deliveries can race past the per-origin
    /// high-water-mark check, while keeping the per-tree memory
    /// footprint bounded (~256 KB per tree at typical key sizes).
    /// </summary>
    public const int DefaultShadowForwardDedupeCacheSize = 4096;

    /// <summary>
    /// Default value for <see cref="AutoBootstrapOnFallOffLog"/>:
    /// automatically kick off a snapshot bootstrap when the
    /// fall-off-the-log detector observes that the receiver has
    /// fallen off the sender's WAL.
    /// </summary>
    public const bool DefaultAutoBootstrapOnFallOffLog = true;

    /// <summary>
    /// Default value for <see cref="OperatorReseedMinInterval"/>:
    /// one minute between honoured operator re-seed requests for
    /// the same <c>(tree, sourceClusterId)</c> pair. Sized to
    /// absorb double-clicks and rapid retries while still allowing
    /// a deliberate operator command to land within an interactive
    /// session.
    /// </summary>
    public static readonly TimeSpan DefaultOperatorReseedMinInterval = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Default value for the
    /// <see cref="BootstrapTransientRetry"/>'s
    /// <see cref="BoundedExponentialRetryPolicyOptions.MaxAttempts"/>:
    /// four attempts total (one initial attempt + three retries).
    /// Sized to absorb a brief gRPC channel reset or a peer-side
    /// hiccup without dragging a bootstrap through a long retry
    /// cascade. Operators that want a longer recovery window raise
    /// this value; operators that prefer fail-fast set it to 1 to
    /// disable retries entirely.
    /// </summary>
    public const int DefaultBootstrapMaxAttempts = 4;

    /// <summary>
    /// Default value for the
    /// <see cref="BootstrapTransientRetry"/>'s
    /// <see cref="BoundedExponentialRetryPolicyOptions.InitialDelay"/>:
    /// 500 ms. Sized for cross-cluster transport recovery cadence
    /// (a TCP retransmit / gRPC channel re-establish typically
    /// completes within a single-digit-second window), not for the
    /// in-cluster Orleans RPC fault model.
    /// </summary>
    public static readonly TimeSpan DefaultBootstrapInitialRetryDelay = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// Default value for the
    /// <see cref="BootstrapTransientRetry"/>'s
    /// <see cref="BoundedExponentialRetryPolicyOptions.MaxDelay"/>:
    /// 30 seconds. Caps the worst-case wall-clock delay between
    /// retries so a long transient outage does not extend the
    /// bootstrap window indefinitely; the bootstrap's keepalive
    /// reminder period (1 minute) sets the practical ceiling on
    /// how long any retry can sleep.
    /// </summary>
    public static readonly TimeSpan DefaultBootstrapMaxRetryDelay = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Default value for <see cref="ShipBatchSize"/>: 256 entries per
    /// outbound batch. Sized to amortise per-batch RPC overhead at a
    /// few-millisecond per-batch latency budget while keeping cursor
    /// advance granularity fine enough that a transport failure
    /// re-ships at most a few hundred entries.
    /// </summary>
    public const int DefaultShipBatchSize = 256;

    /// <summary>
    /// Default value for <see cref="ShipPartitionPageSize"/>: 256
    /// entries per partition per pump tick. Sized to match
    /// <see cref="DefaultShipBatchSize"/> for the canonical
    /// <c>ReplogPartitions=1</c> case so the per-tick page-read
    /// fits exactly one outbound batch with no carry-over; raise
    /// for trees with many partitions where one partition can
    /// supply the whole batch on its own.
    /// </summary>
    public const int DefaultShipPartitionPageSize = 256;

    /// <summary>
    /// Default value for <see cref="ShipCursorWriteInterval"/>: 16
    /// successful acks between durable cursor writes. At canonical
    /// configuration (<see cref="DefaultShipBatchSize"/>=256, ~10 ms
    /// ack RTT, ~10 ms persist RTT) this lifts the per-batch persist
    /// round-trip out of the steady-state hot path (one persist per
    /// 16 batches instead of one per batch - ~94% reduction in
    /// storage write amplification) while keeping the
    /// crash-recovery re-ship window to ~4 096 entries (under 1 MB
    /// at typical entry sizes; on the order of a few seconds of
    /// re-ship work at benchmark throughput rates). Doubling to 32
    /// trims another ~3% off write amplification but doubles the
    /// recovery window, so 16 sits at the knee of the curve.
    /// </summary>
    public const int DefaultShipCursorWriteInterval = 16;

    /// <summary>
    /// Default value for <see cref="ShipMaxInFlight"/>: strict serial
    /// sends per <c>(tree, peer)</c>. Multi-batch pipelining lifts
    /// this once the typed-envelope transport shape lands.
    /// </summary>
    public const int DefaultShipMaxInFlight = 1;

    /// <summary>
    /// Default value for <see cref="ShipBackoffInitial"/>: 100 ms.
    /// </summary>
    public static readonly TimeSpan DefaultShipBackoffInitial = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Default value for <see cref="ShipPhaseTimerPeriod"/>: 100 ms.
    /// Matches the legacy hard-coded period of the shipper's phase
    /// timer so the option is a strict superset of prior behaviour.
    /// </summary>
    public static readonly TimeSpan DefaultShipPhaseTimerPeriod = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Default value for <see cref="ShipBackoffMax"/>: 30 seconds.
    /// </summary>
    public static readonly TimeSpan DefaultShipBackoffMax = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Default value for <see cref="ShipBackoffJitter"/>: 20 % spread
    /// each side of the nominal delay.
    /// </summary>
    public const double DefaultShipBackoffJitter = 0.2;

    /// <summary>
    /// Default value for <see cref="MaintenanceGcInterval"/>: 5 seconds.
    /// </summary>
    public static readonly TimeSpan DefaultMaintenanceGcInterval = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Default value for <see cref="MaintenanceFallOffCheckInterval"/>:
    /// 30 seconds.
    /// </summary>
    public static readonly TimeSpan DefaultMaintenanceFallOffCheckInterval = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Default value for <see cref="ShipDoorbellEnabled"/>: doorbell
    /// signalling enabled.
    /// </summary>
    public const bool DefaultShipDoorbellEnabled = true;

    /// <summary>
    /// Default value for <see cref="FramingCompression"/>: no
    /// compression. Hosts that want bandwidth-optimised replication
    /// over a constrained link opt in by setting this to
    /// <see cref="LatticeCompression.Zstd"/>.
    /// </summary>
    public const LatticeCompression DefaultFramingCompression = LatticeCompression.None;

    /// <summary>
    /// Default value for <see cref="FramingCompressionLevel"/>: <c>3</c>,
    /// the standard Zstandard "fast" preset that is the canonical
    /// online-compression knee on modern hardware.
    /// </summary>
    public const int DefaultFramingCompressionLevel = 3;

    /// <summary>
    /// Default value for <see cref="FramingCompressionMinBatchBytes"/>:
    /// 512 bytes. A batch whose uncompressed tail is shorter than this
    /// is shipped as <see cref="LatticeCompression.None"/> regardless
    /// of the configured algorithm; the per-batch fixed overhead
    /// dominates the bandwidth saving on tiny batches.
    /// </summary>
    public const int DefaultFramingCompressionMinBatchBytes = 512;
}
