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
    /// participates in replication and which <see cref="ReplicationMode"/>
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
    public IReadOnlyDictionary<string, ReplicationMode>? ReplicatedTrees { get; set; }

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
    /// captured <see cref="ReplogEntry"/> is routed to a single
    /// <c>IReplogShardGrain</c> activation keyed by
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
    /// Optional wall-clock hard ceiling for WAL retention. When set,
    /// the WAL garbage collector
    /// (<see cref="ILatticeReplicationGc"/>) trims entries whose
    /// <see cref="HybridLogicalClock.WallClockTicks"/> is older than
    /// <c>now - WalRetention</c> regardless of consumer cursor
    /// position - bounding worst-case disk usage even when a
    /// registered consumer is hopelessly behind. The lagging consumer
    /// then "falls off the log" on its next read, surfacing the gap
    /// to the auto-bootstrap trigger (later phase).
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
}
