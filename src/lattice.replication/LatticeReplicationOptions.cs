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
    /// Maximum number of independent <c>(treeId, originClusterId)</c>
    /// runs the receiver-side batch-apply path may apply concurrently
    /// within a single inbound batch. Independence is defined at the
    /// <em>tree</em> granularity: runs that target distinct trees may
    /// apply in parallel, while runs that share a tree are applied
    /// strictly sequentially in write-ahead-log order. This bounds the
    /// receiver-side apply latency (and the resulting
    /// <c>apply.lag</c>, which now also drives receiver back-pressure)
    /// under multi-tree load without ever reordering work inside a
    /// tree.
    /// <para>
    /// <strong>Ordering invariants are preserved unconditionally.</strong>
    /// Parallelism is only ever introduced <em>across</em> independent
    /// runs, never within one: per-origin FIFO, the causal dependency
    /// gate and its bounded per-tree buffer, the per-origin
    /// high-water-mark monotonicity, and atomic-batch (saga) apply
    /// boundaries all hold exactly as in the fully-sequential path.
    /// Because the per-tree causal-apply buffer and shadow-forward
    /// dedupe cache are shared across a tree's origins, same-tree runs
    /// stay serialized so those structures see the identical access
    /// order they would under sequential apply.
    /// </para>
    /// <para>
    /// Defaults to <see cref="DefaultApplyMaxParallelRuns"/>
    /// (<c>1</c>), which is exactly the historical fully-sequential
    /// behaviour: the batch-apply path walks every run in order and
    /// awaits each before starting the next. Raise this value
    /// (conservatively) to allow distinct trees to apply concurrently
    /// once the parallel path has been validated for a workload. The
    /// effective degree of parallelism per batch is the configured
    /// value clamped to the number of distinct trees present in that
    /// batch, surfaced on the
    /// <see cref="LatticeReplicationMetrics.ApplyParallelRuns"/>
    /// histogram. Must be at least <c>1</c>; the registered options
    /// validator rejects non-positive values at first-resolve time.
    /// </para>
    /// </summary>
    public int ApplyMaxParallelRuns { get; set; } = DefaultApplyMaxParallelRuns;

    /// <summary>
    /// Whether the per-<c>(tree, peer)</c> shipper measures the
    /// content-hash payload re-send rate: the fraction of shipped
    /// <see cref="MutationKind.Set"/> entries whose value bytes are
    /// byte-identical to the value most recently shipped for the same
    /// key. Defaults to <see langword="false"/>; when off the shipper
    /// behaves and frames bytes exactly as it does today (no extra
    /// hashing, no cache, no metric), so the on-the-wire output is
    /// byte-identical to a build without this option.
    /// <para>
    /// Idempotent upstream retry logic - a caller that re-sets the same
    /// value on every retry - is the canonical source of redundant
    /// payload re-sends. Enabling this option records the
    /// <see cref="LatticeReplicationMetrics.ShipRedundantPayloads"/> and
    /// <see cref="LatticeReplicationMetrics.ShipRedundantPayloadBytes"/>
    /// counters so an operator can decide whether the re-send rate
    /// justifies a future sender-manifest / receiver-pull-missing
    /// round trip. The measurement itself never elides, reorders, or
    /// alters the bytes shipped: every entry is still shipped verbatim,
    /// so last-writer-wins / HLC convergence semantics are unaffected.
    /// </para>
    /// </summary>
    public bool ContentHashDedupEnabled { get; set; } = DefaultContentHashDedupEnabled;

    /// <summary>
    /// Maximum number of distinct keys the per-<c>(tree, peer)</c>
    /// content-hash dedup measurement cache retains. The cache maps each
    /// recently-shipped key to the content hash of the last value
    /// shipped for it; a re-send of byte-identical content for a key
    /// still in the cache increments the redundant-payload counters.
    /// Larger values measure the re-send rate accurately across a wider
    /// working set of keys at the cost of more per-shipper memory;
    /// eviction is least-recently-shipped first on overflow.
    /// <para>
    /// Only consulted when <see cref="ContentHashDedupEnabled"/> is
    /// <see langword="true"/>. Defaults to
    /// <see cref="DefaultContentHashDedupCacheSize"/>. Must be at least
    /// <c>64</c>; the registered options validator rejects smaller
    /// values at first-resolve time so a single pathological key burst
    /// cannot evict the cache faster than it fills and starve the
    /// measurement.
    /// </para>
    /// </summary>
    public int ContentHashDedupCacheSize { get; set; } = DefaultContentHashDedupCacheSize;

    /// <summary>
    /// Whether the per-<c>(tree, peer)</c> shipper collapses redundant
    /// per-key versions out of an outbound batch before they reach the
    /// cross-cluster wire (pre-ship coalescing). Defaults to
    /// <see langword="false"/>; when off the drain / ship path is
    /// byte-identical to a build without this option - no coalescing
    /// pass runs, no entry is elided, and the framed bytes match today's
    /// output exactly.
    /// <para>
    /// When enabled, coalescing applies to trees declared
    /// <see cref="LatticeMergeMode.LwwRegister"/> and to recognised CRDT
    /// trees, with a mode-specific strategy. For a last-writer-wins tree
    /// the receiver applies entries last-writer-wins on the value bytes,
    /// so an intermediate version a later same-key write supersedes is
    /// invisible after convergence: within a single drained batch the
    /// shipper keeps only the highest-<see cref="HybridLogicalClock"/>
    /// entry per key and drops the earlier same-key point writes - exactly
    /// the version the receiver's last-writer-wins apply would have
    /// converged to. Because the shipper only ever drains its own cluster's
    /// authored writes, the ordering tie-break collapses to a pure HLC
    /// comparison (single origin), so the kept entry is unambiguous.
    /// </para>
    /// <para>
    /// For a recognised CRDT tree the receiver applies entries by folding
    /// each one's typed delta into the loaded state, so a naive drop would
    /// lose an intermediate delta's contribution. The shipper instead
    /// merges the same-key deltas into a single combined delta - a join
    /// over the primitive's semilattice (union for OR-Set adds / removes,
    /// pointwise-max for PN-Counter and version-vector components,
    /// dot-dominance merge for the multi-value register, grow-only union
    /// for the sequence CRDT) - re-encodes that one delta onto the kept
    /// (highest-HLC) entry, and elides the earlier same-key ones. Because
    /// each combine and the receiver-side apply are both commutative,
    /// associative, and idempotent, the merged entry converges to the
    /// identical state as shipping the run individually. The generic
    /// OR-Map mode is not combined (its value CRDT is type-erased on the
    /// shipper); its entries ship individually, which is loss-free but
    /// forgoes the bandwidth saving. A CRDT entry carrying no typed delta
    /// (an opaque or legacy payload) also ships verbatim.
    /// </para>
    /// <para>
    /// Coalescing never elides a range delete, a saga terminal mark
    /// (<see cref="MutationKind.TxCommit"/> / <see cref="MutationKind.TxAbort"/>),
    /// an atomic-batch prepare-phase entry, or any entry carrying
    /// <see cref="HybridLogicalClock.Zero"/>: those are left verbatim so
    /// atomic-batch boundaries, causal dependencies, and per-origin FIFO
    /// ordering are preserved. The per-elided-entry win is surfaced on the
    /// <see cref="LatticeReplicationMetrics.CoalesceEntriesElided"/> and
    /// <see cref="LatticeReplicationMetrics.CoalesceBytesElided"/> counters
    /// for both modes; the CRDT-specific count of source deltas folded into
    /// a combined delta is surfaced on
    /// <see cref="LatticeReplicationMetrics.CoalesceDeltasMerged"/>.
    /// The coalesced output is a valid subset (LWW) or delta-merge
    /// (CRDT) of the verbatim batch, so an unmodified receiver decodes and
    /// applies it to the identical converged state - the cursor still
    /// advances past every elided entry's sequence because the
    /// per-partition resume bookkeeping is updated at drain time, before
    /// the coalescing pass runs. The on-wire entry shape is unchanged
    /// (fewer / merged entries of the existing format); there is no wire
    /// version bump.
    /// </para>
    /// </summary>
    public bool PreShipCoalescingEnabled { get; set; } = DefaultPreShipCoalescingEnabled;

    /// <summary>
    /// Whether the per-<c>(tree, peer)</c> shipper attempts the
    /// sender-manifest / receiver-pull-missing content-hash round trip to
    /// elide redundant payloads from an outbound batch: the sender
    /// advertises a per-entry content-hash manifest, the receiver replies
    /// with the entries it is actually missing, and only those payloads are
    /// shipped. Defaults to <see langword="false"/>; when off no manifest
    /// exchange is ever attempted and the drain / ship path is
    /// byte-identical to a build without this option.
    /// <para>
    /// This option is an <em>additional</em> opt-in on top of the
    /// content-hash dedup master switch: it has no effect unless
    /// <see cref="ContentHashDedupEnabled"/> is also <see langword="true"/>
    /// (the registered options validator rejects enabling elision without
    /// the master switch at first-resolve time). Keeping the two switches
    /// independent lets an operator first turn on the measurement-only
    /// re-send-rate counters
    /// (<see cref="LatticeReplicationMetrics.ShipRedundantPayloads"/>),
    /// observe whether the redundant-payload rate justifies the extra
    /// round trip, and only then opt into the elision exchange.
    /// </para>
    /// <para>
    /// Capability is negotiated lazily and per shipper activation: the
    /// exchange seam
    /// (<see cref="IReplicationDigestProbeTransport.ExchangeContentManifestAsync"/>)
    /// defaults to a no-op that reports the peer cannot perform the
    /// exchange, in which case the shipper permanently falls back to
    /// shipping the full batch verbatim for the rest of the activation - so
    /// a peer (or transport) that has not implemented the pull-missing RPC
    /// is wire-identical to today and rolling-upgrade safe. The elision
    /// runs on the strict-serial ship path only; a configured pipelining
    /// window (<see cref="ShipMaxInFlight"/> &gt; 1) is collapsed to one
    /// while elision is enabled so the per-batch exchange composes with
    /// per-origin FIFO and atomic-batch boundaries without reordering.
    /// </para>
    /// <para>
    /// Correctness is preserved across the elision path: a manifest entry
    /// whose content the receiver already holds but whose
    /// <see cref="HybridLogicalClock"/> is newer (the idempotent re-set of
    /// an identical value) advances the receiver's per-origin
    /// high-water-mark via a metadata-only apply during the exchange, so
    /// the high-water-mark still advances even though the payload is never
    /// re-shipped. Range deletes, saga terminal marks, prepared
    /// atomic-batch entries, and zero-HLC entries are never placed in the
    /// manifest and are always shipped verbatim. The per-elided-entry win
    /// is surfaced on the
    /// <see cref="LatticeReplicationMetrics.ShipElidedPayloads"/> /
    /// <see cref="LatticeReplicationMetrics.ShipElidedPayloadBytes"/>
    /// counters and the exchange volume on
    /// <see cref="LatticeReplicationMetrics.ManifestExchanges"/>.
    /// </para>
    /// </summary>
    public bool ContentHashDedupElisionEnabled { get; set; } = DefaultContentHashDedupElisionEnabled;

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
    /// merges the pages by <see cref="Orleans.Lattice.HybridLogicalClock"/>
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
    /// Maximum wall-clock time the per-<c>(tree, peer)</c> shipper grain
    /// defers a pending durable cursor write before forcing a flush,
    /// independent of how many successful acks have accumulated. Together
    /// with <see cref="ShipCursorWriteInterval"/> this forms an
    /// "either/or" coalescing rule: the cursor is persisted whenever
    /// <em>either</em> <see cref="ShipCursorWriteInterval"/> acks have
    /// accumulated <em>or</em> this much wall-clock time has elapsed since
    /// the first un-flushed advance - whichever comes first.
    /// <para>
    /// The time dimension bounds how stale a durable cursor can become on
    /// a low-throughput or bursty stream that ships fewer than
    /// <see cref="ShipCursorWriteInterval"/> batches before quiescing: a
    /// pure batch-count rule would leave the last few advances un-flushed
    /// indefinitely while the stream is idle, widening the crash-replay
    /// window and pinning the WAL GC trim frontier at the last reported
    /// cursor. The elapsed check is evaluated both when a new advance is
    /// booked and on idle pump ticks (the empty-drain path), so a stream
    /// that goes completely silent still checkpoints within this bound.
    /// </para>
    /// <para>
    /// <strong>Crash safety.</strong> Lowering this value only ever makes
    /// the durable cursor <em>fresher</em> - it can never widen the
    /// crash-replay window beyond the
    /// <see cref="ShipCursorWriteInterval"/> &#xD7; <see cref="ShipBatchSize"/>
    /// bound the batch-count rule already guarantees. Receiver-side apply
    /// is HLC-monotonic and dedupes on <c>(originClusterId, originHlc)</c>,
    /// so any entries re-shipped inside the window are no-op'd at the
    /// receiver and no data is lost.
    /// </para>
    /// <para>
    /// Defaults to <see cref="DefaultShipCursorWriteMaxDelay"/>. Set to
    /// <see cref="System.Threading.Timeout.InfiniteTimeSpan"/> to disable
    /// the time dimension entirely and coalesce purely by
    /// <see cref="ShipCursorWriteInterval"/>; any other value must be
    /// strictly greater than <see cref="TimeSpan.Zero"/>.
    /// </para>
    /// </summary>
    public TimeSpan ShipCursorWriteMaxDelay { get; set; } = DefaultShipCursorWriteMaxDelay;

    /// <summary>
    /// Maximum number of in-flight <see cref="IReplicationTransport.SendAsync"/>
    /// calls the per-<c>(tree, peer)</c> shipper keeps open at once -
    /// the depth of the sender-side pipelining window. With the default
    /// of <c>1</c> the shipper is strictly serial: ship one batch, await
    /// its ack, advance the cursor, ship the next. Raising it lets the
    /// shipper keep up to this many shipped-but-unacknowledged batches
    /// outstanding so transport round-trip latency is overlapped with
    /// draining the next batch, improving throughput on high-latency
    /// links. Defaults to <see cref="DefaultShipMaxInFlight"/>. Must be
    /// at least <c>1</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <strong>Ordering and cursor safety.</strong> Acks are consumed
    /// in strict FIFO order and the durable per-peer cursor advances
    /// past a batch only once that batch <em>and</em> every lower-HLC
    /// batch before it have been acknowledged - advance-strictly-on-ack
    /// with no cursor hole - so the per-origin FIFO invariant holds
    /// regardless of window size. A transport failure or ack rejection
    /// anywhere in the window stops the cursor advancing; the next tick
    /// re-ships from the durable cursor and the receiver dedupes the
    /// overlap.
    /// </para>
    /// <para>
    /// <strong>Receiver flow-control.</strong> When the receiver stamps
    /// a <see cref="ReplicationAck.SuggestedBatchSize"/> hint (asking the
    /// sender to slow down) the window collapses back to <c>1</c> until
    /// the receiver clears the hint, so a saturated receiver throttles
    /// both batch size and pipeline depth together. The live window
    /// depth is surfaced on the
    /// <see cref="LatticeReplicationMetrics.ShipInFlightName"/> gauge.
    /// </para>
    /// <para>
    /// <strong>Transport contract.</strong> A window &gt; 1 issues
    /// concurrent <see cref="IReplicationTransport.SendAsync"/> calls for
    /// the same <c>(tree, peer)</c> pair, so a transport used with
    /// pipelining must tolerate concurrent invocation against one pair.
    /// The default of <c>1</c> preserves the strictly-serial-per-pair
    /// behaviour for transports that do not.
    /// </para>
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
    /// Maximum interval between successful outbound contacts with a
    /// peer. When the per-<c>(tree, peer)</c> shipper grain's pump
    /// tick finds the drain buffer empty AND the wall-clock interval
    /// since the last successful contact equals or exceeds this
    /// value, the shipper sends an empty
    /// <see cref="ReplicationBatch"/> as a liveness probe. The peer
    /// acks the empty batch and the standard success-recording path
    /// runs, so the outbound
    /// <c>peer.last_contact_seconds{direction="outbound"}</c> gauge
    /// resets and no longer climbs unbounded between local-write
    /// bursts on a healthy idle link.
    /// <para>
    /// Defaults to <see cref="DefaultLivenessProbeInterval"/>. Set
    /// to <see cref="System.Threading.Timeout.InfiniteTimeSpan"/>
    /// to disable the empty-tick probe entirely; any other value
    /// must be strictly greater than <see cref="TimeSpan.Zero"/>.
    /// </para>
    /// </summary>
    public TimeSpan LivenessProbeInterval { get; set; } = DefaultLivenessProbeInterval;

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
    /// Master switch for the anti-entropy peer digest-probe scheduler -
    /// the first detection stage of the cross-cluster reconciliation
    /// chain. When <see langword="true"/>, a low-frequency per-tree
    /// scheduler reads each shard's local
    /// <see cref="ILattice.GetLeafProjectionDigestAsync(int, CancellationToken)"/>
    /// and compares it against every peer's digest fetched over the
    /// replication push transport, emitting the
    /// <see cref="LatticeReplicationMetrics.DigestProbeCompared"/> and
    /// <see cref="LatticeReplicationMetrics.DigestProbeMismatch"/>
    /// counters. The probe is strictly read-only: it never mutates data
    /// or advances any cursor.
    /// <para>
    /// Defaults to <see cref="DefaultDigestProbeEnabled"/>
    /// (<see langword="false"/>). The detection feature ships dark so it
    /// does not change replication behaviour for a host that has not
    /// opted in; enabling it adds only the periodic read-and-compare
    /// telemetry pass.
    /// </para>
    /// </summary>
    public bool DigestProbeEnabled { get; set; } = DefaultDigestProbeEnabled;

    /// <summary>
    /// Base cadence at which the anti-entropy digest-probe scheduler runs
    /// a comparison pass for each replicated tree. Each pass is jittered
    /// by <see cref="DigestProbeJitter"/> so a fleet of silos does not
    /// probe in lock-step. Only consulted when
    /// <see cref="DigestProbeEnabled"/> is <see langword="true"/>.
    /// Defaults to <see cref="DefaultDigestProbeInterval"/> (5 minutes).
    /// Must be strictly greater than <see cref="TimeSpan.Zero"/>.
    /// </summary>
    public TimeSpan DigestProbeInterval { get; set; } = DefaultDigestProbeInterval;

    /// <summary>
    /// Multiplicative jitter applied to <see cref="DigestProbeInterval"/>
    /// so peers do not probe in lock-step. Each effective interval is
    /// scaled by a random factor in <c>[1.0 - jitter, 1.0 + jitter]</c>.
    /// Defaults to <see cref="DefaultDigestProbeJitter"/> (20% spread).
    /// Must be in <c>[0.0, 1.0]</c>; <c>0.0</c> disables jitter entirely.
    /// </summary>
    public double DigestProbeJitter { get; set; } = DefaultDigestProbeJitter;

    /// <summary>
    /// Master switch for the read-only anti-entropy Merkle-walk drift
    /// localisation pass - the localise stage that follows the digest probe's
    /// detect stage. When <see langword="true"/> and
    /// <see cref="DigestProbeEnabled"/> is also <see langword="true"/>, a
    /// shard-level digest mismatch found by the probe triggers a top-down walk
    /// of the shard's internal-node tree by separator-key range, narrowing the
    /// divergence to a single leaf or a small set of leaves and emitting the
    /// <see cref="LatticeReplicationMetrics.MerkleWalkLocalised"/> and
    /// <see cref="LatticeReplicationMetrics.MerkleWalkAborted"/> counters. The
    /// walk is strictly read-only: it never mutates data or advances any
    /// cursor, and it never attempts repair.
    /// <para>
    /// Defaults to <see cref="DefaultMerkleWalkEnabled"/>
    /// (<see langword="false"/>). The localisation feature ships dark and
    /// opt-in - it runs only when the probe is enabled, the probe reports a
    /// mismatch, and this flag is set - so an un-opted host pays nothing.
    /// </para>
    /// </summary>
    public bool MerkleWalkEnabled { get; set; } = DefaultMerkleWalkEnabled;

    /// <summary>
    /// Maximum recursion depth the Merkle-walk localisation pass descends into
    /// a shard's internal-node tree before aborting with reason
    /// <see cref="LatticeReplicationMetrics.MerkleWalkAbortDepthCap"/>. The
    /// shard root is depth <c>0</c>; each descended level adds one. Bounds the
    /// worst-case number of grain hops and remote probes a single localisation
    /// can issue against a pathologically deep or skewed tree. Only consulted
    /// when <see cref="MerkleWalkEnabled"/> is <see langword="true"/>.
    /// Defaults to <see cref="DefaultMerkleWalkMaxDepth"/>. Must be at least
    /// <c>1</c>; the registered options validator rejects non-positive values
    /// at first-resolve time.
    /// </summary>
    public int MerkleWalkMaxDepth { get; set; } = DefaultMerkleWalkMaxDepth;

    /// <summary>
    /// Maximum cumulative number of digest hash bytes the Merkle-walk
    /// localisation pass inspects (summed across every local and remote digest
    /// it compares) before aborting with reason
    /// <see cref="LatticeReplicationMetrics.MerkleWalkAbortByteBudget"/>. Bounds
    /// the work a single localisation can do against a high-fan-out tree
    /// independently of the depth cap. Only consulted when
    /// <see cref="MerkleWalkEnabled"/> is <see langword="true"/>. Defaults to
    /// <see cref="DefaultMerkleWalkMaxBytes"/>. Must be at least <c>1</c>; the
    /// registered options validator rejects non-positive values at
    /// first-resolve time.
    /// </summary>
    public long MerkleWalkMaxBytes { get; set; } = DefaultMerkleWalkMaxBytes;

    /// <summary>
    /// Master switch for the targeted leaf re-replay repair stage - the repair
    /// step that follows the read-only Merkle-walk localise stage. When
    /// <see langword="true"/> (and the digest probe found a mismatch,
    /// <see cref="MerkleWalkEnabled"/> is also <see langword="true"/>, and the
    /// walk localised at least one diverging leaf), the localised leaf
    /// <c>[StartKey, EndKey)</c> covering ranges are used to select retained
    /// write-ahead-log entries above the diverged peer's high-water-mark cursor
    /// and re-ship them to that peer through the ordinary causal-stable apply
    /// pipeline, so the repair travels the same TX-aware path as ordinary
    /// replication and respects atomic-batch boundaries. Re-shipped entries
    /// carry their source clock verbatim and are deduplicated at the receiver,
    /// so re-sending is idempotent.
    /// <para>
    /// Defaults to <see cref="DefaultLeafReReplayEnabled"/>
    /// (<see langword="false"/>). The repair feature ships dark and opt-in - it
    /// runs only when localisation is enabled and reports a localised leaf and
    /// this flag is set - so an un-opted host pays nothing and observes no
    /// behaviour change.
    /// </para>
    /// </summary>
    public bool LeafReReplayEnabled { get; set; } = DefaultLeafReReplayEnabled;

    /// <summary>
    /// Maximum number of write-ahead-log entries a single targeted leaf
    /// re-replay pass re-ships to the diverged peer. A soft cap: whole
    /// atomic-batch units are added until the next unit would exceed this
    /// ceiling, but at least one unit is always shipped and an atomic batch is
    /// never split across the boundary. Bounds the repair amplification a
    /// single localisation can produce. Only consulted when
    /// <see cref="LeafReReplayEnabled"/> is <see langword="true"/>. Defaults to
    /// <see cref="DefaultLeafReReplayMaxEntries"/>. Must be at least <c>1</c>;
    /// the registered options validator rejects non-positive values at
    /// first-resolve time.
    /// </summary>
    public int LeafReReplayMaxEntries { get; set; } = DefaultLeafReReplayMaxEntries;

    /// <summary>
    /// Maximum cumulative encoded-payload byte count a single targeted leaf
    /// re-replay pass re-ships to the diverged peer. A soft cap applied with
    /// the same whole-atomic-batch-unit semantics as
    /// <see cref="LeafReReplayMaxEntries"/>. Bounds the repair bandwidth a
    /// single localisation can produce independently of the entry-count cap.
    /// Only consulted when <see cref="LeafReReplayEnabled"/> is
    /// <see langword="true"/>. Defaults to
    /// <see cref="DefaultLeafReReplayMaxBytes"/>. Must be at least <c>1</c>;
    /// the registered options validator rejects non-positive values at
    /// first-resolve time.
    /// </summary>
    public long LeafReReplayMaxBytes { get; set; } = DefaultLeafReReplayMaxBytes;

    /// <summary>
    /// Master switch for the bootstrap-snapshot fallback - the
    /// garbage-collected-divergence repair step that follows a targeted leaf
    /// re-replay which could not reach the divergence point because the local
    /// write-ahead log had been trimmed past it (the
    /// <see cref="LeafReReplaySkipReason.WalTrimmed"/> signal). When
    /// <see langword="true"/> (and the re-replay reported
    /// <see cref="LeafReReplaySkipReason.WalTrimmed"/> and at least one leaf
    /// range was localised), the fallback re-derives the committed projection
    /// of just the divergent leaf range from the live tree via the range-scoped
    /// <see cref="ISnapshotProvider.ExportAsync(string, IReadOnlyList{LeafReReplayRange}, Orleans.Lattice.HybridLogicalClock, CancellationToken)"/>
    /// overload (the live tree is immune to WAL trimming) and re-ships those
    /// committed entries to the diverged peer through the ordinary replication
    /// transport, so the repair travels the same causal-stable apply path as
    /// ordinary replication and the receiver dedupes by the verbatim source
    /// clock. Snapshot scope is bounded to the localised leaf ranges, so the
    /// repair cost is proportional to the drift, not the whole tree.
    /// <para>
    /// Defaults to <see cref="DefaultBootstrapFallbackEnabled"/>
    /// (<see langword="false"/>). The fallback ships dark and opt-in - it runs
    /// only when targeted leaf re-replay is also enabled, the re-replay hits a
    /// trimmed WAL, and this flag is set - so an un-opted host pays nothing and
    /// observes no behaviour change. When the WAL-trimmed signal fires while
    /// this flag is off, a single
    /// <see cref="LatticeReplicationMetrics.BootstrapFallbackSkipped"/> count
    /// with reason <see cref="LatticeReplicationMetrics.BootstrapFallbackSkipDisabled"/>
    /// is emitted so operators can see the fallback was available but not taken.
    /// </para>
    /// </summary>
    public bool BootstrapFallbackEnabled { get; set; } = DefaultBootstrapFallbackEnabled;

    /// <summary>
    /// Maximum number of committed-projection snapshot entries a single
    /// bootstrap-snapshot fallback pass re-ships to the diverged peer. A soft
    /// cap: the fallback re-ships entries until the next entry would exceed this
    /// ceiling, but always ships at least one entry. Bounds the repair
    /// amplification a single GC'd-divergence fallback can produce. Only
    /// consulted when <see cref="BootstrapFallbackEnabled"/> is
    /// <see langword="true"/>. Defaults to
    /// <see cref="DefaultBootstrapFallbackMaxEntries"/>. Must be at least
    /// <c>1</c>; the registered options validator rejects non-positive values
    /// at first-resolve time.
    /// </summary>
    public int BootstrapFallbackMaxEntries { get; set; } = DefaultBootstrapFallbackMaxEntries;

    /// <summary>
    /// Maximum cumulative estimated payload byte count a single
    /// bootstrap-snapshot fallback pass re-ships to the diverged peer. A soft
    /// cap applied with the same always-ship-at-least-one semantics as
    /// <see cref="BootstrapFallbackMaxEntries"/>. Bounds the repair bandwidth a
    /// single GC'd-divergence fallback can produce independently of the
    /// entry-count cap. Only consulted when
    /// <see cref="BootstrapFallbackEnabled"/> is <see langword="true"/>.
    /// Defaults to <see cref="DefaultBootstrapFallbackMaxBytes"/>. Must be at
    /// least <c>1</c>; the registered options validator rejects non-positive
    /// values at first-resolve time.
    /// </summary>
    public long BootstrapFallbackMaxBytes { get; set; } = DefaultBootstrapFallbackMaxBytes;

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
    /// Stable id of the shared compression dictionary the shipper
    /// requests when <see cref="FramingCompression"/> is
    /// <see cref="LatticeCompression.ZstdDictionary"/>. The id is
    /// resolved to dictionary bytes by the registered
    /// <see cref="ILatticeCompressionDictionaryProvider"/> and carried
    /// in the framed tail so the receiver selects the matching
    /// dictionary. The reserved value <c>0</c> (the default) means "no
    /// dictionary": a shipper configured for
    /// <see cref="LatticeCompression.ZstdDictionary"/> with id <c>0</c>
    /// gracefully degrades to plain dictionary-less
    /// <see cref="LatticeCompression.Zstd"/>, so a default build is
    /// byte-identical on the wire. Defaults to
    /// <see cref="DefaultFramingCompressionDictionaryId"/>.
    /// <para>
    /// Like every other compression knob this requires a coordinated
    /// rollout: the dictionary bytes behind this id must be registered
    /// (via the same operator-supplied provider configuration) on every
    /// receiver before any sender flips to a non-zero id. A receiver
    /// that cannot resolve the id surfaces
    /// <see cref="NotSupportedException"/> from the framing decoder and
    /// routes through the existing transient-backoff path; an encoder
    /// that cannot resolve the id locally degrades to plain Zstd rather
    /// than emitting an unreadable frame.
    /// </para>
    /// </summary>
    public uint FramingCompressionDictionaryId { get; set; } = DefaultFramingCompressionDictionaryId;

    /// <summary>
    /// Opts in to per-peer shared-dictionary capability negotiation on the
    /// outbound ship path. When <see langword="true"/> and the tree is
    /// configured for <see cref="LatticeCompression.ZstdDictionary"/> with a
    /// non-zero <see cref="FramingCompressionDictionaryId"/>, the
    /// per-<c>(tree, peer)</c> shipper reads each peer's advertised
    /// <see cref="ReplicationAck.AdvertisedDictionaryIds"/> from its acks and
    /// negotiates the effective dictionary id via
    /// <see cref="SharedDictionaryNegotiation.Negotiate(uint, System.Collections.Generic.IReadOnlyCollection{uint})"/>:
    /// it compresses with the configured dictionary id only for a peer that
    /// has advertised that id, and otherwise falls back to dictionary-less
    /// <see cref="LatticeCompression.Zstd"/> for that peer. This guarantees a
    /// sender never ships a frame compressed with a dictionary the target
    /// peer cannot resolve, so mixed fleets where some peers lack the
    /// dictionary keep working during a rolling upgrade. The per-peer
    /// negotiated state is activation-scoped and refreshed on every ack, so
    /// it adapts when a peer reconnects or changes its advertised capability.
    /// The negotiation outcome and the share of batches shipped with versus
    /// without a shared dictionary are published to the
    /// <c>ship.dictionary_negotiation</c> and <c>ship.dictionary_batches</c>
    /// counters.
    /// <para>
    /// Defaults to <see langword="false"/>: negotiation is off and the
    /// shipper stamps the configured dictionary id exactly as it did before
    /// this option existed - the bytes on the wire are byte-identical for
    /// hosts that do not opt in.
    /// </para>
    /// </summary>
    public bool DictionaryNegotiationEnabled { get; set; } = DefaultDictionaryNegotiationEnabled;

    /// <summary>
    /// Opts in to wire-version capability negotiation on the outbound
    /// ship path. When <see langword="true"/>, the per-<c>(tree, peer)</c>
    /// shipper reads each peer's advertised
    /// <see cref="ReplicationAck.SupportedWireVersion"/> from its acks
    /// and computes the negotiated target framing wire version
    /// via <see cref="WireVersionNegotiation.Negotiate(int, int, int, int?)"/>:
    /// <c>min(localCurrent, peerAdvertised)</c> once the peer's
    /// capability is known, or <see cref="UnknownPeerWireVersionFloor"/>
    /// until it advertises one. A peer that advertises a version below
    /// <see cref="MinimumSupportedWireVersion"/> surfaces the canonical
    /// fail-fast hard error (the genuinely-unsupported case). The
    /// negotiated target and a downgrade-active signal are published
    /// to the <c>wire_version.negotiated</c> and
    /// <c>wire_version.downgrade_active</c> gauges so a mixed-version
    /// fleet is observable during a rolling upgrade. When the negotiated
    /// target is below the current wire version the shipper down-stamps
    /// the outbound framing header to that target via
    /// <see cref="WireVersionDownEncoder"/> so a not-yet-upgraded
    /// receiver decodes and applies the frame; when the target equals
    /// the current version the verbatim pre-encoded entry hot path is
    /// preserved with zero re-encode cost (a true same-version no-op).
    /// A negotiated down-stamp this build cannot produce for the batch's
    /// shape - a CRDT-mode tree (whose per-entry merge dispatch depends
    /// on the wire-version-5 hoisted header mode an older receiver cannot
    /// read) or a compression-configured tree (whose compressor an older
    /// receiver is not guaranteed to carry) - surfaces the same fail-fast
    /// hard error on the ship path rather than emitting a frame the older
    /// peer would mis-apply.
    /// <para>
    /// Defaults to <see langword="false"/>: negotiation is off and the
    /// shipper encodes every batch at
    /// <see cref="EncodedBatchHeader.CurrentWireVersion"/> exactly as it
    /// did before this option existed - no behavioural change for hosts
    /// that do not opt in.
    /// </para>
    /// </summary>
    public bool WireVersionNegotiationEnabled { get; set; } = DefaultWireVersionNegotiationEnabled;

    /// <summary>
    /// The oldest framing wire version the local sender is willing to
    /// interoperate with when
    /// <see cref="WireVersionNegotiationEnabled"/> is set. A peer that
    /// advertises a <see cref="ReplicationAck.SupportedWireVersion"/>
    /// strictly below this value cannot be down-encoded for and
    /// surfaces a fail-fast error on the ship path. Must lie in the
    /// closed interval <c>[1, EncodedBatchHeader.CurrentWireVersion]</c>.
    /// Note that the sender can only actually down-stamp a frame as far
    /// back as <see cref="WireVersionDownEncoder.MinimumDownEncodableWireVersion"/>
    /// (older receivers expect per-entry field shapes this build no
    /// longer carries on the encoded entry segments); a peer that
    /// advertises a version in the half-open interval
    /// <c>[MinimumSupportedWireVersion, MinimumDownEncodableWireVersion)</c>
    /// still surfaces a fail-fast error on the ship path rather than
    /// receiving a frame it cannot decode.
    /// Defaults to <see cref="DefaultMinimumSupportedWireVersion"/>.
    /// </summary>
    public int MinimumSupportedWireVersion { get; set; } = DefaultMinimumSupportedWireVersion;

    /// <summary>
    /// The conservative framing wire version the local sender targets
    /// for a peer whose capability is not yet known (no ack has
    /// advertised a <see cref="ReplicationAck.SupportedWireVersion"/>
    /// yet) while <see cref="WireVersionNegotiationEnabled"/> is set.
    /// Must lie in the closed interval
    /// <c>[MinimumSupportedWireVersion, EncodedBatchHeader.CurrentWireVersion]</c>.
    /// Defaults to <see cref="EncodedBatchHeader.CurrentWireVersion"/>
    /// (<see cref="DefaultUnknownPeerWireVersionFloor"/>) so the
    /// negotiated target for the first batches before any ack is the
    /// current version, as it is today; hosts performing a heterogeneous
    /// rolling upgrade can lower this so the negotiated target for
    /// un-acked first batches is conservative.
    /// </summary>
    public int UnknownPeerWireVersionFloor { get; set; } = DefaultUnknownPeerWireVersionFloor;

    /// <summary>
    /// Whether the per-<c>(tree, peer)</c> shipper grain adapts its
    /// effective outbound batch size below the configured
    /// <see cref="ShipBatchSize"/> ceiling using a sender-side
    /// additive-increase / multiplicative-decrease (AIMD) controller
    /// driven by measured ack latency and error rate. Defaults to
    /// <see langword="false"/>: with the flag off the shipper sizes
    /// every batch exactly as it does today (at <see cref="ShipBatchSize"/>,
    /// modulated only downward by an active receiver flow-control hint),
    /// so steady-state behaviour is byte-identical to the static path.
    /// <para>
    /// When enabled, the shipper grows the effective batch size additively
    /// toward <see cref="ShipBatchSize"/> while acks stay below
    /// <see cref="AdaptiveBatchLatencyThreshold"/>, and backs it off
    /// multiplicatively (by <see cref="AdaptiveBatchDecreaseFactor"/>) when
    /// ack latency rises above the threshold or a send fails - *before*
    /// the receiver has to raise its WAL-saturation hint. The adaptive
    /// size only ever operates in the headroom beneath the receiver hint:
    /// the effective per-tick cap is
    /// <c>min(adaptive size, receiver-suggested size, ShipBatchSize)</c>,
    /// floored at <c>1</c>, so the receiver flow-control hint
    /// (<see cref="ReplicationAck.SuggestedBatchSize"/>) remains the hard
    /// upper bound and always wins. The adaptation never reorders work,
    /// never crosses a batch boundary, and never affects the per-origin
    /// FIFO or advance-strictly-on-ack cursor semantics; it only shrinks
    /// or grows the per-tick entry cap.
    /// </para>
    /// <para>
    /// Controller state is in-memory and activation-scoped (per
    /// <c>(tree, peer)</c> shipper activation); a grain re-activation
    /// resets the effective size to <see cref="ShipBatchSize"/> and the
    /// controller re-learns from the live link. Off by default until the
    /// adaptive path has been validated against the replication chaos
    /// fixtures.
    /// </para>
    /// </summary>
    public bool AdaptiveBatchSizingEnabled { get; set; } = DefaultAdaptiveBatchSizingEnabled;

    /// <summary>
    /// Additive-increase step the adaptive batch-size controller adds to
    /// the effective batch size on each ack whose measured latency is at
    /// or below <see cref="AdaptiveBatchLatencyThreshold"/>, capped at
    /// <see cref="ShipBatchSize"/>. Only consulted when
    /// <see cref="AdaptiveBatchSizingEnabled"/> is <see langword="true"/>.
    /// Larger values re-accelerate faster after a back-off at the cost of
    /// a coarser approach to the ceiling. Defaults to
    /// <see cref="DefaultAdaptiveBatchIncrement"/>. Must be at least
    /// <c>1</c>; the registered options validator rejects non-positive
    /// values at first-resolve time.
    /// </summary>
    public int AdaptiveBatchIncrement { get; set; } = DefaultAdaptiveBatchIncrement;

    /// <summary>
    /// Multiplicative-decrease factor the adaptive batch-size controller
    /// multiplies the effective batch size by when ack latency rises above
    /// <see cref="AdaptiveBatchLatencyThreshold"/> or a send fails (the
    /// "MD" half of AIMD), floored at <c>1</c>. Only consulted when
    /// <see cref="AdaptiveBatchSizingEnabled"/> is <see langword="true"/>.
    /// Must be strictly greater than <c>0.0</c> and strictly less than
    /// <c>1.0</c>; the registered options validator rejects values outside
    /// the open interval <c>(0.0, 1.0)</c> at first-resolve time. Smaller
    /// values back off more aggressively. Defaults to
    /// <see cref="DefaultAdaptiveBatchDecreaseFactor"/> (halving).
    /// </summary>
    public double AdaptiveBatchDecreaseFactor { get; set; } = DefaultAdaptiveBatchDecreaseFactor;

    /// <summary>
    /// Ack-latency threshold the adaptive batch-size controller compares
    /// the sliding-window mean ack latency against to decide between
    /// additive increase (mean at or below the threshold) and
    /// multiplicative decrease (mean above it). Only consulted when
    /// <see cref="AdaptiveBatchSizingEnabled"/> is <see langword="true"/>.
    /// Set this near the steady-state per-batch ack round-trip the link is
    /// expected to sustain; a rising mean above it is the early
    /// back-pressure signal the controller acts on before the receiver
    /// raises its WAL-saturation hint. Defaults to
    /// <see cref="DefaultAdaptiveBatchLatencyThreshold"/>. Must be strictly
    /// greater than <see cref="TimeSpan.Zero"/>.
    /// </summary>
    public TimeSpan AdaptiveBatchLatencyThreshold { get; set; } = DefaultAdaptiveBatchLatencyThreshold;

    /// <summary>
    /// Number of recent ack latencies the adaptive batch-size controller
    /// averages over its sliding window when deciding increase vs.
    /// decrease. Only consulted when
    /// <see cref="AdaptiveBatchSizingEnabled"/> is <see langword="true"/>.
    /// A longer window smooths transient latency spikes at the cost of a
    /// slower reaction to a sustained shift; a shorter window reacts faster
    /// but is noisier. Defaults to
    /// <see cref="DefaultAdaptiveBatchWindowLength"/>. Must be at least
    /// <c>1</c>; the registered options validator rejects non-positive
    /// values at first-resolve time.
    /// </summary>
    public int AdaptiveBatchWindowLength { get; set; } = DefaultAdaptiveBatchWindowLength;

    /// <summary>
    /// Master gate for <em>all</em> automatic anti-entropy remediation - both
    /// the targeted leaf re-replay repair stage and the GC'd-divergence
    /// bootstrap-snapshot fallback. When <see langword="false"/> (the default),
    /// a localised drift is still detected and probed exactly as before (the
    /// digest probe and the read-only Merkle walk are unaffected), but no
    /// automatic re-replay or bootstrap-fallback re-ship is attempted - the
    /// repair stage records a single skip-with-reason
    /// <see cref="LatticeReplicationMetrics.DigestRemediationReasonOptOut"/>
    /// signal and the <see cref="LatticeReplicationMetrics.DigestRemediationDisabledName"/>
    /// gauge reports the disabled state for the affected <c>(tree, peer)</c>.
    /// <para>
    /// This flag is an additional AND-gate in front of the repair stage: the
    /// existing per-feature flags (<see cref="MerkleWalkEnabled"/>,
    /// <see cref="LeafReReplayEnabled"/>, <see cref="BootstrapFallbackEnabled"/>)
    /// still apply on top of it. Detection (the digest probe and the Merkle
    /// walk) is intentionally <em>not</em> gated by this flag so an operator can
    /// observe drift telemetry without having opted into automatic repair.
    /// </para>
    /// <para>
    /// Defaults to <see cref="DefaultAutoRemediateOnDigestMismatch"/>
    /// (<see langword="false"/>). The remediation guards ship dark and opt-in so
    /// an un-opted host observes no behaviour change.
    /// </para>
    /// </summary>
    public bool AutoRemediateOnDigestMismatch { get; set; } = DefaultAutoRemediateOnDigestMismatch;

    /// <summary>
    /// Fraction of the per-tick <see cref="ShipBatchSize"/> entry budget that a
    /// single <c>(tree, peer)</c> may spend on automatic anti-entropy
    /// remediation traffic within each
    /// <see cref="RemediationTrafficWindow"/> accounting window. The effective
    /// per-window entry budget is
    /// <c>max(1, ceil(RemediationTrafficBudgetFraction * ShipBatchSize))</c>, so
    /// remediation re-ship volume is rate-limited to a small fraction of the
    /// ordinary ship-batch budget. Once a <c>(tree, peer)</c> has already
    /// re-shipped at least its budget within the current window, further
    /// remediation passes for that pair are skipped (recording a
    /// <see cref="LatticeReplicationMetrics.DigestRemediationReasonBudgetExhausted"/>
    /// signal and reporting the disabled gauge) until the window rolls over. The
    /// first pass in a fresh window always runs, so a single large repair burst
    /// is permitted and the cap bounds how often such bursts may recur.
    /// <para>
    /// Only consulted when <see cref="AutoRemediateOnDigestMismatch"/> is
    /// <see langword="true"/>. Defaults to
    /// <see cref="DefaultRemediationTrafficBudgetFraction"/> (1% of
    /// <see cref="ShipBatchSize"/>). Must be in the half-open interval
    /// <c>(0.0, 1.0]</c>; the registered options validator rejects values at or
    /// below <c>0</c> or above <c>1</c> at first-resolve time.
    /// </para>
    /// </summary>
    public double RemediationTrafficBudgetFraction { get; set; } = DefaultRemediationTrafficBudgetFraction;

    /// <summary>
    /// Length of the deterministic accounting window over which the per-tree,
    /// per-peer remediation traffic budget
    /// (<see cref="RemediationTrafficBudgetFraction"/>) is measured. The
    /// consumed-entry counter for each <c>(tree, peer)</c> resets to zero the
    /// first time a remediation pass is evaluated at or after this much
    /// wall-clock time has elapsed since the window opened. A shorter window
    /// lets remediation recur more often; a longer window throttles it harder.
    /// Only consulted when <see cref="AutoRemediateOnDigestMismatch"/> is
    /// <see langword="true"/>. Defaults to
    /// <see cref="DefaultRemediationTrafficWindow"/> (1 minute). Must be strictly
    /// greater than <see cref="TimeSpan.Zero"/>; the registered options
    /// validator rejects non-positive values at first-resolve time.
    /// </summary>
    public TimeSpan RemediationTrafficWindow { get; set; } = DefaultRemediationTrafficWindow;

    /// <summary>
    /// Number of consecutive automatic-remediation failures for a single
    /// <c>(tree, peer)</c> that trips the remediation circuit breaker open.
    /// While the breaker is open, automatic remediation for that pair is skipped
    /// (recording a
    /// <see cref="LatticeReplicationMetrics.DigestRemediationReasonCircuitOpen"/>
    /// signal and reporting the disabled gauge) until the
    /// <see cref="RemediationCircuitResetInterval"/> cooldown elapses, after
    /// which one half-open trial pass is allowed. A successful pass at any point
    /// resets the consecutive-failure count and closes the breaker; a failed
    /// half-open trial re-opens it for another cooldown. A "failure" is a
    /// remediation pass that threw or whose re-ship sink reported zero entries
    /// shipped despite candidate entries having been selected.
    /// <para>
    /// Only consulted when <see cref="AutoRemediateOnDigestMismatch"/> is
    /// <see langword="true"/>. Defaults to
    /// <see cref="DefaultRemediationFailureThreshold"/>. Must be at least
    /// <c>1</c>; the registered options validator rejects non-positive values at
    /// first-resolve time (a value of <c>1</c> opens the breaker on the first
    /// failure).
    /// </para>
    /// </summary>
    public int RemediationFailureThreshold { get; set; } = DefaultRemediationFailureThreshold;

    /// <summary>
    /// Cooldown the remediation circuit breaker stays open for after it trips on
    /// <see cref="RemediationFailureThreshold"/> consecutive failures for a
    /// <c>(tree, peer)</c>. Once this much wall-clock time has elapsed since the
    /// breaker opened (or since the most recent failed half-open trial), the
    /// breaker moves to half-open and the next remediation evaluation is allowed
    /// to run one trial pass: success closes the breaker and clears the disabled
    /// gauge, failure re-opens it for a fresh cooldown. Only consulted when
    /// <see cref="AutoRemediateOnDigestMismatch"/> is <see langword="true"/>.
    /// Defaults to <see cref="DefaultRemediationCircuitResetInterval"/>
    /// (5 minutes). Must be strictly greater than <see cref="TimeSpan.Zero"/>;
    /// the registered options validator rejects non-positive values at
    /// first-resolve time.
    /// </summary>
    public TimeSpan RemediationCircuitResetInterval { get; set; } = DefaultRemediationCircuitResetInterval;


    /// <summary>
    /// Default value for <see cref="ClusterId"/>: an empty sentinel that
    /// represents "unset". This default is rejected by
    /// <c>LatticeReplicationOptionsValidator</c> so a host that calls
    /// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>
    /// without supplying a cluster id fails fast on first options resolution.
    /// </summary>
    public const string DefaultClusterId = "";

    /// <summary>
    /// Default value for <see cref="ReplogPartitions"/>: matches the
    /// core's <c>LatticeOptions.DefaultWalPartitions</c> (8). The
    /// replication shipper iterates <c>[0, ReplogPartitions)</c> per
    /// pump tick, so this value must equal the routing-truth
    /// <c>WalPartitions</c> count or the shipper will miss writes
    /// authored against partitions <c>[ReplogPartitions, WalPartitions)</c>.
    /// Adequate for low-fan-in workloads; raise for hot trees that
    /// benefit from parallel WAL append paths. Hosts that explicitly
    /// configure <see cref="LatticeOptions.WalPartitions"/> get the
    /// reverse-mirrored value on this option via
    /// <c>LatticeReplicationServiceCollectionExtensions</c>'s
    /// post-configure step, so this default only applies when neither
    /// option is touched.
    /// </summary>
    public const int DefaultReplogPartitions = 8;

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
    /// Default value for <see cref="ApplyMaxParallelRuns"/>: <c>1</c>,
    /// i.e. fully-sequential receiver apply identical to the behaviour
    /// before cross-tree parallel apply landed. The conservative
    /// default is deliberate: enabling parallel apply is an opt-in
    /// per-workload decision, so a host that never touches the option
    /// gets the exact historical apply ordering and concurrency.
    /// </summary>
    public const int DefaultApplyMaxParallelRuns = 1;

    /// <summary>
    /// Default value for <see cref="ContentHashDedupEnabled"/>: the
    /// content-hash payload-re-send measurement is off so the shipper's
    /// behaviour and on-the-wire output are byte-identical to a build
    /// without the option. Operators opt in only when they suspect
    /// idempotent upstream retries are inflating replication bandwidth
    /// and want to measure the re-send rate.
    /// </summary>
    public const bool DefaultContentHashDedupEnabled = false;

    /// <summary>
    /// Default value for <see cref="ContentHashDedupCacheSize"/>: 4096
    /// retained keys per <c>(tree, peer)</c>. Sized to track the
    /// re-send rate across a comfortable hot-key working set while
    /// keeping the per-shipper memory footprint bounded (~64 KB per
    /// shipper at typical key sizes - one key string plus an 8-byte
    /// digest per slot).
    /// </summary>
    public const int DefaultContentHashDedupCacheSize = 4096;

    /// <summary>
    /// Default value for <see cref="ContentHashDedupElisionEnabled"/>: the
    /// content-hash payload-elision round trip is off so the shipper never
    /// attempts a manifest exchange and its on-the-wire output is
    /// byte-identical to a build without the option. Operators opt in only
    /// after the re-send-rate measurement
    /// (<see cref="ContentHashDedupEnabled"/>) shows the redundant-payload
    /// rate justifies the extra round trip.
    /// </summary>
    public const bool DefaultContentHashDedupElisionEnabled = false;

    /// <summary>
    /// Default value for <see cref="PreShipCoalescingEnabled"/>: pre-ship
    /// coalescing is off so the shipper's drain / ship path and its
    /// on-the-wire output are byte-identical to a build without the
    /// option. Operators opt in per tree once they have a hot
    /// last-writer-wins key that is rewritten several times within a
    /// single ship window and want to collapse the redundant versions
    /// off the cross-cluster link.
    /// </summary>
    public const bool DefaultPreShipCoalescingEnabled = false;

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
    /// Default value for <see cref="ShipCursorWriteMaxDelay"/>: 2 seconds.
    /// On a high-throughput stream the batch-count rule
    /// (<see cref="DefaultShipCursorWriteInterval"/>=16) fires long before
    /// this elapses, so the default is inert on the hot path; it engages
    /// only when a stream ships fewer than
    /// <see cref="ShipCursorWriteInterval"/> batches and then quiesces,
    /// bounding how stale the durable cursor (and therefore the WAL GC
    /// trim frontier) can become to a couple of seconds. The value sits
    /// well above the canonical per-batch ack RTT (~10 ms) so a steady
    /// stream never trips the time dimension, yet low enough that an idle
    /// stream's last few advances are durable within an interactive
    /// window. Set to
    /// <see cref="System.Threading.Timeout.InfiniteTimeSpan"/> to disable
    /// the time dimension and coalesce purely by
    /// <see cref="ShipCursorWriteInterval"/>.
    /// </summary>
    public static readonly TimeSpan DefaultShipCursorWriteMaxDelay = TimeSpan.FromSeconds(2);

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
    /// Default value for <see cref="LivenessProbeInterval"/>: 30 s.
    /// Aligned with the maintenance fall-off probe cadence so an
    /// idle but healthy outbound link refreshes its
    /// <c>peer.last_contact_seconds{direction="outbound"}</c> gauge
    /// at least once per maintenance cycle.
    /// </summary>
    public static readonly TimeSpan DefaultLivenessProbeInterval = TimeSpan.FromSeconds(30);

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
    /// Default value for <see cref="DigestProbeEnabled"/>:
    /// <see langword="false"/>. The anti-entropy detection feature ships
    /// dark and opt-in so it does not change replication behaviour for a
    /// host that has not enabled it.
    /// </summary>
    public const bool DefaultDigestProbeEnabled = false;

    /// <summary>
    /// Default value for <see cref="DigestProbeInterval"/>: 5 minutes.
    /// A deliberately low frequency - the probe is a background drift
    /// detector, not a hot-path check.
    /// </summary>
    public static readonly TimeSpan DefaultDigestProbeInterval = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Default value for <see cref="DigestProbeJitter"/>: 20 % spread
    /// each side of the nominal interval.
    /// </summary>
    public const double DefaultDigestProbeJitter = 0.2;

    /// <summary>
    /// Default value for <see cref="MerkleWalkEnabled"/>:
    /// <see langword="false"/>. The drift-localisation feature ships dark and
    /// opt-in so it does not change replication behaviour for a host that has
    /// not enabled it.
    /// </summary>
    public const bool DefaultMerkleWalkEnabled = false;

    /// <summary>
    /// Default value for <see cref="MerkleWalkMaxDepth"/>: 16 levels. A B+ tree
    /// with a realistic fan-out reaches billions of leaves well within this
    /// depth, so the cap only bites on a pathologically skewed tree while still
    /// bounding the worst-case walk.
    /// </summary>
    public const int DefaultMerkleWalkMaxDepth = 16;

    /// <summary>
    /// Default value for <see cref="MerkleWalkMaxBytes"/>: 1 MB of cumulative
    /// inspected digest bytes. At a 16-byte digest hash per node this allows on
    /// the order of tens of thousands of node comparisons per localisation -
    /// far more than a healthy walk needs - while still capping a pathological
    /// high-fan-out descent.
    /// </summary>
    public const long DefaultMerkleWalkMaxBytes = 1024L * 1024L;

    /// <summary>
    /// Default value for <see cref="LeafReReplayEnabled"/>:
    /// <see langword="false"/>. The targeted leaf re-replay repair feature
    /// ships dark and opt-in so it does not change replication behaviour for a
    /// host that has not enabled it.
    /// </summary>
    public const bool DefaultLeafReReplayEnabled = false;

    /// <summary>
    /// Default value for <see cref="LeafReReplayMaxEntries"/>: 4096 entries.
    /// Generous enough to repair a divergent leaf in a single pass under
    /// realistic fan-out while still bounding repair amplification on a
    /// pathologically large divergence.
    /// </summary>
    public const int DefaultLeafReReplayMaxEntries = 4096;

    /// <summary>
    /// Default value for <see cref="LeafReReplayMaxBytes"/>: 1 MB of cumulative
    /// re-shipped encoded payload. Bounds the repair bandwidth a single
    /// localisation can produce independently of the entry-count cap.
    /// </summary>
    public const long DefaultLeafReReplayMaxBytes = 1024L * 1024L;

    /// <summary>
    /// Default value for <see cref="BootstrapFallbackEnabled"/>:
    /// <see langword="false"/>. The GC'd-divergence bootstrap-snapshot fallback
    /// ships dark and opt-in so it does not change replication behaviour for a
    /// host that has not enabled it.
    /// </summary>
    public const bool DefaultBootstrapFallbackEnabled = false;

    /// <summary>
    /// Default value for <see cref="BootstrapFallbackMaxEntries"/>: 4096
    /// committed-projection entries. Generous enough to repair a divergent leaf
    /// range in a single pass under realistic fan-out while still bounding
    /// repair amplification on a pathologically large divergence. Mirrors the
    /// targeted leaf re-replay entry cap so a host that tunes one rarely needs
    /// to tune the other.
    /// </summary>
    public const int DefaultBootstrapFallbackMaxEntries = 4096;

    /// <summary>
    /// Default value for <see cref="BootstrapFallbackMaxBytes"/>: 1 MB of
    /// cumulative re-shipped payload. Bounds the repair bandwidth a single
    /// GC'd-divergence fallback can produce independently of the entry-count
    /// cap.
    /// </summary>
    public const long DefaultBootstrapFallbackMaxBytes = 1024L * 1024L;

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

    /// <summary>
    /// Default value for <see cref="FramingCompressionDictionaryId"/>:
    /// <c>0</c>, the reserved "no dictionary" id. A default build never
    /// requests a shared dictionary, so its framed bytes are identical
    /// to a dictionary-less build.
    /// </summary>
    public const uint DefaultFramingCompressionDictionaryId = 0u;

    /// <summary>
    /// Default value for <see cref="DictionaryNegotiationEnabled"/>:
    /// <see langword="false"/>. Per-peer shared-dictionary negotiation
    /// ships dark; hosts opt in explicitly once they want a sender to gate
    /// dictionary compression on each peer's advertised capability.
    /// </summary>
    public const bool DefaultDictionaryNegotiationEnabled = false;

    /// <summary>
    /// Default value for <see cref="WireVersionNegotiationEnabled"/>:
    /// <see langword="false"/>. Negotiation ships dark; hosts opt in
    /// explicitly once their fleet is ready to interoperate across wire
    /// versions during a rolling upgrade.
    /// </summary>
    public const bool DefaultWireVersionNegotiationEnabled = false;

    /// <summary>
    /// Default value for <see cref="MinimumSupportedWireVersion"/>:
    /// <c>1</c>, the oldest framing wire version. A peer advertising
    /// anything below this is genuinely unsupported and fails fast on
    /// the ship path.
    /// </summary>
    public const int DefaultMinimumSupportedWireVersion = 1;

    /// <summary>
    /// Default value for <see cref="UnknownPeerWireVersionFloor"/>:
    /// <see cref="EncodedBatchHeader.CurrentWireVersion"/>. Until a
    /// peer advertises its capability the sender targets its current
    /// version, matching the pre-negotiation behaviour; hosts running a
    /// heterogeneous upgrade lower this to a conservative floor.
    /// </summary>
    public const int DefaultUnknownPeerWireVersionFloor = EncodedBatchHeader.CurrentWireVersion;

    /// <summary>
    /// Default value for <see cref="AdaptiveBatchSizingEnabled"/>:
    /// <see langword="false"/>. Sender-side adaptive batch sizing is a
    /// dark-launched opt-in: with the flag off the shipper sizes every
    /// batch exactly as the static path does today, so a host that never
    /// touches the option gets byte-identical steady-state behaviour.
    /// </summary>
    public const bool DefaultAdaptiveBatchSizingEnabled = false;

    /// <summary>
    /// Default value for <see cref="AdaptiveBatchIncrement"/>: <c>8</c>
    /// entries per healthy ack. Sized so the controller re-approaches the
    /// canonical <see cref="DefaultShipBatchSize"/> of 256 over a few tens
    /// of acks after a back-off without overshooting on a single fast ack.
    /// </summary>
    public const int DefaultAdaptiveBatchIncrement = 8;

    /// <summary>
    /// Default value for <see cref="AdaptiveBatchDecreaseFactor"/>:
    /// <c>0.5</c> (halving). The canonical AIMD multiplicative-decrease
    /// factor - aggressive enough to shed load quickly when ack latency
    /// climbs, while the additive increase rebuilds the batch size
    /// gradually as the link recovers.
    /// </summary>
    public const double DefaultAdaptiveBatchDecreaseFactor = 0.5;

    /// <summary>
    /// Default value for <see cref="AdaptiveBatchLatencyThreshold"/>:
    /// 50 ms. Sits above the canonical in-cluster ack round-trip yet low
    /// enough that a sustained climb into the tens-of-milliseconds range
    /// trips the controller's back-off before the receiver's
    /// WAL-saturation hint engages. Tune per link.
    /// </summary>
    public static readonly TimeSpan DefaultAdaptiveBatchLatencyThreshold = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Default value for <see cref="AdaptiveBatchWindowLength"/>: <c>16</c>
    /// recent acks. Smooths a single transient latency spike (one slow ack
    /// in sixteen barely moves the mean) while still reacting within a
    /// couple of pump ticks to a sustained shift.
    /// </summary>
    public const int DefaultAdaptiveBatchWindowLength = 16;

    /// <summary>
    /// Default value for <see cref="AutoRemediateOnDigestMismatch"/>:
    /// <see langword="false"/>. The automatic anti-entropy remediation guards
    /// (master gate, rate cap, circuit breaker) ship dark and opt-in: with the
    /// flag off, drift is still detected and probed but no automatic repair is
    /// attempted, so an un-opted host observes no behaviour change.
    /// </summary>
    public const bool DefaultAutoRemediateOnDigestMismatch = false;

    /// <summary>
    /// Default value for <see cref="RemediationTrafficBudgetFraction"/>:
    /// <c>0.01</c> (1% of <see cref="ShipBatchSize"/>). Caps automatic
    /// remediation re-ship volume per <c>(tree, peer)</c> per window at a small
    /// fraction of the ordinary ship-batch budget so repair traffic cannot
    /// starve forward replication progress.
    /// </summary>
    public const double DefaultRemediationTrafficBudgetFraction = 0.01;

    /// <summary>
    /// Default value for <see cref="RemediationTrafficWindow"/>: 1 minute. The
    /// deterministic accounting window over which the per-tree, per-peer
    /// remediation traffic budget is measured before its consumed-entry counter
    /// resets.
    /// </summary>
    public static readonly TimeSpan DefaultRemediationTrafficWindow = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Default value for <see cref="RemediationFailureThreshold"/>: <c>3</c>
    /// consecutive failures. A small threshold so a peer or transport that is
    /// persistently rejecting repair traffic is fenced off quickly rather than
    /// re-attempted on every probe.
    /// </summary>
    public const int DefaultRemediationFailureThreshold = 3;

    /// <summary>
    /// Default value for <see cref="RemediationCircuitResetInterval"/>:
    /// 5 minutes. The cooldown the remediation circuit breaker stays open for
    /// before it half-opens and allows one trial repair pass.
    /// </summary>
    public static readonly TimeSpan DefaultRemediationCircuitResetInterval = TimeSpan.FromMinutes(5);
}
