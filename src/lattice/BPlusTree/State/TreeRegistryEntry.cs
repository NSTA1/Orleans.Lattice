using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Metadata stored per tree in the internal registry tree.
/// Serialized as the <c>byte[]</c> value for each tree ID key.
/// Contains optional <see cref="LatticeOptions"/> overrides that take
/// priority over <c>IOptionsMonitor&lt;LatticeOptions&gt;</c> at runtime.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TreeRegistryEntry)]
internal sealed record TreeRegistryEntry
{
    /// <summary>Maximum number of keys per leaf node, or <c>null</c> to use configured defaults.</summary>
    [Id(0)] public int? MaxLeafKeys { get; init; }

    /// <summary>Maximum number of children per internal node, or <c>null</c> to use configured defaults.</summary>
    [Id(1)] public int? MaxInternalChildren { get; init; }

    /// <summary>Number of shards, or <c>null</c> to use configured defaults.</summary>
    [Id(2)] public int? ShardCount { get; init; }

    /// <summary>
    /// Physical tree ID that this logical tree ID maps to, or <c>null</c> if the
    /// logical ID is the physical ID (the default). Used by tree aliasing to redirect
    /// reads and writes to a different physical tree after a resize operation.
    /// Only a single level of indirection is supported - a physical tree must not
    /// itself have a <see cref="PhysicalTreeId"/>.
    /// </summary>
    [Id(3)] public string? PhysicalTreeId { get; init; }

    /// <summary>
    /// Persistent shard map for this tree, or <c>null</c> if the tree uses
    /// the default identity map derived from
    /// <see cref="Orleans.Lattice.BPlusTree.LatticeConstants.DefaultVirtualShardCount"/>
    /// and the pinned <see cref="ShardCount"/>. The map records which
    /// physical shard owns each virtual slot and is rewritten when adaptive
    /// shard splits change the topology.
    /// </summary>
    [Id(4)] public ShardMap? ShardMap { get; init; }

    /// <summary>
    /// Highest physical shard index that has been allocated for this tree by
    /// adaptive splits, or <c>null</c> if no split has yet occurred.
    /// Used by <see cref="ILatticeRegistry.AllocateNextShardIndexAsync"/> to
    /// hand out unique target shard indices when multiple splits run
    /// concurrently for the same tree ( - <c>MaxConcurrentAutoSplits</c> &gt; 1).
    /// </summary>
    [Id(5)] public int? NextShardIndex { get; init; }

    /// <summary>
    /// Per-tree override for <see cref="LatticeOptions.PublishEvents"/>.
    /// When <c>null</c> (the default), the silo-wide option value is used.
    /// When set to <c>true</c> or <c>false</c>, the override takes priority over
    /// the silo option for this tree only. Mutated at runtime through
    /// <see cref="ILattice.SetPublishEventsEnabledAsync(bool?, CancellationToken)"/>.
    /// Propagation to other silo activations is best-effort: each activation
    /// refreshes its cached value every few seconds.
    /// </summary>
    [Id(6)] public bool? PublishEvents { get; init; }

    /// <summary>
    /// Per-tree override for <see cref="LatticeOptions.MaintainProjectionDigest"/>.
    /// When <c>null</c> (the default), the silo-wide option value is used.
    /// When set to <c>true</c> or <c>false</c>, the override takes priority
    /// over the silo option for this tree only. Note this override is
    /// itself superseded by <see cref="ProjectionDigestPermanentlyDisabled"/>:
    /// once a tree has accepted mutations while digest maintenance was
    /// disabled, the latch overrides any configured <c>true</c> value
    /// because the persisted aggregate is no longer the source of truth
    /// and re-enabling would expose a stale digest through the public API.
    /// </summary>
    [Id(7)] public bool? MaintainProjectionDigest { get; init; }

    /// <summary>
    /// One-way latch recording that this tree has accepted at least one
    /// mutation while <see cref="LatticeOptions.MaintainProjectionDigest"/>
    /// resolved to <c>false</c>. Stamped lazily by
    /// <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/> on the first trimmed-path mutation
    /// after activation, persists for the lifetime of the tree, and is
    /// never cleared. Once <c>true</c>, the effective resolved value of
    /// <see cref="LatticeOptions.MaintainProjectionDigest"/> for this
    /// tree is forced to <c>false</c> regardless of the silo-wide
    /// configuration or the per-tree
    /// <see cref="MaintainProjectionDigest"/> override - the digest API
    /// stays unavailable because the persisted aggregate has gaps that
    /// cannot be reconstructed without rewriting every key. Cross-cluster
    /// reconciliation tools must treat a latched tree as a non-participant.
    /// <para>
    /// A <c>null</c> value is equivalent to <c>false</c> (not latched);
    /// the nullable shape is purely for backwards compatibility with
    /// registry rows persisted before this field was added.
    /// </para>
    /// </summary>
    [Id(8)] public bool? ProjectionDigestPermanentlyDisabled { get; init; }

    /// <summary>
    /// Pinned WAL partition count for this tree. Stamped at first
    /// <see cref="ILatticeRegistry.RegisterAsync"/> from the silo's
    /// then-current <see cref="LatticeOptions.WalPartitions"/> value;
    /// never mutated thereafter. <see cref="Orleans.Lattice.BPlusTree.LatticeOptionsResolver"/>
    /// prefers this slot over the live <c>IOptionsMonitor&lt;T&gt;</c>
    /// value so the resolved <c>WalPartitions</c> seen by every grain
    /// is tree-immutable for the lifetime of the tree.
    /// <para>
    /// Tree-structural pinning is required because the foreground
    /// commit-log writer hashes each mutation key modulo this value to
    /// route the write to a WAL partition grain; flipping the value
    /// after the tree has accepted writes would silently re-route new
    /// writes into grains that the activation-time materialiser is
    /// not configured to read from. The pin protects both the
    /// single-silo "operator retuned the value" case and the multi-
    /// silo "two silos in the cluster disagree" case.
    /// </para>
    /// <para>
    /// <see langword="null"/> on registry rows persisted before this
    /// slot was introduced; the resolver falls back to the live
    /// <c>IOptionsMonitor&lt;T&gt;</c> value in that case, exactly
    /// matching the legacy pre-pin behaviour. Once any first-class
    /// caller of <see cref="ILatticeRegistry.RegisterAsync"/> runs
    /// against the upgraded library, the slot is stamped and every
    /// subsequent resolve reads from the pin.
    /// </para>
    /// </summary>
    [Id(9)] public int? WalPartitions { get; init; }

    /// <summary>
    /// Durable per-partition WAL storage placement for this tree, or
    /// <see langword="null"/> on rows persisted before the placement slot was
    /// introduced (and on trees that never moved a partition away from the
    /// baseline provider). When <see langword="null"/>, every partition
    /// resolves to <see cref="IWalStorageProviderCatalog.DefaultProviderKey"/>,
    /// exactly matching pre-placement behaviour. Seeded to the default pin at
    /// first <see cref="ILatticeRegistry.RegisterAsync"/> and mutated only
    /// through the managed <see cref="ILatticeAdmin"/> move surface via
    /// <see cref="ILatticeRegistry.UpdateWalPlacementAsync"/>, which version-
    /// stamps each change for fail-closed fencing.
    /// </summary>
    [Id(10)] public WalPlacementPin? WalPlacement { get; init; }

    /// <summary>
    /// Per-tree durable-history retention mode override, or <see langword="null"/>
    /// (the default) to use <see cref="HistoryRetentionMode.MetadataOnly"/>. Read
    /// by the view maintainer at drain time to shape each LWW history revision row
    /// (full value, recent-hybrid, or metadata-only). Mutated at runtime through
    /// <see cref="ILattice.SetHistoryRetentionAsync(HistoryRetentionMode?, System.TimeSpan?, CancellationToken)"/>;
    /// propagation to other activations is best-effort (each resolve reads the
    /// registry fresh). Kept out of the projection's code identity so a mode change
    /// never trips a view rebuild.
    /// </summary>
    [Id(11)] public HistoryRetentionMode? HistoryRetentionMode { get; init; }

    /// <summary>
    /// Per-tree durable-history age bound in ticks: a history revision row written
    /// while this is set expires that many ticks after it is drained. <c>null</c>
    /// (the default) means no age bound - the timeline is retained until an
    /// explicit rebuild. Set through the same runtime setter as
    /// <see cref="HistoryRetentionMode"/> and validated to be strictly positive
    /// when supplied.
    /// </summary>
    [Id(12)] public long? HistoryRetentionWindowTicks { get; init; }

    /// <summary>
    /// Provenance marker set when this physical tree was created as the shadow
    /// target of a shadow-cutover restore: it carries the logical tree id the
    /// restore was performed for (the alias that now resolves to this physical
    /// tree). <c>null</c> (the default) for every ordinary tree and for the
    /// logical alias itself. Stamped once at the shadow tree's first
    /// <see cref="ILatticeRegistry.RegisterAsync"/> and never mutated. It lets
    /// callers classify a restore shadow as a first-class fact rather than
    /// inferring it from the tree name, so the state catalog can hide restore
    /// shadows from the default tree list and group them under their logical
    /// alias without a naming convention.
    /// </summary>
    [Id(13)] public string? RestoreShadowOfTreeId { get; init; }

    /// <summary>
    /// Per-tree runtime override for <see cref="LatticeOptions.MaxCacheValueBytes"/>.
    /// When <c>null</c> (the default), the silo-wide option value is used;
    /// when set to a positive byte count, the override takes priority over the
    /// silo option for this tree only, capping the resident value-payload bytes
    /// per read-through cache activation with LRU payload eviction. Mutated at
    /// runtime through <see cref="ILatticeRegistry.SetMaxCacheValueBytesAsync(string, long?)"/>
    /// and read back through <see cref="Orleans.Lattice.BPlusTree.LatticeOptionsResolver"/>
    /// (both the full <c>ResolveAsync</c> record and the lightweight
    /// <see cref="Orleans.Lattice.BPlusTree.LatticeOptionsResolver.GetMaxCacheValueBytesAsync(string)"/>
    /// fast path). Propagation to other silo activations is best-effort: each
    /// <see cref="Orleans.Lattice.BPlusTree.Grains.LeafCacheGrain"/> activation
    /// re-resolves the cap on each cache refresh, so toggling it on a warm
    /// activation only bounds payloads merged after the change. Validated to be
    /// greater than or equal to 1 when supplied, exactly mirroring the silo-wide
    /// option's validation.
    /// </summary>
    [Id(14)] public long? MaxCacheValueBytes { get; init; }
}
