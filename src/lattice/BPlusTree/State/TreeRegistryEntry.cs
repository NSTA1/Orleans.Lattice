using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Metadata stored per tree in the internal registry tree.
/// Serialized as the <c>byte[]</c> value for each tree ID key.
/// Contains optional <see cref="LatticeOptions"/> overrides that take
/// priority over <see cref="IOptionsMonitor{LatticeOptions}"/> at runtime.
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
    /// <see cref="BPlusLeafGrain"/> on the first trimmed-path mutation
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
}
