using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for <see cref="Grains.TreeResizeGrain"/>.
/// Tracks the progress of an in-flight resize operation so that it can be
/// resumed after a silo restart.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TreeResizeState)]
internal sealed class TreeResizeState
{
    /// <summary>Whether a resize operation is currently in progress.</summary>
    [Id(0)] public bool InProgress { get; set; }

    /// <summary>The current phase of the resize operation.</summary>
    [Id(1)] public ResizePhase Phase { get; set; }

    /// <summary>The new <see cref="Orleans.Lattice.BPlusTree.ResolvedLatticeOptions.MaxLeafKeys"/> value to apply.</summary>
    [Id(2)] public int NewMaxLeafKeys { get; set; }

    /// <summary>The new <see cref="Orleans.Lattice.BPlusTree.ResolvedLatticeOptions.MaxInternalChildren"/> value to apply.</summary>
    [Id(3)] public int NewMaxInternalChildren { get; set; }

    /// <summary>
    /// Unique operation ID for the overall resize. Used to derive the
    /// snapshot destination tree ID and for idempotency.
    /// </summary>
    [Id(4)] public string? OperationId { get; set; }

    /// <summary>
    /// The total number of shards in the tree. Captured at the start of the
    /// resize so that it is consistent even if options change mid-operation.
    /// </summary>
    [Id(5)] public int ShardCount { get; set; }

    /// <summary>Whether the resize has fully completed.</summary>
    [Id(6)] public bool Complete { get; set; }

    /// <summary>
    /// The physical tree ID of the snapshot destination tree created during
    /// the resize. Used for alias swap and cleanup.
    /// </summary>
    [Id(7)] public string? SnapshotTreeId { get; set; }

    /// <summary>
    /// The previous physical tree ID before the alias swap.
    /// Used by <c>UndoResizeAsync</c> to recover the old tree.
    /// </summary>
    [Id(8)] public string? OldPhysicalTreeId { get; set; }

    /// <summary>
    /// The registry entry that was in effect before the resize started.
    /// Captured at initiation so that <c>UndoResizeAsync</c> can restore
    /// the original configuration. May be <c>null</c> if the tree had no
    /// registry entry before the resize.
    /// </summary>
    [Id(9)] public TreeRegistryEntry? OldRegistryEntry { get; set; }

    /// <summary>Persisted before reserving the logical alias; retained until release succeeds.</summary>
    [Id(10)] public string? AliasReservationId { get; set; }

    /// <summary>
    /// The physical shard indices of the old physical tree the resize rejects
    /// and, on undo, releases, in ascending order: the union of <c>0</c> to
    /// <see cref="ShardCount"/><c> - 1</c> and every index the logical tree's
    /// routing map named when the resize started. An adaptive split routes keys
    /// to a shard above the pinned count, so the pinned count alone misses it
    /// (issue 3880). Legacy persisted state decodes the missing slot to
    /// <see langword="null"/>, which falls back to <c>0</c> to
    /// <c>ShardCount - 1</c>, the set such a resize's snapshot shadow-forwarded.
    /// </summary>
    [Id(11)] public int[]? ShardIndices { get; set; }

    /// <summary>Whether this online copy is held for receiver-bootstrap import before cutover.</summary>
    [Id(12)] public bool HoldForBootstrap { get; set; }

    /// <summary>Whether alias cutover belongs to a receiver-bootstrap shadow copy.</summary>
    [Id(13)] public bool BootstrapCopyCutover { get; set; }
}

/// <summary>
/// The phase of a resize operation.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ResizePhase)]
internal enum ResizePhase
{
    /// <summary>
    /// The online snapshot to the new physical tree is in progress.
    /// The <see cref="Grains.TreeSnapshotGrain"/> handles the actual work;
    /// during this phase every accepted mutation on the source tree except a
    /// typed CRDT delta apply or a bulk append is shadow-forwarded to the
    /// destination via the shadow-forwarding primitive.
    /// </summary>
    Snapshot = 0,

    /// <summary>
    /// The snapshot is complete. The alias needs to be swapped from the
    /// old physical tree to the new snapshot tree.
    /// </summary>
    Swap = 1,

    /// <summary>
    /// The alias has been swapped. The old physical tree needs to be
    /// soft-deleted to reclaim storage.
    /// </summary>
    Cleanup = 2,

    /// <summary>
    /// The alias has been swapped. Every shard on the old physical tree
    /// must transition to <c>ShadowForwardPhase.Rejecting</c> so that any
    /// remaining client request routed to the old tree throws
    /// <see cref="StaleTreeRoutingException"/> and retries against the new
    /// alias target.
    /// </summary>
    Reject = 3,

    /// <summary>
    /// The online copy is complete and is waiting for a receiver bootstrap to
    /// finish importing before alias cutover.
    /// </summary>
    BootstrapHold = 4,
}
