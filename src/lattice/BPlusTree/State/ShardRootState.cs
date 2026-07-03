using Orleans.Lattice;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for the shard root grain. Tracks whether the root of this
/// shard is currently a leaf or has been promoted to an internal node.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardRootState)]
internal sealed class ShardRootState
{
    /// <summary>The grain identity of the current root node (leaf or internal).</summary>
    [Id(0)] public GrainId? RootNodeId { get; set; }

    /// <summary>Whether the current root is a leaf (<c>true</c>) or internal node (<c>false</c>).</summary>
    [Id(1)] public bool RootIsLeaf { get; set; } = true;

    /// <summary>
    /// If a root promotion is in progress, the split result that triggered it.
    /// Persisted before creating the new root so that a crash-retry can resume.
    /// </summary>
    [Id(2)] public SplitResult? PendingPromotion { get; set; }

    /// <summary>
    /// Whether <see cref="RootIsLeaf"/> was <c>true</c> when the pending promotion
    /// started. Used to pass the correct <c>childrenAreLeaves</c> value when
    /// creating the new internal root.
    /// </summary>
    [Id(3)] public bool PendingPromotionRootWasLeaf { get; set; }

    /// <summary>
    /// If a bulk-append graft is in progress, the intent record describing the
    /// new leaves and separators to wire into the existing tree. Persisted before
    /// mutating the tree so that a crash-retry can resume the graft.
    /// </summary>
    [Id(4)] public PendingBulkGraft? PendingBulkGraft { get; set; }

    /// <summary>
    /// The operation ID of the last successfully completed bulk operation on this shard.
    /// Used for idempotency - if a retry arrives with the same ID, it is a no-op.
    /// </summary>
    [Id(5)] public string? LastCompletedBulkOperationId { get; set; }

    /// <summary>
    /// Whether this shard has been soft-deleted. When <c>true</c>, all reads and writes
    /// throw <see cref="InvalidOperationException"/>.
    /// </summary>
    [Id(6)] public bool IsDeleted { get; set; }

    /// <summary>
    /// Whether this shard's tree has been registered in the
    /// <see cref="LatticeConstants.RegistryTreeId"/> registry tree.
    /// Set once on first write; avoids redundant registration calls on
    /// subsequent operations.
    /// </summary>
    [Id(7)] public bool IsRegistered { get; set; }

    /// <summary>
    /// Non-null when this shard is participating in an adaptive split
    /// as the source. Drives shadow-write and reject-routing behaviour on the
    /// hot path of every operation. Cleared once the split coordinator
    /// completes the post-cleanup phase.
    /// </summary>
    [Id(8)] public ShardSplitInProgress? SplitInProgress { get; set; }

    /// <summary>
    /// Virtual slots that this shard has permanently split away to other
    /// physical shards (key = virtual slot, value = new owner shard index).
    /// Accumulated on every successful split completion; never cleared.
    /// <para>
    /// Used by the hot-path reject gate after <see cref="SplitInProgress"/>
    /// has been cleared so that stale <c>LatticeGrain</c> activations whose
    /// cached <c>ShardMap</c> still routes to this shard always observe a
    /// <see cref="StaleShardRoutingException"/> and refresh their map. Without
    /// this, a stale <see cref="Orleans.Concurrency.StatelessWorkerAttribute"/> activation could
    /// indefinitely return orphan data from a slot it no longer owns.
    /// </para>
    /// </summary>
    [Id(9)] public Dictionary<int, int> MovedAwaySlots { get; set; } = new();

    /// <summary>
    /// The virtual shard count under which <see cref="MovedAwaySlots"/> entries
    /// were recorded. Once a split completes for this shard, all subsequent
    /// splits of the same tree must use the same virtual shard count;
    /// otherwise the recorded slot indices would lose meaning.
    /// </summary>
    [Id(10)] public int? MovedAwayVirtualShardCount { get; set; }

    /// <summary>
    /// Non-null when this shard is participating in an online tree-level
    /// operation (e.g. online resize) as the <em>source</em>. Drives
    /// parallel shadow-forwarding of every accepted mutation to the
    /// corresponding shard on
    /// <c>ShadowForwardState.DestinationPhysicalTreeId</c>, and post-swap
    /// rejection of new operations with <see cref="StaleTreeRoutingException"/>.
    /// Cleared by the coordinator after the destination tree has been
    /// promoted to the primary alias and the source is safe to tear down.
    /// <para>
    /// A <c>null</c> value is the steady state - no online operation is in
    /// flight for this shard. Adding this slot is backward-compatible with
    /// Orleans serialization: activations persisted before this field was
    /// introduced deserialize with <c>ShadowForward = null</c>, which is
    /// the correct "no operation in flight" state.
    /// </para>
    /// </summary>
    [Id(11)] public ShadowForwardState? ShadowForward { get; set; }

    /// <summary>
    /// Map of leaf grain ids on this shard to the
    /// <see cref="HybridLogicalClock"/> at which the leaf was most recently
    /// marked dirty by a routed <c>Delete</c> mutation. Populated by
    /// <c>ShardRootGrain.MarkLeafDirtyAsync</c> as deletes route through this
    /// shard, and consumed by <c>TombstoneCompactionGrain</c> via
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.GetDirtyLeavesSinceLastCompactionAsync"/>
    /// to skip activating leaves with nothing to reap. The per-entry HLC
    /// gates <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.ClearDirtyLeavesUpToAsync"/> so a
    /// delete that arrives during the in-flight pass is preserved for the
    /// next pass instead of being silently dropped.
    /// <para>
    /// Bounded by the shard's leaf count. Persists alongside the rest of the
    /// shard-root state so a silo restart does not lose pending dirty signal.
    /// </para>
    /// </summary>
    [Id(12)] public Dictionary<string, HybridLogicalClock> DirtyLeavesSinceLastCompaction { get; set; } = new();

    /// <summary>
    /// HLC watermark below which the dirty-set has been drained. The
    /// compaction coordinator passes this value to
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.ClearDirtyLeavesUpToAsync"/> after a
    /// successful shard pass; entries whose mark HLC compares as
    /// less-than-or-equal to the watermark are removed, while entries
    /// marked with a strictly greater HLC are preserved.
    /// </summary>
    [Id(13)] public HybridLogicalClock LastDirtyAdvance { get; set; }
}

/// <summary>
/// Per-leaf byte-accurate storage footprint snapshot published by a leaf
/// grain to its owning shard root. The pair feeds the shard root's
/// activation-scoped running <c>LeafStateBytesTotal</c> /
/// <c>SnapshotBytesTotal</c> so the byte-accurate storage-usage aggregator
/// can read the shard total in O(1) rather than walking the leaf chain on
/// every dashboard scrape. Activation-scoped (not persisted): a shard root
/// that reactivates starts with zero totals and converges as leaves
/// re-publish on their next commit; the operator-driven
/// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.RefreshLeafByteFootprintsAsync"/>
/// re-anchors the totals exactly when an authoritative figure is needed.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafByteFootprint)]
[Immutable]
internal readonly record struct LeafByteFootprint
{
    /// <summary>
    /// Sentinel published by a leaf that has been removed from the shard
    /// (split-donated or merged-away). The shard root drops the leaf's
    /// entry from its activation-scoped footprint map and subtracts the
    /// leaf's last-known totals from the running sums.
    /// </summary>
    public static readonly LeafByteFootprint Removed = new() { StateBytes = -1, SnapshotBytes = -1 };

    /// <summary>Per-leaf state-byte footprint (UTF-8 key + value bytes per row).</summary>
    [Id(0)] public long StateBytes { get; init; }

    /// <summary>Per-leaf snapshot-byte footprint; <c>0</c> when the leaf has no captured snapshot.</summary>
    [Id(1)] public long SnapshotBytes { get; init; }

    /// <summary>
    /// Per-leaf live (non-tombstone) key count, fed into the shard root's
    /// activation-scoped live-key total that backs the per-tree admission
    /// aggregate. Best-effort: a time-expired-but-not-yet-reaped entry is still
    /// counted as live until compaction reaps it, and the shard total is
    /// re-anchored to the exact figure on the operator-driven deep refresh.
    /// Ignored on the <see cref="Removed"/> sentinel (the shard subtracts the
    /// leaf's last-known total in that case).
    /// </summary>
    [Id(2)] public long LiveKeys { get; init; }
}

/// <summary>
/// Intent record for a bulk-append graft that has been committed to state but
/// not yet fully wired into the tree. Contains all the information needed to
/// resume the graft after a crash.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.PendingBulkGraft)]
[Immutable]
internal sealed record PendingBulkGraft
{
    /// <summary>Unique operation ID for idempotency.</summary>
    [Id(0)] public required string OperationId { get; init; }

    /// <summary>The GrainId of the existing rightmost leaf to wire the first new leaf to.</summary>
    [Id(1)] public required GrainId ExistingRightmostLeafId { get; init; }

    /// <summary>Separators and leaf IDs for the new leaves, in order.</summary>
    [Id(2)] public required List<GraftEntry> NewLeaves { get; init; }

    /// <summary>Whether the root was a leaf when the graft started.</summary>
    [Id(3)] public required bool RootWasLeaf { get; init; }
}

/// <summary>
/// A single leaf in a pending bulk graft - its separator key and grain identity.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.GraftEntry)]
[Immutable]
internal sealed record GraftEntry
{
    [Id(0)] public required string SeparatorKey { get; init; }
    [Id(1)] public required GrainId LeafId { get; init; }
}
