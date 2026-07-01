using Orleans.Lattice;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for an internal (non-leaf) node grain.
/// Children are stored in separator-key order; the first child has a <c>null</c>
/// separator and acts as the leftmost catch-all.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.InternalNodeState)]
internal sealed class InternalNodeState
{
    /// <summary>
    /// Ordered list of children. The first entry has <see cref="ChildEntry.SeparatorKey"/> == <c>null</c>
    /// (leftmost). Subsequent entries represent "keys ≥ separator go to this child."
    /// </summary>
    [Id(0)] public List<ChildEntry> Children { get; set; } = [];

    /// <summary>Monotonic split lifecycle for this internal node.</summary>
    [Id(1)] public SplitState SplitState { get; set; }

    /// <summary>The logical clock for this grain.</summary>
    [Id(2)] public HybridLogicalClock Clock { get; set; }

    /// <summary>Whether this node's children are leaves (<c>true</c>) or internal nodes (<c>false</c>).</summary>
    [Id(3)] public bool ChildrenAreLeaves { get; set; } = true;

    /// <summary>The tree this node belongs to. Used to resolve named <see cref="Orleans.Lattice.LatticeOptions"/>.</summary>
    [Id(4)] public string? TreeId { get; set; }

    /// <summary>If a split is in progress, the separator key being promoted.</summary>
    [Id(5)] public string? SplitKey { get; set; }

    /// <summary>If a split is in progress, the grain identity of the new right sibling.</summary>
    [Id(6)] public GrainId? SplitSiblingId { get; set; }

    /// <summary>If a split is in progress, the children that belong to the right sibling.</summary>
    [Id(7)] public List<ChildEntry>? SplitRightChildren { get; set; }

    /// <summary>
    /// Grain reference to this internal node's parent, or
    /// <see langword="null"/> when this node is the shard root.
    /// Persisted exactly once by <c>SetParentAsync</c>, called by the
    /// shard root at creation time and updated on root-promotion when
    /// this node is grafted beneath a new parent. Consulted by
    /// <see cref="IBPlusInternalGrain.OnChildDigestPublishedAsync"/> handling so this node
    /// can forward its own fold change upward.
    /// </summary>
    [Id(8)] public GrainId? ParentId { get; set; }

    /// <summary>
    /// Running XOR-fold of every descendant leaf's
    /// <c>ProjectionHash</c>. Maintained incrementally on every
    /// <see cref="IBPlusInternalGrain.OnChildDigestPublishedAsync"/> call by
    /// XORing the old child contribution out and the new child
    /// contribution in. Bitwise-identical across silos at the same
    /// applied-prefix because the XOR fold is commutative and
    /// self-inverse. <see langword="null"/> on a freshly-initialised
    /// node until the first child digest publishes; equivalent to a
    /// 16-byte zero buffer for fold arithmetic.
    /// </summary>
    [Id(9)] public byte[]? SubtreeProjectionHash { get; set; }

    /// <summary>
    /// Sum of <c>EntryCount</c> across every descendant leaf
    /// in this subtree. Maintained alongside
    /// <see cref="SubtreeProjectionHash"/>.
    /// </summary>
    [Id(10)] public long SubtreeEntryCount { get; set; }

    /// <summary>
    /// Highest <c>ProjectionCheckpointOffset</c> across descendant
    /// leaves (max-reduced upward, not summed, so two silos at the same
    /// applied-prefix observe the same value regardless of shard
    /// layout). Maintained alongside
    /// <see cref="SubtreeProjectionHash"/>.
    /// </summary>
    [Id(11)] public long SubtreeHighestCheckpointOffset { get; set; }

    /// <summary>
    /// Per-child snapshot table indexed by child <see cref="GrainId"/>.
    /// Records the last <see cref="ChildDigestSnapshot"/> each child
    /// published, so a re-publish can XOR the prior contribution out
    /// even when the parent's activation has been recycled between
    /// calls. Persisted alongside the aggregates so crash recovery
    /// reconstructs the exact same fold state on the next activation.
    /// </summary>
    [Id(12)] public Dictionary<GrainId, ChildDigestSnapshot> ChildDigests { get; set; } = new();

    /// <summary>
    /// Durable high-water mark of the strictly-increasing publish sequence this
    /// internal node stamps onto every <see cref="ChildDigestSnapshot"/> it
    /// publishes upward. Persisting it keeps the sequence monotonic across
    /// activations and silos so a grandparent's staleness guard never permanently
    /// drops this node's publishes after a re-activation seeded a lower stamp from
    /// a skewed wall clock. Zero on first activation; seeded lazily.
    /// </summary>
    [Id(13)] public long DigestPublishSequence { get; set; }

    /// <summary>
    /// Routes a key to the correct child grain by finding the rightmost separator ≤ key.
    /// </summary>
    public GrainId Route(string key)
    {
        // Walk backwards through children to find the first separator that is ≤ key.
        for (int i = Children.Count - 1; i >= 0; i--)
        {
            var sep = Children[i].SeparatorKey;
            if (sep is null || string.Compare(key, sep, StringComparison.Ordinal) >= 0)
            {
                return Children[i].ChildId;
            }
        }

        // Should not happen if the node is well-formed (first child has null separator).
        return Children[0].ChildId;
    }
}
