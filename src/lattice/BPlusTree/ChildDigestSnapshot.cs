namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A point-in-time snapshot of a child node's contribution to its parent
/// internal node's subtree fold. Carried by
/// <see cref="IBPlusInternalGrain.OnChildDigestPublishedAsync"/> so the
/// parent can XOR the old contribution out and the new contribution in
/// without re-walking every sibling. Defined as a serialisable value
/// type because the propagation hook is a cross-grain RPC.
/// <para>
/// The <see cref="Hash"/> field is the child's 16-byte XOR-fold
/// projection hash (a leaf's <c>state.State.ProjectionHash</c>, or an
/// internal node's <c>SubtreeProjectionHash</c>). The
/// <see cref="EntryCount"/> field is the sum of live and tombstoned
/// entries in the child's subtree, and <see cref="CheckpointOffset"/>
/// is the highest <c>ProjectionCheckpointOffset</c> across descendant
/// leaves (max-reduced upward, not summed, so two silos at the same
/// applied-prefix observe the same value regardless of how the chain
/// is sharded).
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ChildDigestSnapshot)]
[Immutable]
internal readonly record struct ChildDigestSnapshot
{
    /// <summary>The child's 16-byte XOR-fold projection hash (may be <see langword="null"/> when the child has never published).</summary>
    [Id(0)] public byte[]? Hash { get; init; }

    /// <summary>Total entry count folded into the subtree.</summary>
    [Id(1)] public long EntryCount { get; init; }

    /// <summary>Highest projection-checkpoint offset across descendant leaves.</summary>
    [Id(2)] public long CheckpointOffset { get; init; }
}
