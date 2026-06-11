namespace Orleans.Lattice;

/// <summary>
/// Typed delta record for a Replicated Growable Array (RGA) sequence
/// mutation. Carries the inserted nodes (each as the dot-explicit triple
/// <c>(dot, parentDot, value)</c>) and the tombstoned dots authored since
/// the receiver's cursor, so the receiver replays the producer's
/// structural intent rather than a post-merge materialised order.
/// <para>
/// Apply semantics on the receiver mirror
/// <see cref="Rga.MergeDelta(RgaDelta)"/>: add each insert as a live node
/// keyed by its dot, then tombstone each dot in <see cref="Tombstones"/>.
/// The result is independent of arrival order, duplicate delivery, and
/// partial overlap with the local state, so the merge is commutative,
/// associative, and idempotent. Sibling order under a shared parent is
/// the deterministic descending <c>(Counter, ReplicaId)</c> tie-break
/// resolved at materialise time, so every replica that applies the same
/// set of deltas yields an identical ordered traversal.
/// </para>
/// <para>
/// Emitters always populate both collections (use empty arrays for
/// "no inserts" / "no removes"); use <see cref="Empty"/> to author a
/// no-op delta without allocating fresh empty arrays. The
/// <see langword="default"/> instance has <c>null</c> collections and is
/// intended only as the zero-value of the struct - consumers should
/// either treat <c>null</c> as empty or assert non-null at the apply
/// boundary.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.RgaDelta)]
[Immutable]
public readonly record struct RgaDelta
{
    /// <summary>
    /// The nodes inserted since the receiver's cursor, each carrying its
    /// dot, parent dot, and value bytes. An empty list indicates a delta
    /// that contains only removes.
    /// </summary>
    [Id(0)] public IReadOnlyList<RgaDeltaNode> Inserts { get; init; }

    /// <summary>
    /// The dots whose nodes the originator has now observed-as-removed.
    /// An empty list indicates a delta that contains only inserts.
    /// </summary>
    [Id(1)] public IReadOnlyList<OrSetDot> Tombstones { get; init; }

    /// <summary>
    /// A reusable no-op delta with empty (but non-null)
    /// <see cref="Inserts"/> and <see cref="Tombstones"/> collections.
    /// Backed by <see cref="Array.Empty{T}"/> so repeated access does not
    /// allocate.
    /// </summary>
    public static RgaDelta Empty { get; } = new()
    {
        Inserts = Array.Empty<RgaDeltaNode>(),
        Tombstones = Array.Empty<OrSetDot>(),
    };
}
