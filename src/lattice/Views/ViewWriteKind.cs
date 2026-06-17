namespace Orleans.Lattice;

/// <summary>
/// Classifies a single <see cref="ViewWrite"/> emitted by an
/// <see cref="ILatticeViewProjection"/>. A projection lowers each observed
/// <see cref="LatticeMutation"/> into zero or more <see cref="ViewWrite"/>s; the
/// kind tells the view maintainer how to fold the write into the
/// <c>view-{name}</c> tree.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ViewWriteKind)]
public enum ViewWriteKind
{
    /// <summary>Insert or update the view key with the carried value (last-writer-wins by source HLC).</summary>
    Upsert = 0,

    /// <summary>Remove the view key (idempotent when the key is already absent).</summary>
    Delete = 1,

    /// <summary>
    /// Reserved for a later phase: a commutative CRDT delta merged into the view
    /// entry rather than an LWW upsert. Phase 1 maintainers never emit or apply
    /// this kind; it is declared now so the wire shape and the apply switch are
    /// forward-compatible with aggregation views.
    /// </summary>
    CrdtDelta = 2,

    /// <summary>
    /// Remove every view key in the half-open range
    /// <c>[<see cref="ViewWrite.Key"/>, <see cref="ViewWrite.EndKey"/>)</c> in a
    /// single view-side range delete. Emitted by a key-preserving projection for
    /// a source <see cref="MutationKind.DeleteRange"/> that carries no
    /// <see cref="LatticeMutation.MatchedKeys"/> (an unconstrained range delete):
    /// because the view key equals the source key, deleting the view's slice of
    /// the range removes exactly the affected entries.
    /// </summary>
    RangeDelete = 3,

    /// <summary>
    /// Re-derive the view over the affected source range
    /// <c>[<see cref="ViewWrite.Key"/>, <see cref="ViewWrite.EndKey"/>)</c> from
    /// current source state. Emitted by a re-keyed projection for a source
    /// <see cref="MutationKind.DeleteRange"/> that carries no
    /// <see cref="LatticeMutation.MatchedKeys"/>: the deleted source keys' view
    /// keys cannot be recovered without a reverse index, so the maintainer
    /// reconciles the range (escalating to a full rebuild) rather than guessing.
    /// Supplying <see cref="LatticeMutation.MatchedKeys"/> (predicate-filtered
    /// deletes do) yields exact per-key retraction and avoids this path.
    /// </summary>
    RangeReconcile = 4,
}
