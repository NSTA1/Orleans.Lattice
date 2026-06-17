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
}
