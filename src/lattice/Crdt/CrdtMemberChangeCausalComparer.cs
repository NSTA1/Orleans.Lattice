using System.Collections.Generic;

namespace Orleans.Lattice;

/// <summary>
/// Orders two <see cref="CrdtMemberChange"/> events into a deterministic,
/// replica-stable sequence by replica id, then causal ordinal, then kind (an
/// add sorts before a remove that carries the same ordinal). Shared as a
/// single stateless instance so the folded-state decoders that need a
/// deterministic cross-event order never allocate a comparer.
/// <para>
/// Sort through <see cref="Comparison"/>, not through <see cref="Instance"/>.
/// The singleton removes the <em>comparer</em> allocation but not the
/// comparison delegate: every <c>Sort</c> overload taking an
/// <see cref="IComparer{T}"/> funnels into
/// <c>ArraySortHelper&lt;T&gt;.Sort(Span&lt;T&gt;, IComparer&lt;T&gt;)</c>, whose body is
/// <c>IntrospectiveSort(keys, comparer.Compare)</c> - a method-group
/// conversion, so a fresh <see cref="Comparison{T}"/> is minted per call and
/// never cached. Measured at a flat 64 bytes per sort call on net10.0,
/// independent of element count. <see cref="Comparison"/> is that same
/// delegate constructed once, so the order is identical and the per-call
/// allocation is gone.
/// </para>
/// <para>
/// The order is a presentation order only - it is stable across replicas
/// because it depends solely on the events' own fields, not on dictionary
/// enumeration order - and carries no causal-dominance meaning of its own.
/// </para>
/// </summary>
internal sealed class CrdtMemberChangeCausalComparer : IComparer<CrdtMemberChange>
{
    /// <summary>A shared, stateless instance.</summary>
    public static CrdtMemberChangeCausalComparer Instance { get; } = new();

    /// <summary>
    /// <see cref="Instance"/>'s comparison, constructed once. Prefer this at
    /// every sort call site; see the type remarks. Declared below
    /// <see cref="Instance"/> because static initialisers run in declaration
    /// order.
    /// </summary>
    public static readonly Comparison<CrdtMemberChange> Comparison = Instance.Compare;

    /// <inheritdoc />
    public int Compare(CrdtMemberChange x, CrdtMemberChange y)
    {
        var byReplica = string.CompareOrdinal(x.ReplicaId, y.ReplicaId);
        if (byReplica != 0) return byReplica;
        var byOrdinal = x.Ordinal.CompareTo(y.Ordinal);
        if (byOrdinal != 0) return byOrdinal;
        return ((int)x.Kind).CompareTo((int)y.Kind);
    }
}
