using System.Collections.Generic;

namespace Orleans.Lattice;

/// <summary>
/// Orders two <see cref="CrdtMemberChange"/> events into a deterministic,
/// replica-stable sequence by replica id, then causal ordinal, then kind (an
/// add sorts before a remove that carries the same ordinal). Shared as a
/// single stateless instance so the folded-state decoders that need a
/// deterministic cross-event order never allocate a comparison delegate.
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
