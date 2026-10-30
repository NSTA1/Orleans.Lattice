namespace Orleans.Lattice;

/// <summary>
/// Orders changes for one set element by ordinal, then replica id, then kind.
/// Unlike <see cref="CrdtMemberChangeCausalComparer"/>, this presentation order
/// leads on ordinal rather than replica id; it does not imply causal dominance.
/// </summary>
internal sealed class CrdtMemberChangeOrdinalFirstComparer : IComparer<CrdtMemberChange>
{
    public static CrdtMemberChangeOrdinalFirstComparer Instance { get; } = new();

    /// <summary>
    /// Cached below <see cref="Instance"/> so in-place sorts do not allocate
    /// a comparison delegate on each call.
    /// </summary>
    public static readonly Comparison<CrdtMemberChange> Comparison = Instance.Compare;

    public int Compare(CrdtMemberChange x, CrdtMemberChange y)
    {
        var byOrdinal = x.Ordinal.CompareTo(y.Ordinal);
        if (byOrdinal != 0) return byOrdinal;
        var byReplica = string.CompareOrdinal(x.ReplicaId, y.ReplicaId);
        if (byReplica != 0) return byReplica;
        return ((int)x.Kind).CompareTo((int)y.Kind);
    }
}
