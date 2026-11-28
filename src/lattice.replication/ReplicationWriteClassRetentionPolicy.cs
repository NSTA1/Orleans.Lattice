namespace Orleans.Lattice.Replication;

/// <summary>
/// The retention choice, per write class, for a shipper whose tree stops
/// resolving a write class (issue #5699). Stated per class so a governed
/// write class can choose its own; today every class detaches and re-seeds.
/// </summary>
internal static class ReplicationWriteClassRetentionPolicy
{
    /// <summary>
    /// Returns the retention for a tree whose last resolved write class was
    /// <paramref name="lastWriteClass"/>, or <see langword="null"/> when the
    /// shipper never resolved one.
    /// </summary>
    public static ReplicationWriteClassRetention For(LatticeMergeMode? lastWriteClass) => lastWriteClass switch
    {
        LatticeMergeMode.LwwRegister => ReplicationWriteClassRetention.DetachAndReseed,
        LatticeMergeMode.OrSet => ReplicationWriteClassRetention.DetachAndReseed,
        LatticeMergeMode.PnCounter => ReplicationWriteClassRetention.DetachAndReseed,
        LatticeMergeMode.VersionVector => ReplicationWriteClassRetention.DetachAndReseed,
        LatticeMergeMode.MvRegister => ReplicationWriteClassRetention.DetachAndReseed,
        LatticeMergeMode.OrMap => ReplicationWriteClassRetention.DetachAndReseed,
        LatticeMergeMode.Sequence => ReplicationWriteClassRetention.DetachAndReseed,
        LatticeMergeMode.OrFlag => ReplicationWriteClassRetention.DetachAndReseed,
        LatticeMergeMode.RwFlag => ReplicationWriteClassRetention.DetachAndReseed,
        LatticeMergeMode.GCounter => ReplicationWriteClassRetention.DetachAndReseed,
        LatticeMergeMode.GSet => ReplicationWriteClassRetention.DetachAndReseed,
        LatticeMergeMode.RwSet => ReplicationWriteClassRetention.DetachAndReseed,
        LatticeMergeMode.MaxRegister => ReplicationWriteClassRetention.DetachAndReseed,
        LatticeMergeMode.MinRegister => ReplicationWriteClassRetention.DetachAndReseed,
        _ => ReplicationWriteClassRetention.DetachAndReseed,
    };
}
