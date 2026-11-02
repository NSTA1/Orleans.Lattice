namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A tree's append-fence sweep claim (#5634): when a silo last started sweeping the
/// tree's append-epoch directories. Silos claim a sweep with a conditional write, so
/// one silo sweeps a tree per interval.
/// </summary>
/// <remarks>
/// Stored under a reserved grain type keyed by tree
/// (<c>WalAppendFenceRows.SweepKey</c>). Binary persisted; every member is a public
/// property of a concrete type so the JSON grain-storage serializer round trips it
/// too. An absent row reads as never swept.
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.WalAppendFenceSweepState)]
internal sealed class WalAppendFenceSweepState : ILatticeBinaryPersistedState
{
    /// <summary>When the last sweep of the tree started, in UTC ticks.</summary>
    [Id(0)]
    public long LastStartedUtcTicks { get; set; }
}
