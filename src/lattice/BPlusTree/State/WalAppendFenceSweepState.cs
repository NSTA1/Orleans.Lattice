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

    /// <summary>
    /// The last leaf the previous sweep examined, in the sweep's order; the next sweep
    /// starts after it, wrapping around, so every listed leaf is examined in turn
    /// however many there are (#5656). <see cref="Guid.Empty"/> starts at the beginning.
    /// </summary>
    [Id(1)]
    public Guid ResumeAfter { get; set; }
}
