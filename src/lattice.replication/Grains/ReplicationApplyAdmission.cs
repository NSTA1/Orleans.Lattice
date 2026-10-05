using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// One origin's admission read on a receiver tree's high-water-mark grain
/// (issue #4549): the origin's high-water mark and the tree's bootstrap drop
/// floor for it, read together in the call the applier already makes per
/// entry, so the floor adds no grain call to the apply path.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(ReplicationTypeAliases.ReplicationApplyAdmission)]
internal readonly record struct ReplicationApplyAdmission
{
    /// <summary>The origin's high-water mark for the tree.</summary>
    [Id(0)] public HybridLogicalClock HighWaterMark { get; init; }

    /// <summary>
    /// The bootstrap drop floor for the origin: a delivery stamped below it is
    /// already reflected in the tree, unless listed in <see cref="HeldBelowFloor"/>.
    /// <see cref="HybridLogicalClock.Zero"/> when the tree holds no floor for it.
    /// </summary>
    [Id(1)] public HybridLogicalClock BootstrapFloor { get; init; }

    /// <summary>The origin's writes below the floor the bootstrap source held without applying.</summary>
    [Id(2)] public HybridLogicalClock[] HeldBelowFloor { get; init; }

    /// <summary>
    /// The tree's bootstrap drop-floor epoch, which the applier stamps on the
    /// write it admits so a shard root can refuse a write admitted before a later
    /// floor was installed. <c>0</c> when no floor was ever installed.
    /// </summary>
    [Id(3)] public long FloorEpoch { get; init; }

    /// <summary>
    /// Whether the floor is provisional: its import has not yet closed against a
    /// stable source, so a delivery below it is deferred rather than dropped.
    /// </summary>
    [Id(4)] public bool FloorProvisional { get; init; }

    /// <summary>
    /// Whether a delivery of the origin's write at <paramref name="timestamp"/>
    /// is below the floor and not held, so it is dropped.
    /// </summary>
    public bool Drops(HybridLogicalClock timestamp) =>
        BootstrapFloor != HybridLogicalClock.Zero
        && timestamp < BootstrapFloor
        && (HeldBelowFloor is null || Array.IndexOf(HeldBelowFloor, timestamp) < 0);
}
