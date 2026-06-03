namespace Orleans.Lattice;

/// <summary>
/// Cheap WAL-only storage-usage report for a single tree. Returned by the
/// per-tree WAL-usage aggregator (the leaf-free polling path) and consumed
/// by the cluster-wide poller so the byte-pressure WAL retention policy and
/// the <c>storage.wal_bytes</c> / <c>storage.policy.over_threshold</c> gauges
/// stay timely without activating any leaf, internal-node, or snapshot grain.
/// <para>
/// Unlike <see cref="TreeStorageUsageReport"/>, this surface intentionally
/// excludes leaf-state and snapshot bytes: those are byte-accurate only at
/// the cost of walking the tree, and walking the tree on every poll tick is
/// what defeats the activation-on-demand model for idle trees. Callers that
/// want the full byte breakdown use
/// <see cref="ILattice.GetStorageUsageAsync"/> (on demand) or
/// <see cref="ILatticeAdmin.RefreshStorageUsageAsync"/> (operator-driven
/// deep refresh).
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TreeWalUsageReport)]
[Immutable]
public readonly record struct TreeWalUsageReport
{
    /// <summary>Logical tree id this report describes.</summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>
    /// Sum of retained payload bytes across every WAL partition for this
    /// tree. <c>0</c> for a tree with no WAL retained bytes; <c>-1</c> is
    /// never used here (a partial WAL surface is surfaced through
    /// <see cref="Partial"/> instead).
    /// </summary>
    [Id(1)] public long WalRetainedBytes { get; init; }

    /// <summary>
    /// <c>true</c> when at least one WAL partition's provider reported the
    /// "byte accounting unsupported" sentinel, so <see cref="WalRetainedBytes"/>
    /// is a lower bound. The aggregator does not publish a wrong byte count
    /// for a partial surface.
    /// </summary>
    [Id(2)] public bool Partial { get; init; }

    /// <summary>UTC time at which this WAL-only sample was assembled.</summary>
    [Id(3)] public DateTimeOffset SampledAt { get; init; }
}
