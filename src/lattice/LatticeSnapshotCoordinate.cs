using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Immutable tree-wide coordinate
/// that a zero-observable-writes snapshot cursor was opened. Captured
/// by <see cref="ILattice.OpenSnapshotKeyCursorAsync"/> /
/// <see cref="ILattice.OpenSnapshotEntryCursorAsync"/> at open time and
/// carried on the cursor's persisted state so its view is stable
/// across silo failovers and grain reactivations.
/// <para>
/// The coordinate has three components:
/// </para>
/// <list type="bullet">
/// <item>
/// <description>
/// <see cref="TreeMapVersion"/> - the routing-map version observed at
/// open time. Topology changes that publish a new shard map after
/// capture are invisible to the snapshot; the cursor continues to
/// route to the pre-change layout.
/// </description>
/// </item>
/// <item>
/// <description>
/// <see cref="PerShardWalOffsets"/> - the next-to-be-assigned WAL
/// sequence number on every shard the snapshot covers, captured by
/// <c>IShardRootGrain.SnapshotWalHeadAsync</c>. The snapshot leaves
/// replay records <c>[0, offset)</c> on each shard; writes that
/// append after the capture are invisible by construction.
/// </description>
/// </item>
/// <item>
/// <description>
/// <see cref="RegistrySnapshotHlc"/> - the HLC stamped on the
/// <see cref="LatticeRegistrySnapshotContext"/> snapshot taken at
/// open time. Saga decisions that commit after the capture are
/// hidden uniformly across every shard the snapshot replays.
/// </description>
/// </item>
/// </list>
/// <para>
/// The fan-out that builds the coordinate is concurrent across
/// shards, so the captured WAL offsets are not linearisable in
/// real time - but determinism does not require real-time
/// linearisability. The <see cref="RegistrySnapshotHlc"/> half
/// resolves saga visibility uniformly across shards, so the
/// snapshot view of any single atomic write is all-or-nothing on
/// every shard the write touched.
/// </para>
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.LatticeSnapshotCoordinate)]
public readonly record struct LatticeSnapshotCoordinate
{
    /// <summary>
    /// Initialises a new snapshot coordinate.
    /// </summary>
    /// <param name="treeMapVersion">Routing-map version observed at open time.</param>
    /// <param name="perShardWalOffsets">
    /// Per-shard WAL head offsets at open time. The dictionary's keys
    /// are virtual shard indices; values are next-to-be-assigned
    /// sequence numbers (so replay covers offsets <c>[0, value)</c>).
    /// May be empty for a coordinate that covers no shards.
    /// </param>
    /// <param name="registrySnapshotHlc">
    /// HLC stamped on the registry-snapshot scope captured at open
    /// time. <see cref="HybridLogicalClock.Zero"/> if the snapshot
    /// covers no saga decisions.
    /// </param>
    public LatticeSnapshotCoordinate(
        long treeMapVersion,
        IReadOnlyDictionary<int, long> perShardWalOffsets,
        HybridLogicalClock registrySnapshotHlc)
    {
        ArgumentNullException.ThrowIfNull(perShardWalOffsets);
        TreeMapVersion = treeMapVersion;
        PerShardWalOffsets = perShardWalOffsets;
        RegistrySnapshotHlc = registrySnapshotHlc;
    }

    /// <summary>
    /// Routing-map version observed at <c>OpenSnapshot*Async</c> time.
    /// A topology change that publishes a new shard map after capture
    /// does not perturb the snapshot's view: the cursor continues to
    /// route to the layout pinned by this version.
    /// </summary>
    [Id(0)] public long TreeMapVersion { get; init; }

    /// <summary>
    /// Per-shard WAL head offsets captured at open time. Each entry
    /// maps a virtual shard index to that shard's next-to-be-assigned
    /// WAL sequence number; replay materialises offsets
    /// <c>[0, value)</c>. Writes appended after capture are invisible
    /// by construction.
    /// </summary>
    [Id(1)] public IReadOnlyDictionary<int, long> PerShardWalOffsets { get; init; } = new Dictionary<int, long>();

    /// <summary>
    /// HLC stamped on the registry-snapshot scope captured at open
    /// time. Saga decisions that commit after this HLC are hidden
    /// uniformly across every shard the snapshot replays;
    /// <see cref="HybridLogicalClock.Zero"/> indicates the snapshot
    /// covers no saga decisions.
    /// </summary>
    [Id(2)] public HybridLogicalClock RegistrySnapshotHlc { get; init; }
}
