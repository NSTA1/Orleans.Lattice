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
    /// Initialises a new snapshot coordinate carrying per-partition WAL
    /// head offsets captured at open time. Required when the snapshot
    /// covers a tree configured with <see cref="LatticeOptions.WalPartitions"/>
    /// greater than 1: each shard has independent per-partition offset
    /// spaces and the snapshot leaf needs every partition's head to
    /// replay the shard's full WAL slice. The scalar
    /// <see cref="PerShardWalOffsets"/> companion is filled with the
    /// maximum offset across each shard's partitions for diagnostic
    /// continuity with legacy consumers that read only the scalar slot.
    /// </summary>
    /// <param name="treeMapVersion">Routing-map version observed at open time.</param>
    /// <param name="perShardPerPartitionWalOffsets">
    /// Per-shard, per-partition WAL head offsets at open time. Each
    /// key is a virtual shard index; each value is an array of
    /// next-to-be-assigned offsets, indexed by WAL partition number.
    /// Must not be <see langword="null"/>.
    /// </param>
    /// <param name="registrySnapshotHlc">HLC stamped on the registry-snapshot scope captured at open time.</param>
    public LatticeSnapshotCoordinate(
        long treeMapVersion,
        IReadOnlyDictionary<int, IReadOnlyList<long>> perShardPerPartitionWalOffsets,
        HybridLogicalClock registrySnapshotHlc)
    {
        ArgumentNullException.ThrowIfNull(perShardPerPartitionWalOffsets);
        TreeMapVersion = treeMapVersion;
        var scalar = new Dictionary<int, long>(perShardPerPartitionWalOffsets.Count);
        foreach (var (shard, partitionOffsets) in perShardPerPartitionWalOffsets)
        {
            long max = 0;
            for (var i = 0; i < partitionOffsets.Count; i++)
            {
                if (partitionOffsets[i] > max) max = partitionOffsets[i];
            }
            scalar[shard] = max;
        }
        PerShardWalOffsets = scalar;
        PerShardPerPartitionWalOffsets = perShardPerPartitionWalOffsets;
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

    /// <summary>
    /// Per-shard, per-partition WAL head offsets captured at open time
    /// when the snapshot covers a tree configured with
    /// <see cref="LatticeOptions.WalPartitions"/> greater than 1. Each
    /// key is a virtual shard index; each value is an array of
    /// next-to-be-assigned offsets, indexed by WAL partition number.
    /// <para>
    /// <see langword="null"/> on coordinates persisted before this slot
    /// was introduced, or on coordinates whose origin tree was
    /// configured with <see cref="LatticeOptions.WalPartitions"/> = 1
    /// (in which case the legacy scalar <see cref="PerShardWalOffsets"/>
    /// fully describes the captured frontier). The cursor grain
    /// prefers this slot when non-null and falls back to wrapping each
    /// scalar offset in <see cref="PerShardWalOffsets"/> as a single-
    /// element array when this slot is null, preserving the legacy
    /// replay semantics for single-partition snapshots.
    /// </para>
    /// </summary>
    [Id(3)] public IReadOnlyDictionary<int, IReadOnlyList<long>>? PerShardPerPartitionWalOffsets { get; init; }
}
