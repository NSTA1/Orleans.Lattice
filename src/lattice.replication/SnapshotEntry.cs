using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// A single live key-value record produced by an
/// <see cref="ISnapshotProvider"/> export. Each entry carries the
/// committed value and the
/// <see cref="HybridLogicalClock"/> stamped on it at write time so the
/// receiver can pin the value at the same logical timestamp on apply,
/// preserving the snapshot's as-of cut on every replica.
/// <para>
/// The record is intentionally minimal in v1: only live, non-expired
/// entries are exported, and per-entry <c>OriginClusterId</c> /
/// <c>VectorClock</c> slots are omitted because the public
/// <see cref="Orleans.Lattice.ILattice"/> read surface does not yet
/// expose them. The snapshot's tree-level causal-stable frontier is
/// carried on <see cref="SnapshotStream.CausalStableFrontier"/>;
/// per-entry VC preservation is reserved for a future revision once
/// the core library exposes a vector-clock slot on <c>LwwEntry</c>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.SnapshotEntry)]
[Immutable]
public readonly record struct SnapshotEntry
{
    /// <summary>The exported key.</summary>
    [Id(0)] public string Key { get; init; }

    /// <summary>The exported value bytes.</summary>
    [Id(1)] public byte[] Value { get; init; }

    /// <summary>
    /// The <see cref="HybridLogicalClock"/> stamped on the value at
    /// commit time. The receiver applies the value at exactly this
    /// timestamp so the snapshot's as-of cut is preserved across
    /// replicas (including for transitive replication paths).
    /// </summary>
    [Id(2)] public HybridLogicalClock Timestamp { get; init; }
}
