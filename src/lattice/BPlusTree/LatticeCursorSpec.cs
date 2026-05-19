using System.ComponentModel;

namespace Orleans.Lattice;

/// <summary>
/// The immutable specification of a stateful-cursor scan: the range
/// to scan, the direction, and the kind of work the cursor performs. Captured
/// on <see cref="ILattice.OpenKeyCursorAsync"/> /
/// <see cref="ILattice.OpenEntryCursorAsync"/> /
/// <see cref="ILattice.OpenDeleteRangeCursorAsync"/> and persisted so the
/// cursor grain can resume across silo failovers.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeCursorSpec)]
[Immutable]
public readonly record struct LatticeCursorSpec
{
    /// <summary>The kind of scan this cursor performs.</summary>
    [Id(0)] public LatticeCursorKind Kind { get; init; }

    /// <summary>
    /// Inclusive lower bound of the scan range, or <c>null</c> to start from
    /// the first key. For reverse scans this is still the lexicographic lower
    /// bound (the scan walks from high to low).
    /// </summary>
    [Id(1)] public string? StartInclusive { get; init; }

    /// <summary>
    /// Exclusive upper bound of the scan range, or <c>null</c> to scan to the
    /// end of the tree. For reverse scans this is still the lexicographic
    /// upper bound.
    /// </summary>
    [Id(2)] public string? EndExclusive { get; init; }

    /// <summary>
    /// When <c>true</c>, the cursor walks keys in descending lexicographic
    /// order. Not applicable to <see cref="LatticeCursorKind.DeleteRange"/> -
    /// range deletes are always forward.
    /// </summary>
    [Id(3)] public bool Reverse { get; init; }

    /// <summary>
    /// When <c>true</c>, the cursor is opened in point-in-time mode:
    /// every page is served against the registry snapshot captured at
    /// open time, and the registry pins the snapshot's saga decisions
    /// against tombstone-prune eviction for the cursor's lifetime.
    /// Repeated <c>Next*Async</c> calls observe a stable, linearizable
    /// view of the tree even as concurrent atomic writes commit.
    /// <para>
    /// A point-in-time cursor whose pin TTL elapses (because the
    /// caller stalled past <see cref="LatticeOptions.MaxCursorSnapshotPinTtl"/>)
    /// fails its next step with
    /// <see cref="LatticeCursorSnapshotExpiredException"/>; opening a
    /// point-in-time cursor when the registry-wide pin footprint cap
    /// would be exceeded throws
    /// <see cref="LatticeCursorRegistryPinExhaustedException"/>.
    /// </para>
    /// </summary>
    [Id(4)] public bool PointInTime { get; init; }

    /// <summary>
    /// When <c>true</c>, the cursor is opened in zero-observable-writes
    /// snapshot mode: every page is served by replaying each shard's
    /// WAL up to the offset captured at open time, so foreground
    /// non-saga writes that append after capture are invisible. Pairs
    /// with <see cref="PointInTime"/> (which freezes saga decisions)
    /// to deliver strict tree-wide snapshot isolation against every
    /// dimension the live read path is subject to (foreground writes,
    /// saga decisions, replication apply, topology changes).
    /// <para>
    /// Set internally by
    /// <see cref="ILattice.OpenSnapshotKeyCursorAsync"/> /
    /// <see cref="ILattice.OpenSnapshotEntryCursorAsync"/>; not a
    /// direct opt-in on the existing open methods because snapshot
    /// cursors have a different cost profile, a different failure
    /// mode (<see cref="LatticeSnapshotExpiredException"/>), and a
    /// different acceptance gate
    /// (<see cref="LatticeOptions.MaxSnapshotReplayEntries"/>).
    /// </para>
    /// </summary>
    [Id(5)] public bool ZeroObservableWrites { get; init; }
}

