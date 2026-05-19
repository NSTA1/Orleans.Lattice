namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Lifecycle phase of a stateful cursor. Persisted so that a
/// reactivated cursor grain can resume exactly where it left off, even across
/// silo failovers.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeCursorPhase)]
internal enum LatticeCursorPhase : byte
{
    /// <summary>The cursor has never been opened (default on fresh activation).</summary>
    NotStarted = 0,

    /// <summary>The cursor is open and may be paged.</summary>
    Open = 1,

    /// <summary>The cursor has been fully drained (no more keys in range).</summary>
    Exhausted = 2,

    /// <summary>The cursor has been closed by the client and will self-deactivate.</summary>
    Closed = 3,
}

/// <summary>
/// Persistent state for <see cref="Grains.LatticeCursorGrain"/>. Tracks the
/// scan spec and last-yielded key so the cursor can resume after silo
/// failovers and topology changes. Key format: <c>{treeId}/{cursorId}</c>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeCursorState)]
internal sealed class LatticeCursorState
{
    /// <summary>Current lifecycle phase of the cursor.</summary>
    [Id(0)] public LatticeCursorPhase Phase { get; set; } = LatticeCursorPhase.NotStarted;

    /// <summary>
    /// Tree ID the cursor scans. Captured on
    /// <see cref="ILatticeCursorGrain.OpenAsync"/> so the grain can deterministically
    /// address the correct <see cref="ILattice"/> on reactivation.
    /// </summary>
    [Id(1)] public string TreeId { get; set; } = string.Empty;

    /// <summary>The scan specification the cursor was opened with.</summary>
    [Id(2)] public LatticeCursorSpec Spec { get; set; }

    /// <summary>
    /// The last key yielded to the client (or tombstoned by a delete-range
    /// step). <c>null</c> before the first page. On the next step the cursor
    /// resumes with
    /// <c>startInclusive = LastYieldedKey + "\0"</c> for forward scans or
    /// <c>endExclusive = LastYieldedKey</c> for reverse scans.
    /// </summary>
    [Id(3)] public string? LastYieldedKey { get; set; }

    /// <summary>
    /// Running count of keys tombstoned by a <see cref="LatticeCursorKind.DeleteRange"/>
    /// cursor across all steps. Zero for key/entry cursors.
    /// </summary>
    [Id(4)] public int DeletedTotal { get; set; }

    /// <summary>
    /// The registry snapshot captured on
    /// <see cref="ILatticeCursorGrain.OpenAsync"/> when
    /// <see cref="LatticeCursorSpec.PointInTime"/> is <c>true</c>.
    /// Replayed on every <c>Next*Async</c> step via
    /// <see cref="LatticeRegistrySnapshotContext.BeginScope"/> so a
    /// resumed cursor (after silo failover or activation recycling)
    /// continues to serve the same point-in-time view it opened with.
    /// <c>null</c> for non-point-in-time cursors and on every closed /
    /// exhausted cursor (we proactively clear it on
    /// <see cref="LatticeCursorPhase.Closed"/> /
    /// <see cref="LatticeCursorPhase.Exhausted"/> to free state - the
    /// pin has already been released by then).
    /// </summary>
    [Id(5)] public Dictionary<Guid, TxStatus>? PointInTimeSnapshot { get; set; }

    /// <summary>
    /// Identifier of the registry-side pin that holds the cursor's
    /// captured saga-decision snapshot against tombstone-prune
    /// eviction. Server-assigned (<see cref="Guid.NewGuid"/>) at open
    /// time, persisted so a reactivated cursor can refresh / release
    /// the same pin its snapshot belongs to. <see cref="Guid.Empty"/>
    /// for non-point-in-time cursors.
    /// </summary>
    [Id(6)] public Guid SnapshotPinId { get; set; }
}
