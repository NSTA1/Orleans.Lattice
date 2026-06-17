using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Views;

/// <summary>
/// Durable per-view checkpoint persisted by the view maintainer grain. Records,
/// for each source WAL partition, the offset of the last entry applied to the
/// view, plus the <see cref="ILatticeViewProjection.ProjectionVersion"/> the view
/// was built with and the highest source <see cref="HybridLogicalClock"/> applied
/// so far (reported to the WAL cursor registry to pin garbage collection).
/// <para>
/// On activation the maintainer compares the persisted
/// <see cref="ProjectionVersion"/> against the live projection's version; a
/// mismatch means the projection logic changed and the view is rebuilt from the
/// current source state.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ViewCheckpointState)]
internal sealed class ViewCheckpointState
{
    /// <summary>
    /// Last applied WAL offset per source partition index. An absent partition
    /// (or value <c>-1</c>) means nothing has been applied from that partition;
    /// the next read uses the value as the exclusive lower bound.
    /// </summary>
    [Id(0)]
    public Dictionary<int, long> AppliedOffsets { get; set; } = new();

    /// <summary>
    /// The projection version the view was built with. Empty until the first
    /// successful activation. A mismatch with the live projection triggers a
    /// rebuild.
    /// </summary>
    [Id(1)]
    public string ProjectionVersion { get; set; } = string.Empty;

    /// <summary>
    /// The highest source HLC the maintainer has applied to the view, reported
    /// to the WAL cursor registry so the source WAL is not trimmed past it.
    /// </summary>
    [Id(2)]
    public HybridLogicalClock HighestAppliedTimestamp { get; set; }

    /// <summary>
    /// Monotonic rebuild generation, bumped once per in-place rebuild. It seeds
    /// the deterministic idempotency key of an aggregation view's atomic
    /// membership + accumulator flip, so a crash-replay of a normal drain reuses
    /// the same key (and dedups), while a rebuild - which clears the view tree
    /// but not the completed sagas (they are retained for up to
    /// <see cref="LatticeOptions.AtomicWriteRetention"/>) - mints fresh keys and
    /// therefore re-applies from scratch rather than re-attaching to the
    /// pre-rebuild sagas of the now-deleted rows. Unused by the filter /
    /// re-project view kind.
    /// </summary>
    [Id(3)]
    public long RebuildGeneration { get; set; }
}
