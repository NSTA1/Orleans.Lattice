namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The receiver's per-origin causal frontier (issue #4586), keyed by the
/// origin cluster id. It records the low watermark the origin ships - every
/// write of the origin stamped below it has been acknowledged here - and which
/// of the origin's writes this receiver acknowledged without applying: held in
/// any tree's causal-apply buffer or dead-letter queue, or marked lost. With
/// those, <see cref="CheckAsync"/> decides a dependency whose exact identity the
/// tree no longer remembers, across every tree, because a dependency names an
/// origin's write and not a tree.
/// </summary>
[Alias(ReplicationTypeAliases.IReplicationOriginFrontierGrain)]
internal interface IReplicationOriginFrontierGrain : IGrainWithStringKey
{
    /// <summary>
    /// Raises the recorded low watermark to <paramref name="lowWatermark"/> when
    /// it is higher, and returns whether it moved. The watermark is kept in
    /// memory only: a reactivation forgets it until the origin ships the next
    /// one, which only delays dependents.
    /// </summary>
    Task<bool> RecordLowWatermarkAsync(HybridLogicalClock lowWatermark, CancellationToken cancellationToken = default);

    /// <summary>The recorded low watermark, or zero when none was received since activation.</summary>
    Task<HybridLogicalClock> GetLowWatermarkAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Replaces the set of this origin's writes that <paramref name="source"/>
    /// (one tree's causal-apply buffer or dead-letter queue) holds without having
    /// applied them. A source publishes before it acknowledges a newly held
    /// write, and publishes the destination of a move before it releases the
    /// origin, so the recorded sets always cover every held write. Durable.
    /// </summary>
    Task SetHeldAsync(string source, IReadOnlyCollection<HybridLogicalClock> held, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks this origin's writes at <paramref name="lost"/> as lost for good:
    /// acknowledged, never to be applied. A dependent of one is dead-lettered.
    /// Durable and permanent.
    /// </summary>
    Task RecordLostAsync(IReadOnlyCollection<HybridLogicalClock> lost, CancellationToken cancellationToken = default);

    /// <summary>
    /// Decides each dependency on this origin's write at an HLC in
    /// <paramref name="required"/> by <see cref="CausalFrontierCore.Decide"/>. A
    /// write a source still lists is confirmed with that source first, so a
    /// listing left behind by a crash cannot block a dependent forever.
    /// </summary>
    Task<CausalDependencyVerdict[]> CheckAsync(IReadOnlyList<HybridLogicalClock> required, CancellationToken cancellationToken = default);
}
