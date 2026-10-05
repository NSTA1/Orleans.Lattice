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
    /// Records the origin's shipped aggregate low watermark under its aggregate
    /// <paramref name="generation"/> (issue #4586 part 2b), and returns whether
    /// the recorded value changed. A generation older than one already seen, or
    /// than the oldest still accepted, is ignored; a newer one replaces the
    /// value, which may lower it; the same generation only raises it. Persisted
    /// lazily, which only delays dependents.
    /// </summary>
    Task<bool> RecordLowWatermarkAsync(HybridLogicalClock lowWatermark, long generation, CancellationToken cancellationToken = default);

    /// <summary>
    /// The effective low watermark: the recorded aggregate, capped by every
    /// tree whose lineage changed and that the origin has not yet re-covered.
    /// </summary>
    Task<HybridLogicalClock> GetLowWatermarkAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Caps the effective low watermark at <paramref name="cap"/> on behalf of
    /// <paramref name="treeId"/>, replacing any cap that tree had: the tree's
    /// contents changed lineage, so the origin's aggregate may still count
    /// coverage of writes the new contents lack. Durable before it returns.
    /// </summary>
    Task SetTreeCapAsync(string treeId, HybridLogicalClock cap, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lifts <paramref name="treeId"/>'s cap once the origin re-covered the tree
    /// in its new lineage at aggregate <paramref name="generation"/>, and from
    /// then on ignores any aggregate of an older generation. Durable before it
    /// returns.
    /// </summary>
    Task LiftTreeCapAsync(string treeId, long generation, CancellationToken cancellationToken = default);

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
