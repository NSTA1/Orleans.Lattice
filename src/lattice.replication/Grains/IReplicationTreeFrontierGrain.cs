namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The receiver's per-tree causal frontier (issue #4586 part 2b), keyed by the
/// tree id. It owns the frontier epoch it acknowledges to senders, records each
/// origin's shipped applied low watermark for the tree under that epoch, and on
/// every possible replacement of the tree's contents re-mints the epoch, zeroes
/// every origin's watermark and caps each origin's aggregate on its frontier,
/// until a full bootstrap re-seeds the tree and the origin re-covers it.
/// </summary>
[Alias(ReplicationTypeAliases.IReplicationTreeFrontierGrain)]
internal interface IReplicationTreeFrontierGrain : IGrainWithStringKey
{
    /// <summary>
    /// Records a push to the tree from the authenticated
    /// <paramref name="originClusterId"/> and the watermark it shipped, if any,
    /// and returns the epoch to acknowledge (<see cref="Guid.Empty"/> in
    /// degraded mode). A watermark tagged with any other epoch, or arriving
    /// before a re-seed the tree awaits, is ignored.
    /// </summary>
    Task<Guid> ObserveAsync(string originClusterId, ReplicationSourceFrontier? shipped, CancellationToken cancellationToken = default);

    /// <summary>
    /// The tree's contents may be about to be replaced: re-mint the epoch, zero
    /// every origin's watermark, cap each origin's aggregate and forget the
    /// tree's applied identities. Durable before it returns; a failure must
    /// propagate to the caller, which must not replace the contents.
    /// </summary>
    Task OnContentsReplacingAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// The tree registry is about to persist <paramref name="nextLineage"/> as
    /// the tree's lineage (<see langword="null"/>: the tree is being unregistered
    /// or purged): force the gap as <see cref="OnContentsReplacingAsync"/> does,
    /// and record the lineage the change produces, so settling against the
    /// registry afterwards does not force a second one. Durable before it
    /// returns; a failure propagates and the registry does not persist the
    /// change.
    /// </summary>
    Task OnLineageChangingAsync(Guid? nextLineage, CancellationToken cancellationToken = default);

    /// <summary>
    /// A full bootstrap begun under <paramref name="epoch"/> completed: install
    /// the source's per-origin low watermarks and the writes the source held at
    /// export open. Ignored, returning <see langword="false"/>, when the epoch
    /// changed meanwhile.
    /// </summary>
    Task<bool> PinAsync(
        Guid epoch,
        IReadOnlyDictionary<string, HybridLogicalClock> sourceLowWatermarks,
        IReadOnlyDictionary<string, HybridLogicalClock[]> sourceHeld,
        CancellationToken cancellationToken = default);

    /// <summary>The tree's current epoch and applied low watermarks.</summary>
    Task<ReplicationTreeFrontierSnapshot> GetAsync(CancellationToken cancellationToken = default);
}