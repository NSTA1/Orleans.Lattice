namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// A receiver's record of the last whole-tree export it began draining from one
/// source (issue #4673): the source lineage the export opened under, and the
/// receiver tree frontier epoch the drain began in.
/// </summary>
/// <param name="Lineage">The source lineage the export opened under.</param>
/// <param name="FrontierEpoch">The receiver tree frontier epoch when the drain began.</param>
/// <param name="CutoverPending">
/// Whether a bootstrap from this source is publishing its shadow copy and has not
/// yet recorded the frontier epoch the cutover re-minted (issue #5534). While it
/// is, <paramref name="FrontierEpoch"/> may trail the tree frontier although the
/// tree still holds this drain. A peer that predates the slot decodes
/// <see langword="false"/>.
/// </param>
[GenerateSerializer]
[Immutable]
[Alias(ReplicationTypeAliases.ReplicationDrainedLineage)]
internal readonly record struct ReplicationDrainedLineage(
    [property: Id(0)] Guid Lineage,
    [property: Id(1)] Guid FrontierEpoch,
    [property: Id(2)] bool CutoverPending = false);
