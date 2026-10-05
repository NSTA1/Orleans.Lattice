namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// A receiver's record of the last whole-tree export it began draining from one
/// source (issue #4673): the source lineage the export opened under, and the
/// receiver tree frontier epoch the drain began in.
/// </summary>
/// <param name="Lineage">The source lineage the export opened under.</param>
/// <param name="FrontierEpoch">The receiver tree frontier epoch when the drain began.</param>
[GenerateSerializer]
[Immutable]
[Alias(ReplicationTypeAliases.ReplicationDrainedLineage)]
internal readonly record struct ReplicationDrainedLineage(
    [property: Id(0)] Guid Lineage,
    [property: Id(1)] Guid FrontierEpoch);
