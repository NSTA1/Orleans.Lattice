namespace Orleans.Lattice.Replication;

/// <summary>
/// The source lineage an authenticated sender stamped on the batch an entry
/// arrived in (issue #4673), kept with the entry wherever it waits to be applied
/// - the causal-apply buffer and the dead-letter queue - so the later apply is
/// checked against the lineage the receiver has drained since (issue #4707).
/// </summary>
/// <param name="SourceClusterId">The authenticated sender that stamped the lineage.</param>
/// <param name="Lineage">The source lineage the sender read the batch under.</param>
[GenerateSerializer]
[Immutable]
[Alias(ReplicationTypeAliases.ReplicationSourceLineageStamp)]
internal readonly record struct ReplicationSourceLineageStamp(
    [property: Id(0)] string SourceClusterId,
    [property: Id(1)] Guid Lineage);
