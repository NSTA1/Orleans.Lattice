namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The outcome of an operator-triggered tombstone-compaction pass scoped to a
/// single physical shard. Reports the shard the pass was requested for and whether
/// the coordinator accepted the request. Triggering removes only tombstones and
/// expired entries and never touches live data, so it is mutating but
/// non-destructive; it is idempotent and reminder-durable.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeCompactionTriggerResult)]
[Immutable]
public sealed record TreeCompactionTriggerResult
{
    /// <summary>The tree id whose shard was targeted.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The physical shard index the compaction pass was requested for.</summary>
    [Id(1)] public int ShardIndex { get; init; }

    /// <summary>
    /// <see langword="true"/> when the request was accepted and the coordinator
    /// transitioned into a scoped pass; <see langword="false"/> when compaction is
    /// disabled for the tree (an infinite tombstone grace period) or a pass was
    /// already in flight for the shard.
    /// </summary>
    [Id(2)] public bool Accepted { get; init; }
}
