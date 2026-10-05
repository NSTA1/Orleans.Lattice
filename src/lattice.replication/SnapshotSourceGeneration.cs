namespace Orleans.Lattice.Replication;

/// <summary>
/// Describes the source tree generation observed around a snapshot export.
/// Receivers compare the opening and closing values before synthesising deletes
/// for source-origin keys that disappeared from an in-place re-bootstrap.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.SnapshotSourceGeneration)]
[Immutable]
public readonly record struct SnapshotSourceGeneration
{
    /// <summary>The physical tree id resolved from the logical source tree, or <see langword="null"/> when unknown.</summary>
    [Id(0)] public string? PhysicalTreeId { get; init; }

    /// <summary>The registry shard-map version resolved at the source, or <see langword="null"/> when unknown.</summary>
    [Id(1)] public long? ShardMapVersion { get; init; }

    /// <summary>The content-lineage token stamped on the source logical tree, or <see langword="null"/> when unknown.</summary>
    [Id(2)] public Guid? Lineage { get; init; }

    /// <summary>The source tree soft-delete epoch, or <see langword="null"/> when unknown.</summary>
    [Id(3)] public long? DeleteEpoch { get; init; }

    /// <summary>Whether the source tree was soft-deleted or purging, or <see langword="null"/> when unknown.</summary>
    [Id(4)] public bool? IsDeleted { get; init; }
}
