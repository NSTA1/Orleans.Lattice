namespace Orleans.Lattice.Api.Apps;

/// <summary>Manifest replication intent for an app-local tree, never a composed physical id.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppReplicationDescriptor), Immutable]
public sealed record AppReplicationDescriptor
{
    /// <summary>The declared local tree name to replicate.</summary>
    [Id(0)] public required string Tree { get; init; }
    /// <summary>The requested replication merge mode.</summary>
    [Id(1)] public LatticeMergeMode MergeMode { get; init; }
}
