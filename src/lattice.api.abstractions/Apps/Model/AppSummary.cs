namespace Orleans.Lattice.Api.Apps;

/// <summary>A tree-free summary of an installed app in the active isolation context.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppSummary), Immutable]
public sealed record AppSummary
{
    /// <summary>The installed app slug.</summary>
    [Id(0)] public required string Slug { get; init; }
    /// <summary>The installed semantic version.</summary>
    [Id(1)] public required string Version { get; init; }
    /// <summary>The installation's current lifecycle state.</summary>
    [Id(2)] public AppLifecycleState State { get; init; }
    /// <summary>The recorded source metadata, not an authorization claim.</summary>
    [Id(3)] public required AppProvenanceDescriptor Provenance { get; init; }
}
