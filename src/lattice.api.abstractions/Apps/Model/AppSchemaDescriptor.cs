namespace Orleans.Lattice.Api.Apps;

/// <summary>Manifest schema intent for an app-local tree, never a composed physical id.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppSchemaDescriptor), Immutable]
public sealed record AppSchemaDescriptor
{
    /// <summary>The declared local tree name whose values are versioned.</summary>
    [Id(0)] public required string Tree { get; init; }
    /// <summary>The schema family identifier.</summary>
    [Id(1)] public required string Family { get; init; }
    /// <summary>The requested envelope version.</summary>
    [Id(2)] public int Version { get; init; }
    /// <summary>Whether strict schema ingest is requested.</summary>
    [Id(3)] public bool StrictIngest { get; init; }
}
