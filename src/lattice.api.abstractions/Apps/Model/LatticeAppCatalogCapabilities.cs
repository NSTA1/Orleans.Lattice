namespace Orleans.Lattice.Api.Apps;

/// <summary>Advisory caller permissions for the app catalogue surface; every flag defaults to denied.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.LatticeAppCatalogCapabilities), Immutable]
public sealed record LatticeAppCatalogCapabilities
{
    /// <summary>Whether the caller may list the configured app sources.</summary>
    [Id(0)] public bool CanListSources { get; init; }
    /// <summary>Whether the caller may list the apps the sources make available.</summary>
    [Id(1)] public bool CanListAvailable { get; init; }
    /// <summary>Whether the caller may describe an app version from a source before install.</summary>
    [Id(2)] public bool CanDescribeFromSource { get; init; }
    /// <summary>Whether the caller may read an app version's icon from a source before install.</summary>
    [Id(3)] public bool CanGetIcon { get; init; }
}
