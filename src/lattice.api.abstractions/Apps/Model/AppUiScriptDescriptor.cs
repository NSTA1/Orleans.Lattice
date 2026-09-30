namespace Orleans.Lattice.Api.Apps;

/// <summary>A script in an app UI bundle, loaded in declaration order.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppUiScriptDescriptor), Immutable]
public sealed record AppUiScriptDescriptor
{
    /// <summary>The normalised, bundle-relative script path.</summary>
    [Id(0)] public required string Path { get; init; }
    /// <summary>Whether the script is loaded as an ECMAScript module.</summary>
    [Id(1)] public bool Module { get; init; }
}
