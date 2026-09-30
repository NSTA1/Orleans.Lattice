using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// The untrusted UI bundle an app may ship, mirrored from the manifest's optional ui
/// section. Every file is pinned by digest and inspectable before any code loads.
/// </summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppUiDescriptor), Immutable]
public sealed record AppUiDescriptor
{
    /// <summary>The bundle-relative path of the HTML fragment the frame inserts into its body.</summary>
    [Id(0)] public required string Entry { get; init; }
    /// <summary>The stylesheet paths, in load order.</summary>
    [Id(1)] public ImmutableArray<string> Styles { get; init; } = [];
    /// <summary>The scripts, in load order.</summary>
    [Id(2)] public ImmutableArray<AppUiScriptDescriptor> Scripts { get; init; } = [];
    /// <summary>The complete list of bundle files with their media types and digests.</summary>
    [Id(3)] public ImmutableArray<AppUiAssetDescriptor> Assets { get; init; } = [];
    /// <summary>The digest over every asset path and digest; the bundle's cache identity.</summary>
    [Id(4)] public required string BundleDigest { get; init; }
    /// <summary>
    /// The requested bridge grants, one per operation and tree pair; a grant with a null
    /// tree covers every tree the app declares.
    /// </summary>
    [Id(5)] public ImmutableArray<AppUiBridgeGrantDescriptor> Bridge { get; init; } = [];
    /// <summary>The minimum in-frame protocol version the bundle requires.</summary>
    [Id(6)] public int MinProtocol { get; init; }
}
