using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps;

/// <summary>One page of entries scanned through the app bridge.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppBridgePage), Immutable]
public sealed record AppBridgePage
{
    /// <summary>The entries on this page, in ordinal key order.</summary>
    [Id(0)] public ImmutableArray<AppBridgeValue> Entries { get; init; } = [];
    /// <summary>The opaque continuation for the next page, or null on the last page.</summary>
    [Id(1)] public string? Continuation { get; init; }
}
