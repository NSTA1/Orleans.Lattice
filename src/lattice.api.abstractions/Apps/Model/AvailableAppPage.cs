using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps;

/// <summary>One page of available apps.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AvailableAppPage), Immutable]
public sealed record AvailableAppPage
{
    /// <summary>The apps on this page, ordered by slug and then source key.</summary>
    [Id(0)] public ImmutableArray<AvailableAppSummary> Apps { get; init; } = [];
    /// <summary>The opaque continuation for the next page, or null on the last page.</summary>
    [Id(1)] public string? Continuation { get; init; }
}
