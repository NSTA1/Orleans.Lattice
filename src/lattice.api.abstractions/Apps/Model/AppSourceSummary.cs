namespace Orleans.Lattice.Api.Apps;

/// <summary>A configured app source, as the administrative catalogue reports it.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppSourceSummary), Immutable]
public sealed record AppSourceSummary
{
    /// <summary>The stable source key recorded in provenance, such as in-image.</summary>
    [Id(0)] public required string Key { get; init; }
    /// <summary>The operator-facing source name; descriptive only.</summary>
    [Id(1)] public required string DisplayName { get; init; }
    /// <summary>Whether the source's offer is fixed at startup or can change at run time.</summary>
    [Id(2)] public AppSourceSummaryKind Kind { get; init; }
    /// <summary>The optional capabilities the source supports.</summary>
    [Id(3)] public AppSourceSummaryCapabilities Capabilities { get; init; }
}
