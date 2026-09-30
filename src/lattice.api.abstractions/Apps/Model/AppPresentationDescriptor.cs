using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// How an app is shown, mirrored from the manifest's optional presentation section.
/// </summary>
/// <remarks>
/// Every text member is untrusted, app-supplied content: consumers render it as plain
/// text and never interpret it as HTML or markdown. The publisher display name is
/// descriptive only; provenance remains the trust record.
/// </remarks>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppPresentationDescriptor), Immutable]
public sealed record AppPresentationDescriptor
{
    /// <summary>The app's display name.</summary>
    [Id(0)] public required string DisplayName { get; init; }
    /// <summary>A one-line summary, or null when none is declared.</summary>
    [Id(1)] public string? Summary { get; init; }
    /// <summary>A plain-text description that may contain line breaks, or null when none is declared.</summary>
    [Id(2)] public string? Description { get; init; }
    /// <summary>The declared icon, reduced to its path and digest, or null when none is declared.</summary>
    [Id(3)] public AppIconDescriptor? Icon { get; init; }
    /// <summary>The declared categories; empty when none are declared.</summary>
    [Id(4)] public ImmutableArray<string> Categories { get; init; } = [];
    /// <summary>An absolute https documentation URL, or null when none is declared.</summary>
    [Id(5)] public string? DocumentationUrl { get; init; }
    /// <summary>A descriptive publisher name, never used for trust, or null when none is declared.</summary>
    [Id(6)] public string? PublisherDisplayName { get; init; }
}
