namespace Orleans.Lattice.Apps;

/// <summary>Descriptive origin metadata, never evidence of trust or a grant of authority.</summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppProvenance), Immutable]
public sealed record AppProvenance
{
    /// <summary>Source provider key; defaults to the in-image provider.</summary>
    [Id(0)] public string Source { get; init; } = "in-image";

    /// <summary>Declared publisher; defaults to first-party, but is not authenticated by parsing.</summary>
    [Id(1)] public string Publisher { get; init; } = "first-party";

    /// <summary>Optional source-specific artifact locator, interpreted only by a source provider.</summary>
    [Id(2)] public string? Reference { get; init; }
}
