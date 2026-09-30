namespace Orleans.Lattice.Apps;

/// <summary>
/// How an app is shown, inspectable before any app code loads. Every text member is untrusted
/// plain text: consumers render it as text and never interpret it as HTML or markdown. Nothing
/// here is evidence of trust; the identity's provenance remains the trust record.
/// </summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppPresentation)]
public sealed record AppPresentation
{
    /// <summary>Single-line display name of 1-60 characters.</summary>
    [Id(0)] public required string DisplayName { get; init; }

    /// <summary>Optional single-line summary of at most 160 characters.</summary>
    [Id(1)] public string? Summary { get; init; }

    /// <summary>
    /// Optional description of at most 4000 characters. Plain text with line breaks allowed;
    /// never interpreted as HTML or markdown by any consumer.
    /// </summary>
    [Id(2)] public string? Description { get; init; }

    /// <summary>Optional icon, rendered only through an <c>&lt;img&gt;</c> element.</summary>
    [Id(3)] public AppIconReference? Icon { get; init; }

    /// <summary>Optional list of 0-5 unique categories, each matching <c>^[a-z][a-z0-9-]{1,30}$</c>.</summary>
    [Id(4)] public string[]? Categories { get; init; }

    /// <summary>Optional absolute <c>https</c> documentation URL, without user information.</summary>
    [Id(5)] public string? DocumentationUrl { get; init; }

    /// <summary>
    /// Optional single-line publisher name of at most 80 characters. Descriptive only and never
    /// used for trust decisions.
    /// </summary>
    [Id(6)] public string? PublisherDisplayName { get; init; }
}
