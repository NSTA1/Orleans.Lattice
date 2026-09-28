namespace Orleans.Lattice.Apps;

/// <summary>
/// The size bounds a manifest must fit within. A manifest is inspected before any app code
/// loads, and under runtime installation it is untrusted input, so parsing and validation
/// are bounded: a hostile manifest can fail activation but can never exhaust memory or CPU.
/// The bounds sit well above any real app's shape.
/// </summary>
internal static class AppManifestLimits
{
    /// <summary>The largest manifest, in UTF-16 characters of JSON text (or UTF-8 bytes of a stream), that is parsed.</summary>
    internal const int MaxManifestChars = 1024 * 1024;

    /// <summary>The most entries any one manifest section, or the scopes of one role, may hold.</summary>
    internal const int MaxSectionItems = 256;

    /// <summary>The longest adopted tree id, key or prefix, schema family or provenance field.</summary>
    internal const int MaxTextLength = 1024;

    /// <summary>The longest MCP tool description.</summary>
    internal const int MaxDescriptionLength = 4096;

    /// <summary>The longest presentation display name.</summary>
    internal const int MaxDisplayNameLength = 60;

    /// <summary>The longest presentation summary.</summary>
    internal const int MaxSummaryLength = 160;

    /// <summary>The longest presentation description.</summary>
    internal const int MaxPresentationDescriptionLength = 4000;

    /// <summary>The longest presentation publisher display name.</summary>
    internal const int MaxPublisherDisplayNameLength = 80;

    /// <summary>The most presentation categories.</summary>
    internal const int MaxCategories = 5;

    /// <summary>The longest presentation documentation URL.</summary>
    internal const int MaxUrlLength = 2048;
}
