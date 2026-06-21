namespace Orleans.Lattice.Api.State;

/// <summary>
/// One page of the tree catalog. <see cref="NextPageToken"/> is the cursor to
/// pass back in the next <see cref="CatalogRequest"/> to continue enumeration;
/// it is <see langword="null"/> on the final page.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.TreeCatalogPage)]
[Immutable]
public sealed record TreeCatalogPage
{
    /// <summary>The catalog entries on this page, ordered by tree id.</summary>
    [Id(0)] public IReadOnlyList<TreeCatalogEntry> Entries { get; init; } = Array.Empty<TreeCatalogEntry>();

    /// <summary>
    /// The continuation cursor for the next page, or <see langword="null"/>
    /// when this is the last page.
    /// </summary>
    [Id(1)] public string? NextPageToken { get; init; }
}

/// <summary>
/// One page of the materialised-view catalog. <see cref="NextPageToken"/> is
/// the cursor to pass back in the next <see cref="CatalogRequest"/> to continue
/// enumeration; it is <see langword="null"/> on the final page.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.ViewCatalogPage)]
[Immutable]
public sealed record ViewCatalogPage
{
    /// <summary>The view summaries on this page, ordered by view name.</summary>
    [Id(0)] public IReadOnlyList<ViewStateSummary> Entries { get; init; } = Array.Empty<ViewStateSummary>();

    /// <summary>
    /// The continuation cursor for the next page, or <see langword="null"/>
    /// when this is the last page.
    /// </summary>
    [Id(1)] public string? NextPageToken { get; init; }
}
