namespace Orleans.Lattice.Apps.Sources;

/// <summary>
/// One page request to <see cref="IAppCatalogSource.ListAsync"/>: an optional text filter, a page size
/// clamped to [<see cref="MinPageSize"/>, <see cref="MaxPageSize"/>], and the opaque continuation a previous
/// page returned.
/// </summary>
public sealed record AppSourceQuery
{
    /// <summary>The smallest page size a query can request.</summary>
    public const int MinPageSize = 1;

    /// <summary>The largest page size a query can request.</summary>
    public const int MaxPageSize = 200;

    /// <summary>The page size used when none is set.</summary>
    public const int DefaultPageSize = 50;

    private readonly int pageSize = DefaultPageSize;

    /// <summary>A query for the first page at the default page size, with no text filter.</summary>
    public static AppSourceQuery Default { get; } = new();

    /// <summary>
    /// An optional free-text filter. A source honours it only when it advertises
    /// <see cref="AppSourceCapabilities.Search"/>; any other source ignores it and lists everything.
    /// </summary>
    public string? Text { get; init; }

    /// <summary>The maximum number of entries per page; a value outside the allowed range is clamped into it.</summary>
    public int PageSize
    {
        get => pageSize;
        init => pageSize = Math.Clamp(value, MinPageSize, MaxPageSize);
    }

    /// <summary>
    /// The opaque continuation returned as <see cref="AppSourcePage.Continuation"/> by the previous page, or
    /// null for the first page. Only the source that issued it can interpret it.
    /// </summary>
    public string? Continuation { get; init; }
}
