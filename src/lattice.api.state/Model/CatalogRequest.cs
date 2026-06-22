namespace Orleans.Lattice.Api.State;

/// <summary>
/// Paging and filtering request for the discovery / catalog endpoints
/// (<see cref="ILatticeStateQuery.ListTreesAsync"/> and
/// <see cref="ILatticeStateQuery.ListViewsAsync"/>).
/// </summary>
/// <remarks>
/// The catalog is enumerated in a deterministic, stable order (ascending by
/// id / name). <see cref="PageToken"/> is the exclusive cursor: pass the
/// <c>NextPageToken</c> returned by the previous page to fetch the next one.
/// A request with a <see langword="null"/> token starts from the beginning.
/// </remarks>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.CatalogRequest)]
[Immutable]
public sealed record CatalogRequest
{
    /// <summary>Default page size used when <see cref="PageSize"/> is unset.</summary>
    public const int DefaultPageSize = 100;

    /// <summary>Largest page size honoured; larger values are clamped down.</summary>
    public const int MaxPageSize = 1000;

    /// <summary>
    /// Maximum number of entries to return in a single page. Values below
    /// <c>1</c> fall back to <see cref="DefaultPageSize"/>; values above
    /// <see cref="MaxPageSize"/> are clamped to it.
    /// </summary>
    [Id(0)] public int PageSize { get; init; } = DefaultPageSize;

    /// <summary>
    /// Exclusive continuation cursor: the id / name of the last entry on the
    /// previous page. <see langword="null"/> (the default) starts from the
    /// beginning of the catalog.
    /// </summary>
    [Id(1)] public string? PageToken { get; init; }

    /// <summary>
    /// When <see langword="true"/>, includes reserved internal system trees
    /// (the registry, WAL, queue, and materialised-view backing trees) in the
    /// tree catalog. Defaults to <see langword="false"/> so the catalog shows
    /// only user-facing trees.
    /// </summary>
    [Id(2)] public bool IncludeSystemTrees { get; init; }

    /// <summary>
    /// When <see langword="true"/>, the view catalog samples each view's apply
    /// lag and materialised entry count. This activates the view maintainer and
    /// is therefore more expensive; defaults to <see langword="false"/>, in
    /// which case <see cref="ViewStateSummary.Lag"/> and
    /// <see cref="ViewStateSummary.EntryCount"/> are left <see langword="null"/>.
    /// </summary>
    [Id(3)] public bool IncludeViewStats { get; init; }

    /// <summary>The effective, clamped page size derived from <see cref="PageSize"/>.</summary>
    public int EffectivePageSize => PageSize switch
    {
        < 1 => DefaultPageSize,
        > MaxPageSize => MaxPageSize,
        _ => PageSize,
    };
}
