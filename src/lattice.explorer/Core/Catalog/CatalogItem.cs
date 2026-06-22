namespace Orleans.Lattice.Explorer.Core.Catalog;

/// <summary>
/// A single, display-ready entry in the navigation list. Trees and views are
/// projected into this uniform shape so the panel renders them identically; the
/// optional badge fields carry the small amount of per-kind context the list
/// shows inline. <see cref="Id"/> is the opaque identifier handed straight to
/// the detail tabs.
/// </summary>
public sealed record CatalogItem
{
    /// <summary>The opaque tree or view id, used as-is by the detail panel.</summary>
    public required string Id { get; init; }

    /// <summary>Which discovery call produced this item.</summary>
    public required CatalogKind Kind { get; init; }

    /// <summary>
    /// The tree's lifecycle state (e.g. active / soft-deleted), or
    /// <see langword="null"/> for views.
    /// </summary>
    public string? Lifecycle { get; init; }

    /// <summary>
    /// The number of physical shards configured for a tree, or
    /// <see langword="null"/> for views.
    /// </summary>
    public int? ShardCount { get; init; }

    /// <summary>
    /// For views, the source tree the view projects from; <see langword="null"/>
    /// for trees.
    /// </summary>
    public string? SourceTreeId { get; init; }

    /// <summary>
    /// For views, <see langword="true"/> when the view is a grouped-reduce
    /// (aggregation) view; always <see langword="false"/> for trees.
    /// </summary>
    public bool IsAggregation { get; init; }
}
