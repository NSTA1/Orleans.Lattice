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

    /// <summary>
    /// The human-friendly label shown in the navigation list and detail header.
    /// For trees and tag indexes this equals <see cref="Id"/>; for views it is the
    /// bare view name, while <see cref="Id"/> carries the physical <c>view-</c>
    /// prefixed tree id the detail tabs query. Defaults to <see cref="Id"/> when
    /// not set.
    /// </summary>
    public string? DisplayName { get; init; }

    /// <summary>The label to render for this item: <see cref="DisplayName"/> when set, otherwise <see cref="Id"/>.</summary>
    public string Label => DisplayName ?? Id;

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

    /// <summary>
    /// For views, <see langword="true"/> when the view is a change-history
    /// (accumulative) view whose rows back the History tab on its source tree
    /// rather than holding directly inspectable value data; always
    /// <see langword="false"/> for trees.
    /// </summary>
    public bool IsHistory { get; init; }

    /// <summary>
    /// For tag indexes, the logical index name (the membership tree id with its
    /// reserved <c>tag-</c> prefix removed); <see langword="null"/> otherwise.
    /// </summary>
    public string? IndexName { get; init; }

    /// <summary>
    /// For a tree that is the shadow target of a shadow-cutover restore, the
    /// logical tree id the restore was performed for (the alias it is grouped
    /// under); <see langword="null"/> for every ordinary tree, view, and tag
    /// index. Carried straight from the state API's tree catalog, so it is a
    /// first-class fact and not inferred from the tree name.
    /// </summary>
    public string? RestoreShadowOfTreeId { get; init; }

    /// <summary><see langword="true"/> when this tree is a shadow-cutover restore shadow.</summary>
    public bool IsRestoreShadow => RestoreShadowOfTreeId is not null;
}
