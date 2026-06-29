namespace Orleans.Lattice.Api.State;

/// <summary>
/// Read-only summary of a materialised view as surfaced by the cluster
/// state API's discovery endpoint.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.ViewStateSummary)]
[Immutable]
public sealed record ViewStateSummary
{
    /// <summary>The view's name.</summary>
    [Id(0)] public required string ViewName { get; init; }

    /// <summary>The source tree the view projects from.</summary>
    [Id(1)] public required string SourceTreeId { get; init; }

    /// <summary>
    /// Current apply lag of the view (source WAL entries committed but not yet
    /// applied), or <see langword="null"/> when the catalog did not sample it
    /// (see <see cref="CatalogRequest.IncludeViewStats"/>).
    /// </summary>
    [Id(2)] public long? Lag { get; init; }

    /// <summary>
    /// Number of entries currently materialised in the view, or
    /// <see langword="null"/> when the catalog did not sample it (see
    /// <see cref="CatalogRequest.IncludeViewStats"/>).
    /// </summary>
    [Id(3)] public long? EntryCount { get; init; }

    /// <summary>An opaque last-digest marker for the view, when available.</summary>
    [Id(4)] public string? LastDigest { get; init; }

    /// <summary>
    /// <see langword="true"/> when the view is a grouped-reduce (aggregation)
    /// view rather than a filter / re-project view.
    /// </summary>
    [Id(5)] public bool IsAggregation { get; init; }

    /// <summary>
    /// <see langword="true"/> when the view is a change-history (accumulative)
    /// view whose rows are serialized history blobs backing the History tab on
    /// its source tree, rather than directly inspectable value / CRDT data.
    /// </summary>
    [Id(6)] public bool IsHistory { get; init; }
}
