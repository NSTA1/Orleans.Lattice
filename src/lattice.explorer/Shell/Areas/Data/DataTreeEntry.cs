using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Data;

/// <summary>
/// One tree or view the caller can reach, as the Data area shows and addresses
/// it. <see cref="LogicalId"/> is the only identifier the UI ever renders;
/// <see cref="StateId"/> is the id the state API answered with and is passed to
/// the readers, but is never rendered, because for a tenant-owned tree it is the
/// physical, tenant-composed id.
/// </summary>
internal sealed record DataTreeEntry
{
    /// <summary>The logical tree id: what the address and every label show.</summary>
    public required string LogicalId { get; init; }

    /// <summary>The id the state API reads the tree under. Never rendered.</summary>
    public required string StateId { get; init; }

    /// <summary>Whether this is a tree or a view.</summary>
    public required DataTreeKind Kind { get; init; }

    /// <summary>The owning tenant when tenancy is on, or <see langword="null"/>.</summary>
    public string? Tenant { get; init; }

    /// <summary>The owning app's slug for an <c>a/{slug}/</c> tree, or <see langword="null"/>.</summary>
    public string? AppSlug { get; init; }

    /// <summary>The physical shard count, when the catalogue reports one.</summary>
    public int? ShardCount { get; init; }

    /// <summary>The lifecycle state the catalogue reports, such as <c>Active</c>.</summary>
    public string? Lifecycle { get; init; }

    /// <summary>The view's name, for a view.</summary>
    public string? ViewName { get; init; }

    /// <summary>The source tree's logical id, for a view whose source the caller can reach.</summary>
    public string? SourceLogicalId { get; init; }

    /// <summary>The source tree's state id, for a view. Never rendered.</summary>
    public string? SourceStateId { get; init; }

    /// <summary>Whether a view aggregates rather than projects.</summary>
    public bool IsAggregation { get; init; }

    /// <summary>Whether a view is a change-history view.</summary>
    public bool IsHistory { get; init; }

    /// <summary>A view's projection version, when it reports one.</summary>
    public string? ProjectionVersion { get; init; }

    /// <summary>The name a person reads: the view name for a view, else the logical id.</summary>
    public string DisplayName => ViewName ?? LogicalId;

    /// <summary>A short, fixed description of the kind of object.</summary>
    public string KindText => Kind switch
    {
        DataTreeKind.View when IsHistory => "History view",
        DataTreeKind.View when IsAggregation => "Aggregation view",
        DataTreeKind.View => "Projection view",
        _ when AppSlug is not null => "App tree",
        _ => "Tree",
    };

    /// <summary>The tree workspace's address, rooted at the owning tenant when there is one.</summary>
    public ExplorerAddress Address => ExplorerAddress.ForTree(DataArea.AreaKey, LogicalId).WithTenant(Tenant);
}
