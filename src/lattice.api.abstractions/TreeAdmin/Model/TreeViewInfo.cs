namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// A single entry in the materialised-view catalog listing, describing one
/// runtime-registered view's identity and shape. A pure projection with no side
/// effects.
/// <para>
/// The listing covers views created at runtime through the view factory (durably
/// recorded in the cluster-wide runtime-view registry). Startup-declared views -
/// declared authoritatively through <c>AddLatticeViews</c> - are surfaced by the
/// State facade's view catalog read rather than here, because they are not runtime
/// registrations and cannot be dropped at runtime.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeViewInfo)]
[Immutable]
public sealed record TreeViewInfo
{
    /// <summary>The logical view name; the view tree is <c>view-{ViewName}</c>.</summary>
    [Id(0)] public required string ViewName { get; init; }

    /// <summary>The source tree id whose WAL the view tails.</summary>
    [Id(1)] public required string SourceTreeId { get; init; }

    /// <summary>Whether the view is an aggregation (grouped reduce) view.</summary>
    [Id(2)] public bool IsAggregation { get; init; }

    /// <summary>Whether the view is append-only (a durable history substrate).</summary>
    [Id(3)] public bool Accumulative { get; init; }
}
