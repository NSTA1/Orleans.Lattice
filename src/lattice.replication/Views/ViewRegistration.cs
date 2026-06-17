namespace Orleans.Lattice.Replication.Views;

/// <summary>
/// Immutable binding of a materialised view's logical name to the source tree it
/// derives from and the projection that maintains it. Held in the singleton
/// <see cref="IViewCatalog"/> so the view maintainer grain (keyed by view name)
/// can resolve the source tree id and the live projection instance, neither of
/// which can travel through the grain key or be serialized into grain state.
/// <para>
/// Exactly one of <see cref="Projection"/> (a filter / re-project view) and
/// <see cref="AggregationProjection"/> (a grouped reduce) is set.
/// </para>
/// </summary>
/// <param name="ViewName">The logical view name; the view tree is <c>view-{ViewName}</c>.</param>
/// <param name="SourceTreeId">The source tree id whose WAL the view tails.</param>
/// <param name="Projection">The filter / re-project projection, or <see langword="null"/> for an aggregation view.</param>
/// <param name="AggregationProjection">The aggregation projection, or <see langword="null"/> for a filter / re-project view.</param>
internal sealed record ViewRegistration(
    string ViewName,
    string SourceTreeId,
    ILatticeViewProjection? Projection,
    ILatticeAggregationProjection? AggregationProjection = null)
{
    /// <summary>Whether this view is an aggregation (grouped reduce) view.</summary>
    public bool IsAggregation => AggregationProjection is not null;

    /// <summary>The active projection's stable version, whichever projection kind is configured.</summary>
    public string ProjectionVersion =>
        AggregationProjection?.ProjectionVersion ?? Projection!.ProjectionVersion;
}
