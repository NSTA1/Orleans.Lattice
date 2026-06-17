namespace Orleans.Lattice.Replication.Views;

/// <summary>
/// A startup-time materialised-view registration captured by
/// <c>AddLatticeViews</c>. The projection is resolved from the silo service
/// provider when the hosted activation service starts, so a projection may take
/// service dependencies. Exactly one of <see cref="ProjectionFactory"/> (a filter
/// / re-project view) and <see cref="AggregationProjectionFactory"/> (an
/// aggregation view) is set.
/// </summary>
/// <param name="ViewName">The logical view name; the view tree is <c>view-{ViewName}</c>.</param>
/// <param name="SourceTreeId">The source tree id whose WAL the view tails.</param>
/// <param name="ProjectionFactory">Resolves the filter / re-project projection, or <see langword="null"/> for an aggregation view.</param>
/// <param name="AggregationProjectionFactory">Resolves the aggregation projection, or <see langword="null"/> for a filter / re-project view.</param>
internal sealed record StartupViewRegistration(
    string ViewName,
    string SourceTreeId,
    Func<IServiceProvider, ILatticeViewProjection>? ProjectionFactory,
    Func<IServiceProvider, ILatticeAggregationProjection>? AggregationProjectionFactory = null)
{
    /// <summary>Resolves the view registration for the view catalog from the service provider.</summary>
    public ViewRegistration Resolve(IServiceProvider services) =>
        AggregationProjectionFactory is not null
            ? new ViewRegistration(ViewName, SourceTreeId, Projection: null, AggregationProjectionFactory(services))
            : new ViewRegistration(ViewName, SourceTreeId, ProjectionFactory!(services));
}
