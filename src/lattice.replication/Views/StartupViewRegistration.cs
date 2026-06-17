namespace Orleans.Lattice.Replication.Views;

/// <summary>
/// A startup-time materialised-view registration captured by
/// <c>AddLatticeViews</c>. The projection is resolved from the silo service
/// provider when the hosted activation service starts, so a projection may take
/// service dependencies.
/// </summary>
/// <param name="ViewName">The logical view name; the view tree is <c>view-{ViewName}</c>.</param>
/// <param name="SourceTreeId">The source tree id whose WAL the view tails.</param>
/// <param name="ProjectionFactory">Resolves the projection from the service provider at startup.</param>
internal sealed record StartupViewRegistration(
    string ViewName,
    string SourceTreeId,
    Func<IServiceProvider, ILatticeViewProjection> ProjectionFactory);
