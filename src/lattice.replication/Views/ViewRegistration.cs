namespace Orleans.Lattice.Replication.Views;

/// <summary>
/// Immutable binding of a materialised view's logical name to the source tree it
/// derives from and the projection that maintains it. Held in the singleton
/// <see cref="IViewCatalog"/> so the view maintainer grain (keyed by view name)
/// can resolve the source tree id and the live projection instance, neither of
/// which can travel through the grain key or be serialized into grain state.
/// </summary>
/// <param name="ViewName">The logical view name; the view tree is <c>view-{ViewName}</c>.</param>
/// <param name="SourceTreeId">The source tree id whose WAL the view tails.</param>
/// <param name="Projection">The projection that lowers source mutations into view writes.</param>
internal sealed record ViewRegistration(
    string ViewName,
    string SourceTreeId,
    ILatticeViewProjection Projection);
