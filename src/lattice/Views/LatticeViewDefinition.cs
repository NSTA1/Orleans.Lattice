namespace Orleans.Lattice;

/// <summary>
/// Binds a materialised view's logical name to the
/// <see cref="ILatticeViewProjection"/> that maintains it. Passed to
/// <see cref="ILatticeViewFactory.Create"/> and registered with the host so the
/// view maintainer can resolve the projection for the named view.
/// <para>
/// This is a configuration object resolved as a service, not a serialized DTO:
/// it carries a live projection instance, so it is never sent over the wire.
/// </para>
/// </summary>
public sealed class LatticeViewDefinition
{
    /// <summary>
    /// Creates a view definition.
    /// </summary>
    /// <param name="viewName">
    /// The logical view name; the view tree is resolved as <c>view-{viewName}</c>.
    /// Must not be <see langword="null"/> or empty.
    /// </param>
    /// <param name="projection">The projection that maintains the view. Must not be <see langword="null"/>.</param>
    public LatticeViewDefinition(string viewName, ILatticeViewProjection projection)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        ArgumentNullException.ThrowIfNull(projection);
        ViewName = viewName;
        Projection = projection;
    }

    /// <summary>
    /// Creates an aggregation view definition (a grouped reduce).
    /// </summary>
    /// <param name="viewName">
    /// The logical view name; the view tree is resolved as <c>view-{viewName}</c>.
    /// Must not be <see langword="null"/> or empty.
    /// </param>
    /// <param name="aggregation">The aggregation projection that maintains the view. Must not be <see langword="null"/>.</param>
    public LatticeViewDefinition(string viewName, ILatticeAggregationProjection aggregation)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        ArgumentNullException.ThrowIfNull(aggregation);
        ViewName = viewName;
        AggregationProjection = aggregation;
    }

    /// <summary>The logical view name; the view tree is <c>view-{ViewName}</c>.</summary>
    public string ViewName { get; }

    /// <summary>
    /// The filter / re-project projection that lowers source mutations into view
    /// writes, or <see langword="null"/> when this is an aggregation view (see
    /// <see cref="AggregationProjection"/>). Exactly one of
    /// <see cref="Projection"/> and <see cref="AggregationProjection"/> is set.
    /// </summary>
    public ILatticeViewProjection? Projection { get; }

    /// <summary>
    /// The aggregation projection that lowers source mutations into grouped
    /// contributions, or <see langword="null"/> when this is a filter /
    /// re-project view (see <see cref="Projection"/>). Exactly one of
    /// <see cref="Projection"/> and <see cref="AggregationProjection"/> is set.
    /// </summary>
    public ILatticeAggregationProjection? AggregationProjection { get; }
}
