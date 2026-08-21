namespace Orleans.Lattice;

/// <summary>
/// Binds a materialised view's logical name to the
/// <see cref="ILatticeViewProjection"/> that maintains it. Passed to
/// <see cref="ILatticeViewFactory.CreateAsync(ILattice,string,LatticeViewDefinition,CancellationToken)"/>
/// and registered with the host so the view maintainer can resolve the projection
/// for the named view.
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
    /// <param name="accumulative">
    /// Whether the view is append-only (a durable history substrate). When
    /// <see langword="true"/> the maintainer never auto-clears the view tree: a
    /// projection-version mismatch adopts the new version forward instead of
    /// wipe-and-rebuild, and an unconstrained range reconcile records a marker
    /// instead of rebuilding. Defaults to <see langword="false"/>.
    /// </param>
    public LatticeViewDefinition(string viewName, ILatticeViewProjection projection, bool accumulative = false)
        : this(viewName, projection, runtimeProjection: null, accumulative)
    {
    }

    /// <summary>
    /// Creates a view definition with an explicit durable runtime reconstruction descriptor.
    /// </summary>
    /// <param name="viewName">The logical view name.</param>
    /// <param name="projection">The live projection.</param>
    /// <param name="runtimeProjection">The provider descriptor used after restart.</param>
    /// <param name="accumulative">Whether the view is append-only.</param>
    public LatticeViewDefinition(
        string viewName,
        ILatticeViewProjection projection,
        LatticeRuntimeViewProjectionDescriptor? runtimeProjection,
        bool accumulative = false)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        ArgumentNullException.ThrowIfNull(projection);
        ViewName = viewName;
        Projection = projection;
        RuntimeProjection = runtimeProjection;
        Accumulative = accumulative;
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
        : this(viewName, aggregation, runtimeProjection: null)
    {
    }

    /// <summary>
    /// Creates an aggregation view definition with an explicit durable runtime
    /// reconstruction descriptor.
    /// </summary>
    /// <param name="viewName">The logical view name.</param>
    /// <param name="aggregation">The live aggregation projection.</param>
    /// <param name="runtimeProjection">The provider descriptor used after restart.</param>
    public LatticeViewDefinition(
        string viewName,
        ILatticeAggregationProjection aggregation,
        LatticeRuntimeViewProjectionDescriptor? runtimeProjection)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        ArgumentNullException.ThrowIfNull(aggregation);
        ViewName = viewName;
        AggregationProjection = aggregation;
        RuntimeProjection = runtimeProjection;
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

    /// <summary>
    /// Whether this view is append-only (a durable history substrate). When
    /// <see langword="true"/> the maintainer never auto-clears the view tree;
    /// only an explicit operator rebuild clears it. Always <see langword="false"/>
    /// for an aggregation view.
    /// </summary>
    public bool Accumulative { get; }

    /// <summary>
    /// The provider descriptor used to reconstruct a runtime-created view after a
    /// restart, or <see langword="null"/> when type-based reconstruction is used.
    /// </summary>
    public LatticeRuntimeViewProjectionDescriptor? RuntimeProjection { get; }
}
