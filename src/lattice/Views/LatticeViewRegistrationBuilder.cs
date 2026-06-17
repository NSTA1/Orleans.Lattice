using Orleans.Lattice.Views;

namespace Orleans.Lattice;

/// <summary>
/// Fluent builder used by <c>AddLatticeViews</c> to declare the materialised
/// views a silo maintains at startup. Each <see cref="AddView(string, string, ILatticeViewProjection)"/>
/// call binds a view name to a source tree and a projection; the hosted
/// activation service registers each declared view in the view catalog and
/// brings its maintainer online.
/// </summary>
public sealed class LatticeViewRegistrationBuilder
{
    private readonly List<StartupViewRegistration> _registrations = [];

    internal IReadOnlyList<StartupViewRegistration> Registrations => _registrations;

    /// <summary>
    /// Declares a view maintained by the supplied projection instance.
    /// </summary>
    /// <param name="viewName">The logical view name; the view tree is <c>view-{viewName}</c>.</param>
    /// <param name="sourceTreeId">The source tree id whose WAL the view tails.</param>
    /// <param name="projection">The projection that maintains the view.</param>
    public LatticeViewRegistrationBuilder AddView(string viewName, string sourceTreeId, ILatticeViewProjection projection)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        ArgumentException.ThrowIfNullOrEmpty(sourceTreeId);
        ArgumentNullException.ThrowIfNull(projection);
        _registrations.Add(new StartupViewRegistration(viewName, sourceTreeId, _ => projection));
        return this;
    }

    /// <summary>
    /// Declares a view whose projection is resolved from the service provider at
    /// startup, allowing the projection to take service dependencies.
    /// </summary>
    /// <param name="viewName">The logical view name; the view tree is <c>view-{viewName}</c>.</param>
    /// <param name="sourceTreeId">The source tree id whose WAL the view tails.</param>
    /// <param name="projectionFactory">Resolves the projection from the service provider.</param>
    public LatticeViewRegistrationBuilder AddView(string viewName, string sourceTreeId, Func<IServiceProvider, ILatticeViewProjection> projectionFactory)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        ArgumentException.ThrowIfNullOrEmpty(sourceTreeId);
        ArgumentNullException.ThrowIfNull(projectionFactory);
        _registrations.Add(new StartupViewRegistration(viewName, sourceTreeId, projectionFactory));
        return this;
    }

    /// <summary>
    /// Declares an aggregation view (a grouped reduce) maintained by the supplied
    /// projection instance.
    /// </summary>
    /// <param name="viewName">The logical view name; the view tree is <c>view-{viewName}</c>.</param>
    /// <param name="sourceTreeId">The source tree id whose WAL the view tails.</param>
    /// <param name="projection">The aggregation projection that maintains the view.</param>
    public LatticeViewRegistrationBuilder AddAggregationView(string viewName, string sourceTreeId, ILatticeAggregationProjection projection)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        ArgumentException.ThrowIfNullOrEmpty(sourceTreeId);
        ArgumentNullException.ThrowIfNull(projection);
        _registrations.Add(new StartupViewRegistration(viewName, sourceTreeId, ProjectionFactory: null, _ => projection));
        return this;
    }

    /// <summary>
    /// Declares an aggregation view whose projection is resolved from the service
    /// provider at startup, allowing the projection to take service dependencies.
    /// </summary>
    /// <param name="viewName">The logical view name; the view tree is <c>view-{viewName}</c>.</param>
    /// <param name="sourceTreeId">The source tree id whose WAL the view tails.</param>
    /// <param name="projectionFactory">Resolves the aggregation projection from the service provider.</param>
    public LatticeViewRegistrationBuilder AddAggregationView(string viewName, string sourceTreeId, Func<IServiceProvider, ILatticeAggregationProjection> projectionFactory)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        ArgumentException.ThrowIfNullOrEmpty(sourceTreeId);
        ArgumentNullException.ThrowIfNull(projectionFactory);
        _registrations.Add(new StartupViewRegistration(viewName, sourceTreeId, ProjectionFactory: null, projectionFactory));
        return this;
    }
}
