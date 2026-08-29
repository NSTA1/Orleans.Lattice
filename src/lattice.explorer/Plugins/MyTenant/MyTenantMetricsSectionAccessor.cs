namespace Orleans.Lattice.Explorer.MyTenant;

/// <summary>
/// The Metrics surface's resolution of its optional section.
/// <para>
/// It exists so the panel does not have to inject <see cref="IServiceProvider"/>
/// and ask it for a type. A plugin's reach is meant to be its declared domain
/// contract and nothing else, and a service locator in a panel is precisely the
/// hole that principle exists to close - even when what it resolves is the
/// plugin's own contract. The container performs the optional resolution here
/// instead, through an ordinary optional constructor parameter, and the panel
/// injects this concrete plugin-owned type.
/// </para>
/// </summary>
/// <param name="section">
/// The registered section, or <see langword="null"/> on a head that registered
/// none - which is the ordinary case until the tenant-metrics work lands.
/// </param>
public sealed class MyTenantMetricsSectionAccessor(IMyTenantMetricsSection? section = null)
{
    /// <summary>
    /// The section to render, or <see langword="null"/> when no head registered
    /// one and the surface should render its placeholder.
    /// </summary>
    public IMyTenantMetricsSection? Section { get; } = section;

    /// <summary>Whether a section is available to render.</summary>
    public bool HasSection => Section is not null;
}
