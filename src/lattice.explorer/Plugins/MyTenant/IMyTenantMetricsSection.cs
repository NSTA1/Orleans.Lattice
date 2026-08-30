namespace Orleans.Lattice.Explorer.Plugins.MyTenant;

/// <summary>
/// The seam the tenant-metrics section plugs into.
/// <para>
/// The My Tenant area declares a Metrics surface from the start so the tab strip
/// does not grow an entry later and shift every tab beside it. Until the metrics
/// work lands there is nothing to render there, and a head that registers no
/// implementation of this contract gets the placeholder body.
/// </para>
/// <para>
/// It is a component <em>type</em> rather than a rendered fragment so the
/// section can be an ordinary Razor component in its own package, resolved by
/// the shell's dynamic component host exactly as a plugin panel is.
/// </para>
/// </summary>
public interface IMyTenantMetricsSection
{
    /// <summary>
    /// The component type to render inside the Metrics surface. The component
    /// receives no parameters, so it resolves whatever it needs from the
    /// container itself.
    /// </summary>
    Type ViewType { get; }

    /// <summary>
    /// The heading the surface renders above the section. Never
    /// <see langword="null"/>.
    /// </summary>
    string Label { get; }
}
