using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Shell.Areas.Cluster;
using Orleans.Lattice.Explorer.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Shell;

/// <summary>The Cluster area's registrations (A10, issue #3828).</summary>
internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Cluster area and its per-circuit state: the facade lookup, the
    /// remembered tree catalogue and the palette command relay. The facades
    /// themselves are T1's; the area resolves them optionally and hides itself
    /// when a head serves none.
    /// </summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddCluster(IServiceCollection services)
    {
        services.TryAddScoped<ClusterFacades>();
        services.TryAddScoped<ClusterTreeCatalog>();
        services.TryAddScoped<ClusterCommandSignals>();
        services.AddExplorerArea<ClusterArea>();
    }
}
