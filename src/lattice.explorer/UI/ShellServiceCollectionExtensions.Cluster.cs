using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI;

/// <summary>The Cluster area's registrations (A10, issue #3828).</summary>
internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Cluster area and its per-circuit state: the facade lookup, the
    /// remembered tree catalogue, the palette command relay and the create-tree
    /// service (G4, issue #5368), every one per circuit. The facades
    /// themselves are T1's; the area resolves them optionally and hides itself
    /// when a head serves none.
    /// </summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddCluster(IServiceCollection services)
    {
        services.TryAddScoped<ClusterFacades>();
        services.TryAddScoped<ClusterTreeCatalog>();
        services.TryAddScoped<ClusterTreeChanges>();
        services.TryAddScoped<ClusterCommandSignals>();
        services.TryAddScoped<ClusterTreeCreation>();
        services.AddExplorerArea<ClusterArea>();
    }
}
