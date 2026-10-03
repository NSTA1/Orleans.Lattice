using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI;

internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Access area's delegated tenant administration (epic #4154):
    /// the seam over the tenant directory and tenant policy facades, and the
    /// circuit-scoped catalogue that reads the posture probe. Then each tenant page
    /// owner registers its own services through its partial, in a file of its own.
    /// </summary>
    /// <param name="services">The service collection.</param>
    private static void AddTenantAccess(IServiceCollection services)
    {
        // The facades resolve lazily and optionally, so a head that serves neither
        // still builds (ValidateOnBuild) and its tenant pages stay cluster-wide.
        services.TryAddScoped<ITenantAccessFacades>(provider => new ShellTenantAccessFacades(provider));
        services.TryAddScoped(provider => new TenantAccessCatalog(
            provider.GetRequiredService<ITenantAccessFacades>(),
            ShellCaller.Of(provider)));

        AddTenantAccessGroups(services);
        AddTenantAccessMembers(services);
        AddTenantAccessRules(services);
        AddTenantAccessExplain(services);
    }

    /// <summary>The tenant Groups pages' own services (<c>Areas/Access/Tenant/Groups/</c>).</summary>
    /// <param name="services">The service collection.</param>
    static partial void AddTenantAccessGroups(IServiceCollection services);

    /// <summary>The tenant Members page's own services (<c>Areas/Access/Tenant/Members/</c>).</summary>
    /// <param name="services">The service collection.</param>
    static partial void AddTenantAccessMembers(IServiceCollection services);

    /// <summary>The tenant Rules pages' own services (<c>Areas/Access/Tenant/Rules/</c>).</summary>
    /// <param name="services">The service collection.</param>
    static partial void AddTenantAccessRules(IServiceCollection services);

    /// <summary>The layer-aware Explain's own services (<c>Areas/Access/Tenant/Explain/</c>).</summary>
    /// <param name="services">The service collection.</param>
    static partial void AddTenantAccessExplain(IServiceCollection services);
}
