using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Shell.Areas.Tenancy;
using Orleans.Lattice.Explorer.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Shell;

internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Tenancy area (A5), its circuit-scoped catalogue, the
    /// platform-operator gate, and the accessible-tenant list the address root's
    /// tenant selector reads. The area
    /// binds to the transport-neutral tenant facades the transport registers;
    /// without them, or without tenancy, it hides itself.
    /// </summary>
    /// <remarks>
    /// The accessible-tenant source is registered with <c>TryAdd</c>, so a head
    /// must call Core's <c>AddExplorerTenantView</c> after the Shell for this list
    /// to win over Core's active-tenant-only default. It is a factory, because the
    /// tenant context it reads exists only when tenancy is registered.
    /// </remarks>
    /// <param name="services">The service collection.</param>
    static partial void AddTenancy(IServiceCollection services)
    {
        services.TryAddScoped<TenancyCatalog>();

        // The platform-operator gate Core's tenant view and switcher consult,
        // proven by the Access area's probe. Registered with TryAdd ahead of the
        // head's AddExplorerTenantView, whose fail-closed default it replaces.
        services.TryAddScoped<IExplorerTenantOperatorGate, ShellTenantOperatorGate>();
        services.TryAddScoped<IExplorerAccessibleTenantSource>(provider =>
            new TenancyAccessibleTenantSource(provider.GetRequiredService<TenancyCatalog>(), provider.GetService<IExplorerTenantContext>()));
        services.AddExplorerArea<TenancyArea>();
    }
}
