using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// DI extension that adds the <c>Orleans.Lattice.Api.Mcp</c> tenant self-awareness
/// tool module - the read-only tenant-discovery tools (<c>lattice_tenant_current</c>,
/// <c>lattice_tenant_list</c>, <c>lattice_tenant_get</c>) that adapt the read-only
/// <see cref="Orleans.Lattice.Api.TenantAdmin.ILatticeTenantSelfService"/> facade
/// onto the MCP surface.
/// </summary>
/// <remarks>
/// <para>
/// <b>The module self-gates on whether tenancy is enabled.</b> It introduces no
/// opt-in flag of its own and does not reuse the mutating tenant-admin group's
/// control switch. The tenant self-awareness facade is registered only when the
/// tenant-admin API is wired (which requires the tenancy add-on), so the group
/// contributes its three read-only tools exactly when that facade is present and
/// contributes <b>no</b> tools otherwise. A non-tenancy deployment - even one that
/// calls this method - is therefore byte-for-byte unchanged: no new tools and no
/// change to the fixed capability surface (the tools advertise under the existing
/// read-only <c>State</c> group rather than a new discovery group).
/// </para>
/// <para>
/// The module adds no authorization path. Each tool stamps the caller credential
/// bridged from the request principal onto the ambient credential context and
/// defers to the facade's leak-free, fail-closed per-tenant scoping, so an
/// unauthorized caller sees only its own default context, an empty accessible
/// list, and a fail-closed not-found on inspect.
/// </para>
/// <para>Add it after <c>AddLatticeMcp</c>, on a host where tenancy is enabled:</para>
/// <code>
/// builder.Services.AddLatticeMcp(o =&gt; o.RequireAuthorization = true);
/// builder.Services.AddTenantSelfAwarenessTools();
/// </code>
/// </remarks>
public static class LatticeMcpTenantSelfAwarenessServiceCollectionExtensions
{
    /// <summary>
    /// Registers the read-only tenant self-awareness tool module. The group
    /// self-gates on the presence of the tenancy-gated
    /// <see cref="Orleans.Lattice.Api.TenantAdmin.ILatticeTenantSelfService"/>
    /// facade, so it contributes tools only when tenancy is enabled and sets no
    /// option flag of its own.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <c>null</c>.</exception>
    public static IServiceCollection AddTenantSelfAwarenessTools(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ILatticeApiMcpToolGroup, TenantSelfAwarenessToolGroup>());

        return services;
    }
}
