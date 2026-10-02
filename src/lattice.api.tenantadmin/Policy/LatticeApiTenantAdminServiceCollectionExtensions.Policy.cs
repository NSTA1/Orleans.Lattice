using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>Registration of the tenant policy facade (delegated tenant access administration).</summary>
public static partial class LatticeApiTenantAdminServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <see cref="ILatticeTenantPolicyAdmin"/> facade with its policy
    /// decision source and membership-usage counter. Every registration is a
    /// <c>TryAdd</c>, so a repeated <c>AddLatticeTenantAdminApi</c> call is a no-op.
    /// </summary>
    /// <remarks>
    /// The facade reads the live delegated-access flag through the tenancy add-on's
    /// <see cref="DelegatedTenantAccessFlag"/>, bound once as a method group (feature
    /// off, fail-closed, when a host registered a registry without the tenancy
    /// add-on), and reuses the tenant-tier
    /// <see cref="TenantRegionResidencyAuthorizer"/> the other tenant-tier facades
    /// share. Nothing here runs on a data-plane path, so the feature costs nothing
    /// while it is off.
    /// </remarks>
    /// <param name="services">The silo's service collection.</param>
    static partial void AddTenantPolicyAdmin(IServiceCollection services)
    {
        services.TryAddSingleton<ITenantPolicyDecisionSource>(sp => new EngineTenantPolicyDecisionSource(
            sp.GetRequiredService<ILatticeDecisionEngine>(),
            sp.GetRequiredService<IOptionsMonitor<LatticeAuthOptions>>()));
        services.TryAddSingleton<ITenantMembershipUsage>(sp =>
            new ScopedStoreTenantMembershipUsage(sp.GetService<ILatticeMembershipDirectory>()));
        services.TryAddSingleton<ILatticeTenantPolicyAdmin>(sp => new LatticeTenantPolicyAdmin(
            sp.GetRequiredService<TenantRegionResidencyAuthorizer>(),
            sp.GetRequiredService<ILatticeAuthorizationPolicyStore>(),
            sp.GetRequiredService<ILatticeMembershipDirectory>(),
            sp.GetRequiredService<ITenantPolicyEngine>(),
            sp.GetRequiredService<ITenantPolicyDecisionSource>(),
            sp.GetRequiredService<ILatticeAccessGate>(),
            sp.GetService<DelegatedTenantAccessFlag>() is { } flag ? flag.ReadIsEnabled : static () => false,
            sp.GetService<ILatticeMembershipContext>(),
            sp.GetService<ITenantMembershipUsage>()));
    }
}
