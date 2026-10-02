using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

public static partial class LatticeApiTenantAdminServiceCollectionExtensions
{
    /// <summary>
    /// Registers the tenant directory facade (<see cref="ILatticeTenantDirectoryAdmin"/>)
    /// and its membership and policy underlays, and makes the shared tenant-tier
    /// authorizer group-aware while delegated tenant access administration is enabled.
    /// Every registration is idempotent, so a repeated
    /// <see cref="AddLatticeTenantAdminApi"/> call changes nothing.
    /// </summary>
    /// <param name="services">The silo's service collection.</param>
    static partial void AddTenantDirectoryAdmin(IServiceCollection services)
    {
        // The tenant-tier authorizer every tenant facade shares reads the live
        // delegated-access flag, so a group admin entry authorizes while the feature
        // is on and only the exact id counts while it is off. Only the built-in
        // registration is upgraded: a host that registered its own authorizer keeps it.
        if (FindDescriptor(services, typeof(TenantRegionResidencyAuthorizer)) is not { } existing
            || IsBuiltInRegistration(existing))
        {
            services.Replace(ServiceDescriptor.Singleton(sp => new TenantRegionResidencyAuthorizer(
                sp.GetRequiredService<ILatticeAccessGate>(),
                sp.GetRequiredService<ITenantRegistry>(),
                sp.GetService<ILatticeMembershipContext>(),
                DelegatedAccessReader(sp))));
        }

        services.TryAddSingleton<ITenantDirectoryStore>(sp => new MembershipTenantDirectoryStore(
            sp.GetRequiredService<ILatticeMembershipDirectory>(),
            sp.GetTenantScopedMembershipStore()));

        services.TryAddSingleton<ITenantGroupRuleCascade>(sp => new PolicyStoreTenantGroupRuleCascade(
            sp.GetRequiredService<ILatticeAuthorizationPolicyStore>()));

        services.TryAddSingleton<ILatticeTenantDirectoryAdmin>(sp => new LatticeTenantDirectoryAdmin(
            sp.GetRequiredService<ITenantRegistry>(),
            sp.GetRequiredService<TenantRegionResidencyAuthorizer>(),
            sp.GetRequiredService<ITenantAdminClock>(),
            sp.GetRequiredService<IOptions<ClusterOptions>>(),
            sp.GetRequiredService<ITenantDirectoryStore>(),
            sp.GetRequiredService<ITenantGroupRuleCascade>(),
            DelegatedAccessReader(sp),
            sp.GetService<ILatticeIdentityDirectory>(),
            sp.GetService<IOptionsMonitor<LatticeIdentityDirectoryOptions>>()));
    }

    /// <summary>
    /// The live read of the delegated-access flag: the tenancy add-on's flag as a
    /// method group bound once (each call is one field read), or a constant
    /// <see langword="false"/> when no flag is registered (fail closed).
    /// </summary>
    internal static Func<bool> DelegatedAccessReader(IServiceProvider services) =>
        services.GetService<DelegatedTenantAccessFlag>() is { } flag ? flag.ReadIsEnabled : static () => false;

    /// <summary>
    /// <see langword="true"/> when <paramref name="descriptor"/> is a factory
    /// registration declared by this extension class (its lambdas compile into a
    /// nested closure type), so it may be upgraded in place.
    /// </summary>
    internal static bool IsBuiltInRegistration(ServiceDescriptor descriptor)
    {
        if (descriptor.IsKeyedService || descriptor.ImplementationFactory is not { } factory)
        {
            return false;
        }

        var declaring = factory.Method.DeclaringType;
        while (declaring is not null)
        {
            if (declaring == typeof(LatticeApiTenantAdminServiceCollectionExtensions))
            {
                return true;
            }

            declaring = declaring.DeclaringType;
        }

        return false;
    }

    private static ServiceDescriptor? FindDescriptor(IServiceCollection services, Type serviceType)
    {
        foreach (var descriptor in services)
        {
            if (descriptor.ServiceType == serviceType && !descriptor.IsKeyedService)
            {
                return descriptor;
            }
        }

        return null;
    }
}
