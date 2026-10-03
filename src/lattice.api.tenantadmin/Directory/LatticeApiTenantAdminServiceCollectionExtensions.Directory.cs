using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
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
    /// and its membership and policy underlays, makes the shared tenant-tier authorizer
    /// group-aware while delegated tenant access administration is enabled, and lets
    /// the admin-subject facade verify tenant-group admin entries. Every registration
    /// is idempotent, so a repeated <see cref="AddLatticeTenantAdminApi"/> call changes
    /// nothing, and a host's own registration of either upgraded service is kept.
    /// </summary>
    /// <param name="services">The silo's service collection.</param>
    static partial void AddTenantDirectoryAdmin(IServiceCollection services)
    {
        services.TryAddSingleton<ITenantDirectoryStore>(sp => new MembershipTenantDirectoryStore(
            sp.GetRequiredService<ILatticeMembershipDirectory>(),
            sp.GetTenantScopedMembershipStore()));

        services.TryAddSingleton<ITenantGroupRuleCascade>(sp => new PolicyStoreTenantGroupRuleCascade(
            sp.GetRequiredService<ILatticeAuthorizationPolicyStore>()));

        // The tenant-tier authorizer every tenant facade shares reads the live
        // delegated-access flag, so a group admin entry authorizes while the feature
        // is on and only the exact id counts while it is off.
        UpgradeBuiltIn(services, typeof(TenantRegionResidencyAuthorizer), CreateFlagAwareAuthorizer);

        // The admin-subject facade verifies that a tenant group named as an admin
        // entry exists, so a typo can never count as an admin entry that resolves to
        // nobody.
        UpgradeBuiltIn(services, typeof(ILatticeTenantAccessAdmin), CreateGroupVerifyingAccessAdmin);

        services.TryAddSingleton<ILatticeTenantDirectoryAdmin>(sp => new LatticeTenantDirectoryAdmin(
            sp.GetRequiredService<ITenantRegistry>(),
            sp.GetRequiredService<TenantRegionResidencyAuthorizer>(),
            sp.GetRequiredService<ITenantAdminClock>(),
            sp.GetRequiredService<IOptions<ClusterOptions>>(),
            sp.GetRequiredService<ITenantDirectoryStore>(),
            sp.GetRequiredService<ITenantGroupRuleCascade>(),
            DelegatedAccessReader(sp),
            sp.GetService<ILatticeIdentityDirectory>(),
            sp.GetService<IOptionsMonitor<LatticeIdentityDirectoryOptions>>(),
            sp.GetService<ILogger<LatticeTenantDirectoryAdmin>>()));
    }

    /// <summary>
    /// The live read of the delegated-access flag: the tenancy add-on's flag as a
    /// method group bound once (each call is one field read), or a constant
    /// <see langword="false"/> when no flag is registered (fail closed).
    /// </summary>
    internal static Func<bool> DelegatedAccessReader(IServiceProvider services) =>
        services.GetService<DelegatedTenantAccessFlag>() is { } flag ? flag.ReadIsEnabled : static () => false;

    /// <summary>
    /// Replaces the <b>effective</b> (last, non-keyed) registration of
    /// <paramref name="serviceType"/>, in place, with <paramref name="factory"/> -
    /// but only when that effective registration is the built-in one this class made
    /// and not already the upgrade. A host override registered after the built-in
    /// one, and an earlier upgrade, are both left untouched.
    /// </summary>
    internal static void UpgradeBuiltIn(
        IServiceCollection services, Type serviceType, Func<IServiceProvider, object> factory)
    {
        var index = -1;
        for (var i = services.Count - 1; i >= 0; i--)
        {
            if (services[i].ServiceType == serviceType && !services[i].IsKeyedService)
            {
                index = i;
                break;
            }
        }

        if (index < 0)
        {
            services.Add(new ServiceDescriptor(serviceType, factory, ServiceLifetime.Singleton));
            return;
        }

        var effective = services[index];
        if (!IsBuiltInRegistration(effective) || effective.ImplementationFactory!.Method == factory.Method)
        {
            return;
        }

        services[index] = new ServiceDescriptor(serviceType, factory, ServiceLifetime.Singleton);
    }

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

    private static object CreateFlagAwareAuthorizer(IServiceProvider sp) =>
        new TenantRegionResidencyAuthorizer(
            sp.GetRequiredService<ILatticeAccessGate>(),
            sp.GetRequiredService<ITenantRegistry>(),
            sp.GetService<ILatticeMembershipContext>(),
            DelegatedAccessReader(sp));

    private static object CreateGroupVerifyingAccessAdmin(IServiceProvider sp) =>
        new LatticeTenantAccessAdmin(
            sp.GetRequiredService<ITenantRegistry>(),
            sp.GetRequiredService<TenantRegionResidencyAuthorizer>(),
            sp.GetRequiredService<ITenantAdminClock>(),
            sp.GetRequiredService<IOptions<ClusterOptions>>(),
            sp.GetService<ILatticeIdentityDirectory>(),
            sp.GetService<IOptionsMonitor<LatticeIdentityDirectoryOptions>>(),
            sp.GetRequiredService<ITenantDirectoryStore>());
}
