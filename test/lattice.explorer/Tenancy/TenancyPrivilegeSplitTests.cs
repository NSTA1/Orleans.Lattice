using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// The tenancy seam's privilege split, expressed in the type system rather than
/// only in a source scan (issue #1785).
/// </summary>
/// <remarks>
/// <para>
/// <c>MyTenantPrivilegeBoundaryTests</c> scans the My Tenant plugin's
/// source for a call to an operator-only operation. That gate is deliberately
/// kept, but these assertions are what make it redundant: a surface handed
/// <see cref="IMyTenantDomain"/> cannot name one of those operations at all,
/// because they are not on the contract it holds. The source scan then guards
/// against the narrowing being undone rather than being the only thing standing
/// between a tenant admin and a control the cluster would refuse.
/// </para>
/// <para>
/// The mirror assertions matter as much: the platform-operator Tenants plugin
/// must keep the full surface, which is correct for an operator surface, so
/// these also pin that <see cref="ITenancyDomain"/> lost nothing.
/// </para>
/// </remarks>
[TestFixture]
public sealed class TenancyPrivilegeSplitTests
{
    /// <summary>
    /// The operations the tenant-administration facade reserves for a platform
    /// operator: authoring quota ceilings, widening the operator-authorized
    /// allowed region set, and the tenant lifecycle.
    /// </summary>
    private static readonly string[] OperatorOnlyOperations =
    [
        nameof(ITenantAdminService.SetQuotasAsync),
        nameof(ITenantAdminService.AuthorizeAllowedRegionsAsync),
        nameof(ITenantAdminService.CreateTenantAsync),
        nameof(ITenantAdminService.SuspendTenantAsync),
        nameof(ITenantAdminService.ResumeTenantAsync),
        nameof(ITenantAdminService.DeleteTenantAsync),
    ];

    /// <summary>
    /// The operations a tenant administrator genuinely does get, so the guards
    /// below cannot pass by narrowing the contract to nothing.
    /// </summary>
    private static readonly string[] TenantAdminOperations =
    [
        nameof(ITenantSelfAdminService.GetQuotaUsageAsync),
        nameof(ITenantSelfAdminService.SetResidencyAsync),
        nameof(ITenantSelfAdminService.GetRegionStatusAsync),
        nameof(ITenantSelfAdminService.ListAdminSubjectsAsync),
        nameof(ITenantSelfAdminService.AddAdminSubjectAsync),
        nameof(ITenantSelfAdminService.RemoveAdminSubjectAsync),
        nameof(ITenantSelfAdminService.ListGrantsAsync),
        nameof(ITenantSelfAdminService.OfferGrantAsync),
        nameof(ITenantSelfAdminService.ApproveGrantAsync),
        nameof(ITenantSelfAdminService.RejectGrantAsync),
        nameof(ITenantSelfAdminService.RevokeGrantAsync),
        nameof(ITenantSelfAdminService.GetCurrentTenantAsync),
        nameof(ITenantSelfAdminService.ListAccessibleTenantsAsync),
        nameof(ITenantSelfAdminService.GetTenantAsync),
    ];

    [Test]
    public void The_narrowed_operations_surface_declares_no_operator_only_operation()
    {
        var declared = MethodNames(typeof(ITenantSelfAdminService));

        Assert.That(
            OperatorOnlyOperations.Where(declared.Contains),
            Is.Empty,
            $"{nameof(ITenantSelfAdminService)} is the tenant administrator's contract: an "
            + "operator-only operation on it would be reachable from the My Tenant plugin's source.");
    }

    [Test]
    public void The_narrowed_operations_surface_declares_everything_a_tenant_admin_uses()
    {
        // The mirror of the guard: narrowing to nothing would satisfy the check
        // above and break every surface.
        var declared = MethodNames(typeof(ITenantSelfAdminService));

        Assert.That(TenantAdminOperations.Where(name => !declared.Contains(name)), Is.Empty);
    }

    [Test]
    public void The_full_operations_surface_still_carries_every_operator_only_operation()
    {
        // The Tenants plugin is a platform-operator surface and keeps the whole
        // thing; the split must have moved operations, not dropped them.
        var reachable = MethodNames(typeof(ITenantAdminService))
            .Union(MethodNames(typeof(ITenantSelfAdminService)))
            .ToHashSet(StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(OperatorOnlyOperations.Where(name => !reachable.Contains(name)), Is.Empty);
            Assert.That(TenantAdminOperations.Where(name => !reachable.Contains(name)), Is.Empty);
        });
    }

    [Test]
    public void The_full_operations_surface_widens_the_narrow_one() =>
        Assert.That(
            typeof(ITenantSelfAdminService).IsAssignableFrom(typeof(ITenantAdminService)),
            Is.True,
            "the operator contract must extend the tenant-administrator one, so one service satisfies both");

    [Test]
    public void The_narrowed_domain_contract_hands_out_only_the_narrowed_operations_surface()
    {
        var tenants = typeof(IMyTenantDomain).GetProperty(nameof(IMyTenantDomain.Tenants));

        Assert.Multiple(() =>
        {
            Assert.That(tenants, Is.Not.Null);
            Assert.That(tenants!.PropertyType, Is.EqualTo(typeof(ITenantSelfAdminService)));
        });
    }

    [Test]
    public void The_narrowed_domain_contract_withholds_platform_operator_validation() =>
        // A tenant-administrator surface has no use for it, and D3 says a plugin
        // gets what it uses and nothing more.
        Assert.That(
            MethodNames(typeof(IMyTenantDomain)),
            Has.None.EqualTo(nameof(ITenancyDomain.IsPlatformOperatorAsync)));

    [Test]
    public void The_operator_domain_contract_still_hands_out_the_full_operations_surface()
    {
        var tenants = typeof(ITenancyDomain).GetProperty(nameof(ITenancyDomain.Tenants));

        Assert.Multiple(() =>
        {
            Assert.That(tenants, Is.Not.Null);
            Assert.That(tenants!.PropertyType, Is.EqualTo(typeof(ITenantAdminService)));
        });
    }

    [Test]
    public void Both_domain_contracts_resolve_to_one_instance_per_scope()
    {
        // The seam supplies the narrow contract by deriving it from the wide one,
        // so a head cannot end up with the two halves pointing at different
        // objects - a tenant switch made through one would otherwise be invisible
        // to the other.
        var services = new ServiceCollection();
        services.AddScoped<ITenantAdminClient>(_ => new FakeTenantAdminClient());
        services.AddExplorerTenancy();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        var wide = scope.ServiceProvider.GetRequiredService<ITenancyDomain>();
        var narrow = scope.ServiceProvider.GetRequiredService<IMyTenantDomain>();

        Assert.That(narrow, Is.SameAs(wide));
    }

    [Test]
    public void The_operator_domain_contract_widens_the_narrow_one() =>
        Assert.That(
            typeof(IMyTenantDomain).IsAssignableFrom(typeof(ITenancyDomain)),
            Is.True,
            "the operator domain must extend the tenant-administrator one, so one adapter satisfies both");

    private static HashSet<string> MethodNames(Type contract) =>
        contract
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Select(method => method.Name)
            .ToHashSet(StringComparer.Ordinal);
}
