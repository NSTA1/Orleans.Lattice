using Orleans.Lattice;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Delegated tenant access administration (epic #4154, D5): the accessible-tenant
/// listing and the inspect path consult the policy engine with the caller's resolved
/// groups, so a member admitted only through a tenant group in the member set can
/// find and select its tenant. With the feature off the engine answers the exact-id
/// admin set, so the result is what it always was.
/// </summary>
public sealed partial class LatticeTenantSelfServiceTests
{
    private const string GlobexGroup = "t/globex/operators";

    [Test]
    public async Task ListAccessibleTenantsAsync_lists_a_tenant_the_caller_reaches_only_through_a_tenant_group()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord("globex"));
        var service = GroupAwareService(registry, delegatedAccessEnabled: true, new LatticeSubject("dave", [GlobexGroup]));

        var tenants = await service.ListAccessibleTenantsAsync();

        Assert.That(tenants.Select(tenant => tenant.TenantId), Is.EqualTo(new[] { "globex" }));
    }

    [Test]
    public async Task GetTenantAsync_inspects_a_tenant_the_caller_reaches_only_through_a_tenant_group()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord("globex"));
        var service = GroupAwareService(registry, delegatedAccessEnabled: true, new LatticeSubject("dave", [GlobexGroup]));

        var report = await service.GetTenantAsync("globex");

        Assert.That(report.TenantId, Is.EqualTo("globex"));
    }

    [Test]
    public async Task With_the_feature_off_a_group_only_member_is_not_listed_and_admins_are()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord("globex"));
        registry.Seed(SeededRecord("acme"));

        var member = GroupAwareService(registry, delegatedAccessEnabled: false, new LatticeSubject("dave", [GlobexGroup]));
        var admin = GroupAwareService(registry, delegatedAccessEnabled: false, new LatticeSubject("acme-admin", [GlobexGroup]));

        var memberTenants = await member.ListAccessibleTenantsAsync();
        var adminTenants = await admin.ListAccessibleTenantsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(memberTenants, Is.Empty);
            Assert.That(() => member.GetTenantAsync("globex"), Throws.InstanceOf<TenantNotFoundException>());
            Assert.That(adminTenants.Select(tenant => tenant.TenantId), Is.EqualTo(new[] { "acme" }));
        });
    }

    [Test]
    public async Task The_caller_groups_reach_the_engine_unchanged()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord("globex"));
        var engine = new GroupAwareTenantPolicyEngine(delegatedAccessEnabled: true);
        var service = new LatticeTenantSelfService(
            new FakeTenantContextResolver(TenantId.Default),
            engine,
            registry,
            new FixedMembershipContext(new LatticeSubject("dave", [GlobexGroup, "operators"])));

        await service.ListAccessibleTenantsAsync();

        Assert.That(engine.SeenGroups, Is.EquivalentTo(new[] { GlobexGroup, "operators" }));
    }

    private static LatticeTenantSelfService GroupAwareService(FakeTenantRegistry registry, bool delegatedAccessEnabled, LatticeSubject subject) =>
        new(
            new FakeTenantContextResolver(TenantId.Default),
            new GroupAwareTenantPolicyEngine(delegatedAccessEnabled),
            registry,
            new FixedMembershipContext(subject));

    /// <summary>
    /// Models the tenancy engine's two answers: the exact-id overload is the admin
    /// set alone (<c>acme-admin</c> administers acme); the group-aware overload adds
    /// globex for a caller in <c>t/globex/operators</c>, which globex's member set
    /// holds, only while delegated tenant access administration is on - otherwise it
    /// is the exact-id answer, as the real engine's is.
    /// </summary>
    private sealed class GroupAwareTenantPolicyEngine(bool delegatedAccessEnabled) : ITenantPolicyEngine
    {
        public IReadOnlyCollection<string>? SeenGroups { get; private set; }

        public long CurrentEpoch => 0;

        public IReadOnlyList<TenantId> ResolveAllowedTenants(string subjectId)
        {
            ArgumentNullException.ThrowIfNull(subjectId);
            return subjectId == "acme-admin" ? [TenantId.Parse("acme")] : [];
        }

        public IReadOnlyList<TenantId> ResolveAllowedTenants(string subjectId, IReadOnlyCollection<string> groupIds)
        {
            ArgumentNullException.ThrowIfNull(groupIds);
            SeenGroups = groupIds;
            var allowed = new List<TenantId>(ResolveAllowedTenants(subjectId));
            if (delegatedAccessEnabled && groupIds.Contains(GlobexGroup))
            {
                allowed.Add(TenantId.Parse("globex"));
            }

            return allowed;
        }

        public TenantAccessDecision ValidateActiveTenant(string subjectId, TenantId activeTenant)
            => throw new NotSupportedException();

        public TenantAccessDecision ResolveCrossTenantGrant(
            TenantId sourceTenant, TenantId targetTenant, string scope, TenantGrantOperations operation)
            => throw new NotSupportedException();
    }
}
