using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Directory;

/// <summary>
/// Pins the D4 admin-set confinement on
/// <see cref="LatticeTenantAccessAdmin.AddAdminSubjectAsync"/>: an admin entry names a
/// user, a cluster group, or one of the tenant's own groups; another tenant's group
/// and a malformed id in the reserved <c>t/</c> namespace are refused for every caller
/// before anything is written, and a tenant group is not resolved upstream.
/// </summary>
[TestFixture]
public sealed class LatticeTenantAccessAdminAdminSetConfinementTests
{
    private const string Tenant = "acme";

    private static (LatticeTenantAccessAdmin Admin, FakeTenantRegistry Registry) Build(FakeIdentityDirectory? directory = null)
    {
        var registry = new FakeTenantRegistry();
        var record = TenantRecord.Create(
            TenantId.Parse(Tenant),
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            new HybridLogicalClock { WallClockTicks = 1 },
            "seed");
        record.AddAdminSubject("alice", new HybridLogicalClock { WallClockTicks = 2 }, "seed");
        registry.Seed(record);

        var admin = new LatticeTenantAccessAdmin(
            registry,
            new TenantRegionResidencyAuthorizer(new FixedGate(allow: true), registry, new FixedMembershipContext(new LatticeSubject("op"))),
            new IncrementingClock(),
            Options.Create(new ClusterOptions { ClusterId = "region-a" }),
            directory,
            new FixedOptionsMonitor<LatticeIdentityDirectoryOptions>(new LatticeIdentityDirectoryOptions { ValidationRequired = true }));
        return (admin, registry);
    }

    [TestCase("t/globex/admins")]
    [TestCase("t/acme/Admins")]
    [TestCase("t/acme/")]
    [TestCase("t/default/admins")]
    [TestCase("t/")]
    public void AddAdminSubjectAsync_refuses_a_reserved_namespace_entry_that_is_not_the_tenants_own_group(string subjectId)
    {
        var (admin, registry) = Build();

        var ex = Assert.ThrowsAsync<TenantAccessConfinementException>(async () => await admin.AddAdminSubjectAsync(Tenant, subjectId));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Rule, Is.EqualTo(TenantAccessConfinementRule.ForeignTenantGroup));
            Assert.That(ex.TenantId, Is.EqualTo(Tenant));
            Assert.That(ex.ParamName, Is.EqualTo("subjectId"));
            Assert.That(registry.Puts, Is.Zero);
        });
    }

    [Test]
    public async Task AddAdminSubjectAsync_accepts_the_tenants_own_group_without_resolving_it_upstream()
    {
        var directory = new FakeIdentityDirectory(principal: null);
        var (admin, registry) = Build(directory);

        var result = await admin.AddAdminSubjectAsync(Tenant, "t/acme/admins");

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True);
            Assert.That(result.Subjects, Is.EqualTo(new[] { "alice", "t/acme/admins" }));
            Assert.That(directory.Resolved, Is.Empty);
            Assert.That(registry.Puts, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task AddAdminSubjectAsync_accepts_a_cluster_group_that_resolves_upstream()
    {
        var directory = new FakeIdentityDirectory(new DirectoryPrincipal("entra-ops", "Ops", DirectoryPrincipalKind.Group));
        var (admin, _) = Build(directory);

        var result = await admin.AddAdminSubjectAsync(Tenant, "entra-ops");

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True);
            Assert.That(directory.Resolved, Is.EqualTo(new[] { "entra-ops" }));
        });
    }

    [Test]
    public void AddAdminSubjectAsync_still_validates_a_user_upstream()
    {
        var (admin, registry) = Build(new FakeIdentityDirectory(principal: null));

        Assert.That(async () => await admin.AddAdminSubjectAsync(Tenant, "typo"), Throws.TypeOf<LatticeDirectoryValidationException>());
        Assert.That(registry.Puts, Is.Zero);
    }

    [Test]
    public void EnsureAdmissibleAdminEntry_classifies_each_shape()
    {
        var tenant = TenantId.Parse(Tenant);

        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantAccessAdmin.EnsureAdmissibleAdminEntry(tenant, "alice"), Is.False);
            Assert.That(LatticeTenantAccessAdmin.EnsureAdmissibleAdminEntry(tenant, "entra-ops"), Is.False);
            Assert.That(LatticeTenantAccessAdmin.EnsureAdmissibleAdminEntry(tenant, "t/acme/admins"), Is.True);
            Assert.That(() => LatticeTenantAccessAdmin.EnsureAdmissibleAdminEntry(tenant, "t/globex/admins"), Throws.TypeOf<TenantAccessConfinementException>());
        });
    }
}
