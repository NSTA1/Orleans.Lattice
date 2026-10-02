using Orleans.Lattice;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.Directory.DirectoryTestSupport;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Directory;

/// <summary>
/// Pins the group-aware tenant-admin check of <see cref="TenantRegionResidencyAuthorizer"/>
/// (D6): while delegated tenant access administration is enabled, an admin-set entry
/// naming one of the caller's resolved groups authorizes it; while it is off, and for
/// an authorizer built through the public constructor, only the exact id does. The
/// flag is read on every check, never snapshotted.
/// </summary>
[TestFixture]
public sealed class TenantRegionResidencyAuthorizerGroupAwareTests
{
    private const string Tenant = "acme";
    private const string AdminsGroup = "t/acme/admins";

    private static TenantRecord Record(params string[] adminEntries)
    {
        var record = TenantRecord.Create(
            TenantId.Parse(Tenant),
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            new HybridLogicalClock { WallClockTicks = 1 },
            "seed");

        var ticks = 2L;
        foreach (var entry in adminEntries)
        {
            record.AddAdminSubject(entry, new HybridLogicalClock { WallClockTicks = ticks++ }, "seed");
        }

        return record;
    }

    private static TenantRegionResidencyAuthorizer Authorizer(
        FakeTenantRegistry registry, LatticeSubject caller, SettableFlag? flag) =>
        flag is null
            ? new TenantRegionResidencyAuthorizer(new FixedGate(allow: false), registry, new FixedMembershipContext(caller))
            : new TenantRegionResidencyAuthorizer(new FixedGate(allow: false), registry, new FixedMembershipContext(caller), flag.Read);

    private static FakeTenantRegistry Registry(params string[] adminEntries)
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(Record(adminEntries));
        return registry;
    }

    [Test]
    public async Task A_caller_whose_group_is_an_admin_entry_is_authorized_while_the_feature_is_enabled()
    {
        var authorizer = Authorizer(Registry("alice", AdminsGroup), new LatticeSubject("bob", [AdminsGroup]), new SettableFlag(true));

        var record = await authorizer.AuthorizeTenantAdminAsync(TenantId.Parse(Tenant), "groups", CancellationToken.None);
        var probe = await authorizer.TryAuthorizeTenantAdminAsync(TenantId.Parse(Tenant));

        Assert.Multiple(() =>
        {
            Assert.That(record.Id.Value, Is.EqualTo(Tenant));
            Assert.That(probe, Is.Not.Null);
        });
    }

    [Test]
    public async Task A_group_admin_is_denied_while_the_feature_is_disabled()
    {
        var authorizer = Authorizer(Registry("alice", AdminsGroup), new LatticeSubject("bob", [AdminsGroup]), new SettableFlag(false));

        Assert.That(
            async () => await authorizer.AuthorizeTenantAdminAsync(TenantId.Parse(Tenant), "groups", CancellationToken.None),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        Assert.That(await authorizer.TryAuthorizeTenantAdminAsync(TenantId.Parse(Tenant)), Is.Null);
    }

    [Test]
    public void The_public_constructor_keeps_the_exact_id_check()
    {
        var authorizer = Authorizer(Registry("alice", AdminsGroup), new LatticeSubject("bob", [AdminsGroup]), flag: null);

        Assert.Multiple(() =>
        {
            Assert.That(authorizer.IsDelegatedAccessAware, Is.False);
            Assert.That(
                async () => await authorizer.AuthorizeTenantAdminAsync(TenantId.Parse(Tenant), "groups", CancellationToken.None),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
        });
    }

    [Test]
    public async Task The_flag_is_read_on_every_check()
    {
        var flag = new SettableFlag(false);
        var authorizer = Authorizer(Registry("alice", AdminsGroup), new LatticeSubject("bob", [AdminsGroup]), flag);

        Assert.That(authorizer.IsDelegatedAccessAware, Is.True);
        Assert.That(await authorizer.TryAuthorizeTenantAdminAsync(TenantId.Parse(Tenant)), Is.Null);

        flag.Enabled = true;
        Assert.That(await authorizer.TryAuthorizeTenantAdminAsync(TenantId.Parse(Tenant)), Is.Not.Null);

        flag.Enabled = false;
        Assert.That(await authorizer.TryAuthorizeTenantAdminAsync(TenantId.Parse(Tenant)), Is.Null);
    }

    [Test]
    public void Another_tenants_group_in_the_admin_set_never_authorizes()
    {
        // A foreign entry can only arrive by replication or restore; it must not count.
        var authorizer = Authorizer(Registry("alice", "t/globex/admins"), new LatticeSubject("bob", ["t/globex/admins"]), new SettableFlag(true));

        Assert.That(
            async () => await authorizer.AuthorizeTenantAdminAsync(TenantId.Parse(Tenant), "groups", CancellationToken.None),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public async Task A_cluster_group_admin_entry_authorizes_while_the_feature_is_enabled()
    {
        var authorizer = Authorizer(Registry("entra-ops"), new LatticeSubject("carol", ["entra-ops"]), new SettableFlag(true));

        Assert.That(await authorizer.TryAuthorizeTenantAdminAsync(TenantId.Parse(Tenant)), Is.Not.Null);
    }

    [Test]
    public async Task An_exact_id_admin_is_authorized_whatever_the_flag()
    {
        foreach (var enabled in new[] { false, true })
        {
            var authorizer = Authorizer(Registry("alice"), new LatticeSubject("alice"), new SettableFlag(enabled));

            Assert.That(await authorizer.TryAuthorizeTenantAdminAsync(TenantId.Parse(Tenant)), Is.Not.Null, $"enabled={enabled}");
        }
    }

    [Test]
    public void The_anonymous_caller_is_never_a_group_admin()
    {
        var authorizer = Authorizer(Registry("alice", AdminsGroup), LatticeSubject.Anonymous with { GroupIds = [AdminsGroup] }, new SettableFlag(true));

        Assert.That(
            async () => await authorizer.AuthorizeTenantAdminAsync(TenantId.Parse(Tenant), "groups", CancellationToken.None),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void The_internal_constructor_rejects_null_dependencies()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new TenantRegionResidencyAuthorizer(null!, new FakeTenantRegistry(), null, static () => true), Throws.ArgumentNullException);
            Assert.That(() => new TenantRegionResidencyAuthorizer(new FixedGate(true), null!, null, static () => true), Throws.ArgumentNullException);
        });
    }
}
