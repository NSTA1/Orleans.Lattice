using NSubstitute;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Auth.Tests;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// The tenant admin audit reader (#3638): fixed to the caller's validated
/// <c>t/{tenant}</c> partition, refused to anybody who does not administer that tenant,
/// and never showing an in-flight intent.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticeTenantAuditLogTests
{
    private static AdminAuditTreeFake Seeded()
    {
        var tree = new AdminAuditTreeFake();
        tree.Final(AdminAuditTreeFake.Record("cluster", 1));
        tree.Final(AdminAuditTreeFake.Record("acme-1", 2, scope: "t/acme", actor: "alice"));
        tree.Final(AdminAuditTreeFake.Record("other-1", 3, scope: "t/other", actor: "bob"));
        tree.Final(AdminAuditTreeFake.Record("acme-2", 4, scope: "t/acme", actor: "op", outcome: LatticeAdminAuditOutcome.Failed));
        tree.Final(AdminAuditTreeFake.Record("acme-3", 5, scope: "t/acme", actor: "alice"));
        return tree;
    }

    private static LatticeTenantAuditLog Create(
        AdminAuditTreeFake tree, string caller, bool operatorAllowed = false, ILatticeAdminAuditTrail? trail = null)
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(Tenant("acme", "alice"));
        registry.Seed(Tenant("other", "bob"));
        var authorizer = new TenantRegionResidencyAuthorizer(
            new FixedGate(allow: operatorAllowed), registry, new FixedMembershipContext(new LatticeSubject(caller)));
        return new LatticeTenantAuditLog(authorizer, tree.GrainFactory, trail);
    }

    private static TenantRecord Tenant(string id, string admin)
    {
        var record = TenantRecord.Create(
            TenantId.Parse(id),
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            new HybridLogicalClock { WallClockTicks = 1 },
            "seed");
        record.AddAdminSubject(admin, new HybridLogicalClock { WallClockTicks = 2 }, "seed");
        return record;
    }

    private static string[] Ids(AdminAuditPage page) => page.Records.Select(r => r.OperationId).ToArray();

    [Test]
    public async Task ListAuditAsync_returns_only_the_callers_tenant_partition_whatever_the_query_scope()
    {
        var log = Create(Seeded(), caller: "alice");

        var own = await log.ListAuditAsync("acme", new AdminAuditQuery());
        var spoofed = await log.ListAuditAsync("acme", new AdminAuditQuery { Scope = "t/other" });
        var cluster = await log.ListAuditAsync("acme", new AdminAuditQuery { Scope = LatticeAdminAuditRequest.ClusterScope });

        Assert.Multiple(() =>
        {
            Assert.That(Ids(own), Is.EqualTo(new[] { "acme-3", "acme-2", "acme-1" }));
            Assert.That(Ids(spoofed), Is.EqualTo(Ids(own)));
            Assert.That(Ids(cluster), Is.EqualTo(Ids(own)));
        });
    }

    [Test]
    public async Task ListAuditAsync_applies_filters_and_pages_newest_first()
    {
        var log = Create(Seeded(), caller: "alice");

        var byActor = await log.ListAuditAsync("acme", new AdminAuditQuery { Actor = "alice" });
        var byOutcome = await log.ListAuditAsync("acme", new AdminAuditQuery { Outcome = LatticeAdminAuditOutcome.Failed });
        var first = await log.ListAuditAsync("acme", new AdminAuditQuery { PageSize = 2 });
        var second = await log.ListAuditAsync("acme", new AdminAuditQuery { PageSize = 2, PageToken = first.NextPageToken });

        Assert.Multiple(() =>
        {
            Assert.That(Ids(byActor), Is.EqualTo(new[] { "acme-3", "acme-1" }));
            Assert.That(Ids(byOutcome), Is.EqualTo(new[] { "acme-2" }));
            Assert.That(Ids(first), Is.EqualTo(new[] { "acme-3", "acme-2" }));
            Assert.That(first.NextPageToken, Is.Not.Null);
            Assert.That(Ids(second), Is.EqualTo(new[] { "acme-1" }));
            Assert.That(second.NextPageToken, Is.Null);
        });
    }

    [Test]
    public async Task ListAuditAsync_hides_in_flight_intents()
    {
        var tree = Seeded();
        tree.Pending(AdminAuditTreeFake.Record("acme-pending", 6, scope: "t/acme", outcome: LatticeAdminAuditOutcome.Pending));

        var page = await Create(tree, caller: "alice").ListAuditAsync(
            "acme", new AdminAuditQuery { Outcome = LatticeAdminAuditOutcome.Pending });
        var all = await Create(tree, caller: "alice").ListAuditAsync("acme", new AdminAuditQuery());

        Assert.Multiple(() =>
        {
            Assert.That(page.Records, Is.Empty);
            Assert.That(Ids(all), Does.Not.Contain("acme-pending"));
        });
    }

    [Test]
    public void ListAuditAsync_refuses_an_admin_of_another_tenant_and_records_the_denial()
    {
        var trail = Substitute.For<ILatticeAdminAuditTrail>();
        trail.IsEnabled.Returns(true);
        var log = Create(Seeded(), caller: "bob", trail: trail);

        Assert.That(async () => await log.ListAuditAsync("acme", new AdminAuditQuery()), Throws.InstanceOf<UnauthorizedAccessException>());
        _ = trail.Received(1).RecordDeniedAsync(
            Arg.Is<LatticeAdminAuditRequest>(r => r.Action == LatticeAdminAuditAction.Read && r.TargetScope == "t/acme"),
            Arg.Any<string>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public void ListAuditAsync_refuses_a_caller_who_administers_no_tenant()
    {
        var log = Create(Seeded(), caller: "mallory");

        Assert.That(async () => await log.ListAuditAsync("acme", new AdminAuditQuery()), Throws.InstanceOf<UnauthorizedAccessException>());
    }

    [Test]
    public async Task ListAuditAsync_admits_a_platform_operator_to_the_named_partition()
    {
        var page = await Create(Seeded(), caller: "op", operatorAllowed: true).ListAuditAsync("other", new AdminAuditQuery());

        Assert.That(Ids(page), Is.EqualTo(new[] { "other-1" }));
    }

    [Test]
    public void ListAuditAsync_rejects_an_invalid_tenant_and_a_null_query()
    {
        var log = Create(Seeded(), caller: "alice");

        Assert.Multiple(() =>
        {
            Assert.That(async () => await log.ListAuditAsync("", new AdminAuditQuery()), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await log.ListAuditAsync("acme", null!), Throws.ArgumentNullException);
        });
    }
}
