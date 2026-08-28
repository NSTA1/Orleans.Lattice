using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="GrpcTenantRegionVisibilityResolver"/>, the remote-host
/// <see cref="ITenantRegionVisibilityResolver"/> that resolves a tenant's per-region
/// standing over the region-residency facade. Proves it reports itself active, maps
/// every lifecycle status onto its residency counterpart, and - the security-relevant
/// property - resolves to <see cref="TenantRegionVisibilityMap.Unresolved"/> on every
/// outcome that leaves the standing unknown, so the region catalog degrades to the
/// current region rather than disclosing the full topology. Cancellation is proven to
/// propagate rather than be swallowed into a fail-closed verdict. Deterministic over a
/// substituted facade - no network, no host.
/// </summary>
[TestFixture]
public sealed class GrpcTenantRegionVisibilityResolverTests
{
    private static TenantRegionStatusDescriptor Row(
        string regionId, TenantRegionLifecycleStatus status, bool isAllowed)
        => new() { RegionId = regionId, Status = status, IsAllowed = isAllowed };

    private static ILatticeTenantRegionAdmin Facade(params TenantRegionStatusDescriptor[] rows)
    {
        var admin = Substitute.For<ILatticeTenantRegionAdmin>();
        admin.GetTenantRegionStatusAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new TenantRegionStatusReport { TenantId = "acme", Regions = rows });
        return admin;
    }

    [Test]
    public void Constructor_null_facade_throws()
        => Assert.That(() => new GrpcTenantRegionVisibilityResolver(null!), Throws.ArgumentNullException);

    [Test]
    public void Resolver_reports_itself_active()
    {
        var resolver = new GrpcTenantRegionVisibilityResolver(Facade());

        Assert.That(resolver.IsActive, Is.True,
            "A head configured with a tenant-admin endpoint can resolve standing, so the catalog must consult it.");
    }

    [Test]
    public async Task ResolveAsync_projects_every_reported_row()
    {
        var resolver = new GrpcTenantRegionVisibilityResolver(Facade(
            Row("eu-west", TenantRegionLifecycleStatus.Online, isAllowed: true),
            Row("ap-south", TenantRegionLifecycleStatus.None, isAllowed: true)));

        var map = await resolver.ResolveAsync(TenantId.Parse("acme"));

        Assert.Multiple(() =>
        {
            Assert.That(map.IsResolved, Is.True);
            Assert.That(map.Count, Is.EqualTo(2));
            Assert.That(map.TryGet("eu-west", out var west), Is.True);
            Assert.That(west.IsAllowed, Is.True);
            Assert.That(west.Status, Is.EqualTo(TenantRegionResidencyStatus.Online));
            Assert.That(west.IsResident, Is.True);
            Assert.That(map.TryGet("ap-south", out var south), Is.True);
            Assert.That(south.Status, Is.EqualTo(TenantRegionResidencyStatus.None));
            Assert.That(south.IsResident, Is.False);
            Assert.That(south.IsVisible, Is.True, "Allowed-but-not-resident is still actionable.");
        });
    }

    [Test]
    public async Task ResolveAsync_forwards_the_tenant_id_to_the_facade()
    {
        var admin = Facade();
        var resolver = new GrpcTenantRegionVisibilityResolver(admin);

        _ = await resolver.ResolveAsync(TenantId.Parse("acme"));

        await admin.Received(1).GetTenantRegionStatusAsync("acme", Arg.Any<CancellationToken>());
    }

    [TestCase(TenantRegionLifecycleStatus.Provisioning, TenantRegionResidencyStatus.Provisioning)]
    [TestCase(TenantRegionLifecycleStatus.Backfilling, TenantRegionResidencyStatus.Backfilling)]
    [TestCase(TenantRegionLifecycleStatus.Online, TenantRegionResidencyStatus.Online)]
    [TestCase(TenantRegionLifecycleStatus.Draining, TenantRegionResidencyStatus.Draining)]
    [TestCase(TenantRegionLifecycleStatus.Offline, TenantRegionResidencyStatus.Offline)]
    [TestCase(TenantRegionLifecycleStatus.Removed, TenantRegionResidencyStatus.Removed)]
    [TestCase(TenantRegionLifecycleStatus.None, TenantRegionResidencyStatus.None)]
    public async Task ResolveAsync_maps_each_lifecycle_status_onto_its_residency_counterpart(
        TenantRegionLifecycleStatus wire, TenantRegionResidencyStatus expected)
    {
        var resolver = new GrpcTenantRegionVisibilityResolver(Facade(Row("eu-west", wire, isAllowed: false)));

        var map = await resolver.ResolveAsync(TenantId.Parse("acme"));

        Assert.That(map.TryGet("eu-west", out var visibility), Is.True);
        Assert.That(visibility.Status, Is.EqualTo(expected));
    }

    [Test]
    public async Task ResolveAsync_an_empty_report_resolves_to_the_empty_map_not_the_unresolved_one()
    {
        var resolver = new GrpcTenantRegionVisibilityResolver(Facade());

        var map = await resolver.ResolveAsync(TenantId.Parse("acme"));

        Assert.Multiple(() =>
        {
            Assert.That(map.IsResolved, Is.True,
                "A tenant with no standing anywhere is a resolved answer, not an unresolvable one.");
            Assert.That(map.Count, Is.Zero);
        });
    }

    [Test]
    public async Task ResolveAsync_a_denied_call_fails_closed()
    {
        var admin = Substitute.For<ILatticeTenantRegionAdmin>();
        admin.GetTenantRegionStatusAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<Task<TenantRegionStatusReport>>(_ => throw new LatticeAuthorizationDeniedException());
        var resolver = new GrpcTenantRegionVisibilityResolver(admin);

        var map = await resolver.ResolveAsync(TenantId.Parse("acme"));

        Assert.That(map.IsResolved, Is.False,
            "A denial must never widen the catalog back to the full topology.");
    }

    [Test]
    public async Task ResolveAsync_an_unreachable_endpoint_fails_closed()
    {
        var admin = Substitute.For<ILatticeTenantRegionAdmin>();
        admin.GetTenantRegionStatusAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<Task<TenantRegionStatusReport>>(_ => throw new InvalidOperationException("unreachable"));
        var resolver = new GrpcTenantRegionVisibilityResolver(admin);

        var map = await resolver.ResolveAsync(TenantId.Parse("acme"));

        Assert.That(map.IsResolved, Is.False);
    }

    [Test]
    public async Task ResolveAsync_a_default_tenant_fails_closed_without_a_round_trip()
    {
        var admin = Facade();
        var resolver = new GrpcTenantRegionVisibilityResolver(admin);

        var map = await resolver.ResolveAsync(default);

        Assert.That(map.IsResolved, Is.False);
        await admin.DidNotReceive().GetTenantRegionStatusAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void ResolveAsync_propagates_cancellation_rather_than_failing_closed()
    {
        var admin = Substitute.For<ILatticeTenantRegionAdmin>();
        admin.GetTenantRegionStatusAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<Task<TenantRegionStatusReport>>(_ => throw new OperationCanceledException());
        var resolver = new GrpcTenantRegionVisibilityResolver(admin);

        Assert.CatchAsync<OperationCanceledException>(
            async () => await resolver.ResolveAsync(TenantId.Parse("acme")),
            "Cancellation is the caller's own signal, not an unresolvable standing.");
    }

    [Test]
    public async Task ResolveAsync_forwards_the_cancellation_token_to_the_facade()
    {
        using var cts = new CancellationTokenSource();
        var admin = Facade();
        var resolver = new GrpcTenantRegionVisibilityResolver(admin);

        _ = await resolver.ResolveAsync(TenantId.Parse("acme"), cts.Token);

        await admin.Received(1).GetTenantRegionStatusAsync("acme", cts.Token);
    }
}
