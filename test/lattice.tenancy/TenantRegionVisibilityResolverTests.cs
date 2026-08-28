using NSubstitute;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantRegionVisibilityResolver"/>: the registry-backed
/// implementation of the core <see cref="ITenantRegionVisibilityResolver"/> seam a
/// region-discovery surface prunes and annotates against. Proves the projection is
/// the union of the operator-authorized allowed set and the residency status map,
/// and that every outcome leaving the standing unknown fails closed to
/// <see cref="TenantRegionVisibilityMap.Unresolved"/> rather than to a permissive
/// or an empty-but-resolved answer.
/// </summary>
[TestFixture]
public sealed class TenantRegionVisibilityResolverTests
{
    private static TenantRecord NewRecord(string tenant = "acme") =>
        TenantRecord.Create(
            TenantId.Parse(tenant),
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            TestClocks.Clock(1),
            writerId: "op");

    private static TenantRegionVisibilityResolver ResolverFor(TenantRecord? record)
    {
        var registry = Substitute.For<ITenantRegistry>();
        registry.GetAsync(Arg.Any<TenantId>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(record));
        return new TenantRegionVisibilityResolver(registry);
    }

    [Test]
    public void Null_registry_throws()
        => Assert.That(() => new TenantRegionVisibilityResolver(null!), Throws.ArgumentNullException);

    [Test]
    public void The_resolver_is_active()
        => Assert.That(ResolverFor(NewRecord()).IsActive, Is.True,
            "The tenancy add-on is registered, so the discovery surface must consult it.");

    // ----- Project: the allowed-union-status projection -----

    [Test]
    public void Project_rejects_a_null_record()
        => Assert.That(() => TenantRegionVisibilityResolver.Project(null!), Throws.ArgumentNullException);

    [Test]
    public void Project_over_a_record_with_no_regions_yields_a_resolved_empty_map()
    {
        var map = TenantRegionVisibilityResolver.Project(NewRecord());

        Assert.Multiple(() =>
        {
            Assert.That(map.IsResolved, Is.True,
                "A known tenant with no regions is resolved, not unresolvable.");
            Assert.That(map.Count, Is.Zero);
        });
    }

    [Test]
    public void Project_records_an_allowed_region_that_is_not_yet_resident()
    {
        var record = NewRecord();
        record.AuthorizeRegion("eu", TestClocks.Clock(2), "op");

        var map = TenantRegionVisibilityResolver.Project(record);

        Assert.Multiple(() =>
        {
            Assert.That(map.TryGet("eu", out var eu), Is.True);
            Assert.That(eu.IsAllowed, Is.True);
            Assert.That(eu.Status, Is.EqualTo(TenantRegionResidencyStatus.None));
            Assert.That(eu.IsResident, Is.False);
            Assert.That(eu.IsVisible, Is.True);
        });
    }

    /// <summary>
    /// <c>Draining</c> is not resident (it matches
    /// <c>TenantRegionLifecycle.IsResident</c>, which excludes it because the
    /// region has stopped serving), so a revoked draining region is in neither
    /// set. It stays in the projection - its lifecycle is still reportable - but
    /// it is not visible to the discovery filter.
    /// </summary>
    [Test]
    public void Project_records_a_draining_region_that_is_no_longer_allowed()
    {
        var record = NewRecord();
        record.AuthorizeRegion("eu", TestClocks.Clock(2), "op");
        record.SetRegionStatus("eu", TenantRegionStatus.Draining, TestClocks.Clock(3), "admin");
        record.RevokeRegion("eu", TestClocks.Clock(4), "op");

        var map = TenantRegionVisibilityResolver.Project(record);

        Assert.Multiple(() =>
        {
            Assert.That(map.TryGet("eu", out var eu), Is.True,
                "A revoked region the tenant is still draining out of stays in the projection.");
            Assert.That(eu.IsAllowed, Is.False);
            Assert.That(eu.Status, Is.EqualTo(TenantRegionResidencyStatus.Draining));
            Assert.That(eu.IsResident, Is.False);
            Assert.That(eu.IsVisible, Is.False,
                "Draining plus revoked is in neither the allowed nor the resident set.");
        });
    }

    /// <summary>
    /// The union arm: a region the tenant is genuinely resident in stays visible
    /// even after the allow-set no longer carries it, so a tenant is never blinded
    /// to a region it is actually holding data in.
    /// </summary>
    [Test]
    public void Project_records_a_resident_region_that_is_no_longer_allowed()
    {
        var record = NewRecord();
        record.AuthorizeRegion("eu", TestClocks.Clock(2), "op");
        record.SetRegionStatus("eu", TenantRegionStatus.Online, TestClocks.Clock(3), "admin");
        record.RevokeRegion("eu", TestClocks.Clock(4), "op");

        var map = TenantRegionVisibilityResolver.Project(record);

        Assert.Multiple(() =>
        {
            Assert.That(map.TryGet("eu", out var eu), Is.True);
            Assert.That(eu.IsAllowed, Is.False);
            Assert.That(eu.Status, Is.EqualTo(TenantRegionResidencyStatus.Online));
            Assert.That(eu.IsResident, Is.True);
            Assert.That(eu.IsVisible, Is.True,
                "The resident arm keeps the region visible on its own.");
        });
    }

    [Test]
    public void Project_is_the_union_of_the_allowed_set_and_the_status_map()
    {
        var record = NewRecord();
        record.AuthorizeRegion("eu", TestClocks.Clock(2), "op");
        record.AuthorizeRegion("ap", TestClocks.Clock(3), "op");
        record.SetRegionStatus("us", TenantRegionStatus.Removed, TestClocks.Clock(4), "admin");

        var map = TenantRegionVisibilityResolver.Project(record);

        Assert.Multiple(() =>
        {
            Assert.That(map.Count, Is.EqualTo(3));
            Assert.That(map.TryGet("eu", out _), Is.True);
            Assert.That(map.TryGet("ap", out _), Is.True);
            Assert.That(map.TryGet("us", out var us), Is.True);
            Assert.That(us.IsAllowed, Is.False);
            Assert.That(us.IsVisible, Is.False,
                "A removed, unallowed region is in the projection but outside the actionable set.");
        });
    }

    [Test]
    public void Project_maps_every_registry_status_onto_the_core_seam(
        [Values] TenantRegionStatus status)
    {
        var expected = status switch
        {
            TenantRegionStatus.Provisioning => TenantRegionResidencyStatus.Provisioning,
            TenantRegionStatus.Backfilling => TenantRegionResidencyStatus.Backfilling,
            TenantRegionStatus.Online => TenantRegionResidencyStatus.Online,
            TenantRegionStatus.Draining => TenantRegionResidencyStatus.Draining,
            TenantRegionStatus.Offline => TenantRegionResidencyStatus.Offline,
            TenantRegionStatus.Removed => TenantRegionResidencyStatus.Removed,
            _ => TenantRegionResidencyStatus.None,
        };

        var record = NewRecord();
        record.AuthorizeRegion("eu", TestClocks.Clock(2), "op");
        record.SetRegionStatus("eu", status, TestClocks.Clock(3), "admin");

        var map = TenantRegionVisibilityResolver.Project(record);

        Assert.That(map.TryGet("eu", out var eu), Is.True);
        Assert.That(eu.Status, Is.EqualTo(expected));
    }

    // ----- ResolveAsync -----

    [Test]
    public async Task ResolveAsync_projects_the_registered_tenant_record()
    {
        var record = NewRecord();
        record.AuthorizeRegion("eu", TestClocks.Clock(2), "op");
        record.SetRegionStatus("eu", TenantRegionStatus.Online, TestClocks.Clock(3), "admin");

        var map = await ResolverFor(record).ResolveAsync(TenantId.Parse("acme"));

        Assert.Multiple(() =>
        {
            Assert.That(map.IsResolved, Is.True);
            Assert.That(map.TryGet("eu", out var eu), Is.True);
            Assert.That(eu.Status, Is.EqualTo(TenantRegionResidencyStatus.Online));
        });
    }

    [Test]
    public async Task ResolveAsync_reads_the_registry_for_the_requested_tenant()
    {
        var registry = Substitute.For<ITenantRegistry>();
        registry.GetAsync(Arg.Any<TenantId>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<TenantRecord?>(NewRecord()));

        await new TenantRegionVisibilityResolver(registry).ResolveAsync(TenantId.Parse("acme"));

        _ = registry.Received(1).GetAsync(TenantId.Parse("acme"), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ResolveAsync_fails_closed_for_an_uninitialised_tenant_id()
    {
        var registry = Substitute.For<ITenantRegistry>();

        var map = await new TenantRegionVisibilityResolver(registry).ResolveAsync(default);

        Assert.Multiple(() =>
        {
            Assert.That(map, Is.SameAs(TenantRegionVisibilityMap.Unresolved));
            _ = registry.DidNotReceive().GetAsync(Arg.Any<TenantId>(), Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public async Task ResolveAsync_fails_closed_for_an_unregistered_tenant()
    {
        var map = await ResolverFor(record: null).ResolveAsync(TenantId.Parse("ghost"));

        Assert.That(map, Is.SameAs(TenantRegionVisibilityMap.Unresolved),
            "An unknown tenant has no standing to establish, so nothing is disclosed.");
    }

    [Test]
    public async Task ResolveAsync_fails_closed_when_the_registry_faults()
    {
        var registry = Substitute.For<ITenantRegistry>();
        registry.GetAsync(Arg.Any<TenantId>(), Arg.Any<CancellationToken>())
            .Returns<Task<TenantRecord?>>(_ => throw new InvalidOperationException("registry down"));

        var map = await new TenantRegionVisibilityResolver(registry).ResolveAsync(TenantId.Parse("acme"));

        Assert.That(map, Is.SameAs(TenantRegionVisibilityMap.Unresolved));
    }

    [Test]
    public void ResolveAsync_propagates_a_cancellation()
    {
        var registry = Substitute.For<ITenantRegistry>();
        registry.GetAsync(Arg.Any<TenantId>(), Arg.Any<CancellationToken>())
            .Returns<Task<TenantRecord?>>(_ => throw new OperationCanceledException());

        Assert.That(
            async () => await new TenantRegionVisibilityResolver(registry)
                .ResolveAsync(TenantId.Parse("acme"), new CancellationToken(canceled: true)),
            Throws.InstanceOf<OperationCanceledException>(),
            "A cancellation is the caller's own signal, not a resolution failure to swallow.");
    }

    [Test]
    public async Task ResolveAsync_resolves_the_default_tenant_when_it_is_registered()
    {
        var record = TenantRecord.CreateDefault(TestClocks.Clock(1), "op");
        record.AuthorizeRegion("eu", TestClocks.Clock(2), "op");

        var map = await ResolverFor(record).ResolveAsync(TenantId.Default);

        Assert.That(map.IsResolved, Is.True,
            "The resolver answers for whatever it is asked; skipping the default tenant is the "
            + "discovery surface's decision, not this seam's.");
    }
}
