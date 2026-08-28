using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Region;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for the tenant scoping <see cref="LatticeApiMcpRegionCatalog"/>
/// applies to the regions it advertises. Proves the three properties the scoping
/// exists for: a cluster with no tenancy pays nothing and its answer is unchanged
/// down to the reference, a tenant-asserted call sees only its actionable regions
/// annotated with its own standing, and a call whose standing cannot be
/// established fails closed to the current region rather than leaking the full
/// routing topology.
/// </summary>
[TestFixture]
public sealed class LatticeApiMcpRegionCatalogTenantScopeTests
{
    private static LatticeApiMcpRegionRouter Router(params string[] peerRegionIds)
    {
        var definitions = new List<LatticeApiMcpRegionDefinition>
        {
            new()
            {
                RegionId = "us",
                ClusterId = "cluster-us",
                IsCurrent = true,
                Groups = new Dictionary<LatticeApiMcpGroup, string?> { [LatticeApiMcpGroup.State] = null },
            },
        };

        foreach (var peer in peerRegionIds)
        {
            definitions.Add(new LatticeApiMcpRegionDefinition
            {
                RegionId = peer,
                ClusterId = $"cluster-{peer}",
                IsCurrent = false,
                Groups = new Dictionary<LatticeApiMcpGroup, string?>
                {
                    [LatticeApiMcpGroup.State] = $"https://{peer}-state:5001",
                },
            });
        }

        return new LatticeApiMcpRegionRouter("us", definitions);
    }

    private static IServiceProvider Services(
        ITenantRegionVisibilityResolver? resolver = null,
        ILatticeApiMcpRegionIdentityVerifier? verifier = null)
    {
        var services = new ServiceCollection();
        if (resolver is not null)
        {
            services.AddSingleton(resolver);
        }

        if (verifier is not null)
        {
            services.AddSingleton(verifier);
        }

        return services.BuildServiceProvider();
    }

    private static TenantRegionVisibilityMap MapOf(
        params (string RegionId, bool IsAllowed, TenantRegionResidencyStatus Status)[] entries)
        => TenantRegionVisibilityMap.Create(entries.Select(e =>
            new KeyValuePair<string, TenantRegionVisibility>(
                e.RegionId, new TenantRegionVisibility(e.IsAllowed, e.Status))));

    // ----- tenancy off: the answer must be byte-for-byte unchanged -----

    [Test]
    public async Task No_tenant_asserted_returns_the_router_snapshot_by_reference()
    {
        var router = Router("eu", "ap");
        var catalog = new LatticeApiMcpRegionCatalog(router, Services());

        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions, Is.SameAs(router.Snapshot()),
            "With no tenancy registered the catalog must return the frozen snapshot itself - "
            + "same reference, no allocation, byte-for-byte the pre-tenancy answer.");
    }

    [Test]
    public async Task A_registered_but_inactive_resolver_still_returns_the_snapshot_by_reference()
    {
        var router = Router("eu");
        var catalog = new LatticeApiMcpRegionCatalog(
            router, Services(resolver: new FakeResolver(TenantRegionVisibilityMap.Empty, isActive: false)));

        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions, Is.SameAs(router.Snapshot()),
            "The null-object resolver is never consulted when no tenant is asserted, so the fast path holds.");
    }

    [Test]
    public async Task An_asserted_tenant_with_an_inactive_resolver_returns_the_snapshot_by_reference()
    {
        // The deployed tenancy-off shape, and the one the sibling test above does
        // NOT cover: it asserts no tenant, so it exercises inactive-resolver
        // WITHOUT an assertion. The MCP head's active-tenant bridge is registered
        // unconditionally (TryAddSingleton, no opt-in), so a caller can stamp an
        // ambient tenant on a cluster running no tenancy add-on at all. Scoping on
        // that alone changed the response shape purely because a header was
        // present. Verified against the local-dev harness with TENANCY_ENABLED
        // unset, where it emitted a tenantScope for `acme`.
        var router = Router("eu", "ap");
        var resolver = new FakeResolver(TenantRegionVisibilityMap.Empty, isActive: false);
        var catalog = new LatticeApiMcpRegionCatalog(router, Services(resolver));

        using (LatticeActiveTenantContext.With(TenantId.Parse("acme")))
        {
            var regions = await catalog.ListRegionsAsync();

            Assert.Multiple(() =>
            {
                Assert.That(regions, Is.SameAs(router.Snapshot()),
                    "With no tenancy engine the answer must stay byte-for-byte the pre-tenancy one, "
                    + "whatever header the caller sends.");
                Assert.That(resolver.Calls, Is.Zero,
                    "An inactive resolver must never be consulted.");
            });
        }
    }

    [Test]
    public async Task An_asserted_tenant_with_no_resolver_registered_returns_the_snapshot_by_reference()
    {
        // The same shape with the resolver absent entirely rather than inactive -
        // a remote head with no tenancy binding at all.
        var router = Router("eu", "ap");
        var catalog = new LatticeApiMcpRegionCatalog(router, Services());

        using (LatticeActiveTenantContext.With(TenantId.Parse("acme")))
        {
            var regions = await catalog.ListRegionsAsync();

            Assert.That(regions, Is.SameAs(router.Snapshot()),
                "No resolver registered is the same answer as an inactive one: unscoped topology.");
        }
    }

    [Test]
    public async Task An_asserted_tenant_with_no_tenancy_engine_carries_no_tenant_annotation()
    {
        // The disclosure half of the same defect: with no engine to validate the
        // assertion against, annotating echoed the caller's own unvalidated header
        // value back as a tenantScope. Live, a nonsense tenant id was reflected
        // verbatim, so this pins the annotation's absence explicitly rather than
        // relying on reference identity alone.
        var catalog = new LatticeApiMcpRegionCatalog(
            Router("eu"), Services(resolver: new FakeResolver(TenantRegionVisibilityMap.Empty, isActive: false)));

        using (LatticeActiveTenantContext.With(TenantId.Parse("does-not-exist")))
        {
            var regions = await catalog.ListRegionsAsync();

            Assert.That(regions.Select(r => r.TenantScope), Is.All.Null,
                "A cluster with no tenancy engine must never echo a caller-supplied tenant id back.");
        }
    }

    [Test]
    public async Task The_default_tenant_returns_the_router_snapshot_by_reference()
    {
        var router = Router("eu", "ap");
        var resolver = new FakeResolver(TenantRegionVisibilityMap.Empty);
        var catalog = new LatticeApiMcpRegionCatalog(router, Services(resolver));

        using (LatticeActiveTenantContext.With(TenantId.Default))
        {
            var regions = await catalog.ListRegionsAsync();

            Assert.Multiple(() =>
            {
                Assert.That(regions, Is.SameAs(router.Snapshot()),
                    "The reserved default tenant names the pre-tenancy behaviour, so it is not scoped.");
                Assert.That(resolver.Calls, Is.Zero, "The resolver must not be consulted for the default tenant.");
            });
        }
    }

    [Test]
    public async Task An_unscoped_answer_carries_no_tenant_annotation()
    {
        var catalog = new LatticeApiMcpRegionCatalog(Router("eu"), Services());

        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions.Select(r => r.TenantScope), Is.All.Null,
            "A non-tenant answer must be indistinguishable from the pre-tenancy answer.");
    }

    // ----- tenant asserted: filter to the actionable set -----

    [Test]
    public async Task An_allowed_peer_is_advertised()
    {
        var catalog = new LatticeApiMcpRegionCatalog(
            Router("eu", "ap"),
            Services(new FakeResolver(MapOf(("eu", true, TenantRegionResidencyStatus.None)))));

        using var scope = LatticeActiveTenantContext.With(TenantId.Parse("acme"));
        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions.Select(r => r.RegionId), Is.EqualTo(new[] { "us", "eu" }),
            "A region the tenant is authorized into is actionable even before it is resident.");
    }

    /// <summary>
    /// The union arm of the actionable set. The facade's invariants keep residency
    /// a subset of the allowed set, so this pairing should not arise in practice -
    /// but the resolver is a projection of remote state, and the catalog must union
    /// rather than intersect so a tenant that <b>is</b> holding data somewhere is
    /// never denied sight of it by a stale or partial allow-set read.
    /// </summary>
    [Test]
    public async Task A_resident_but_not_allowed_peer_is_advertised()
    {
        var catalog = new LatticeApiMcpRegionCatalog(
            Router("eu", "ap"),
            Services(new FakeResolver(MapOf(("eu", false, TenantRegionResidencyStatus.Provisioning)))));

        using var scope = LatticeActiveTenantContext.With(TenantId.Parse("acme"));
        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions.Select(r => r.RegionId), Is.EqualTo(new[] { "us", "eu" }),
            "A tenant holding data in a region must be able to see it even if the allow-set read disagrees.");
    }

    /// <summary>
    /// <c>Draining</c> is deliberately not resident - it matches the tenancy
    /// package's own <c>TenantRegionLifecycle.IsResident</c>, which excludes it
    /// because the region is already leaving and has stopped serving. A draining
    /// region the operator has also revoked is therefore in neither set and the
    /// tenant can do nothing there, so it is pruned from the routing catalog. Its
    /// lifecycle stays fully observable through <c>lattice_tenant_region_status</c>.
    /// </summary>
    [Test]
    public async Task A_draining_peer_outside_the_allowed_set_is_pruned()
    {
        var catalog = new LatticeApiMcpRegionCatalog(
            Router("eu"),
            Services(new FakeResolver(MapOf(("eu", false, TenantRegionResidencyStatus.Draining)))));

        using var scope = LatticeActiveTenantContext.With(TenantId.Parse("acme"));
        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions.Select(r => r.RegionId), Is.EqualTo(new[] { "us" }),
            "Draining is not resident, so a revoked draining region is outside the actionable set.");
    }

    /// <summary>
    /// The common revocation path: an operator may only revoke a region once the
    /// tenant has stopped being resident, so a draining region is normally still
    /// allowed. It must stay advertised for the whole drain.
    /// </summary>
    [Test]
    public async Task A_draining_peer_still_in_the_allowed_set_is_advertised()
    {
        var catalog = new LatticeApiMcpRegionCatalog(
            Router("eu"),
            Services(new FakeResolver(MapOf(("eu", true, TenantRegionResidencyStatus.Draining)))));

        using var scope = LatticeActiveTenantContext.With(TenantId.Parse("acme"));
        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions.Select(r => r.RegionId), Is.EqualTo(new[] { "us", "eu" }),
            "The allowed arm keeps a draining region visible until the operator revokes it.");
    }

    [Test]
    public async Task A_peer_the_tenant_has_no_relationship_with_is_pruned()
    {
        var catalog = new LatticeApiMcpRegionCatalog(
            Router("eu", "ap"),
            Services(new FakeResolver(MapOf(("eu", true, TenantRegionResidencyStatus.Online)))));

        using var scope = LatticeActiveTenantContext.With(TenantId.Parse("acme"));
        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions.Select(r => r.RegionId), Does.Not.Contain("ap"),
            "A region outside the tenant's actionable set is not that tenant's business.");
    }

    [Test]
    public async Task A_peer_present_but_neither_allowed_nor_resident_is_pruned()
    {
        var catalog = new LatticeApiMcpRegionCatalog(
            Router("eu"),
            Services(new FakeResolver(MapOf(("eu", false, TenantRegionResidencyStatus.Removed)))));

        using var scope = LatticeActiveTenantContext.With(TenantId.Parse("acme"));
        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions.Select(r => r.RegionId), Is.EqualTo(new[] { "us" }),
            "A fully removed region is neither allowed nor resident, so it drops out of the actionable set.");
    }

    [Test]
    public async Task The_current_region_is_always_advertised_even_with_no_standing_in_it()
    {
        var catalog = new LatticeApiMcpRegionCatalog(
            Router("eu"),
            Services(new FakeResolver(MapOf(("eu", true, TenantRegionResidencyStatus.Online)))));

        using var scope = LatticeActiveTenantContext.With(TenantId.Parse("acme"));
        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions.Select(r => r.RegionId), Does.Contain("us"),
            "The caller is already talking to the current region; omitting it would break its own session.");
    }

    // ----- tenant asserted: annotate -----

    [Test]
    public async Task An_advertised_region_carries_the_tenant_standing()
    {
        var catalog = new LatticeApiMcpRegionCatalog(
            Router("eu"),
            Services(new FakeResolver(MapOf(("eu", true, TenantRegionResidencyStatus.Backfilling)))));

        using var scope = LatticeActiveTenantContext.With(TenantId.Parse("acme"));
        var regions = await catalog.ListRegionsAsync();
        var eu = regions.Single(r => r.RegionId == "eu");

        Assert.Multiple(() =>
        {
            Assert.That(eu.TenantScope, Is.Not.Null);
            Assert.That(eu.TenantScope!.TenantId, Is.EqualTo("acme"));
            Assert.That(eu.TenantScope.IsAllowed, Is.True);
            Assert.That(eu.TenantScope.Status, Is.EqualTo(TenantRegionLifecycleStatus.Backfilling));
            Assert.That(eu.TenantScope.IsResident, Is.True);
        });
    }

    [Test]
    public async Task The_current_region_is_annotated_truthfully_when_the_tenant_has_no_standing()
    {
        var catalog = new LatticeApiMcpRegionCatalog(
            Router("eu"),
            Services(new FakeResolver(MapOf(("eu", true, TenantRegionResidencyStatus.Online)))));

        using var scope = LatticeActiveTenantContext.With(TenantId.Parse("acme"));
        var regions = await catalog.ListRegionsAsync();
        var us = regions.Single(r => r.RegionId == "us");

        Assert.Multiple(() =>
        {
            Assert.That(us.TenantScope, Is.Not.Null);
            Assert.That(us.TenantScope!.IsAllowed, Is.False);
            Assert.That(us.TenantScope.IsResident, Is.False);
            Assert.That(us.TenantScope.Status, Is.EqualTo(TenantRegionLifecycleStatus.None),
                "The current region is advertised unconditionally but never flattered.");
        });
    }

    [Test]
    public async Task Every_residency_status_maps_to_its_api_counterpart(
        [Values(
            TenantRegionResidencyStatus.None,
            TenantRegionResidencyStatus.Provisioning,
            TenantRegionResidencyStatus.Backfilling,
            TenantRegionResidencyStatus.Online,
            TenantRegionResidencyStatus.Draining,
            TenantRegionResidencyStatus.Offline,
            TenantRegionResidencyStatus.Removed)]
        TenantRegionResidencyStatus status)
    {
        var expected = status switch
        {
            TenantRegionResidencyStatus.Provisioning => TenantRegionLifecycleStatus.Provisioning,
            TenantRegionResidencyStatus.Backfilling => TenantRegionLifecycleStatus.Backfilling,
            TenantRegionResidencyStatus.Online => TenantRegionLifecycleStatus.Online,
            TenantRegionResidencyStatus.Draining => TenantRegionLifecycleStatus.Draining,
            TenantRegionResidencyStatus.Offline => TenantRegionLifecycleStatus.Offline,
            TenantRegionResidencyStatus.Removed => TenantRegionLifecycleStatus.Removed,
            _ => TenantRegionLifecycleStatus.None,
        };

        // Allowed, so the region survives the filter whatever its status.
        var catalog = new LatticeApiMcpRegionCatalog(
            Router("eu"), Services(new FakeResolver(MapOf(("eu", true, status)))));

        using var scope = LatticeActiveTenantContext.With(TenantId.Parse("acme"));
        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions.Single(r => r.RegionId == "eu").TenantScope!.Status, Is.EqualTo(expected));
    }

    // ----- fail closed -----

    [Test]
    public async Task An_unresolved_verdict_sees_only_the_current_region()
    {
        var catalog = new LatticeApiMcpRegionCatalog(
            Router("eu", "ap"), Services(new FakeResolver(TenantRegionVisibilityMap.Unresolved)));

        using var scope = LatticeActiveTenantContext.With(TenantId.Parse("acme"));
        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions.Select(r => r.RegionId), Is.EqualTo(new[] { "us" }),
            "The load-bearing distinction: an INACTIVE resolver means no tenancy engine exists, so the "
            + "answer stays unscoped. An ACTIVE resolver that cannot answer means the engine exists and "
            + "failed, so the answer fails closed to the current region rather than leaking topology.");
    }

    [Test]
    public async Task An_empty_resolved_verdict_sees_only_the_current_region()
    {
        var catalog = new LatticeApiMcpRegionCatalog(
            Router("eu", "ap"), Services(new FakeResolver(TenantRegionVisibilityMap.Empty)));

        using var scope = LatticeActiveTenantContext.With(TenantId.Parse("acme"));
        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions.Select(r => r.RegionId), Is.EqualTo(new[] { "us" }),
            "A tenant resident nowhere sees only the region it is talking to.");
    }

    // ----- ordering against the identity verifier -----

    [Test]
    public async Task A_tenant_pruned_peer_is_never_probed_for_identity()
    {
        var verifier = new CountingVerifier(RegionIdentityVerdict.Verified);
        var catalog = new LatticeApiMcpRegionCatalog(
            Router("eu", "ap"),
            Services(new FakeResolver(MapOf(("eu", true, TenantRegionResidencyStatus.Online))), verifier));

        using var scope = LatticeActiveTenantContext.With(TenantId.Parse("acme"));
        var regions = await catalog.ListRegionsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(regions.Select(r => r.RegionId), Is.EqualTo(new[] { "us", "eu" }));
            Assert.That(verifier.Probed, Is.EqualTo(new[] { "eu" }),
                "Pruning before the probe spares a round trip to a region the tenant cannot use anyway.");
        });
    }

    [Test]
    public async Task A_visible_peer_that_fails_identity_verification_is_still_omitted()
    {
        var catalog = new LatticeApiMcpRegionCatalog(
            Router("eu"),
            Services(
                new FakeResolver(MapOf(("eu", true, TenantRegionResidencyStatus.Online))),
                new CountingVerifier(RegionIdentityVerdict.Mismatch)));

        using var scope = LatticeActiveTenantContext.With(TenantId.Parse("acme"));
        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions.Select(r => r.RegionId), Is.EqualTo(new[] { "us" }),
            "Tenant scoping narrows the answer; it never widens it past the identity gate.");
    }

    private sealed class FakeResolver(TenantRegionVisibilityMap map, bool isActive = true)
        : ITenantRegionVisibilityResolver
    {
        public int Calls { get; private set; }

        public TenantId? LastTenant { get; private set; }

        public bool IsActive => isActive;

        public ValueTask<TenantRegionVisibilityMap> ResolveAsync(
            TenantId tenant, CancellationToken cancellationToken = default)
        {
            Calls++;
            LastTenant = tenant;
            return ValueTask.FromResult(map);
        }
    }

    private sealed class CountingVerifier(RegionIdentityVerdict verdict) : ILatticeApiMcpRegionIdentityVerifier
    {
        private readonly List<string> _probed = [];

        public IReadOnlyList<string> Probed => _probed;

        public ValueTask<RegionIdentityVerdict> VerifyAsync(
            string regionId, CancellationToken cancellationToken = default)
        {
            _probed.Add(regionId);
            return ValueTask.FromResult(verdict);
        }
    }
}
