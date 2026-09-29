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
        ILatticeApiMcpRegionIdentityVerifier? verifier = null,
        ITenantContextResolver? tenantContext = null)
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

        // The tenancy add-on registers the validating resolver alongside the
        // visibility resolver, so a fixture exercising scoped behaviour must model
        // both. Default to one that validates whatever the caller asserted, which
        // is the post-validation state every scoping test below is about; the
        // refusal path gets its own explicit cases.
        services.AddSingleton(tenantContext ?? ValidatingTenantContext.Permissive);

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

            // A catalog that returned nothing at all would satisfy "no region
            // carries a tenant annotation" vacuously, so the disclosure claim
            // needs a region to actually be present to be about anything.
            Assert.That(regions, Is.Not.Empty, "the router advertises a region, so the catalog must return one");
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

        // Same vacuity as the asserted-tenant case above: with no regions the
        // "carries no annotation" claim is satisfied without being tested.
        Assert.That(regions, Is.Not.Empty, "the router advertises a region, so the catalog must return one");
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

    // ----- the assertion itself must validate: refusal fails closed -----

    [Test]
    public async Task An_unvalidated_tenant_assertion_is_refused_and_falls_back_to_the_current_region()
    {
        // The header is caller-controlled. Without validation, asserting any
        // tenant id enumerated that tenant's actionable region set - a routing
        // topology disclosure available to anyone who can reach the head.
        var visibility = new FakeResolver(MapOf(
            ("eu", true, TenantRegionResidencyStatus.Online),
            ("ap", true, TenantRegionResidencyStatus.Online)));
        var catalog = new LatticeApiMcpRegionCatalog(
            Router("eu", "ap"),
            Services(visibility, tenantContext: ValidatingTenantContext.Refusing));

        using var scope = LatticeActiveTenantContext.With(TenantId.Parse("victim"));
        var regions = await catalog.ListRegionsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(regions.Select(r => r.RegionId), Is.EqualTo(new[] { "us" }),
                "A refused assertion must fail closed to the current region, never the full topology.");
            Assert.That(visibility.Calls, Is.Zero,
                "The visibility engine must not be consulted for a tenant the caller never proved, "
                + "which would otherwise make the catalog a tenant-existence oracle.");
        });
    }

    [Test]
    public async Task A_refused_assertion_never_echoes_the_asserted_tenant_back()
    {
        var catalog = new LatticeApiMcpRegionCatalog(
            Router("eu"),
            Services(
                new FakeResolver(MapOf(("eu", true, TenantRegionResidencyStatus.Online))),
                tenantContext: ValidatingTenantContext.Refusing));

        using var scope = LatticeActiveTenantContext.With(TenantId.Parse("victim"));
        var regions = await catalog.ListRegionsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(regions.Select(r => r.TenantScope), Is.All.Null,
                "Annotating a refused call would confirm the assertion was understood, and the "
                + "annotation carries the asserted tenant id straight back to the caller.");
            Assert.That(regions.Select(r => r.RegionId), Is.EqualTo(new[] { "us" }));
        });
    }

    [Test]
    public async Task An_assertion_that_validates_to_a_different_tenant_is_refused()
    {
        // Resolution succeeding is not enough: it must resolve to the tenant that
        // was asserted. A caller authorized for `acme` asserting `victim` must not
        // be handed `victim`'s topology merely because resolution returned a tenant.
        var visibility = new FakeResolver(MapOf(("eu", true, TenantRegionResidencyStatus.Online)));
        var catalog = new LatticeApiMcpRegionCatalog(
            Router("eu", "ap"),
            Services(visibility, tenantContext: ValidatingTenantContext.Resolving(TenantId.Parse("acme"))));

        using var scope = LatticeActiveTenantContext.With(TenantId.Parse("victim"));
        var regions = await catalog.ListRegionsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(regions.Select(r => r.RegionId), Is.EqualTo(new[] { "us" }));
            Assert.That(visibility.Calls, Is.Zero);
        });
    }

    [Test]
    public async Task A_validating_resolver_that_is_not_registered_is_refused()
    {
        // An active visibility resolver with no validating seam beside it is a
        // mis-wired host, not a licence to honour the header. The tenancy add-on
        // registers the two together, so this shape can only arise by hand.
        var services = new ServiceCollection();
        services.AddSingleton<ITenantRegionVisibilityResolver>(
            new FakeResolver(MapOf(("eu", true, TenantRegionResidencyStatus.Online))));
        var catalog = new LatticeApiMcpRegionCatalog(
            Router("eu", "ap"), services.BuildServiceProvider());

        using var scope = LatticeActiveTenantContext.With(TenantId.Parse("victim"));
        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions.Select(r => r.RegionId), Is.EqualTo(new[] { "us" }),
            "A missing validating resolver must fail closed, not open.");
    }

    [Test]
    public async Task A_validated_assertion_resolved_asynchronously_is_still_honoured()
    {
        // The warm synchronous path is an optimisation, not the contract: a
        // membership cache miss must reach the same verdict, or the gate would
        // reject legitimate callers under cold cache.
        var catalog = new LatticeApiMcpRegionCatalog(
            Router("eu"),
            Services(
                new FakeResolver(MapOf(("eu", true, TenantRegionResidencyStatus.Online))),
                tenantContext: ValidatingTenantContext.PermissiveAsyncOnly));

        using var scope = LatticeActiveTenantContext.With(TenantId.Parse("acme"));
        var regions = await catalog.ListRegionsAsync();

        Assert.That(regions.Select(r => r.RegionId), Is.EqualTo(new[] { "us", "eu" }),
            "An asynchronous resolution of the same tenant must scope exactly as the warm path does.");
    }

    /// <summary>
    /// A stand-in for the tenancy add-on's validating resolver. The tenancy
    /// package validates the ambient assertion against the caller's membership and
    /// returns the uninitialised "no tenant" value when the caller may not act as
    /// it; this models the three outcomes the catalog must distinguish - validated,
    /// refused, and validated-as-someone-else - and whether resolution is warm.
    /// </summary>
    private sealed class ValidatingTenantContext(TenantId? resolved, bool useAmbient, bool warm)
        : ITenantContextResolver
    {
        /// <summary>Validates whatever the caller asserted, synchronously.</summary>
        public static ValidatingTenantContext Permissive { get; } = new(null, useAmbient: true, warm: true);

        /// <summary>Validates the assertion, but only via the async fallback.</summary>
        public static ValidatingTenantContext PermissiveAsyncOnly { get; }
            = new(null, useAmbient: true, warm: false);

        /// <summary>Refuses every assertion, as for a caller outside the tenant.</summary>
        public static ValidatingTenantContext Refusing { get; } = new(default(TenantId), useAmbient: false, warm: true);

        /// <summary>Validates to a fixed tenant regardless of what was asserted.</summary>
        public static ValidatingTenantContext Resolving(TenantId tenant)
            => new(tenant, useAmbient: false, warm: true);

        public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default)
            => ValueTask.FromResult(Resolve());

        public bool TryResolveCurrent(out TenantId tenant)
        {
            if (!warm)
            {
                tenant = default;
                return false;
            }

            tenant = Resolve();
            return true;
        }

        private TenantId Resolve()
            => useAmbient ? LatticeActiveTenantContext.Current ?? default : resolved ?? default;
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
