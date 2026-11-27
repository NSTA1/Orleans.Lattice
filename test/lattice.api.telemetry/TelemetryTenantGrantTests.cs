using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Tenant-scoped telemetry (issue #5582): a <see cref="LatticeOperation.Telemetry"/>
/// grant on one tenant's capability scope
/// (<see cref="LatticeScope.TenantCapability(TenantId)"/>) admits its holder to that
/// tenant's series only. Covers the authorizer's tenant check, the resolver's
/// composed decision, and what reaches the backend - including the cross-tenant
/// leak cases in both directions (ada on acme, gus on globex).
/// </summary>
[TestFixture]
public sealed class TelemetryTenantGrantTests
{
    private const string ReadRate = "tree.read.operation_rate";

    private static TelemetryFacadeHarness Caller(string activeTenant, ScopedAccessGate gate) =>
        new TelemetryFacadeHarness().ForTenant(activeTenant).WithGate(gate);

    private static TelemetryTenantScopeResolver Resolver(ITenantContextResolver tenants, ILatticeAccessGate? gate) =>
        new(tenants, new TelemetryAccessAuthorizer(gate));

    // ---- the authorizer's tenant check -----------------------------------

    [Test]
    public async Task AuthorizeTenantTelemetryAsync_admits_a_grant_on_the_tenants_capability_scope()
    {
        var gate = new ScopedAccessGate().AllowingTenantTelemetry("acme");

        await new TelemetryAccessAuthorizer(gate).AuthorizeTenantTelemetryAsync(TenantId.Parse("acme"));

        Assert.That(gate.Requests, Is.EqualTo(new[] { ("_lattice_tenant_admin_acme", LatticeOperation.Telemetry) }),
            "the check must ask the gate for Telemetry on exactly the tenant's capability id");
    }

    [Test]
    public void AuthorizeTenantTelemetryAsync_refuses_a_grant_for_another_tenant()
    {
        var gate = new ScopedAccessGate().AllowingTenantTelemetry("acme");

        Assert.That(
            async () => await new TelemetryAccessAuthorizer(gate).AuthorizeTenantTelemetryAsync(TenantId.Parse("globex")),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void AuthorizeTenantTelemetryAsync_refuses_the_default_and_the_uninitialised_tenant()
    {
        var authorizer = new TelemetryAccessAuthorizer(new ScopedAccessGate());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await authorizer.AuthorizeTenantTelemetryAsync(TenantId.Default),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(async () => await authorizer.AuthorizeTenantTelemetryAsync(default),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
        });
    }

    [Test]
    public void AuthorizeTenantTelemetryAsync_refuses_a_key_filtered_allow()
    {
        var gate = new ScopedAccessGate().For(
            ScopedAccessGate.CapabilityId("acme"), LatticeOperation.Telemetry, LatticeAccessDecision.Filtered(static _ => true));

        Assert.That(
            async () => await new TelemetryAccessAuthorizer(gate).AuthorizeTenantTelemetryAsync(TenantId.Parse("acme")),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void AuthorizeTenantTelemetryAsync_is_not_conferred_by_admin_on_the_same_scope()
    {
        var gate = new ScopedAccessGate().For(
            ScopedAccessGate.CapabilityId("acme"), LatticeOperation.Admin, LatticeAccessDecision.Allow());

        Assert.That(
            async () => await new TelemetryAccessAuthorizer(gate).AuthorizeTenantTelemetryAsync(TenantId.Parse("acme")),
            Throws.TypeOf<LatticeAuthorizationDeniedException>(),
            "the delegated tenant admin capability must not confer the tenant's telemetry");
    }

    [Test]
    public async Task AuthorizeTenantTelemetryAsync_admits_system_origin_without_consulting_the_gate()
    {
        var gate = new ScopedAccessGate();

        using (LatticeSystemOrigin.Enter())
        {
            await new TelemetryAccessAuthorizer(gate).AuthorizeTenantTelemetryAsync(TenantId.Parse("acme"));
        }

        Assert.That(gate.Requests, Is.Empty);
    }

    // ---- the resolver's composed decision --------------------------------

    [Test]
    public async Task AuthorizeAsync_prefers_the_cluster_wide_grant()
    {
        var gate = new ScopedAccessGate()
            .For(LatticeScope.ClusterWideTreeId, LatticeOperation.Telemetry, LatticeAccessDecision.Allow())
            .AllowingTenantTelemetry("acme");

        var grant = await Resolver(new StubTenantContextResolver(TenantId.Parse("acme")), gate).AuthorizeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(grant.IsClusterWide, Is.True);
            Assert.That(grant.TenantId, Is.Null);
            Assert.That(gate.Requests, Has.Count.EqualTo(1), "a cluster-wide grant ends the decision");
        });
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task AuthorizeAsync_confines_a_tenant_grant_to_the_validated_active_tenant(bool resolvesSynchronously)
    {
        var gate = new ScopedAccessGate().AllowingTenantTelemetry("acme");

        var grant = await Resolver(new StubTenantContextResolver(TenantId.Parse("acme"), resolvesSynchronously), gate)
            .AuthorizeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(grant.IsClusterWide, Is.False);
            Assert.That(grant.TenantId, Is.EqualTo("acme"));
        });
    }

    [Test]
    public void AuthorizeAsync_refuses_a_tenant_grant_while_another_tenant_is_active()
    {
        // ada holds acme's grant, but her validated active tenant is globex: the grant
        // is decided on the resolved tenant, never on the tenant she is granted for.
        var gate = new ScopedAccessGate().AllowingTenantTelemetry("acme");

        Assert.That(
            async () => await Resolver(new StubTenantContextResolver(TenantId.Parse("globex")), gate).AuthorizeAsync(),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void AuthorizeAsync_refuses_a_caller_on_the_default_tenant_without_a_cluster_grant()
    {
        var gate = new ScopedAccessGate().AllowingTenantTelemetry("acme");

        Assert.That(
            async () => await Resolver(new StubTenantContextResolver(TenantId.Default), gate).AuthorizeAsync(),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void AuthorizeAsync_refuses_an_unattributable_caller_as_an_authorization_denial()
    {
        var gate = new ScopedAccessGate().AllowingTenantTelemetry("acme");

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await Resolver(new StubTenantContextResolver(default), gate).AuthorizeAsync(),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(
                async () => await Resolver(new RefusingTenantContextResolver(), gate).AuthorizeAsync(),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "a refused tenant assertion must not reveal itself through a different exception");
        });
    }

    [Test]
    public async Task ResolveAsync_pins_a_tenant_grant_whatever_was_requested()
    {
        var resolver = Resolver(new StubTenantContextResolver(TenantId.Parse("acme")), new ScopedAccessGate());
        var grant = TelemetryAccessGrant.ForTenant("acme");

        var all = await resolver.ResolveAsync(grant, TelemetryTenantVisibility.AllTenants);
        var other = await resolver.ResolveAsync(grant, TelemetryTenantVisibility.SingleTenant, "globex");
        var own = await resolver.ResolveAsync(grant, TelemetryTenantVisibility.ActiveTenant);

        Assert.Multiple(() =>
        {
            Assert.That(all.TenantId, Is.EqualTo("acme"));
            Assert.That(all.WasDowngraded, Is.True);
            Assert.That(other.TenantId, Is.EqualTo("acme"));
            Assert.That(other.WasDowngraded, Is.True);
            Assert.That(own.TenantId, Is.EqualTo("acme"));
            Assert.That(own.WasDowngraded, Is.False);
        });
    }

    [Test]
    public void TelemetryAccessGrant_ForTenant_refuses_an_empty_tenant() =>
        Assert.That(() => TelemetryAccessGrant.ForTenant(string.Empty), Throws.ArgumentException);

    [Test]
    public void TelemetryAccessGrant_ClusterWide_names_no_tenant()
    {
        var grant = TelemetryAccessGrant.ClusterWide;

        Assert.Multiple(() =>
        {
            Assert.That(grant.IsClusterWide, Is.True);
            Assert.That(grant.TenantId, Is.Null);
        });
    }

    // ---- what reaches the backend ----------------------------------------

    [Test]
    public async Task A_tenant_grantee_queries_only_its_own_tenants_series()
    {
        var harness = Caller("acme", new ScopedAccessGate().AllowingTenantTelemetry("acme"));

        var response = await harness.Build().QueryAsync(TelemetryFacadeHarness.RangeRequest(ReadRate));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Backend.SingleQuery, Is.EqualTo(
                """sum by (tree) (rate(orleans_lattice_shard_reads_total{tenant="acme",}[5m]))"""));
            Assert.That(response.Scope.TenantId, Is.EqualTo("acme"));
            Assert.That(response.Scope.WasDowngraded, Is.False);
        });
    }

    [Test]
    public async Task A_tenant_grantee_asking_for_every_tenant_is_answered_for_its_own_without_operator_validation()
    {
        var gate = new ScopedAccessGate().AllowingTenantTelemetry("acme");
        var harness = Caller("acme", gate);
        var request = TelemetryFacadeHarness.RangeRequest(ReadRate) with
        {
            RequestedVisibility = TelemetryTenantVisibility.AllTenants,
        };

        var response = await harness.Build().QueryAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Backend.SingleQuery, Does.Contain("""tenant="acme","""));
            Assert.That(response.Scope.WasDowngraded, Is.True);
            Assert.That(response.Scope.IsCrossTenant, Is.False);
            Assert.That(gate.Requests.Select(request => request.TreeId), Has.No.Member(LatticeAuthReservedTrees.PolicyTreeId),
                "a tenant-confined grant never consults platform-operator validation");
        });
    }

    [Test]
    public async Task A_tenant_grantee_naming_another_tenant_is_answered_for_its_own()
    {
        var harness = Caller("acme", new ScopedAccessGate().AllowingTenantTelemetry("acme"));
        var request = TelemetryFacadeHarness.RangeRequest(ReadRate) with
        {
            RequestedVisibility = TelemetryTenantVisibility.SingleTenant,
            RequestedTenantId = "globex",
        };

        var response = await harness.Build().QueryAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Backend.SingleQuery, Does.Contain("""tenant="acme","""));
            Assert.That(harness.Backend.SingleQuery, Does.Not.Contain("globex"));
            Assert.That(response.Scope.TenantId, Is.EqualTo("acme"));
        });
    }

    [Test]
    public async Task A_tenant_grantee_naming_another_tenants_tree_gets_the_empty_intersection()
    {
        var harness = Caller("acme", new ScopedAccessGate().AllowingTenantTelemetry("acme"));
        var request = TelemetryFacadeHarness.RangeRequest(ReadRate) with { TreeId = "t/globex/orders" };

        await harness.Build().QueryAsync(request);

        Assert.That(harness.Backend.SingleQuery, Does.Contain("""tenant="acme",tree="t/globex/orders","""),
            "the tree filter narrows beside the pinned tenant matcher, so it can never widen to globex");
    }

    [Test]
    public async Task Gus_on_globex_sees_only_globex_and_never_acme()
    {
        var harness = Caller("globex", new ScopedAccessGate().AllowingTenantTelemetry("globex"));

        await harness.Build().QueryAsync(TelemetryFacadeHarness.RangeRequest(ReadRate));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Backend.SingleQuery, Does.Contain("""tenant="globex","""));
            Assert.That(harness.Backend.SingleQuery, Does.Not.Contain("acme"));
        });
    }

    [Test]
    public void A_holder_of_acmes_grant_on_globex_is_refused_and_reaches_no_backend()
    {
        var harness = Caller("globex", new ScopedAccessGate().AllowingTenantTelemetry("acme"));

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await harness.Build().QueryAsync(TelemetryFacadeHarness.RangeRequest(ReadRate)),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(harness.Backend.Queries, Is.Empty);
        });
    }

    [Test]
    public void A_holder_of_globexs_grant_on_acme_is_refused_and_reaches_no_backend()
    {
        var harness = Caller("acme", new ScopedAccessGate().AllowingTenantTelemetry("globex"));

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await harness.Build().QueryAsync(TelemetryFacadeHarness.RangeRequest(ReadRate)),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(harness.Backend.Queries, Is.Empty);
        });
    }

    [Test]
    public async Task GetCatalogAsync_serves_a_tenant_grantee_and_hides_everything_from_a_foreign_one()
    {
        var granted = await Caller("acme", new ScopedAccessGate().AllowingTenantTelemetry("acme")).Build().GetCatalogAsync();
        var foreign = await Caller("globex", new ScopedAccessGate().AllowingTenantTelemetry("acme")).Build().GetCatalogAsync();

        Assert.Multiple(() =>
        {
            Assert.That(granted.Count, Is.GreaterThan(0));
            Assert.That(foreign.Count, Is.EqualTo(0));
        });
    }

    private sealed class RefusingTenantContextResolver : ITenantContextResolver
    {
        public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            throw new LatticeTenantAccessDeniedException();

        public bool TryResolveCurrent(out TenantId resolved)
        {
            resolved = default;
            return false;
        }
    }
}
