namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Covers the server-side tenant-scope derivation: the effective tenant comes from
/// the authenticated caller, a widening request is honoured only after
/// platform-operator validation, and an unvalidated one degrades rather than
/// throwing.
/// </summary>
[TestFixture]
public sealed class TelemetryTenantScopeResolverTests
{
    private static TelemetryTenantScopeResolver Resolver(
        string tenantId = "acme",
        bool isOperator = false,
        bool resolvesSynchronously = true) =>
        new(
            new StubTenantContextResolver(TenantId.Parse(tenantId), resolvesSynchronously),
            new TelemetryAccessAuthorizer(
                isOperator ? StubAccessGate.PlatformOperator() : StubAccessGate.TelemetryOnly()));

    [Test]
    public async Task ResolveAsync_pins_the_callers_own_tenant_by_default()
    {
        var scope = await Resolver().ResolveAsync(TelemetryTenantVisibility.ActiveTenant);

        Assert.Multiple(() =>
        {
            Assert.That(scope.TenantId, Is.EqualTo("acme"));
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(TelemetryTenantVisibility.ActiveTenant));
            Assert.That(scope.WasDowngraded, Is.False);
            Assert.That(scope.IsCrossTenant, Is.False);
        });
    }

    [Test]
    public async Task ResolveAsync_ignores_a_requested_tenant_id_at_active_tenant_visibility()
    {
        var scope = await Resolver().ResolveAsync(
            TelemetryTenantVisibility.ActiveTenant, requestedTenantId: "victim");

        Assert.That(scope.TenantId, Is.EqualTo("acme"),
            "The effective tenant is derived from the caller; a request field can never set it.");
    }

    [Test]
    public async Task ResolveAsync_derives_the_tenant_through_the_asynchronous_path_too()
    {
        var resolver = new StubTenantContextResolver(TenantId.Parse("acme"), resolvesSynchronously: false);
        var scope = await new TelemetryTenantScopeResolver(
                resolver, new TelemetryAccessAuthorizer(StubAccessGate.TelemetryOnly()))
            .ResolveAsync(TelemetryTenantVisibility.ActiveTenant);

        Assert.Multiple(() =>
        {
            Assert.That(scope.TenantId, Is.EqualTo("acme"));
            Assert.That(resolver.AsyncResolutions, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task ResolveAsync_resolves_the_default_tenant_when_tenancy_is_absent()
    {
        var scope = await new TelemetryTenantScopeResolver(
                NullTelemetryTenantContext.Instance,
                new TelemetryAccessAuthorizer(StubAccessGate.TelemetryOnly()))
            .ResolveAsync(TelemetryTenantVisibility.ActiveTenant);

        Assert.Multiple(() =>
        {
            Assert.That(scope.TenantId, Is.EqualTo(LatticeTenantLabel.DefaultTenant));
            Assert.That(scope.TenantId, Is.Not.EqualTo(LatticeTenantLabel.PlatformTenant),
                "The default tenant is a real, queryable tenant and is never the platform sentinel.");
        });
    }

    [Test]
    public void ResolveAsync_refuses_a_caller_that_cannot_be_attributed_to_a_tenant()
    {
        var resolver = new TelemetryTenantScopeResolver(
            new StubTenantContextResolver(default),
            new TelemetryAccessAuthorizer(StubAccessGate.TelemetryOnly()));

        Assert.That(
            async () => await resolver.ResolveAsync(TelemetryTenantVisibility.ActiveTenant),
            Throws.TypeOf<LatticeTenantAccessDeniedException>(),
            "A request that cannot be attributed to a tenant is denied, not silently defaulted.");
    }

    // -----------------------------------------------------------------
    // AllTenants
    // -----------------------------------------------------------------

    [Test]
    public async Task ResolveAsync_degrades_an_unvalidated_all_tenants_request()
    {
        var scope = await Resolver().ResolveAsync(TelemetryTenantVisibility.AllTenants);

        Assert.Multiple(() =>
        {
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(TelemetryTenantVisibility.ActiveTenant));
            Assert.That(scope.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.AllTenants));
            Assert.That(scope.TenantId, Is.EqualTo("acme"));
            Assert.That(scope.WasDowngraded, Is.True);
            Assert.That(scope.IsCrossTenant, Is.False);
        });
    }

    [Test]
    public async Task ResolveAsync_honours_a_validated_all_tenants_request()
    {
        var scope = await Resolver(isOperator: true).ResolveAsync(TelemetryTenantVisibility.AllTenants);

        Assert.Multiple(() =>
        {
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(TelemetryTenantVisibility.AllTenants));
            Assert.That(scope.TenantId, Is.Null);
            Assert.That(scope.WasDowngraded, Is.False);
            Assert.That(scope.IsCrossTenant, Is.True);
        });
    }

    // -----------------------------------------------------------------
    // SingleTenant
    // -----------------------------------------------------------------

    [Test]
    public async Task ResolveAsync_degrades_an_unvalidated_single_tenant_request()
    {
        var scope = await Resolver().ResolveAsync(
            TelemetryTenantVisibility.SingleTenant, requestedTenantId: "victim");

        Assert.Multiple(() =>
        {
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(TelemetryTenantVisibility.ActiveTenant));
            Assert.That(scope.TenantId, Is.EqualTo("acme"),
                "The requested tenant id is ignored in full for an unvalidated caller.");
            Assert.That(scope.WasDowngraded, Is.True);
        });
    }

    [Test]
    public async Task ResolveAsync_honours_a_validated_single_tenant_request_without_downgrading_it()
    {
        var scope = await Resolver(isOperator: true).ResolveAsync(
            TelemetryTenantVisibility.SingleTenant, requestedTenantId: "other");

        Assert.Multiple(() =>
        {
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(TelemetryTenantVisibility.SingleTenant));
            Assert.That(scope.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.SingleTenant));
            Assert.That(scope.TenantId, Is.EqualTo("other"));
            Assert.That(scope.WasDowngraded, Is.False,
                "An honoured operator view reported as downgraded would be a real bug: it is a "
                + "genuine single-tenant scope, not a refusal.");
            Assert.That(scope.IsCrossTenant, Is.False,
                "A single-tenant view is scoped to one tenant even when that tenant is not the "
                + "caller's own.");
        });
    }

    [Test]
    public async Task ResolveAsync_lets_an_operator_pin_the_platform_sentinel()
    {
        var scope = await Resolver(isOperator: true).ResolveAsync(
            TelemetryTenantVisibility.SingleTenant,
            requestedTenantId: LatticeTenantLabel.PlatformTenant);

        Assert.Multiple(() =>
        {
            Assert.That(scope.TenantId, Is.EqualTo(LatticeTenantLabel.PlatformTenant));
            Assert.That(scope.WasDowngraded, Is.False);
        });
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    [TestCase("Not A Tenant")]
    [TestCase("UPPER")]
    [TestCase("has/slash")]
    [TestCase("-leading-hyphen")]
    [TestCase("""quote"injection""")]
    public async Task ResolveAsync_degrades_an_operator_that_named_an_unusable_tenant(string? requested)
    {
        var scope = await Resolver(isOperator: true).ResolveAsync(
            TelemetryTenantVisibility.SingleTenant, requested);

        Assert.Multiple(() =>
        {
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(TelemetryTenantVisibility.ActiveTenant));
            Assert.That(scope.TenantId, Is.EqualTo("acme"));
            Assert.That(scope.WasDowngraded, Is.True,
                "An unvalidatable tenant id is refused rather than embedded in a query.");
        });
    }

    [Test]
    public async Task ResolveAsync_degrades_a_visibility_value_outside_the_contract()
    {
        var scope = await Resolver(isOperator: true).ResolveAsync((TelemetryTenantVisibility)99);

        Assert.Multiple(() =>
        {
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(TelemetryTenantVisibility.ActiveTenant));
            Assert.That(scope.TenantId, Is.EqualTo("acme"));
        });
    }

    [Test]
    public void The_resolver_rejects_null_dependencies()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new TelemetryTenantScopeResolver(null!, new TelemetryAccessAuthorizer()),
                Throws.ArgumentNullException);
            Assert.That(
                () => new TelemetryTenantScopeResolver(NullTelemetryTenantContext.Instance, null!),
                Throws.ArgumentNullException);
        });
    }
}
