using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Api.Abstractions.Tests.Telemetry;

/// <summary>
/// Exercises the tenant-scoping shape that carries a caller's <em>requested</em>
/// visibility alongside the visibility the facade actually pinned. The load-bearing
/// property is that the two can differ and the difference is observable: an
/// unvalidated cross-tenant request is served at active-tenant scope and reports
/// itself as downgraded rather than silently presenting a narrow view as a
/// cluster-wide one.
/// </summary>
[TestFixture]
public sealed class TelemetryTenantScopeTests
{
    [Test]
    public void Default_scope_is_active_tenant_with_no_tenant_pinned()
    {
        var scope = default(TelemetryTenantScope);

        Assert.Multiple(() =>
        {
            Assert.That(scope.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.ActiveTenant));
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(TelemetryTenantVisibility.ActiveTenant));
            Assert.That(scope.TenantId, Is.Null);
            Assert.That(scope.WasDowngraded, Is.False);
            Assert.That(scope.IsCrossTenant, Is.False);
        });
    }

    [Test]
    public void PinnedTo_records_the_server_derived_tenant_for_an_active_tenant_request()
    {
        var scope = TelemetryTenantScope.PinnedTo("acme", TelemetryTenantVisibility.ActiveTenant);

        Assert.Multiple(() =>
        {
            Assert.That(scope.TenantId, Is.EqualTo("acme"));
            Assert.That(scope.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.ActiveTenant));
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(TelemetryTenantVisibility.ActiveTenant));
            Assert.That(scope.WasDowngraded, Is.False);
            Assert.That(scope.IsCrossTenant, Is.False);
        });
    }

    [Test]
    public void PinnedTo_reports_a_downgrade_when_an_unvalidated_cross_tenant_request_fails_closed()
    {
        var scope = TelemetryTenantScope.PinnedTo("acme", TelemetryTenantVisibility.AllTenants);

        Assert.Multiple(() =>
        {
            Assert.That(scope.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.AllTenants));
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(TelemetryTenantVisibility.ActiveTenant),
                "An unvalidated cross-tenant request must degrade to the caller's active tenant.");
            Assert.That(scope.TenantId, Is.EqualTo("acme"));
            Assert.That(scope.WasDowngraded, Is.True);
            Assert.That(scope.IsCrossTenant, Is.False);
        });
    }

    [Test]
    public void PinnedTo_rejects_a_null_tenant_id()
    {
        Assert.That(
            () => TelemetryTenantScope.PinnedTo(null!, TelemetryTenantVisibility.ActiveTenant),
            Throws.ArgumentNullException);
    }

    [Test]
    public void PinnedTo_rejects_an_empty_tenant_id()
    {
        Assert.That(
            () => TelemetryTenantScope.PinnedTo(string.Empty, TelemetryTenantVisibility.ActiveTenant),
            Throws.ArgumentException);
    }

    [Test]
    public void PinnedTo_rejects_a_white_space_tenant_id()
    {
        Assert.That(
            () => TelemetryTenantScope.PinnedTo("   ", TelemetryTenantVisibility.ActiveTenant),
            Throws.ArgumentException);
    }

    [Test]
    public void AcrossAllTenants_pins_no_tenant_and_reports_no_downgrade()
    {
        var scope = TelemetryTenantScope.AcrossAllTenants();

        Assert.Multiple(() =>
        {
            Assert.That(scope.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.AllTenants));
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(TelemetryTenantVisibility.AllTenants));
            Assert.That(scope.TenantId, Is.Null,
                "A validated cross-tenant evaluation is pinned to no single tenant.");
            Assert.That(scope.WasDowngraded, Is.False);
            Assert.That(scope.IsCrossTenant, Is.True);
        });
    }

    [Test]
    public void Equal_scopes_compare_equal_by_value()
    {
        var a = TelemetryTenantScope.PinnedTo("acme", TelemetryTenantVisibility.AllTenants);
        var b = TelemetryTenantScope.PinnedTo("acme", TelemetryTenantVisibility.AllTenants);

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Active_tenant_is_the_fail_closed_zero_value_of_the_visibility_enum()
    {
        Assert.Multiple(() =>
        {
            Assert.That((int)TelemetryTenantVisibility.ActiveTenant, Is.EqualTo(0),
                "The narrow scope must be the default so an unset field never widens a query.");
            Assert.That((int)TelemetryTenantVisibility.AllTenants, Is.EqualTo(1));
        });
    }

    [Test]
    public void A_request_defaults_to_the_narrow_visibility()
    {
        var request = new TelemetryQueryRequest { QueryId = "tree.write.ops" };

        Assert.Multiple(() =>
        {
            Assert.That(request.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.ActiveTenant));
            Assert.That(request.TreeId, Is.Null);
            Assert.That(request.Range, Is.EqualTo(default(TelemetryTimeRange)));
        });
    }

    [Test]
    public void A_request_preserves_the_visibility_it_asks_for()
    {
        var request = new TelemetryQueryRequest
        {
            QueryId = "tree.write.ops",
            RequestedVisibility = TelemetryTenantVisibility.AllTenants,
            TreeId = "t/acme/orders",
        };

        Assert.Multiple(() =>
        {
            Assert.That(request.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.AllTenants));
            Assert.That(request.TreeId, Is.EqualTo("t/acme/orders"));
        });
    }
}
