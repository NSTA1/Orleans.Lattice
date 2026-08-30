using Orleans.Lattice.Explorer.Plugins.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// The scope caption: the one surface between a fail-closed degrade and a chart
/// that silently mislabels one tenant's data as the cluster's.
/// </summary>
[TestFixture]
public sealed class TelemetryScopeCaptionsTests
{
    [Test]
    public void An_honoured_active_tenant_scope_names_the_tenant_the_facade_pinned()
    {
        var caption = TelemetryScopeCaptions.For(ExplorerTelemetrySample.ActiveScope());

        Assert.Multiple(() =>
        {
            Assert.That(caption.Severity, Is.EqualTo(TelemetryScopeSeverity.Informational));
            Assert.That(caption.IsDegraded, Is.False);
            Assert.That(caption.Text, Does.Contain(ExplorerTelemetrySample.TenantId));
        });
    }

    [Test]
    public void An_honoured_cross_tenant_scope_says_every_tenant()
    {
        var caption = TelemetryScopeCaptions.For(ExplorerTelemetrySample.CrossTenantScope());

        Assert.Multiple(() =>
        {
            Assert.That(caption.IsDegraded, Is.False);
            Assert.That(caption.Text, Does.Contain("every tenant"));
        });
    }

    [Test]
    public void A_refused_cross_tenant_request_is_reported_as_a_degrade_and_says_the_figures_are_one_tenants()
    {
        // The load-bearing case. The facade fails closed rather than refusing,
        // so an operator who asked for the cluster is served one tenant; a
        // caption that did not say so would label one tenant's traffic with the
        // question the operator actually asked.
        var caption = TelemetryScopeCaptions.For(ExplorerTelemetrySample.DowngradedScope());

        Assert.Multiple(() =>
        {
            Assert.That(caption.Severity, Is.EqualTo(TelemetryScopeSeverity.Degraded));
            Assert.That(caption.IsDegraded, Is.True);
            Assert.That(caption.Text, Does.Contain("every tenant"));
            Assert.That(caption.Text, Does.Contain(ExplorerTelemetrySample.TenantId));
            Assert.That(caption.Text, Does.Contain("not the cluster's"));
        });
    }

    [Test]
    public void A_refused_single_tenant_request_is_not_described_as_a_narrowing_of_a_wider_view()
    {
        // A refused SingleTenant means "you may not read that tenant", not "your
        // request was too broad". Collapsing the two would tell an operator the
        // wrong thing about which entitlement they lack.
        var scope = new ExplorerTelemetryScope(
            ExplorerTelemetryVisibility.SingleTenant,
            ExplorerTelemetryVisibility.ActiveTenant,
            ExplorerTelemetrySample.TenantId);

        var caption = TelemetryScopeCaptions.For(scope);

        Assert.Multiple(() =>
        {
            Assert.That(caption.IsDegraded, Is.True);
            Assert.That(caption.Text, Does.Contain("another tenant"));
            Assert.That(caption.Text, Does.Contain("not the tenant you asked for"));
        });
    }

    [Test]
    public void A_degrade_with_no_tenant_named_still_reports_a_degrade()
    {
        var scope = new ExplorerTelemetryScope(
            ExplorerTelemetryVisibility.AllTenants,
            ExplorerTelemetryVisibility.ActiveTenant,
            TenantId: null);

        Assert.That(TelemetryScopeCaptions.For(scope).IsDegraded, Is.True);
    }

    [Test]
    public void With_tenancy_absent_the_caption_drops_the_tenant_wording_entirely()
    {
        var caption = TelemetryScopeCaptions.For(
            ExplorerTelemetrySample.ActiveScope(tenantId: null),
            tenancyEnabled: false);

        Assert.Multiple(() =>
        {
            Assert.That(caption.IsDegraded, Is.False);
            Assert.That(caption.Text, Does.Contain("one tenant"));
            Assert.That(
                caption.Text,
                Does.Not.Contain("active tenant"),
                "a deployment with no tenancy add-on has no active tenant to speak of");
        });
    }

    [Test]
    public void With_tenancy_absent_a_degrade_is_still_reported()
    {
        // A facade that narrowed a request has said something the caller needs
        // to know however the head is configured.
        var caption = TelemetryScopeCaptions.For(
            ExplorerTelemetrySample.DowngradedScope(),
            tenancyEnabled: false);

        Assert.That(caption.IsDegraded, Is.True);
    }

    [Test]
    public void The_fail_closed_none_scope_captions_without_a_degrade() =>
        // Nothing has been asked for yet, so nothing has been refused.
        Assert.That(TelemetryScopeCaptions.For(ExplorerTelemetryScope.None).IsDegraded, Is.False);

    [Test]
    public void Every_caption_carries_text() =>
        Assert.Multiple(() =>
        {
            foreach (var tenancy in new[] { true, false })
            {
                foreach (var scope in new[]
                {
                    ExplorerTelemetryScope.None,
                    ExplorerTelemetrySample.ActiveScope(),
                    ExplorerTelemetrySample.ActiveScope(tenantId: null),
                    ExplorerTelemetrySample.CrossTenantScope(),
                    ExplorerTelemetrySample.DowngradedScope(),
                    ExplorerTelemetrySample.DowngradedScope(tenantId: null),
                })
                {
                    Assert.That(
                        TelemetryScopeCaptions.For(scope, tenancy).Text,
                        Is.Not.Null.And.Not.Empty,
                        $"scope {scope} at tenancy={tenancy}");
                }
            }
        });

    [Test]
    public void The_badge_names_the_effective_tenant_and_never_the_requested_one() =>
        Assert.Multiple(() =>
        {
            Assert.That(
                TelemetryScopeCaptions.BadgeFor(ExplorerTelemetrySample.DowngradedScope(), tenancyEnabled: true),
                Is.EqualTo(ExplorerTelemetrySample.TenantId),
                "a refused cross-tenant request must not badge as 'all tenants'");
            Assert.That(
                TelemetryScopeCaptions.BadgeFor(ExplorerTelemetrySample.CrossTenantScope(), tenancyEnabled: true),
                Is.EqualTo("all tenants"));
            Assert.That(
                TelemetryScopeCaptions.BadgeFor(
                    ExplorerTelemetrySample.ActiveScope(tenantId: null),
                    tenancyEnabled: true),
                Is.EqualTo("active tenant"));
            Assert.That(
                TelemetryScopeCaptions.BadgeFor(ExplorerTelemetrySample.ActiveScope(), tenancyEnabled: false),
                Is.EqualTo("all data"),
                "with no tenancy add-on there is no tenant to badge");
        });
}
