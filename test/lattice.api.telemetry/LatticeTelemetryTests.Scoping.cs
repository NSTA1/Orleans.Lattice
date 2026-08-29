namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// The tenant-isolation half of the facade's behaviour: what the tenant matcher
/// actually looks like in the text that reaches the backend, that platform series
/// can never leak into a tenant's view, and that the three visibility modes behave
/// exactly as the contract declares - including both degrade paths.
/// </summary>
public sealed partial class LatticeTelemetryTests
{
    [Test]
    public async Task QueryAsync_scopes_a_tenant_caller_to_its_own_tenant_matcher()
    {
        var harness = new TelemetryFacadeHarness().ForTenant("acme");

        await harness.Build().QueryAsync(TelemetryFacadeHarness.RangeRequest(ReadRate));

        Assert.That(harness.Backend.SingleQuery, Is.EqualTo(
            """sum by (tree) (rate(orleans_lattice_shard_reads_total{tenant="acme",}[5m]))"""));
    }

    [Test]
    public async Task QueryAsync_scopes_on_the_derived_tenant_label_and_never_on_a_tree_regex()
    {
        var harness = new TelemetryFacadeHarness().ForTenant("acme");

        await harness.Build().QueryAsync(TelemetryFacadeHarness.RangeRequest(ReadRate));

        var query = harness.Backend.SingleQuery;

        Assert.Multiple(() =>
        {
            Assert.That(query, Does.Contain("""tenant="acme","""));
            Assert.That(query, Does.Not.Contain("=~"),
                "A tree regex is the hazard the derived tenant label exists to avoid: the default "
                + "tenant's matcher would have to be tree!~\"^t/.*\", which also matches the "
                + "_lattice_ and sys- platform namespaces.");
            Assert.That(query, Does.Not.Contain("!~"));
            Assert.That(query, Does.Not.Contain("^t/"));
        });
    }

    [Test]
    public async Task A_tenant_scoped_query_can_never_match_a_platform_series()
    {
        // The platform sentinel is a distinct label value, and an exact matcher on a
        // tenant id cannot equal it, so platform-owned series are structurally outside
        // every tenant-scoped query.
        foreach (var tenantId in new[] { "acme", LatticeTenantLabel.DefaultTenant })
        {
            var harness = new TelemetryFacadeHarness().ForTenant(tenantId);
            await harness.Build().QueryAsync(TelemetryFacadeHarness.RangeRequest(ReadRate));

            Assert.Multiple(() =>
            {
                Assert.That(harness.Backend.SingleQuery, Does.Contain($$"""tenant="{{tenantId}}","""));
                Assert.That(
                    harness.Backend.SingleQuery,
                    Does.Not.Contain(LatticeTenantLabel.PlatformTenant),
                    $"A '{tenantId}' scope must never admit the platform sentinel.");
            });
        }
    }

    [Test]
    public async Task The_default_tenant_scope_excludes_platform_series_on_a_tenancy_off_cluster()
    {
        var harness = new TelemetryFacadeHarness().WithTenantResolver(NullTelemetryTenantContext.Instance);

        await harness.Build().QueryAsync(TelemetryFacadeHarness.RangeRequest(ReadRate));

        Assert.That(harness.Backend.SingleQuery, Is.EqualTo(
            """sum by (tree) (rate(orleans_lattice_shard_reads_total{tenant="default",}[5m]))"""),
            "The bare legacy ids a tenancy-off cluster uses all derive the default tenant, and the "
            + "_lattice_ / sys- namespaces derive the platform sentinel, so one exact matcher "
            + "separates them.");
    }

    [Test]
    public async Task An_unvalidated_all_tenants_request_degrades_rather_than_throwing()
    {
        var harness = new TelemetryFacadeHarness().ForTenant("acme");
        var request = TelemetryFacadeHarness.RangeRequest(ReadRate) with
        {
            RequestedVisibility = TelemetryTenantVisibility.AllTenants,
        };

        var response = await harness.Build().QueryAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(response.Scope.WasDowngraded, Is.True);
            Assert.That(response.Scope.EffectiveVisibility,
                Is.EqualTo(TelemetryTenantVisibility.ActiveTenant));
            Assert.That(response.Scope.TenantId, Is.EqualTo("acme"));
            Assert.That(harness.Backend.SingleQuery, Does.Contain("""tenant="acme","""),
                "The degraded query must still carry the caller's own tenant matcher.");
        });
    }

    [Test]
    public async Task An_unvalidated_single_tenant_request_degrades_and_ignores_the_named_tenant()
    {
        var harness = new TelemetryFacadeHarness().ForTenant("acme");
        var request = TelemetryFacadeHarness.RangeRequest(ReadRate) with
        {
            RequestedVisibility = TelemetryTenantVisibility.SingleTenant,
            RequestedTenantId = "victim",
        };

        var response = await harness.Build().QueryAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(response.Scope.WasDowngraded, Is.True);
            Assert.That(response.Scope.TenantId, Is.EqualTo("acme"));
            Assert.That(harness.Backend.SingleQuery, Does.Not.Contain("victim"),
                "A caller must not be able to read another tenant's series by naming it.");
        });
    }

    [Test]
    public async Task A_validated_all_tenants_request_is_honoured_and_pins_no_tenant()
    {
        var harness = new TelemetryFacadeHarness().ForTenant("acme").AsPlatformOperator();
        var request = TelemetryFacadeHarness.RangeRequest(ReadRate) with
        {
            RequestedVisibility = TelemetryTenantVisibility.AllTenants,
        };

        var response = await harness.Build().QueryAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(response.Scope.IsCrossTenant, Is.True);
            Assert.That(response.Scope.WasDowngraded, Is.False);
            Assert.That(response.Scope.TenantId, Is.Null);
            Assert.That(harness.Backend.SingleQuery, Is.EqualTo(
                """sum by (tree) (rate(orleans_lattice_shard_reads_total{}[5m]))"""));
        });
    }

    [Test]
    public async Task A_validated_single_tenant_request_evaluates_at_the_named_tenant()
    {
        var harness = new TelemetryFacadeHarness().ForTenant("acme").AsPlatformOperator();
        var request = TelemetryFacadeHarness.RangeRequest(ReadRate) with
        {
            RequestedVisibility = TelemetryTenantVisibility.SingleTenant,
            RequestedTenantId = "other",
        };

        var response = await harness.Build().QueryAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(response.Scope.EffectiveVisibility,
                Is.EqualTo(TelemetryTenantVisibility.SingleTenant));
            Assert.That(response.Scope.WasDowngraded, Is.False);
            Assert.That(response.Scope.TenantId, Is.EqualTo("other"));
            Assert.That(harness.Backend.SingleQuery, Does.Contain("""tenant="other","""));
        });
    }

    [Test]
    public async Task A_tree_filter_narrows_within_the_tenant_scope()
    {
        var harness = new TelemetryFacadeHarness().ForTenant("acme");
        var request = TelemetryFacadeHarness.RangeRequest(ReadRate) with { TreeId = "t/acme/orders" };

        await harness.Build().QueryAsync(request);

        Assert.That(harness.Backend.SingleQuery, Is.EqualTo(
            """sum by (tree) (rate(orleans_lattice_shard_reads_total{tenant="acme",tree="t/acme/orders",}[5m]))"""));
    }

    [Test]
    public async Task A_tree_filter_naming_another_tenants_tree_still_carries_the_tenant_matcher()
    {
        var harness = new TelemetryFacadeHarness().ForTenant("acme");
        var request = TelemetryFacadeHarness.RangeRequest(ReadRate) with { TreeId = "t/victim/secrets" };

        await harness.Build().QueryAsync(request);

        Assert.That(harness.Backend.SingleQuery, Does.Contain("""tenant="acme",tree="t/victim/secrets","""),
            "The two matchers intersect, so naming another tenant's tree yields no series rather "
            + "than that tenant's data.");
    }

    [Test]
    public async Task A_tree_filter_is_ignored_by_an_entry_that_does_not_declare_it()
    {
        var harness = new TelemetryFacadeHarness().ForTenant("acme");
        var request = TelemetryFacadeHarness.InstantRequest(TenantUsage) with { TreeId = "t/acme/orders" };

        await harness.Build().QueryAsync(request);

        Assert.That(harness.Backend.SingleQuery, Does.Not.Contain("tree="),
            "A value supplied for an undeclared parameter is ignored rather than widening the query.");
    }

    [Test]
    public async Task A_hostile_tree_filter_is_escaped_into_one_inert_label_value()
    {
        var harness = new TelemetryFacadeHarness().ForTenant("acme");
        var request = TelemetryFacadeHarness.RangeRequest(ReadRate) with
        {
            TreeId = """x"} or orleans_lattice_shard_reads_total{tenant="victim""",
        };

        await harness.Build().QueryAsync(request);
        var query = harness.Backend.SingleQuery;

        Assert.Multiple(() =>
        {
            Assert.That(query, Does.Contain("""tenant="acme","""));
            Assert.That(query, Does.Not.Contain("""tenant="victim"""),
                "The injected quote must stay escaped, so the hostile text can never open a second "
                + "selector.");
            Assert.That(query, Does.Contain("""\"} or"""));
        });
    }

    [Test]
    public void A_tree_filter_carrying_a_control_character_is_refused()
    {
        var harness = new TelemetryFacadeHarness().ForTenant("acme");
        var request = TelemetryFacadeHarness.RangeRequest(ReadRate) with { TreeId = "orders\nrogue" };

        Assert.That(
            async () => await harness.Build().QueryAsync(request),
            Throws.ArgumentException);
    }

    [Test]
    public async Task An_empty_tree_filter_applies_no_tree_matcher()
    {
        var harness = new TelemetryFacadeHarness().ForTenant("acme");
        var request = TelemetryFacadeHarness.RangeRequest(ReadRate) with { TreeId = string.Empty };

        await harness.Build().QueryAsync(request);

        Assert.That(harness.Backend.SingleQuery, Does.Not.Contain("tree="));
    }

    [Test]
    public async Task A_cross_tenant_query_may_still_be_narrowed_to_one_tree()
    {
        var harness = new TelemetryFacadeHarness().AsPlatformOperator();
        var request = TelemetryFacadeHarness.RangeRequest(ReadRate) with
        {
            RequestedVisibility = TelemetryTenantVisibility.AllTenants,
            TreeId = "t/acme/orders",
        };

        await harness.Build().QueryAsync(request);

        Assert.That(harness.Backend.SingleQuery, Does.Contain("""{tree="t/acme/orders",}"""));
    }

    [Test]
    public async Task An_operator_may_pin_the_platform_sentinel_to_inspect_platform_series()
    {
        var harness = new TelemetryFacadeHarness().AsPlatformOperator();
        var request = TelemetryFacadeHarness.RangeRequest(ReadRate) with
        {
            RequestedVisibility = TelemetryTenantVisibility.SingleTenant,
            RequestedTenantId = LatticeTenantLabel.PlatformTenant,
        };

        var response = await harness.Build().QueryAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(response.Scope.WasDowngraded, Is.False);
            Assert.That(harness.Backend.SingleQuery,
                Does.Contain($$"""tenant="{{LatticeTenantLabel.PlatformTenant}}","""));
        });
    }

    [Test]
    public async Task Every_response_reports_the_scope_that_was_applied()
    {
        var harness = new TelemetryFacadeHarness().ForTenant("acme");

        var response = await harness.Build().QueryAsync(TelemetryFacadeHarness.RangeRequest(ReadRate));

        Assert.Multiple(() =>
        {
            Assert.That(response.Scope.TenantId, Is.EqualTo("acme"));
            Assert.That(response.Scope.RequestedVisibility,
                Is.EqualTo(TelemetryTenantVisibility.ActiveTenant));
            Assert.That(response.Scope.EffectiveVisibility,
                Is.EqualTo(TelemetryTenantVisibility.ActiveTenant));
        });
    }

    [Test]
    public async Task Every_authored_entry_carries_the_tenant_matcher_when_evaluated()
    {
        foreach (var descriptor in LatticeTelemetryQueries.Definitions.Select(d => d.Descriptor))
        {
            var harness = new TelemetryFacadeHarness().ForTenant("acme");
            var request = descriptor.Kind == TelemetryQueryKind.Range
                ? TelemetryFacadeHarness.RangeRequest(descriptor.QueryId)
                : TelemetryFacadeHarness.InstantRequest(descriptor.QueryId);

            await harness.Build().QueryAsync(request);

            Assert.That(harness.Backend.SingleQuery, Does.Contain("""tenant="acme","""),
                $"'{descriptor.QueryId}' reached the backend without a tenant matcher.");
        }
    }
}
