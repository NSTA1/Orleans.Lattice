namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Covers the compiled query template and the scope selector it renders: the two
/// pieces that turn a server-authored template plus a server-derived tenant into
/// the exact text sent to the backend.
/// </summary>
[TestFixture]
public sealed class TelemetryQueryTemplateTests
{
    [Test]
    public void Parse_renders_a_template_with_no_slots_unchanged()
    {
        var template = TelemetryQueryTemplate.Parse("up");

        Assert.Multiple(() =>
        {
            Assert.That(template.Render(TelemetryScopeSelector.Unscoped, "5m"), Is.EqualTo("up"));
            Assert.That(template.HasScopeSlot, Is.False);
        });
    }

    [Test]
    public void Render_substitutes_the_tenant_matcher_at_the_scope_slot()
    {
        var template = TelemetryQueryTemplate.Parse("sum(metric{$scope$})");

        var rendered = template.Render(TelemetryScopeSelector.ForTenant("acme", null), "5m");

        Assert.That(rendered, Is.EqualTo("""sum(metric{tenant="acme",})"""));
    }

    [Test]
    public void Render_emits_an_empty_matcher_list_for_an_unscoped_selector()
    {
        var template = TelemetryQueryTemplate.Parse("sum(metric{$scope$})");

        var rendered = template.Render(TelemetryScopeSelector.Unscoped, "5m");

        Assert.That(rendered, Is.EqualTo("sum(metric{})"),
            "An empty matcher list is well-formed PromQL when the selector is anchored to a "
            + "metric name, so the cross-tenant render needs no separate template.");
    }

    [Test]
    public void Render_places_the_tree_filter_beside_the_tenant_matcher()
    {
        var template = TelemetryQueryTemplate.Parse("metric{$scope$}");

        var rendered = template.Render(TelemetryScopeSelector.ForTenant("acme", "t/acme/orders"), "5m");

        Assert.That(rendered, Is.EqualTo("""metric{tenant="acme",tree="t/acme/orders",}"""),
            "The tree filter narrows within the tenant scope; it is rendered alongside the tenant "
            + "matcher rather than replacing it, so it can never widen the query.");
    }

    [Test]
    public void Render_keeps_a_static_matcher_well_formed_when_the_scope_is_empty()
    {
        var template = TelemetryQueryTemplate.Parse("""metric{$scope$outcome="committed"}""");

        Assert.Multiple(() =>
        {
            Assert.That(
                template.Render(TelemetryScopeSelector.Unscoped, "5m"),
                Is.EqualTo("""metric{outcome="committed"}"""));
            Assert.That(
                template.Render(TelemetryScopeSelector.ForTenant("acme", null), "5m"),
                Is.EqualTo("""metric{tenant="acme",outcome="committed"}"""));
        });
    }

    [Test]
    public void Render_substitutes_every_occurrence_of_both_slots()
    {
        var template = TelemetryQueryTemplate.Parse(
            "rate(a{$scope$}[$window$]) / rate(b{$scope$}[$window$])");

        var rendered = template.Render(TelemetryScopeSelector.ForTenant("acme", null), "10m");

        Assert.That(rendered, Is.EqualTo(
            """rate(a{tenant="acme",}[10m]) / rate(b{tenant="acme",}[10m])"""));
    }

    [Test]
    public void Render_substitutes_a_window_slot_that_precedes_a_scope_slot()
    {
        var template = TelemetryQueryTemplate.Parse("[$window$] metric{$scope$}");

        Assert.That(
            template.Render(TelemetryScopeSelector.ForTenant("acme", null), "2m"),
            Is.EqualTo("""[2m] metric{tenant="acme",}"""));
    }

    [Test]
    public void Render_handles_a_slot_at_the_very_start_and_end_of_a_template()
    {
        var template = TelemetryQueryTemplate.Parse("$scope$|$window$");

        Assert.That(
            template.Render(TelemetryScopeSelector.ForTenant("acme", null), "5m"),
            Is.EqualTo("""tenant="acme",|5m"""));
    }

    [Test]
    public void Render_emits_only_the_tree_matcher_for_a_cross_tenant_scope_with_a_tree_filter()
    {
        var template = TelemetryQueryTemplate.Parse("metric{$scope$}");

        Assert.That(
            template.Render(TelemetryScopeSelector.ForTree("orders"), "5m"),
            Is.EqualTo("""metric{tree="orders",}"""));
    }

    [Test]
    public void Has_scope_slot_reports_whether_the_template_can_be_tenant_scoped()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryQueryTemplate.Parse("metric{$scope$}").HasScopeSlot, Is.True);
            Assert.That(TelemetryQueryTemplate.Parse("metric[$window$]").HasScopeSlot, Is.False);
        });
    }

    [Test]
    public void Parse_rejects_a_null_template()
    {
        Assert.That(() => TelemetryQueryTemplate.Parse(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Render_rejects_a_null_window()
    {
        var template = TelemetryQueryTemplate.Parse("metric{$scope$}");

        Assert.That(
            () => template.Render(TelemetryScopeSelector.Unscoped, null!),
            Throws.ArgumentNullException);
    }
}
