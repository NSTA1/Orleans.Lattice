namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Audits the authored named-query catalogue as data: every entry must be
/// tenant-scopeable, must declare the instruments it actually reads, must be
/// bounded, and must scan cleanly under the deny-all metric-access gate. These are
/// structural facts about the catalogue rather than assertions about one query, so
/// a later entry that violates one fails here instead of shipping.
/// </summary>
[TestFixture]
public sealed class LatticeTelemetryQueriesTests
{
    private static IReadOnlyList<TelemetryQueryDefinition> Definitions => LatticeTelemetryQueries.Definitions;

    [Test]
    public void The_catalogue_is_not_empty_and_is_materialised_once()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Definitions, Is.Not.Empty);
            Assert.That(Definitions, Is.SameAs(LatticeTelemetryQueries.Definitions),
                "The authored set is built once, so serving discovery allocates nothing.");
        });
    }

    [Test]
    public void Every_query_id_is_unique()
    {
        var ids = Definitions.Select(d => d.QueryId).ToArray();

        Assert.That(ids, Is.Unique);
    }

    [Test]
    public void Entries_are_in_ascending_query_id_order()
    {
        var ids = Definitions.Select(d => d.QueryId).ToArray();

        Assert.That(ids, Is.EqualTo(ids.OrderBy(id => id, StringComparer.Ordinal).ToArray()),
            "A stable order keeps a rendered picker stable across calls.");
    }

    [Test]
    public void Every_template_carries_a_scope_placeholder()
    {
        var offenders = Definitions
            .Where(d => !d.QueryTemplate.Contains(TelemetryQueryTemplate.ScopeToken, StringComparison.Ordinal))
            .Select(d => d.QueryId)
            .ToArray();

        Assert.That(offenders, Is.Empty,
            "A template with no scope placeholder could not be tenant-scoped, so an author would "
            + "be able to opt out of isolation by omission. Offenders: " + string.Join(", ", offenders));
    }

    [Test]
    public void Every_range_template_carries_a_window_placeholder_when_it_rates_a_counter()
    {
        var offenders = Definitions
            .Where(d => d.QueryTemplate.Contains("rate(", StringComparison.Ordinal))
            .Where(d => !d.QueryTemplate.Contains(TelemetryQueryTemplate.WindowToken, StringComparison.Ordinal))
            .Select(d => d.QueryId)
            .ToArray();

        Assert.That(offenders, Is.Empty,
            "A hard-coded rate window under-samples at a coarse step. Offenders: "
            + string.Join(", ", offenders));
    }

    [Test]
    public void Every_entry_declares_at_least_one_instrument()
    {
        var offenders = Definitions
            .Where(d => d.Descriptor.Instruments.Count == 0)
            .Select(d => d.QueryId)
            .ToArray();

        Assert.That(offenders, Is.Empty, string.Join(", ", offenders));
    }

    [Test]
    public void Every_declared_instrument_carries_a_meter_a_unit_and_a_declared_semantic()
    {
        var offenders = Definitions
            .SelectMany(d => d.Descriptor.Instruments.Select(i => (d.QueryId, Instrument: i)))
            .Where(x => string.IsNullOrEmpty(x.Instrument.Name)
                || string.IsNullOrEmpty(x.Instrument.Meter)
                || string.IsNullOrEmpty(x.Instrument.Unit)
                || x.Instrument.Semantic == TelemetryMeasurementSemantic.Unspecified)
            .Select(x => $"{x.QueryId}/{x.Instrument.Name}")
            .ToArray();

        Assert.That(offenders, Is.Empty,
            "An undeclared semantic is exactly the drift the instrument reference exists to catch. "
            + "Offenders: " + string.Join(", ", offenders));
    }

    [Test]
    public void Every_entry_declares_a_title_a_description_a_unit_and_a_semantic()
    {
        var offenders = Definitions
            .Where(d => string.IsNullOrWhiteSpace(d.Descriptor.Title)
                || string.IsNullOrWhiteSpace(d.Descriptor.Description)
                || string.IsNullOrWhiteSpace(d.Descriptor.Unit)
                || d.Descriptor.Semantic == TelemetryMeasurementSemantic.Unspecified)
            .Select(d => d.QueryId)
            .ToArray();

        Assert.That(offenders, Is.Empty, string.Join(", ", offenders));
    }

    [Test]
    public void A_single_instrument_entry_reports_that_instruments_own_semantic()
    {
        // A derived entry may legitimately differ (a ratio over two counters), but an
        // entry reading exactly one instrument must not claim a semantic its source
        // does not have - that is the per-operation versus per-record drift.
        var offenders = Definitions
            .Where(d => d.Descriptor.Instruments.Count == 1)
            .Where(d => d.Descriptor.Semantic != TelemetryMeasurementSemantic.Ratio)
            .Where(d => d.Descriptor.Semantic != d.Descriptor.Instruments[0].Semantic)
            .Select(d => $"{d.QueryId} claims {d.Descriptor.Semantic} over "
                + $"{d.Descriptor.Instruments[0].Semantic}")
            .ToArray();

        Assert.That(offenders, Is.Empty, string.Join("; ", offenders));
    }

    [Test]
    public void Every_range_entry_declares_a_bounded_window_a_step_budget_and_a_point_budget()
    {
        var offenders = Definitions
            .Where(d => d.Descriptor.Kind == TelemetryQueryKind.Range)
            .Where(d => d.Descriptor.Bounds.MaxRange <= TimeSpan.Zero
                || d.Descriptor.Bounds.MaxStep <= TimeSpan.Zero
                || d.Descriptor.Bounds.DefaultStep <= TimeSpan.Zero
                || d.Descriptor.Bounds.MaxPoints <= 0)
            .Select(d => d.QueryId)
            .ToArray();

        Assert.That(offenders, Is.Empty,
            "Bounds must never be trusted to drive unbounded work. Offenders: "
            + string.Join(", ", offenders));
    }

    [Test]
    public void No_entry_is_unbounded()
    {
        var offenders = Definitions
            .Where(d => d.Descriptor.Bounds.IsUnbounded)
            .Select(d => d.QueryId)
            .ToArray();

        Assert.That(offenders, Is.Empty, string.Join(", ", offenders));
    }

    [Test]
    public void Every_range_entry_declares_the_time_range_and_step_parameters()
    {
        var offenders = Definitions
            .Where(d => d.Descriptor.Kind == TelemetryQueryKind.Range)
            .Where(d => !d.Descriptor.Accepts(
                TelemetryQueryParameters.TimeRange | TelemetryQueryParameters.Step))
            .Select(d => d.QueryId)
            .ToArray();

        Assert.That(offenders, Is.Empty, string.Join(", ", offenders));
    }

    [Test]
    public void Every_rendered_template_scans_to_a_resolvable_metric_name_set()
    {
        foreach (var definition in Definitions)
        {
            var rendered = TelemetryQueryTemplate
                .Parse(definition.QueryTemplate)
                .Render(TelemetryScopeSelector.Unscoped, TelemetryRateWindow.Default);
            var references = PromQlMetricExtractor.ExtractReferences(rendered);

            Assert.Multiple(() =>
            {
                Assert.That(references.HasUnresolvableNameMatcher, Is.False,
                    $"'{definition.QueryId}' must not name a metric by pattern.");
                Assert.That(references.HasUnconstrainedSelector, Is.False,
                    $"'{definition.QueryId}' must anchor every selector to a metric name.");
                Assert.That(references.Names, Is.Not.Empty,
                    $"'{definition.QueryId}' must name at least one metric.");
            });
        }
    }

    [Test]
    public void Injecting_a_tenant_scope_never_changes_a_templates_metric_name_set()
    {
        // The whole entitlement model depends on this: metric access is decided once
        // from a scope-free probe, so the scope the facade later injects must add
        // only label matchers.
        foreach (var definition in Definitions)
        {
            var template = TelemetryQueryTemplate.Parse(definition.QueryTemplate);
            var unscoped = PromQlMetricExtractor.ExtractReferences(
                template.Render(TelemetryScopeSelector.Unscoped, TelemetryRateWindow.Default));
            var scoped = PromQlMetricExtractor.ExtractReferences(
                template.Render(
                    TelemetryScopeSelector.ForTenant("acme", "t/acme/orders"),
                    TelemetryRateWindow.Default));

            Assert.That(scoped.Names, Is.EqualTo(unscoped.Names),
                $"'{definition.QueryId}' changed its metric footprint when scoped.");
            Assert.That(scoped.HasUnconstrainedSelector, Is.False, definition.QueryId);
        }
    }

    [Test]
    public void Every_entry_compiles_into_a_plan()
    {
        var policy = new TelemetryMetricAccessPolicy(new LatticeTelemetryOptions());

        Assert.That(
            () => new LatticeTelemetryQueryCatalog(Definitions, LatticeTelemetryQueries.Version, policy),
            Throws.Nothing);
    }

    [Test]
    public void The_catalogue_covers_both_the_core_and_the_tenancy_meters()
    {
        var meters = Definitions
            .SelectMany(d => d.Descriptor.Instruments.Select(i => i.Meter))
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(meters, Does.Contain("orleans.lattice"));
            Assert.That(meters, Does.Contain("orleans.lattice.tenancy"),
                "The same catalogue serves both deployment modes, so the tenancy-meter entries are "
                + "present whether or not the add-on is installed.");
        });
    }

    [Test]
    public void The_version_is_positive()
    {
        Assert.That(LatticeTelemetryQueries.Version, Is.GreaterThan(0));
    }
}
