namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Covers the compiled catalogue container: how it indexes entries, how the
/// metric-access allow-list narrows what is offered, and that discovery and
/// execution agree on the same entitlement.
/// </summary>
[TestFixture]
public sealed class LatticeTelemetryQueryCatalogTests
{
    private static TelemetryQueryDefinition Definition(
        string queryId,
        string template = "sum(metric_alpha{$scope$})") => new()
    {
        Descriptor = new TelemetryQueryDescriptor
        {
            QueryId = queryId,
            Title = queryId,
            Description = "test entry",
            Unit = "1",
            Kind = TelemetryQueryKind.Instant,
            Semantic = TelemetryMeasurementSemantic.Level,
            Parameters = TelemetryQueryParameters.None,
            Bounds = new TelemetryQueryBounds { MaxPoints = 1 },
            Instruments = [new TelemetryInstrumentReference(
                "metric.alpha", "orleans.lattice", "1", TelemetryMeasurementSemantic.Level)],
        },
        QueryTemplate = template,
    };

    private static TelemetryMetricAccessPolicy ReadAll() =>
        new(new LatticeTelemetryOptions());

    private static TelemetryMetricAccessPolicy AllowOnly(params string[] metrics)
    {
        var options = new LatticeTelemetryOptions
        {
            MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed,
        };

        foreach (var metric in metrics)
        {
            options.AllowedMetrics.Add(metric);
        }

        return new TelemetryMetricAccessPolicy(options);
    }

    [Test]
    public void The_built_in_catalogue_offers_every_authored_entry_under_read_all()
    {
        var catalog = new LatticeTelemetryQueryCatalog(ReadAll());

        Assert.Multiple(() =>
        {
            Assert.That(catalog.Count, Is.EqualTo(LatticeTelemetryQueries.Definitions.Count));
            Assert.That(catalog.Version, Is.EqualTo(LatticeTelemetryQueries.Version));
            Assert.That(catalog.Catalog.Version, Is.EqualTo(LatticeTelemetryQueries.Version));
        });
    }

    [Test]
    public void Entries_are_served_in_ascending_query_id_order()
    {
        var catalog = new LatticeTelemetryQueryCatalog(
            [Definition("zulu"), Definition("alpha"), Definition("mike")],
            version: 3,
            ReadAll());

        Assert.That(
            catalog.Catalog.Queries.Select(q => q.QueryId),
            Is.EqualTo(new[] { "alpha", "mike", "zulu" }));
    }

    [Test]
    public void The_client_facing_catalogue_is_materialised_once()
    {
        var catalog = new LatticeTelemetryQueryCatalog(ReadAll());

        Assert.That(catalog.Catalog, Is.SameAs(catalog.Catalog));
    }

    [Test]
    public void An_entry_whose_metric_the_allow_list_denies_is_not_offered()
    {
        var catalog = new LatticeTelemetryQueryCatalog(
            [Definition("allowed", "sum(metric_alpha{$scope$})"), Definition("denied", "sum(metric_beta{$scope$})")],
            version: 1,
            AllowOnly("metric_alpha"));

        Assert.Multiple(() =>
        {
            Assert.That(catalog.Offers("allowed"), Is.True);
            Assert.That(catalog.Offers("denied"), Is.False);
            Assert.That(catalog.Catalog.Queries.Select(q => q.QueryId), Is.EqualTo(new[] { "allowed" }));
        });
    }

    [Test]
    public void A_denied_entry_is_unresolvable_by_id_so_discovery_and_execution_agree()
    {
        var catalog = new LatticeTelemetryQueryCatalog(
            [Definition("denied", "sum(metric_beta{$scope$})")],
            version: 1,
            AllowOnly("metric_alpha"));

        Assert.Multiple(() =>
        {
            Assert.That(catalog.Catalog, Is.SameAs(TelemetryQueryCatalog.Empty));
            Assert.That(catalog.Offers("denied"), Is.False,
                "An unentitled id must be indistinguishable from one that does not exist.");
            Assert.That(catalog.Offers("never-existed"), Is.False);
        });
    }

    [Test]
    public void A_wildcard_allow_list_entry_admits_a_matching_query()
    {
        var catalog = new LatticeTelemetryQueryCatalog(
            [Definition("wild", "sum(metric_alpha{$scope$})")],
            version: 1,
            AllowOnly("metric_*"));

        Assert.That(catalog.Offers("wild"), Is.True);
    }

    [Test]
    public void An_empty_catalogue_reports_the_shared_empty_singleton()
    {
        var catalog = new LatticeTelemetryQueryCatalog([], version: 7, ReadAll());

        Assert.Multiple(() =>
        {
            Assert.That(catalog.Count, Is.Zero);
            Assert.That(catalog.Catalog, Is.SameAs(TelemetryQueryCatalog.Empty));
        });
    }

    [Test]
    public void A_duplicate_query_id_is_rejected()
    {
        Assert.That(
            () => new LatticeTelemetryQueryCatalog(
                [Definition("same"), Definition("same")], version: 1, ReadAll()),
            Throws.ArgumentException.With.Message.Contains("more than once"));
    }

    [Test]
    public void A_template_with_no_scope_placeholder_is_rejected()
    {
        Assert.That(
            () => new LatticeTelemetryQueryCatalog(
                [Definition("unscoped", "sum(metric_alpha)")], version: 1, ReadAll()),
            Throws.ArgumentException.With.Message.Contains("scope"),
            "A curated query that cannot be tenant-scoped must never compile.");
    }

    [Test]
    public void A_range_entry_with_no_default_step_is_rejected()
    {
        var definition = Definition("ranged") with
        {
            Descriptor = Definition("ranged").Descriptor with
            {
                Kind = TelemetryQueryKind.Range,
                Bounds = new TelemetryQueryBounds { MaxRange = TimeSpan.FromHours(1) },
            },
        };

        Assert.That(
            () => new LatticeTelemetryQueryCatalog([definition], version: 1, ReadAll()),
            Throws.ArgumentException.With.Message.Contains("default step"));
    }

    [Test]
    public void A_template_naming_a_metric_by_pattern_is_rejected_even_under_read_all()
    {
        // The read-all posture admits without scanning, so a template like this
        // would otherwise compile silently and fail only once an operator tightened
        // the allow-list.
        Assert.That(
            () => new LatticeTelemetryQueryCatalog(
                [Definition("patterned", """sum({__name__=~"metric_.*",$scope$})""")],
                version: 1,
                ReadAll()),
            Throws.ArgumentException.With.Message.Contains("pattern"));
    }

    [Test]
    public void A_template_with_a_selector_anchored_to_no_metric_name_is_rejected()
    {
        Assert.That(
            () => new LatticeTelemetryQueryCatalog(
                [Definition("unanchored", """metric_alpha or {job="api",$scope$}""")],
                version: 1,
                ReadAll()),
            Throws.ArgumentException.With.Message.Contains("anchored to no metric name"));
    }

    [Test]
    public void A_template_naming_no_metric_at_all_is_rejected()
    {
        Assert.That(
            () => new LatticeTelemetryQueryCatalog(
                [Definition("nameless", "sum($scope$)")],
                version: 1,
                ReadAll()),
            Throws.ArgumentException.With.Message.Contains("names no metric"));
    }

    [Test]
    public void The_catalogue_rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new LatticeTelemetryQueryCatalog(null!), Throws.ArgumentNullException);
            Assert.That(
                () => new LatticeTelemetryQueryCatalog(null!, 1, ReadAll()),
                Throws.ArgumentNullException);
            Assert.That(
                () => new LatticeTelemetryQueryCatalog([], 1, null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Offers_rejects_a_null_query_id()
    {
        var catalog = new LatticeTelemetryQueryCatalog(ReadAll());

        Assert.That(() => catalog.Offers(null!), Throws.ArgumentNullException);
    }
}
