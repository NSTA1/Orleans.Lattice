using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Api.Abstractions.Tests.Telemetry;

/// <summary>
/// Exercises the query-result shape: labels, samples, series lookup, and the
/// response wrapper that always reports the tenant scope and the window actually
/// evaluated.
/// </summary>
[TestFixture]
public sealed class TelemetryResultModelTests
{
    private static readonly DateTimeOffset Origin = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static TelemetryTimeSeries Series() => new()
    {
        Labels = [new TelemetryLabel("tree", "t/acme/orders"), new TelemetryLabel("tenant", "acme")],
        Points =
        [
            new TelemetryDataPoint(Origin, 1.5),
            new TelemetryDataPoint(Origin.AddMinutes(1), 2.5),
        ],
    };

    [Test]
    public void Label_preserves_its_name_and_value()
    {
        var label = new TelemetryLabel("tree", "t/acme/orders");

        Assert.Multiple(() =>
        {
            Assert.That(label.Name, Is.EqualTo("tree"));
            Assert.That(label.Value, Is.EqualTo("t/acme/orders"));
        });
    }

    [Test]
    public void Label_rejects_a_null_name()
    {
        Assert.That(() => new TelemetryLabel(null!, "v"), Throws.ArgumentNullException);
    }

    [Test]
    public void Label_rejects_a_null_value()
    {
        Assert.That(() => new TelemetryLabel("n", null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Equal_labels_compare_equal_by_value()
    {
        Assert.That(new TelemetryLabel("tree", "orders"), Is.EqualTo(new TelemetryLabel("tree", "orders")));
    }

    [Test]
    public void Data_point_preserves_its_timestamp_and_value()
    {
        var point = new TelemetryDataPoint(Origin, 42.5);

        Assert.Multiple(() =>
        {
            Assert.That(point.Timestamp, Is.EqualTo(Origin));
            Assert.That(point.Value, Is.EqualTo(42.5));
            Assert.That(point.IsFinite, Is.True);
        });
    }

    [Test]
    public void Data_point_carries_a_not_a_number_value_through_rather_than_coercing_it()
    {
        var point = new TelemetryDataPoint(Origin, double.NaN);

        Assert.Multiple(() =>
        {
            Assert.That(double.IsNaN(point.Value), Is.True);
            Assert.That(point.IsFinite, Is.False);
        });
    }

    [Test]
    public void Data_point_carries_an_infinity_through_rather_than_coercing_it()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new TelemetryDataPoint(Origin, double.PositiveInfinity).IsFinite, Is.False);
            Assert.That(new TelemetryDataPoint(Origin, double.NegativeInfinity).IsFinite, Is.False);
        });
    }

    [Test]
    public void Series_resolves_a_label_it_carries()
    {
        var series = Series();

        Assert.Multiple(() =>
        {
            Assert.That(series.TryGetLabel("tree", out var tree), Is.True);
            Assert.That(tree, Is.EqualTo("t/acme/orders"));
            Assert.That(series.TryGetLabel("tenant", out var tenant), Is.True);
            Assert.That(tenant, Is.EqualTo("acme"));
        });
    }

    [Test]
    public void Series_does_not_resolve_a_label_it_lacks()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Series().TryGetLabel("region", out var value), Is.False);
            Assert.That(value, Is.Null);
        });
    }

    [Test]
    public void Series_matches_a_label_name_ordinally()
    {
        Assert.That(Series().TryGetLabel("Tree", out _), Is.False);
    }

    [Test]
    public void Series_rejects_a_null_label_name()
    {
        Assert.That(() => Series().TryGetLabel(null!, out _), Throws.ArgumentNullException);
    }

    [Test]
    public void Empty_series_is_a_cached_singleton_with_no_labels_or_points()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryTimeSeries.Empty.Labels, Is.Empty);
            Assert.That(TelemetryTimeSeries.Empty.Points, Is.Empty);
            Assert.That(TelemetryTimeSeries.Empty, Is.SameAs(TelemetryTimeSeries.Empty));
            Assert.That(TelemetryTimeSeries.Empty.TryGetLabel("tree", out _), Is.False);
        });
    }

    [Test]
    public void Response_reports_its_series_count_and_non_empty_state()
    {
        var response = new TelemetryQueryResponse
        {
            QueryId = "tree.write.ops",
            Scope = TelemetryTenantScope.PinnedTo("acme", TelemetryTenantVisibility.ActiveTenant),
            ResultKind = TelemetryResultKind.Matrix,
            Series = [Series()],
            Range = TelemetryTimeRange.Between(Origin, Origin.AddMinutes(1), TimeSpan.FromMinutes(1)),
        };

        Assert.Multiple(() =>
        {
            Assert.That(response.QueryId, Is.EqualTo("tree.write.ops"));
            Assert.That(response.SeriesCount, Is.EqualTo(1));
            Assert.That(response.IsEmpty, Is.False);
            Assert.That(response.ResultKind, Is.EqualTo(TelemetryResultKind.Matrix));
            Assert.That(response.Range.Step, Is.EqualTo(TimeSpan.FromMinutes(1)));
            Assert.That(response.Scope.TenantId, Is.EqualTo("acme"));
        });
    }

    [Test]
    public void Response_reports_an_empty_result_when_the_query_matched_nothing()
    {
        var response = new TelemetryQueryResponse
        {
            QueryId = "tree.write.ops",
            Scope = TelemetryTenantScope.AcrossAllTenants(),
            Series = [],
        };

        Assert.Multiple(() =>
        {
            Assert.That(response.SeriesCount, Is.EqualTo(0));
            Assert.That(response.IsEmpty, Is.True);
            Assert.That(response.ResultKind, Is.EqualTo(TelemetryResultKind.Empty),
                "An unpopulated result kind must read as empty rather than as a shape it does not have.");
        });
    }

    [Test]
    public void Response_reports_a_downgraded_scope_it_was_served_under()
    {
        var response = new TelemetryQueryResponse
        {
            QueryId = "tree.write.ops",
            Scope = TelemetryTenantScope.PinnedTo("acme", TelemetryTenantVisibility.AllTenants),
            Series = [],
        };

        Assert.Multiple(() =>
        {
            Assert.That(response.Scope.WasDowngraded, Is.True);
            Assert.That(response.Scope.IsCrossTenant, Is.False);
        });
    }
}
