using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.Plugins.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// Covers what the seam actually sends and returns for an evaluation: a defaulted
/// request travels defaulted, the two tenancy fields travel unchanged, and every
/// series the facade returned reaches the panel.
/// </summary>
[TestFixture]
public class TelemetryQueryServiceTests
{
    private FakeTelemetryQueryClient _client = null!;

    [SetUp]
    public void SetUp() => _client = new FakeTelemetryQueryClient();

    private TelemetryQueryService Create() => new(_client);

    [Test]
    public void Query_rejects_a_null_request() =>
        Assert.That(async () => await Create().QueryAsync(null!), Throws.ArgumentNullException);

    [Test]
    public void A_request_factory_rejects_a_missing_query_id() =>
        Assert.Multiple(() =>
        {
            Assert.That(() => ExplorerTelemetryRequest.For(null!), Throws.ArgumentNullException);
            Assert.That(() => ExplorerTelemetryRequest.For(string.Empty), Throws.InstanceOf<ArgumentException>());
        });

    [Test]
    public async Task A_request_naming_no_query_is_refused_without_reaching_the_wire()
    {
        var result = await Create().QueryAsync(new ExplorerTelemetryRequest { QueryId = string.Empty });

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(TelemetryQueryStatus.InvalidRequest));
            Assert.That(result.Message, Is.Not.Empty);
            Assert.That(_client.QueryCallCount, Is.Zero);
        });
    }

    [Test]
    public async Task A_defaulted_request_reaches_the_wire_with_its_window_still_unset()
    {
        // The request a panel makes before a user has touched a control. A client
        // that expands an unset window into the entry's maximum range overruns the
        // point budget at the default step and turns every range query into a
        // bounds refusal, so the defaults must travel as defaults.
        await Create().QueryAsync(new ExplorerTelemetryRequest { QueryId = SampleTelemetry.RangeQueryId });

        var sent = _client.LastRequest;
        Assert.That(sent, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(sent!.QueryId, Is.EqualTo(SampleTelemetry.RangeQueryId));
            Assert.That(sent.Range, Is.EqualTo(default(TelemetryTimeRange)));
            Assert.That(sent.Range.StartUtc, Is.EqualTo(default(DateTimeOffset)));
            Assert.That(sent.Range.EndUtc, Is.EqualTo(default(DateTimeOffset)));
            Assert.That(sent.Range.Step, Is.EqualTo(TimeSpan.Zero));
            Assert.That(sent.TreeId, Is.Null);
            Assert.That(sent.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.ActiveTenant));
            Assert.That(sent.RequestedTenantId, Is.Null);
        });
    }

    [Test]
    public async Task A_defaulted_request_is_never_refused_before_the_wire()
    {
        // Even with the catalogue already read - so the bounds are known and the
        // pre-flight check is live - a defaulted request must still be sent.
        var service = Create();
        await service.GetCatalogAsync();

        var result = await service.QueryAsync(ExplorerTelemetryRequest.For(SampleTelemetry.RangeQueryId));

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(_client.QueryCallCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_defaulted_request_is_sent_for_every_entry_the_catalogue_offers()
    {
        var service = Create();
        var catalog = (await service.GetCatalogAsync()).Value!;

        foreach (var query in catalog.Queries)
        {
            var result = await service.QueryAsync(ExplorerTelemetryRequest.For(query.QueryId));
            Assert.That(result.IsSuccess, Is.True, $"a defaulted request for '{query.QueryId}' must be sent");
        }

        Assert.That(_client.QueryCallCount, Is.EqualTo(catalog.Count));
    }

    [Test]
    public async Task A_fully_specified_request_travels_field_for_field()
    {
        var window = ExplorerTelemetryWindow.Between(
            SampleTelemetry.Anchor,
            SampleTelemetry.Anchor.AddHours(1),
            TimeSpan.FromMinutes(5));

        await Create().QueryAsync(new ExplorerTelemetryRequest
        {
            QueryId = SampleTelemetry.RangeQueryId,
            Window = window,
            TreeId = "orders",
            RequestedVisibility = ExplorerTelemetryVisibility.AllTenants,
            RequestedTenantId = SampleTelemetry.OtherTenant,
        });

        var sent = _client.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.Range.StartUtc, Is.EqualTo(window.StartUtc));
            Assert.That(sent.Range.EndUtc, Is.EqualTo(window.EndUtc));
            Assert.That(sent.Range.Step, Is.EqualTo(window.Step));
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.AllTenants));
            Assert.That(sent.RequestedTenantId, Is.EqualTo(SampleTelemetry.OtherTenant));
        });
    }

    [Test]
    public async Task Every_requested_visibility_reaches_the_wire_unchanged()
    {
        (ExplorerTelemetryVisibility Requested, TelemetryTenantVisibility Expected)[] cases =
        [
            (ExplorerTelemetryVisibility.ActiveTenant, TelemetryTenantVisibility.ActiveTenant),
            (ExplorerTelemetryVisibility.AllTenants, TelemetryTenantVisibility.AllTenants),
            (ExplorerTelemetryVisibility.SingleTenant, TelemetryTenantVisibility.SingleTenant),
        ];

        var service = Create();
        foreach (var (requested, expected) in cases)
        {
            await service.QueryAsync(new ExplorerTelemetryRequest
            {
                QueryId = SampleTelemetry.RangeQueryId,
                RequestedVisibility = requested,
            });

            Assert.That(
                _client.LastRequest!.RequestedVisibility,
                Is.EqualTo(expected),
                "the seam forwards what the caller requested; it neither widens nor narrows it");
        }
    }

    [Test]
    public async Task The_evaluated_window_the_facade_echoed_is_what_the_panel_reads()
    {
        var result = await Create().QueryAsync(ExplorerTelemetryRequest.For(SampleTelemetry.RangeQueryId));

        Assert.Multiple(() =>
        {
            Assert.That(result.Value!.Window.StartUtc, Is.EqualTo(SampleTelemetry.Anchor));
            Assert.That(result.Value.Window.EndUtc, Is.EqualTo(SampleTelemetry.Anchor.AddHours(1)));
            Assert.That(result.Value.Window.Step, Is.EqualTo(TimeSpan.FromMinutes(1)));
            Assert.That(result.Value.QueryId, Is.EqualTo(SampleTelemetry.RangeQueryId));
            Assert.That(result.Value.Kind, Is.EqualTo(ExplorerTelemetryResultKind.Matrix));
        });
    }

    [Test]
    public async Task Series_labels_and_points_survive_the_projection()
    {
        _client.Series = SampleTelemetry.MixedTenantSeries();

        var result = await Create().QueryAsync(ExplorerTelemetryRequest.For(SampleTelemetry.RangeQueryId));
        var series = result.Value!.Series;

        Assert.Multiple(() =>
        {
            Assert.That(result.Value.SeriesCount, Is.EqualTo(3));
            Assert.That(result.Value.IsEmpty, Is.False);
            Assert.That(series[0].PointCount, Is.EqualTo(2));
            Assert.That(series[0].Points[0].TimestampUtc, Is.EqualTo(SampleTelemetry.Anchor));
            Assert.That(series[0].Points[0].Value, Is.EqualTo(1d));
            Assert.That(series[0].Points[0].IsFinite, Is.True);
            Assert.That(series[0].TryGetLabel("silo", out var silo), Is.True);
            Assert.That(silo, Is.EqualTo("silo-1"));
            Assert.That(series[2].Labels, Is.Empty);
            Assert.That(series[2].Points[0].IsFinite, Is.False, "a gap stays a gap rather than becoming zero");
        });
    }

    [Test]
    public async Task An_empty_result_is_a_success_with_no_series()
    {
        _client.QueryResult = new TelemetryQueryResponse
        {
            QueryId = SampleTelemetry.RangeQueryId,
            Scope = SampleTelemetry.ActiveScope(),
            ResultKind = TelemetryResultKind.Empty,
            Series = [],
        };

        var result = await Create().QueryAsync(ExplorerTelemetryRequest.For(SampleTelemetry.RangeQueryId));

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value!.IsEmpty, Is.True);
            Assert.That(result.Value.Kind, Is.EqualTo(ExplorerTelemetryResultKind.Empty));
            Assert.That(result.Value.Series, Is.Empty);
        });
    }

    [Test]
    public async Task Every_result_shape_projects_onto_its_own_kind()
    {
        (TelemetryResultKind Wire, ExplorerTelemetryResultKind Expected)[] cases =
        [
            (TelemetryResultKind.Empty, ExplorerTelemetryResultKind.Empty),
            (TelemetryResultKind.Vector, ExplorerTelemetryResultKind.Vector),
            (TelemetryResultKind.Matrix, ExplorerTelemetryResultKind.Matrix),
            (TelemetryResultKind.Scalar, ExplorerTelemetryResultKind.Scalar),
        ];

        var service = Create();
        foreach (var (wire, expected) in cases)
        {
            _client.QueryResult = new TelemetryQueryResponse
            {
                QueryId = SampleTelemetry.RangeQueryId,
                Scope = SampleTelemetry.ActiveScope(),
                ResultKind = wire,
                Series = [],
            };

            var result = await service.QueryAsync(ExplorerTelemetryRequest.For(SampleTelemetry.RangeQueryId));
            Assert.That(result.Value!.Kind, Is.EqualTo(expected));
        }
    }

    [Test]
    public async Task An_evaluation_is_never_answered_from_a_remembered_reading()
    {
        var service = Create();

        await service.QueryAsync(ExplorerTelemetryRequest.For(SampleTelemetry.RangeQueryId));
        await service.QueryAsync(ExplorerTelemetryRequest.For(SampleTelemetry.RangeQueryId));

        Assert.That(
            _client.QueryCallCount,
            Is.EqualTo(2),
            "the catalogue is remembered because it rarely changes; a measurement is the point of the poll");
    }

    [Test]
    public void An_empty_placeholder_result_names_its_query() =>
        Assert.Multiple(() =>
        {
            var placeholder = ExplorerTelemetryResult.EmptyFor(SampleTelemetry.RangeQueryId);
            Assert.That(placeholder.QueryId, Is.EqualTo(SampleTelemetry.RangeQueryId));
            Assert.That(placeholder.IsEmpty, Is.True);
            Assert.That(placeholder.SeriesCount, Is.Zero);
            Assert.That(placeholder.Scope, Is.EqualTo(ExplorerTelemetryScope.None));
            Assert.That(placeholder.Kind, Is.EqualTo(ExplorerTelemetryResultKind.Empty));
            Assert.That(() => ExplorerTelemetryResult.EmptyFor(null!), Throws.ArgumentNullException);
        });

    [Test]
    public void The_shared_empty_series_carries_nothing() =>
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerTelemetrySeries.Empty.IsEmpty, Is.True);
            Assert.That(ExplorerTelemetrySeries.Empty.PointCount, Is.Zero);
            Assert.That(ExplorerTelemetrySeries.Empty.Labels, Is.Empty);
            Assert.That(ExplorerTelemetrySeries.Empty.TryGetLabel("tenant", out var value), Is.False);
            Assert.That(value, Is.Null);
            Assert.That(() => ExplorerTelemetrySeries.Empty.TryGetLabel(null!, out _), Throws.ArgumentNullException);
        });

    [Test]
    public void An_entry_declaring_no_instruments_shares_one_empty_list() =>
        Assert.That(ExplorerTelemetryQuery.NoInstrumentsDeclared, Is.Empty);
}
