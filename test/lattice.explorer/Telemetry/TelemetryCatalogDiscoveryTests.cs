using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.Plugins.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// Covers catalogue discovery: a panel is driven by what the <em>server</em>
/// offers, and everything the server said about an entry - its title, unit,
/// measurement semantic, accepted parameters, declared bounds, and the
/// instruments behind it - survives the projection intact.
/// <para>
/// This is the whole point of discovery: if any of it were dropped or invented
/// here, a panel's label could drift from the instrument it is drawing.
/// </para>
/// </summary>
[TestFixture]
public class TelemetryCatalogDiscoveryTests
{
    private FakeTelemetryQueryClient _client = null!;

    [SetUp]
    public void SetUp() => _client = new FakeTelemetryQueryClient();

    private TelemetryQueryService Create() => new(_client);

    [Test]
    public void Constructor_rejects_a_null_client() =>
        Assert.That(() => new TelemetryQueryService(null!), Throws.ArgumentNullException);

    [Test]
    public async Task The_server_authored_catalogue_is_read_and_projected()
    {
        var result = await Create().GetCatalogAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value, Is.Not.Null);
            Assert.That(result.Value!.Version, Is.EqualTo(3));
            Assert.That(result.Value.Count, Is.EqualTo(2));
            Assert.That(result.Value.IsEmpty, Is.False);
            Assert.That(_client.CatalogCallCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Every_descriptor_field_the_panel_renders_survives_the_projection()
    {
        var catalog = (await Create().GetCatalogAsync()).Value!;

        Assert.That(catalog.TryGetQuery(SampleTelemetry.RangeQueryId, out var query), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(query!.Title, Is.EqualTo("Operation rate"));
            Assert.That(query.Description, Is.EqualTo("Completed operations per second."));
            Assert.That(query.Unit, Is.EqualTo("ops/s"));
            Assert.That(query.Kind, Is.EqualTo(ExplorerTelemetryQueryKind.Range));
            Assert.That(
                query.Semantic,
                Is.EqualTo(ExplorerTelemetrySemantic.PerOperation),
                "the semantic is why the catalogue is read rather than hard-coded");
        });
    }

    [Test]
    public async Task The_declared_parameters_arrive_flag_for_flag()
    {
        var catalog = (await Create().GetCatalogAsync()).Value!;
        catalog.TryGetQuery(SampleTelemetry.RangeQueryId, out var range);
        catalog.TryGetQuery(SampleTelemetry.InstantQueryId, out var instant);

        Assert.Multiple(() =>
        {
            Assert.That(range!.Accepts(ExplorerTelemetryParameters.TimeRange), Is.True);
            Assert.That(range.Accepts(ExplorerTelemetryParameters.Step), Is.True);
            Assert.That(range.Accepts(ExplorerTelemetryParameters.TreeFilter), Is.True);
            Assert.That(instant!.Parameters, Is.EqualTo(ExplorerTelemetryParameters.None));
            Assert.That(instant.Accepts(ExplorerTelemetryParameters.TimeRange), Is.False);
            Assert.That(
                instant.Accepts(ExplorerTelemetryParameters.None),
                Is.False,
                "the empty flag set is never 'accepted'");
        });
    }

    [Test]
    public async Task The_declared_bounds_arrive_intact_so_a_panel_builds_legal_controls()
    {
        var catalog = (await Create().GetCatalogAsync()).Value!;
        catalog.TryGetQuery(SampleTelemetry.RangeQueryId, out var query);

        Assert.Multiple(() =>
        {
            Assert.That(query!.Bounds.MinStep, Is.EqualTo(TimeSpan.FromSeconds(15)));
            Assert.That(query.Bounds.MaxStep, Is.EqualTo(TimeSpan.FromHours(1)));
            Assert.That(query.Bounds.DefaultStep, Is.EqualTo(TimeSpan.FromMinutes(1)));
            Assert.That(query.Bounds.MaxRange, Is.EqualTo(TimeSpan.FromHours(24)));
            Assert.That(query.Bounds.MaxLookback, Is.EqualTo(TimeSpan.FromDays(7)));
            Assert.That(query.Bounds.MaxPoints, Is.EqualTo(1440));
            Assert.That(query.Bounds.IsUnbounded, Is.False);
        });
    }

    [Test]
    public async Task An_entry_that_declares_no_bounds_projects_as_unbounded()
    {
        var catalog = (await Create().GetCatalogAsync()).Value!;
        catalog.TryGetQuery(SampleTelemetry.InstantQueryId, out var query);

        Assert.Multiple(() =>
        {
            Assert.That(query!.Bounds.IsUnbounded, Is.True);
            Assert.That(query.Bounds, Is.EqualTo(ExplorerTelemetryBounds.Unbounded));
        });
    }

    [Test]
    public async Task The_instruments_behind_an_entry_arrive_named()
    {
        var catalog = (await Create().GetCatalogAsync()).Value!;
        catalog.TryGetQuery(SampleTelemetry.RangeQueryId, out var query);

        Assert.Multiple(() =>
        {
            Assert.That(query!.Instruments, Has.Count.EqualTo(1));
            Assert.That(query.Instruments[0].Name, Is.EqualTo("lattice.ops.completed"));
            Assert.That(query.Instruments[0].Meter, Is.EqualTo("Orleans.Lattice"));
            Assert.That(query.Instruments[0].Unit, Is.EqualTo("ops"));
            Assert.That(query.Instruments[0].Semantic, Is.EqualTo(ExplorerTelemetrySemantic.PerOperation));
            Assert.That(query.ReadsInstrument("lattice.ops.completed"), Is.True);
            Assert.That(query.ReadsInstrument("something.else"), Is.False);
            Assert.That(() => query.ReadsInstrument(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task An_entry_naming_no_instruments_reports_an_empty_list()
    {
        var catalog = (await Create().GetCatalogAsync()).Value!;
        catalog.TryGetQuery(SampleTelemetry.InstantQueryId, out var query);

        Assert.Multiple(() =>
        {
            Assert.That(query!.Instruments, Is.Empty);
            Assert.That(query.ReadsInstrument("anything"), Is.False);
        });
    }

    [Test]
    public async Task An_empty_catalogue_is_a_success_not_a_failure()
    {
        // The facade reports an empty catalogue both for a cluster with no backend
        // and for a caller entitled to nothing, deliberately indistinguishably.
        // Turning either into an error here would invent a distinction the facade
        // refuses to make.
        _client.CatalogResult = TelemetryQueryCatalog.Empty;

        var result = await Create().GetCatalogAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value!.IsEmpty, Is.True);
            Assert.That(result.Value.Count, Is.Zero);
        });
    }

    [Test]
    public async Task A_successful_read_is_remembered_so_a_polling_panel_discovers_once()
    {
        var service = Create();

        await service.GetCatalogAsync();
        await service.GetCatalogAsync();
        await service.GetCatalogAsync();

        Assert.That(_client.CatalogCallCount, Is.EqualTo(1));
    }

    [Test]
    public async Task The_remembered_catalogue_is_the_same_instance_so_it_costs_nothing_to_reread()
    {
        var service = Create();

        var first = await service.GetCatalogAsync();
        var second = await service.GetCatalogAsync();

        Assert.That(second, Is.SameAs(first));
    }

    [Test]
    public async Task A_refresh_re_reads_from_the_cluster()
    {
        var service = Create();
        await service.GetCatalogAsync();

        var refreshed = await service.RefreshCatalogAsync();

        Assert.Multiple(() =>
        {
            Assert.That(refreshed.IsSuccess, Is.True);
            Assert.That(_client.CatalogCallCount, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task A_refresh_picks_up_a_changed_offering()
    {
        var service = Create();
        await service.GetCatalogAsync();
        _client.CatalogResult = TelemetryQueryCatalog.Empty;

        var refreshed = await service.RefreshCatalogAsync();
        var afterwards = await service.GetCatalogAsync();

        Assert.Multiple(() =>
        {
            Assert.That(refreshed.Value!.IsEmpty, Is.True);
            Assert.That(afterwards.Value!.IsEmpty, Is.True, "the refreshed catalogue replaces the remembered one");
            Assert.That(_client.CatalogCallCount, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task A_failed_read_is_not_remembered_so_an_outage_does_not_pin_the_panel()
    {
        var service = Create();
        _client.CatalogThrows = new TelemetryUnavailableException("not served");

        var failed = await service.GetCatalogAsync();
        _client.CatalogThrows = null;
        var recovered = await service.GetCatalogAsync();

        Assert.Multiple(() =>
        {
            Assert.That(failed.IsSuccess, Is.False);
            Assert.That(recovered.IsSuccess, Is.True);
            Assert.That(_client.CatalogCallCount, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task A_catalogue_lookup_answers_by_ordinal_id()
    {
        var catalog = (await Create().GetCatalogAsync()).Value!;

        Assert.Multiple(() =>
        {
            Assert.That(catalog.Contains(SampleTelemetry.RangeQueryId), Is.True);
            Assert.That(catalog.Contains(SampleTelemetry.UnknownQueryId), Is.False);
            Assert.That(catalog.TryGetQuery(SampleTelemetry.UnknownQueryId, out var missing), Is.False);
            Assert.That(missing, Is.Null);
            Assert.That(
                catalog.Contains(SampleTelemetry.RangeQueryId.ToUpperInvariant()),
                Is.False,
                "ids are compared ordinally, as the facade compares them");
        });
    }

    [Test]
    public void The_catalogue_lookup_rejects_a_null_id() =>
        Assert.Multiple(() =>
        {
            Assert.That(
                () => ExplorerTelemetryCatalog.Empty.Contains(null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => ExplorerTelemetryCatalog.Empty.TryGetQuery(null!, out _),
                Throws.ArgumentNullException);
        });

    [Test]
    public void The_shared_empty_catalogue_offers_nothing() =>
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerTelemetryCatalog.Empty.IsEmpty, Is.True);
            Assert.That(ExplorerTelemetryCatalog.Empty.Count, Is.Zero);
            Assert.That(ExplorerTelemetryCatalog.Empty.Version, Is.Zero);
            Assert.That(ExplorerTelemetryCatalog.Empty, Is.SameAs(ExplorerTelemetryCatalog.Empty));
        });
}
