namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Covers the facade's discovery and evaluation paths: catalogue entitlement, query
/// selection by id, and the shape of a mapped response.
/// </summary>
[TestFixture]
public sealed partial class LatticeTelemetryTests
{
    private const string ReadRate = "tree.read.operation_rate";
    private const string StorageBytes = "tree.storage.bytes";
    private const string TenantUsage = "tenant.usage.bytes";

    [Test]
    public async Task GetCatalogAsync_serves_every_authored_entry_to_an_entitled_caller()
    {
        var catalog = await new TelemetryFacadeHarness().Build().GetCatalogAsync();

        Assert.Multiple(() =>
        {
            Assert.That(catalog.Count, Is.EqualTo(LatticeTelemetryQueries.Definitions.Count));
            Assert.That(catalog.Contains(ReadRate), Is.True);
            Assert.That(catalog.Version, Is.EqualTo(LatticeTelemetryQueries.Version));
        });
    }

    [Test]
    public async Task GetCatalogAsync_serves_entries_in_ascending_id_order()
    {
        var catalog = await new TelemetryFacadeHarness().Build().GetCatalogAsync();
        var ids = catalog.Queries.Select(q => q.QueryId).ToArray();

        Assert.That(ids, Is.EqualTo(ids.OrderBy(id => id, StringComparer.Ordinal).ToArray()));
    }

    [Test]
    public async Task GetCatalogAsync_degrades_to_empty_for_a_caller_without_the_telemetry_capability()
    {
        var facade = new TelemetryFacadeHarness().WithGate(new StubAccessGate()).Build();

        var catalog = await facade.GetCatalogAsync();

        Assert.That(catalog, Is.SameAs(TelemetryQueryCatalog.Empty),
            "A caller entitled to no query renders no panels rather than seeing an error.");
    }

    [Test]
    public async Task GetCatalogAsync_degrades_to_empty_when_no_backend_is_configured()
    {
        var facade = new TelemetryFacadeHarness()
            .WithOptions(options => options.BackendAddress = null)
            .Build();

        var catalog = await facade.GetCatalogAsync();

        Assert.That(catalog, Is.SameAs(TelemetryQueryCatalog.Empty));
    }

    [Test]
    public void QueryAsync_reports_an_unconfigured_backend_as_an_unavailable_query()
    {
        var facade = new TelemetryFacadeHarness()
            .WithOptions(options => options.BackendAddress = null)
            .Build();

        Assert.That(
            async () => await facade.QueryAsync(TelemetryFacadeHarness.RangeRequest(ReadRate)),
            Throws.TypeOf<TelemetryQueryNotFoundException>(),
            "Discovery offers nothing on an unconfigured cluster, so execution must offer nothing "
            + "either.");
    }

    [Test]
    public void QueryAsync_rejects_an_unknown_query_id()
    {
        var facade = new TelemetryFacadeHarness().Build();

        Assert.That(
            async () => await facade.QueryAsync(TelemetryFacadeHarness.RangeRequest("no.such.query")),
            Throws.TypeOf<TelemetryQueryNotFoundException>());
    }

    [Test]
    public void QueryAsync_rejects_an_unentitled_query_id_indistinguishably()
    {
        var facade = new TelemetryFacadeHarness()
            .WithOptions(options =>
            {
                options.MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed;
                options.AllowedMetrics.Add("orleans_lattice_storage_total_bytes");
            })
            .Build();

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await facade.QueryAsync(TelemetryFacadeHarness.RangeRequest(ReadRate)),
                Throws.TypeOf<TelemetryQueryNotFoundException>(),
                "An entry outside the allow-list must report exactly what a non-existent id does.");
            Assert.That(
                async () => await facade.QueryAsync(TelemetryFacadeHarness.InstantRequest(StorageBytes)),
                Throws.Nothing);
        });
    }

    [Test]
    public void QueryAsync_denies_a_caller_without_the_telemetry_capability()
    {
        var facade = new TelemetryFacadeHarness().WithGate(new StubAccessGate()).Build();

        Assert.That(
            async () => await facade.QueryAsync(TelemetryFacadeHarness.RangeRequest(ReadRate)),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void QueryAsync_rejects_a_null_request()
    {
        var facade = new TelemetryFacadeHarness().Build();

        Assert.That(async () => await facade.QueryAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task QueryAsync_echoes_the_query_id_and_the_evaluated_window()
    {
        var harness = new TelemetryFacadeHarness();
        var request = TelemetryFacadeHarness.RangeRequest(ReadRate);

        var response = await harness.Build().QueryAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(response.QueryId, Is.EqualTo(ReadRate));
            Assert.That(response.Range.StartUtc, Is.EqualTo(request.Range.StartUtc));
            Assert.That(response.Range.EndUtc, Is.EqualTo(request.Range.EndUtc));
            Assert.That(response.Range.Step, Is.EqualTo(TimeSpan.FromMinutes(1)));
        });
    }

    [Test]
    public async Task QueryAsync_maps_a_matrix_result_into_series_and_points()
    {
        var harness = new TelemetryFacadeHarness();
        harness.Backend.Response = RecordingPrometheusQueryClient.Success("""
            {
              "resultType": "matrix",
              "result": [
                {
                  "metric": { "tree": "t/acme/orders", "tenant": "acme" },
                  "values": [[1767182400, "1.5"], [1767182460, "2.5"]]
                }
              ]
            }
            """);

        var response = await harness.Build().QueryAsync(TelemetryFacadeHarness.RangeRequest(ReadRate));

        Assert.Multiple(() =>
        {
            Assert.That(response.ResultKind, Is.EqualTo(TelemetryResultKind.Matrix));
            Assert.That(response.SeriesCount, Is.EqualTo(1));
            Assert.That(response.IsEmpty, Is.False);
            Assert.That(response.Series[0].Points, Has.Count.EqualTo(2));
            Assert.That(response.Series[0].Points[1].Value, Is.EqualTo(2.5));
            Assert.That(response.Series[0].TryGetLabel("tenant", out var tenant), Is.True);
            Assert.That(tenant, Is.EqualTo("acme"));
        });
    }

    [Test]
    public async Task QueryAsync_reports_an_empty_match_as_an_empty_response()
    {
        var response = await new TelemetryFacadeHarness()
            .Build()
            .QueryAsync(TelemetryFacadeHarness.RangeRequest(ReadRate));

        Assert.Multiple(() =>
        {
            Assert.That(response.IsEmpty, Is.True);
            Assert.That(response.ResultKind, Is.EqualTo(TelemetryResultKind.Vector));
        });
    }

    [Test]
    public void QueryAsync_surfaces_a_backend_fault_rather_than_reporting_no_data()
    {
        var harness = new TelemetryFacadeHarness();
        harness.Backend.Fault = new HttpRequestException("connection refused");

        Assert.That(
            async () => await harness.Build().QueryAsync(TelemetryFacadeHarness.RangeRequest(ReadRate)),
            Throws.TypeOf<TelemetryBackendException>()
                .With.Property(nameof(TelemetryBackendException.QueryId)).EqualTo(ReadRate),
            "A panel that renders 'no data' when the backend is down misreports an outage as a "
            + "quiet cluster.");
    }

    [Test]
    public void QueryAsync_surfaces_a_non_success_backend_status()
    {
        var harness = new TelemetryFacadeHarness();
        harness.Backend.Response = new PrometheusQueryResponse("error", default);

        Assert.That(
            async () => await harness.Build().QueryAsync(TelemetryFacadeHarness.RangeRequest(ReadRate)),
            Throws.TypeOf<TelemetryBackendException>());
    }

    [Test]
    public async Task QueryAsync_sends_an_instant_query_for_an_instant_entry()
    {
        var harness = new TelemetryFacadeHarness();

        await harness.Build().QueryAsync(TelemetryFacadeHarness.InstantRequest(StorageBytes));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Backend.LastWasRange, Is.False);
            Assert.That(harness.Backend.LastEnd, Is.EqualTo(FixedTimeProvider.Instant));
        });
    }

    [Test]
    public async Task QueryAsync_serves_a_tenancy_meter_entry_without_the_tenancy_add_on()
    {
        var harness = new TelemetryFacadeHarness().WithTenantResolver(NullTelemetryTenantContext.Instance);

        var response = await harness.Build().QueryAsync(TelemetryFacadeHarness.InstantRequest(TenantUsage));

        Assert.Multiple(() =>
        {
            Assert.That(response.IsEmpty, Is.True,
                "A cluster not publishing the tenancy meter returns no series, which is an empty "
                + "result rather than an error.");
            Assert.That(harness.Backend.SingleQuery, Does.Contain("""tenant="default","""));
        });
    }

    [Test]
    public void The_facade_rejects_null_dependencies()
    {
        var harness = new TelemetryFacadeHarness();
        var catalog = harness.BuildCatalog();
        var authorizer = new TelemetryAccessAuthorizer();
        var scopes = new TelemetryTenantScopeResolver(NullTelemetryTenantContext.Instance, authorizer);
        var options = Microsoft.Extensions.Options.Options.Create(new LatticeTelemetryOptions());

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new LatticeTelemetry(null!, scopes, authorizer, harness.Backend, options),
                Throws.ArgumentNullException);
            Assert.That(
                () => new LatticeTelemetry(catalog, null!, authorizer, harness.Backend, options),
                Throws.ArgumentNullException);
            Assert.That(
                () => new LatticeTelemetry(catalog, scopes, null!, harness.Backend, options),
                Throws.ArgumentNullException);
            Assert.That(
                () => new LatticeTelemetry(catalog, scopes, authorizer, null!, options),
                Throws.ArgumentNullException);
            Assert.That(
                () => new LatticeTelemetry(catalog, scopes, authorizer, harness.Backend, null!),
                Throws.ArgumentNullException);
        });
    }
}
