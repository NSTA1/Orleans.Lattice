using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Telemetry.Grpc.Tests;

/// <summary>
/// End-to-end round-trip coverage for every RPC the telemetry binding exposes,
/// driven through the in-memory <see cref="LoopbackCallInvoker"/> so each call
/// crosses the real client mapping, the real Orleans wire encoding, the real
/// server-side service, and back - without a network, a host, or a clock.
/// </summary>
[TestFixture]
public sealed class LatticeTelemetryGrpcRoundTripTests
{
    private ServiceProvider _serializers = null!;
    private FakeTelemetry _facade = null!;
    private LatticeTelemetryGrpcService _service = null!;
    private LatticeTelemetryApiGrpcClient _client = null!;

    [SetUp]
    public void SetUp()
    {
        _serializers = TelemetryGrpcTestSupport.Serializers();
        _facade = new FakeTelemetry();
        _service = TelemetryGrpcTestSupport.Service(_serializers, _facade);
        _client = new LatticeTelemetryApiGrpcClient(
            new LoopbackCallInvoker(_service, _serializers),
            TelemetryGrpcTestSupport.Methods(_serializers));
    }

    [TearDown]
    public void TearDown() => _serializers.Dispose();

    [Test]
    public async Task GetCatalog_round_trips_the_curated_catalogue()
    {
        var catalog = await _client.GetCatalogAsync();

        Assert.Multiple(() =>
        {
            Assert.That(catalog.Version, Is.EqualTo(7));
            Assert.That(catalog.Count, Is.EqualTo(1));
            Assert.That(catalog.Contains("lattice.ops.rate"), Is.True);
            Assert.That(_facade.CatalogCallCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task GetCatalog_round_trips_every_descriptor_field()
    {
        var catalog = await _client.GetCatalogAsync();
        Assert.That(catalog.TryGetQuery("lattice.ops.rate", out var descriptor), Is.True);

        Assert.Multiple(() =>
        {
            Assert.That(descriptor!.Title, Is.EqualTo("Operation rate"));
            Assert.That(descriptor.Description, Is.EqualTo("Operations per second across the caller's trees."));
            Assert.That(descriptor.Unit, Is.EqualTo("ops/s"));
            Assert.That(descriptor.Kind, Is.EqualTo(TelemetryQueryKind.Range));
            Assert.That(descriptor.Semantic, Is.EqualTo(TelemetryMeasurementSemantic.PerOperation));
            Assert.That(descriptor.Parameters, Is.EqualTo(TelemetryQueryParameters.TimeRange | TelemetryQueryParameters.Step));
            Assert.That(descriptor.Bounds.MinStep, Is.EqualTo(TimeSpan.FromSeconds(15)));
            Assert.That(descriptor.Bounds.MaxPoints, Is.EqualTo(1_000));
            Assert.That(descriptor.Instruments, Has.Count.EqualTo(1));
            Assert.That(descriptor.Instruments[0].Name, Is.EqualTo("lattice.ops"));
            Assert.That(descriptor.Instruments[0].Meter, Is.EqualTo("Orleans.Lattice"));
        });
    }

    [Test]
    public async Task GetCatalog_round_trips_an_empty_catalogue()
    {
        _facade.Catalog = TelemetryQueryCatalog.Empty;

        var catalog = await _client.GetCatalogAsync();

        Assert.Multiple(() =>
        {
            Assert.That(catalog.Version, Is.Zero);
            Assert.That(catalog.Count, Is.Zero);
        });
    }

    [Test]
    public async Task Query_round_trips_a_defaulted_request_with_no_window()
    {
        // What a real client actually sends: an id and nothing else. The facade
        // defaults the window server-side, so the binding must carry an unset
        // TelemetryTimeRange faithfully rather than pre-filling one of its own.
        var response = await _client.QueryAsync(new TelemetryQueryRequest { QueryId = "lattice.ops.rate" });

        Assert.Multiple(() =>
        {
            Assert.That(response.QueryId, Is.EqualTo("lattice.ops.rate"));
            Assert.That(_facade.LastRequest, Is.Not.Null);
            Assert.That(
                _facade.LastRequest!.Range,
                Is.EqualTo(default(TelemetryTimeRange)),
                "An unset window must reach the facade unset, so its own defaulting rules apply.");
            Assert.That(_facade.LastRequest.TreeId, Is.Null);
            Assert.That(_facade.LastRequest.RequestedTenantId, Is.Null);
            Assert.That(
                _facade.LastRequest.RequestedVisibility,
                Is.EqualTo(TelemetryTenantVisibility.ActiveTenant),
                "The fail-closed default visibility must survive the wire as itself.");
        });
    }

    [Test]
    public async Task A_defaulted_request_still_reports_the_window_the_facade_evaluated()
    {
        var response = await _client.QueryAsync(new TelemetryQueryRequest { QueryId = "lattice.ops.rate" });

        Assert.Multiple(() =>
        {
            Assert.That(
                response.Range,
                Is.Not.EqualTo(default(TelemetryTimeRange)),
                "The response echoes the window actually evaluated, so a client renders the axis "
                + "it really received rather than the one it omitted.");
            Assert.That(response.Range.Step, Is.EqualTo(TimeSpan.FromSeconds(30)));
        });
    }

    [Test]
    public async Task Query_round_trips_the_selection_and_the_series()
    {
        var response = await _client.QueryAsync(new TelemetryQueryRequest
        {
            QueryId = "lattice.ops.rate",
            Range = TelemetryTimeRange.Between(
                DateTimeOffset.UnixEpoch,
                DateTimeOffset.UnixEpoch.AddMinutes(1),
                TimeSpan.FromSeconds(30)),
            TreeId = "t/acme/orders",
        });

        Assert.Multiple(() =>
        {
            Assert.That(response.QueryId, Is.EqualTo("lattice.ops.rate"));
            Assert.That(response.ResultKind, Is.EqualTo(TelemetryResultKind.Matrix));
            Assert.That(response.SeriesCount, Is.EqualTo(1));
            Assert.That(response.IsEmpty, Is.False);
            Assert.That(response.Series[0].Points, Has.Count.EqualTo(2));
            Assert.That(response.Series[0].Points[1].Value, Is.EqualTo(2.5));
            Assert.That(response.Range.Step, Is.EqualTo(TimeSpan.FromSeconds(30)));
        });
    }

    [Test]
    public async Task Query_forwards_the_request_to_the_facade_verbatim()
    {
        var range = TelemetryTimeRange.Between(
            DateTimeOffset.UnixEpoch,
            DateTimeOffset.UnixEpoch.AddHours(1),
            TimeSpan.FromMinutes(1));

        await _client.QueryAsync(new TelemetryQueryRequest
        {
            QueryId = "lattice.ops.rate",
            Range = range,
            TreeId = "t/acme/orders",
            RequestedVisibility = TelemetryTenantVisibility.SingleTenant,
            RequestedTenantId = "other-tenant",
        });

        Assert.Multiple(() =>
        {
            Assert.That(_facade.LastRequest, Is.Not.Null);
            Assert.That(_facade.LastRequest!.QueryId, Is.EqualTo("lattice.ops.rate"));
            Assert.That(_facade.LastRequest.Range, Is.EqualTo(range));
            Assert.That(_facade.LastRequest.TreeId, Is.EqualTo("t/acme/orders"));
            Assert.That(_facade.LastRequest.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.SingleTenant));
            Assert.That(
                _facade.LastRequest.RequestedTenantId,
                Is.EqualTo("other-tenant"),
                "The binding forwards the caller's requested tenant untouched; validating it is the facade's job.");
        });
    }

    [Test]
    public async Task Query_returns_the_scope_the_facade_pinned_not_the_one_requested()
    {
        var response = await _client.QueryAsync(new TelemetryQueryRequest
        {
            QueryId = "lattice.ops.rate",
            RequestedVisibility = TelemetryTenantVisibility.AllTenants,
        });

        Assert.Multiple(() =>
        {
            Assert.That(
                response.Scope.EffectiveVisibility,
                Is.EqualTo(TelemetryTenantVisibility.ActiveTenant),
                "The transport must surface the facade's decision, never the caller's request.");
            Assert.That(response.Scope.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.AllTenants));
            Assert.That(response.Scope.TenantId, Is.EqualTo(FakeTelemetry.PinnedTenantId));
            Assert.That(response.Scope.WasDowngraded, Is.True);
            Assert.That(response.Scope.IsCrossTenant, Is.False);
        });
    }

    [Test]
    public async Task Query_round_trips_an_honoured_cross_tenant_scope_unchanged()
    {
        var service = TelemetryGrpcTestSupport.Service(_serializers, new CrossTenantTelemetry());
        var client = new LatticeTelemetryApiGrpcClient(
            new LoopbackCallInvoker(service, _serializers),
            TelemetryGrpcTestSupport.Methods(_serializers));

        var response = await client.QueryAsync(new TelemetryQueryRequest
        {
            QueryId = "lattice.ops.rate",
            RequestedVisibility = TelemetryTenantVisibility.AllTenants,
        });

        Assert.Multiple(() =>
        {
            Assert.That(response.Scope.EffectiveVisibility, Is.EqualTo(TelemetryTenantVisibility.AllTenants));
            Assert.That(response.Scope.TenantId, Is.Null);
            Assert.That(response.Scope.IsCrossTenant, Is.True);
            Assert.That(response.Scope.WasDowngraded, Is.False);
        });
    }

    [Test]
    public async Task GetAuthScheme_round_trips_the_advertisement()
    {
        var service = TelemetryGrpcTestSupport.Service(
            _serializers,
            _facade,
            authSchemeSource: new FixedAuthSchemeSource(new AuthSchemeAdvertisement
            {
                Schemes =
                [
                    new AuthSchemeDescriptor
                    {
                        SchemeId = "entra",
                        DisplayName = "Microsoft Entra ID",
                        Parameters = new Dictionary<string, string>(StringComparer.Ordinal)
                        {
                            ["authority"] = "https://login.microsoftonline.com/contoso",
                            ["clientId"] = "00000000-0000-0000-0000-000000000001",
                        },
                    },
                ],
            }));
        var client = new LatticeTelemetryApiGrpcClient(
            new LoopbackCallInvoker(service, _serializers),
            TelemetryGrpcTestSupport.Methods(_serializers));

        var advertisement = await client.GetAuthSchemeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(advertisement.Schemes, Has.Count.EqualTo(1));
            Assert.That(advertisement.Schemes[0].SchemeId, Is.EqualTo("entra"));
            Assert.That(advertisement.Schemes[0].DisplayName, Is.EqualTo("Microsoft Entra ID"));
            Assert.That(advertisement.Schemes[0].Parameters["authority"],
                Is.EqualTo("https://login.microsoftonline.com/contoso"));
        });
    }

    [Test]
    public async Task GetAuthScheme_round_trips_an_empty_advertisement()
    {
        var advertisement = await _client.GetAuthSchemeAsync();

        Assert.That(advertisement.Schemes, Is.Empty);
    }

    [Test]
    public async Task GetAuthScheme_bridges_no_credential()
    {
        var bridge = new ThrowingCredentialBridge();
        var service = TelemetryGrpcTestSupport.Service(_serializers, _facade, credentialBridge: bridge);

        var advertisement = await service.GetAuthScheme(
            new AuthSchemeAdvertisementRequest(),
            new FakeServerCallContext(
                TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.GetAuthSchemeMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(advertisement, Is.Not.Null);
            Assert.That(
                bridge.CallCount,
                Is.Zero,
                "The advertisement RPC is unauthenticated, so it must not touch the credential bridge.");
        });
    }

    [Test]
    public void Service_rejects_a_null_request_on_every_rpc()
    {
        var context = new FakeServerCallContext(
            TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.QueryMethodName));

        Assert.Multiple(() =>
        {
            Assert.That(() => _service.GetCatalog(null!, context), Throws.ArgumentNullException);
            Assert.That(() => _service.Query(null!, context), Throws.ArgumentNullException);
            Assert.That(() => _service.GetAuthScheme(null!, context), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Service_rejects_a_null_context_on_every_rpc()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => _service.GetCatalog(new TelemetryCatalogRequest(), null!), Throws.ArgumentNullException);
            Assert.That(
                () => _service.Query(new TelemetryQueryRequest { QueryId = "q" }, null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => _service.GetAuthScheme(new AuthSchemeAdvertisementRequest(), null!),
                Throws.ArgumentNullException);
        });
    }

    /// <summary>A facade that reports an honoured cross-tenant evaluation.</summary>
    private sealed class CrossTenantTelemetry : ILatticeTelemetry
    {
        public Task<TelemetryQueryCatalog> GetCatalogAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(TelemetryQueryCatalog.Empty);

        public Task<TelemetryQueryResponse> QueryAsync(
            TelemetryQueryRequest request,
            CancellationToken cancellationToken = default)
            => Task.FromResult(new TelemetryQueryResponse
            {
                QueryId = request.QueryId,
                Scope = TelemetryTenantScope.AcrossAllTenants(),
                ResultKind = TelemetryResultKind.Empty,
                Series = [],
            });
    }

    /// <summary>A bridge that records calls and would fault if one were made.</summary>
    private sealed class ThrowingCredentialBridge : ILatticeTelemetryApiCredentialBridge
    {
        public int CallCount { get; private set; }

        public LatticeCredential? Resolve(ServerCallContext context)
        {
            CallCount++;
            return null;
        }
    }
}
