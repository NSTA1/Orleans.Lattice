using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Telemetry.Grpc.Tests;

/// <summary>
/// Coverage for the client type's own contract: its construction guards, its
/// argument validation, the method definitions it drives, and the allocation shape
/// of its two field-less probe requests.
/// </summary>
[TestFixture]
public sealed class LatticeTelemetryApiGrpcClientTests
{
    private ServiceProvider _serializers = null!;

    [SetUp]
    public void SetUp() => _serializers = TelemetryGrpcTestSupport.Serializers();

    [TearDown]
    public void TearDown() => _serializers.Dispose();

    private LatticeTelemetryApiGrpcClient Client()
        => new(
            new LoopbackCallInvoker(
                TelemetryGrpcTestSupport.Service(_serializers, new FakeTelemetry()),
                _serializers),
            TelemetryGrpcTestSupport.Methods(_serializers));

    [Test]
    public void Create_rejects_a_null_call_invoker()
        => Assert.That(
            () => LatticeTelemetryApiGrpcClient.Create(null!, _serializers),
            Throws.ArgumentNullException);

    [Test]
    public void Create_rejects_a_null_serializer_provider()
        => Assert.That(
            () => LatticeTelemetryApiGrpcClient.Create(
                new LoopbackCallInvoker(
                    TelemetryGrpcTestSupport.Service(_serializers, new FakeTelemetry()),
                    _serializers),
                null!),
            Throws.ArgumentNullException);

    [Test]
    public void Create_builds_a_usable_client_from_a_serializer_provider()
    {
        var client = LatticeTelemetryApiGrpcClient.Create(
            new LoopbackCallInvoker(
                TelemetryGrpcTestSupport.Service(_serializers, new FakeTelemetry()),
                _serializers),
            _serializers);

        Assert.That(client, Is.Not.Null);
    }

    [Test]
    public void QueryAsync_rejects_a_null_request()
        => Assert.That(() => Client().QueryAsync(null!), Throws.ArgumentNullException);

    [Test]
    public async Task A_cancelled_token_surfaces_as_a_cancelled_status()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var exception = Assert.ThrowsAsync<RpcException>(
            () => Client().QueryAsync(new TelemetryQueryRequest { QueryId = "lattice.ops.rate" }, cts.Token));

        Assert.That(exception!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
    }

    [Test]
    public void The_methods_carry_the_documented_service_and_method_names()
    {
        var methods = TelemetryGrpcTestSupport.Methods(_serializers);

        Assert.Multiple(() =>
        {
            Assert.That(LatticeTelemetryGrpcMethods.ServiceName, Is.EqualTo("orleans.lattice.api.telemetry"));
            Assert.That(methods.GetCatalog.FullName, Is.EqualTo("/orleans.lattice.api.telemetry/GetCatalog"));
            Assert.That(methods.Query.FullName, Is.EqualTo("/orleans.lattice.api.telemetry/Query"));
            Assert.That(methods.GetAuthScheme.FullName, Is.EqualTo("/orleans.lattice.api.telemetry/GetAuthScheme"));
        });
    }

    [Test]
    public void Every_method_is_a_unary_rpc()
    {
        var methods = TelemetryGrpcTestSupport.Methods(_serializers);

        Assert.Multiple(() =>
        {
            Assert.That(methods.GetCatalog.Type, Is.EqualTo(MethodType.Unary));
            Assert.That(methods.Query.Type, Is.EqualTo(MethodType.Unary));
            Assert.That(methods.GetAuthScheme.Type, Is.EqualTo(MethodType.Unary));
        });
    }

    [Test]
    public void The_query_rpc_carries_the_contract_request_type_unchanged()
    {
        var methods = TelemetryGrpcTestSupport.Methods(_serializers);

        Assert.Multiple(() =>
        {
            Assert.That(
                methods.Query.GetType().GetGenericArguments()[0],
                Is.EqualTo(typeof(TelemetryQueryRequest)),
                "Reusing the contract's request type is what keeps a query-text field off the wire.");
            Assert.That(
                methods.Query.GetType().GetGenericArguments()[1],
                Is.EqualTo(typeof(TelemetryQueryResponse)));
        });
    }

    [Test]
    public void FromServiceProvider_rejects_a_null_provider()
        => Assert.That(
            () => LatticeTelemetryGrpcMethods.FromServiceProvider(null!),
            Throws.ArgumentNullException);

    [Test]
    public void The_methods_constructor_rejects_a_null_serializer()
        => Assert.That(
            () => new LatticeTelemetryGrpcMethods(null!, null!, null!, null!, null!, null!),
            Throws.ArgumentNullException);

    [Test]
    public async Task The_field_less_probes_reuse_one_cached_request_instance()
    {
        // The two probes carry no per-call state, so allocating one per call would
        // be pure garbage on the hottest, cheapest calls on the surface.
        var capturing = new CapturingInvoker(
            TelemetryGrpcTestSupport.Service(_serializers, new FakeTelemetry()),
            _serializers);
        var client = new LatticeTelemetryApiGrpcClient(capturing, TelemetryGrpcTestSupport.Methods(_serializers));

        await client.GetCatalogAsync();
        await client.GetCatalogAsync();
        await client.GetAuthSchemeAsync();
        await client.GetAuthSchemeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(capturing.CatalogRequests, Has.Count.EqualTo(2));
            Assert.That(capturing.CatalogRequests[0], Is.SameAs(capturing.CatalogRequests[1]));
            Assert.That(capturing.AuthSchemeRequests, Has.Count.EqualTo(2));
            Assert.That(capturing.AuthSchemeRequests[0], Is.SameAs(capturing.AuthSchemeRequests[1]));
        });
    }

    /// <summary>
    /// A loopback invoker that additionally records the exact request instances the
    /// client handed it, so reference reuse of the field-less probes is observable.
    /// </summary>
    private sealed class CapturingInvoker(
        LatticeTelemetryGrpcServiceBase service,
        IServiceProvider serializers)
        : CallInvoker
    {
        private readonly LoopbackCallInvoker _inner = new(service, serializers);

        public List<TelemetryCatalogRequest> CatalogRequests { get; } = [];

        public List<AuthSchemeAdvertisementRequest> AuthSchemeRequests { get; } = [];

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options,
            TRequest request)
        {
            switch (request)
            {
                case TelemetryCatalogRequest catalogRequest:
                    CatalogRequests.Add(catalogRequest);
                    break;
                case AuthSchemeAdvertisementRequest authSchemeRequest:
                    AuthSchemeRequests.Add(authSchemeRequest);
                    break;
            }

            return _inner.AsyncUnaryCall(method, host, options, request);
        }

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new NotSupportedException();

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new NotSupportedException();

        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options) =>
            throw new NotSupportedException();

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options) =>
            throw new NotSupportedException();
    }
}
