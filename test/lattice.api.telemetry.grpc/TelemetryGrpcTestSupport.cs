using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Telemetry.Grpc.Tests;

/// <summary>
/// Configurable in-memory <see cref="ILatticeTelemetry"/> facade for the gRPC
/// service tests. Either returns a canned catalogue and query response or throws a
/// pre-seeded exception, so the service's result-mapping and its
/// exception-to-<see cref="StatusCode"/> translation can both be exercised without
/// a metrics backend.
/// </summary>
/// <remarks>
/// The fake stands in for the <b>enforcement point</b>. It deliberately pins a
/// fixed effective scope no matter what visibility the request asked for, so a
/// test can prove the binding forwards the caller's request unchanged and returns
/// the server's decision unchanged, rather than substituting a scope of its own.
/// </remarks>
internal sealed class FakeTelemetry : ILatticeTelemetry
{
    /// <summary>The tenant this fake always pins, standing in for a server-side derivation.</summary>
    public const string PinnedTenantId = "server-derived-tenant";

    /// <summary>When set, every operation faults with this exception.</summary>
    public Exception? Throw { get; set; }

    /// <summary>The last request the binding forwarded, captured verbatim.</summary>
    public TelemetryQueryRequest? LastRequest { get; private set; }

    /// <summary>How many times the catalogue was read.</summary>
    public int CatalogCallCount { get; private set; }

    /// <summary>The credential the ambient context carried when the last call ran.</summary>
    public LatticeCredential? ObservedCredential { get; private set; }

    /// <summary>The active tenant the ambient context carried when the last call ran.</summary>
    public TenantId? ObservedActiveTenant { get; private set; }

    /// <summary>The catalogue this fake returns; replace it to shape a specific case.</summary>
    public TelemetryQueryCatalog Catalog { get; set; } = new()
    {
        Version = 7,
        Queries =
        [
            new TelemetryQueryDescriptor
            {
                QueryId = "lattice.ops.rate",
                Title = "Operation rate",
                Description = "Operations per second across the caller's trees.",
                Unit = "ops/s",
                Kind = TelemetryQueryKind.Range,
                Semantic = TelemetryMeasurementSemantic.PerOperation,
                Parameters = TelemetryQueryParameters.TimeRange | TelemetryQueryParameters.Step,
                Bounds = new TelemetryQueryBounds
                {
                    MinStep = TimeSpan.FromSeconds(15),
                    MaxStep = TimeSpan.FromMinutes(5),
                    DefaultStep = TimeSpan.FromSeconds(30),
                    MaxRange = TimeSpan.FromHours(6),
                    MaxLookback = TimeSpan.FromDays(7),
                    MaxPoints = 1_000,
                },
                Instruments =
                [
                    new TelemetryInstrumentReference(
                        "lattice.ops",
                        "Orleans.Lattice",
                        "ops",
                        TelemetryMeasurementSemantic.PerOperation),
                ],
            },
        ],
    };

    /// <inheritdoc />
    public Task<TelemetryQueryCatalog> GetCatalogAsync(CancellationToken cancellationToken = default)
    {
        CatalogCallCount++;
        Observe();
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromException<TelemetryQueryCatalog>(new OperationCanceledException(cancellationToken));
        }

        return Throw is not null
            ? Task.FromException<TelemetryQueryCatalog>(Throw)
            : Task.FromResult(Catalog);
    }

    /// <inheritdoc />
    public Task<TelemetryQueryResponse> QueryAsync(
        TelemetryQueryRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        LastRequest = request;
        Observe();
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromException<TelemetryQueryResponse>(new OperationCanceledException(cancellationToken));
        }

        if (Throw is not null)
        {
            return Task.FromException<TelemetryQueryResponse>(Throw);
        }

        // Always pin to the server-derived tenant, whatever the caller requested,
        // so a degraded widening request is observable end to end.
        return Task.FromResult(new TelemetryQueryResponse
        {
            QueryId = request.QueryId,
            Scope = TelemetryTenantScope.PinnedTo(PinnedTenantId, request.RequestedVisibility),
            ResultKind = TelemetryResultKind.Matrix,
            Series =
            [
                new TelemetryTimeSeries
                {
                    Labels = [new TelemetryLabel("tenant", PinnedTenantId), new TelemetryLabel("tree", "t/acme/orders")],
                    Points =
                    [
                        new TelemetryDataPoint(DateTimeOffset.UnixEpoch, 1.5),
                        new TelemetryDataPoint(DateTimeOffset.UnixEpoch.AddSeconds(30), 2.5),
                    ],
                },
            ],
            Range = TelemetryTimeRange.Between(
                DateTimeOffset.UnixEpoch,
                DateTimeOffset.UnixEpoch.AddMinutes(1),
                TimeSpan.FromSeconds(30)),
        });
    }

    private void Observe()
    {
        ObservedCredential = LatticeCredentialContext.Current;
        ObservedActiveTenant = LatticeActiveTenantContext.Current;
    }
}

/// <summary>A no-op credential bridge that resolves no credential (anonymous).</summary>
internal sealed class NullCredentialBridge : ILatticeTelemetryApiCredentialBridge
{
    public LatticeCredential? Resolve(ServerCallContext context) => null;
}

/// <summary>A credential bridge that always resolves the same fixed credential.</summary>
internal sealed class FixedCredentialBridge(LatticeCredential credential) : ILatticeTelemetryApiCredentialBridge
{
    public LatticeCredential? Resolve(ServerCallContext context) => credential;
}

/// <summary>A fixed auth-scheme source returning a pre-built advertisement.</summary>
internal sealed class FixedAuthSchemeSource(AuthSchemeAdvertisement advertisement) : ILatticeTelemetryApiAuthSchemeSource
{
    public AuthSchemeAdvertisement GetAdvertisement() => advertisement;
}

/// <summary>
/// An <see cref="ILatticeTelemetryApiAuthorizer"/> that answers a fixed verdict
/// and records the context it was asked about, so the interceptor's decode of the
/// operation and target can be asserted without a live gRPC server.
/// </summary>
internal sealed class RecordingAuthorizer(bool verdict) : ILatticeTelemetryApiAuthorizer
{
    public int CallCount { get; private set; }

    public LatticeTelemetryApiOperation? LastOperation { get; private set; }

    public string? LastTargetId { get; private set; }

    public Task<bool> IsAuthorizedAsync(
        LatticeTelemetryApiAuthorizationContext authorizationContext,
        CancellationToken cancellationToken)
    {
        CallCount++;
        LastOperation = authorizationContext.Operation;
        LastTargetId = authorizationContext.TargetId;
        return Task.FromResult(verdict);
    }
}

/// <summary>Shared construction helpers for the telemetry gRPC binding tests.</summary>
internal static class TelemetryGrpcTestSupport
{
    /// <summary>
    /// Builds a service provider with Orleans serialization registered, so the wire
    /// marshallers resolve exactly as they do in a host.
    /// </summary>
    public static ServiceProvider Serializers()
        => new ServiceCollection().AddSerializer().BuildServiceProvider();

    /// <summary>Builds the method definitions from <paramref name="serializers"/>.</summary>
    public static LatticeTelemetryGrpcMethods Methods(IServiceProvider serializers)
        => LatticeTelemetryGrpcMethods.FromServiceProvider(serializers);

    /// <summary>
    /// Builds the server-side service over <paramref name="telemetry"/>, wiring the
    /// supplied (or defaulted) credential bridge and auth-scheme source.
    /// </summary>
    public static LatticeTelemetryGrpcService Service(
        IServiceProvider serializers,
        ILatticeTelemetry telemetry,
        ILatticeTelemetryApiCredentialBridge? credentialBridge = null,
        ILatticeTelemetryApiAuthSchemeSource? authSchemeSource = null,
        LatticeTelemetryApiGrpcOptions? options = null)
        => new(
            Methods(serializers),
            telemetry,
            credentialBridge ?? new NullCredentialBridge(),
            authSchemeSource ?? new FixedAuthSchemeSource(new AuthSchemeAdvertisement()),
            Options.Create(options ?? new LatticeTelemetryApiGrpcOptions()),
            NullLogger<LatticeTelemetryGrpcService>.Instance);

    /// <summary>Builds the full gRPC method name for <paramref name="methodName"/>.</summary>
    public static string FullMethod(string methodName)
        => $"/{LatticeTelemetryGrpcMethods.ServiceName}/{methodName}";
}

/// <summary>
/// In-memory <see cref="CallInvoker"/> that closes the loop between the
/// <see cref="LatticeTelemetryApiGrpcClient"/> and the
/// <see cref="LatticeTelemetryGrpcService"/> without a network or a host. Every
/// request and response is serialized and deserialized with the same Orleans
/// serializer the production gRPC marshaller uses, so a round-trip through this
/// invoker exercises the full client-mapping -&gt; wire-encoding -&gt; service -&gt;
/// wire-encoding -&gt; client-mapping path deterministically.
/// </summary>
internal sealed class LoopbackCallInvoker(
    LatticeTelemetryGrpcServiceBase service,
    IServiceProvider serializers,
    global::Grpc.Core.Metadata? requestHeaders = null)
    : CallInvoker
{
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options,
        TRequest request)
    {
        var responseTask = DispatchAsync(method, request, options.CancellationToken);
        return new AsyncUnaryCall<TResponse>(
            responseTask,
            Task.FromResult(new global::Grpc.Core.Metadata()),
            () => Status.DefaultSuccess,
            () => new global::Grpc.Core.Metadata(),
            () => { });
    }

    private async Task<TResponse> DispatchAsync<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        CancellationToken cancellationToken)
        where TRequest : class
        where TResponse : class
    {
        var wireRequest = RoundTrip(request);
        var context = new FakeServerCallContext(method.FullName, requestHeaders, cancellationToken);

        object response = method.Name switch
        {
            LatticeTelemetryGrpcMethods.GetCatalogMethodName =>
                await service.GetCatalog((TelemetryCatalogRequest)(object)wireRequest, context),
            LatticeTelemetryGrpcMethods.QueryMethodName =>
                await service.Query((TelemetryQueryRequest)(object)wireRequest, context),
            LatticeTelemetryGrpcMethods.GetAuthSchemeMethodName =>
                await service.GetAuthScheme((AuthSchemeAdvertisementRequest)(object)wireRequest, context),
            _ => throw new NotSupportedException($"Unmapped loopback method '{method.Name}'."),
        };

        return RoundTrip((TResponse)response);
    }

    private T RoundTrip<T>(T value)
    {
        var serializer = serializers.GetRequiredService<Serializer<T>>();
        return serializer.Deserialize(serializer.SerializeToArray(value));
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
