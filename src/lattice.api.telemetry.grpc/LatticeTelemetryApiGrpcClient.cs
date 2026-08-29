using Grpc.Core;

namespace Orleans.Lattice.Api.Telemetry.Grpc;

/// <summary>
/// Strongly-typed, <b>client-safe</b> client for the telemetry gRPC surface. Wraps
/// a gRPC <see cref="CallInvoker"/> and the code-first method definitions,
/// re-exposing the transport-agnostic <see cref="ILatticeTelemetry"/> facade
/// surface over the wire - curated catalogue discovery and curated query
/// evaluation - plus the unauthenticated auth-scheme discovery probe. A client
/// head (the desktop Explorer, a dashboard, a CLI) consumes telemetry through this
/// client rather than hand-rolling channel calls.
/// </summary>
/// <remarks>
/// <para>
/// <b>Client-safe by construction.</b> Every type on this client's surface comes
/// from the shared <c>Orleans.Lattice.Api.Abstractions</c> contract or from this
/// binding. The package references that contract alone, so consuming telemetry
/// never drags in the MCP server surface or the facade's PromQL machinery - the
/// whole point of hoisting that machinery out of the MCP binding. A
/// reference-closure test asserts it, because a transitive re-coupling compiles
/// perfectly well and nothing else in the build would notice.
/// </para>
/// <para>
/// <b>Transport only - the server decides the tenant.</b> The client forwards the
/// visibility (and, for an operator, the tenant) the caller <em>requests</em>, and
/// returns whatever scope the facade <em>pinned</em> on
/// <see cref="TelemetryQueryResponse.Scope"/>. It derives, infers, and asserts no
/// tenant of its own. Editing a request on a desktop head therefore cannot widen
/// what the caller sees: an unvalidated widening comes back degraded, and
/// <see cref="TelemetryTenantScope.WasDowngraded"/> reports it. Render the scope
/// the response carries, never the one that was asked for.
/// </para>
/// <para>
/// The client carries no transport policy of its own: address, TLS, retries,
/// deadlines, and call credentials are configured on the <see cref="CallInvoker"/>
/// / <c>GrpcChannel</c> the caller supplies. Build one with
/// <see cref="Create(CallInvoker, IServiceProvider)"/>, passing a service provider
/// that has Orleans serialization registered (<c>AddSerializer()</c>) so the wire
/// marshallers match the server exactly. Every operation flows through the single
/// <see cref="CallInvoker"/> seam, so the client can adopt region-aware call
/// routing without restructuring.
/// </para>
/// </remarks>
public sealed class LatticeTelemetryApiGrpcClient
{
    // The two field-less probes carry no per-call state, so a single cached
    // instance of each is reused for every call rather than allocated per request.
    private static readonly TelemetryCatalogRequest CatalogRequest = new();
    private static readonly AuthSchemeAdvertisementRequest AuthSchemeRequest = new();

    private readonly CallInvoker _invoker;
    private readonly LatticeTelemetryGrpcMethods _methods;

    internal LatticeTelemetryApiGrpcClient(CallInvoker invoker, LatticeTelemetryGrpcMethods methods)
    {
        _invoker = invoker ?? throw new ArgumentNullException(nameof(invoker));
        _methods = methods ?? throw new ArgumentNullException(nameof(methods));
    }

    /// <summary>
    /// Creates a client over <paramref name="callInvoker"/>, building the wire
    /// marshallers from the Orleans serializers resolved out of
    /// <paramref name="serializerProvider"/>.
    /// </summary>
    /// <param name="callInvoker">
    /// The gRPC call invoker, typically <c>channel.CreateCallInvoker()</c>.
    /// </param>
    /// <param name="serializerProvider">
    /// A service provider with Orleans serialization registered
    /// (<c>AddSerializer()</c>), used to resolve the per-message serializers.
    /// </param>
    /// <returns>A ready-to-use client.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <see langword="null"/>.</exception>
    public static LatticeTelemetryApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(callInvoker);
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeTelemetryApiGrpcClient(
            callInvoker,
            LatticeTelemetryGrpcMethods.FromServiceProvider(serializerProvider));
    }

    /// <summary>
    /// Reads the curated named-query catalogue the caller may select from, in
    /// ascending <see cref="TelemetryQueryDescriptor.QueryId"/> order. The server
    /// scopes it to the caller's entitlement, so an entry the caller may not run is
    /// absent rather than present-and-denied, and a cluster with no telemetry
    /// backend configured reports <see cref="TelemetryQueryCatalog.Empty"/> rather
    /// than failing - a client then degrades to rendering no panels.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The caller's catalogue; empty when it may run none.</returns>
    public Task<TelemetryQueryCatalog> GetCatalogAsync(CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.GetCatalog, CatalogRequest, cancellationToken);

    /// <summary>
    /// Evaluates the curated query named by
    /// <see cref="TelemetryQueryRequest.QueryId"/> with the bounded parameters the
    /// request supplies. The tenant scope is derived server-side; read what was
    /// actually applied from <see cref="TelemetryQueryResponse.Scope"/>.
    /// </summary>
    /// <param name="request">The query selection and its bounded parameters.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The evaluated series, the scope applied, and the window evaluated.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <see langword="null"/>.</exception>
    /// <exception cref="RpcException">
    /// <see cref="StatusCode.NotFound"/> when the id is unknown or not offered to
        /// this caller (deliberately indistinguishable, and also how an unconfigured
        /// backend is reported), <see cref="StatusCode.OutOfRange"/> when the requested
        /// window violates the entry's declared bounds,
        /// <see cref="StatusCode.PermissionDenied"/> when the caller may not read
        /// telemetry, or <see cref="StatusCode.Unavailable"/> when the metrics backend
        /// itself failed - the one status here that is not the caller's fault and is
        /// worth retrying with backoff.
        /// </exception>
    public Task<TelemetryQueryResponse> QueryAsync(TelemetryQueryRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return UnaryAsync(_methods.Query, request, cancellationToken);
    }

    /// <summary>
    /// Reads the endpoint's advertised authentication schemes. Unauthenticated by
    /// design, so a client can discover how to sign in before it holds any
    /// credential. An advertisement with no schemes means the endpoint advertises
    /// nothing and the client falls back to a manually selected or Basic scheme.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The advertised schemes, in the server's preference order.</returns>
    public Task<AuthSchemeAdvertisement> GetAuthSchemeAsync(CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.GetAuthScheme, AuthSchemeRequest, cancellationToken);

    private async Task<TResponse> UnaryAsync<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        CancellationToken cancellationToken)
        where TRequest : class
        where TResponse : class
    {
        using var call = _invoker.AsyncUnaryCall(
            method,
            host: null,
            new CallOptions(cancellationToken: cancellationToken),
            request);

        return await call.ResponseAsync.ConfigureAwait(false);
    }
}
