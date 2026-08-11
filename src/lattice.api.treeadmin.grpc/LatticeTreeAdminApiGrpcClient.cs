using Grpc.Core;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Strongly-typed client for the tree-administration control-API gRPC surface.
/// Wraps a gRPC <see cref="CallInvoker"/> and the code-first method definitions,
/// re-exposing the transport-agnostic <see cref="ILatticeTreeAdmin"/> facade
/// surface over the wire: the capability probe and auth-scheme discovery. A
/// management surface (dashboard, CLI) consumes the API through this client rather
/// than hand-rolling channel calls.
/// </summary>
/// <remarks>
/// The client carries no transport policy of its own: address, TLS, retries,
/// deadlines, and call credentials are configured on the <see cref="CallInvoker"/>
/// / <c>GrpcChannel</c> the caller supplies. Build one with
/// <see cref="Create(CallInvoker, IServiceProvider)"/>, passing a service provider
/// that has Orleans serialization registered (<c>AddSerializer()</c>) so the wire
/// marshallers match the server exactly. The whole-tree lifecycle operations land
/// in later releases; when they do, this client grows a method per RPC and can
/// adopt region-aware call routing without restructuring, because every call
/// already flows through the single <see cref="CallInvoker"/> seam.
/// </remarks>
public sealed class LatticeTreeAdminApiGrpcClient
{
    private readonly CallInvoker _invoker;
    private readonly LatticeTreeAdminGrpcMethods _methods;

    internal LatticeTreeAdminApiGrpcClient(CallInvoker invoker, LatticeTreeAdminGrpcMethods methods)
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
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public static LatticeTreeAdminApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(callInvoker);
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeTreeAdminApiGrpcClient(
            callInvoker,
            LatticeTreeAdminGrpcMethods.FromServiceProvider(serializerProvider));
    }

    /// <summary>
    /// Probes which tree-administration operations the current caller may perform
    /// over <paramref name="treeId"/>, with no side effects.
    /// </summary>
    /// <param name="treeId">The tree to probe. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The caller's allowed tree-administration operation set for <paramref name="treeId"/>.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<LatticeTreeAdminCapabilities> ProbeCapabilitiesAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.ProbeCapabilities,
            new TreeAdminTreeRequest { TreeId = treeId },
            cancellationToken);
    }

    /// <summary>
    /// Reads the endpoint's advertised auth schemes. Unauthenticated: this RPC is
    /// exempt from the server's authorization interceptor, so a client can learn
    /// how to sign in before it holds any credential.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The advertised auth schemes, in the server's preference order.</returns>
    public async Task<IReadOnlyList<AuthSchemeDescriptor>> GetAuthSchemeAsync(CancellationToken cancellationToken = default)
    {
        var response = await UnaryAsync(
            _methods.GetAuthScheme,
            new AuthSchemeAdvertisementRequest(),
            cancellationToken).ConfigureAwait(false);
        return response.Schemes;
    }

    private async Task<TResponse> UnaryAsync<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        CancellationToken cancellationToken)
        where TRequest : class
        where TResponse : class
    {
        ArgumentNullException.ThrowIfNull(request);

        using var call = _invoker.AsyncUnaryCall(
            method,
            host: null,
            new CallOptions(cancellationToken: cancellationToken),
            request);

        return await call.ResponseAsync.ConfigureAwait(false);
    }
}
