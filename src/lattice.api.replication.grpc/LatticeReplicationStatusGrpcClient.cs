using Grpc.Core;

namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Strongly-typed client for the replication peer-status gRPC surface. It
/// implements <see cref="ILatticeReplicationStatus"/> directly, so a consumer
/// (the Explorer, a dashboard, a CLI) programs against the transport-agnostic
/// contract and swaps an in-process facade for this client with no adapter.
/// </summary>
/// <remarks>
/// The client carries no transport policy of its own: address, TLS, retries,
/// deadlines and call credentials are configured on the
/// <see cref="CallInvoker"/> / <c>GrpcChannel</c> the caller supplies. Build one
/// with <see cref="Create(CallInvoker, IServiceProvider)"/>, passing a service
/// provider that has Orleans serialization registered (<c>AddSerializer()</c>).
/// A server-side rejection surfaces as an <see cref="RpcException"/> carrying the
/// mapped status code (for example <see cref="StatusCode.PermissionDenied"/>).
/// </remarks>
public sealed class LatticeReplicationStatusGrpcClient : ILatticeReplicationStatus
{
    private readonly CallInvoker _invoker;
    private readonly LatticeReplicationStatusGrpcMethods _methods;

    internal LatticeReplicationStatusGrpcClient(CallInvoker invoker, LatticeReplicationStatusGrpcMethods methods)
    {
        ArgumentNullException.ThrowIfNull(invoker);
        ArgumentNullException.ThrowIfNull(methods);
        _invoker = invoker;
        _methods = methods;
    }

    /// <summary>
    /// Creates a client over <paramref name="callInvoker"/>, building the wire
    /// marshallers from the Orleans serializers resolved out of
    /// <paramref name="serializerProvider"/>.
    /// </summary>
    /// <param name="callInvoker">The gRPC call invoker, typically <c>channel.CreateCallInvoker()</c>. Must not be <c>null</c>.</param>
    /// <param name="serializerProvider">A service provider with Orleans serialization registered. Must not be <c>null</c>.</param>
    /// <returns>A ready-to-use client.</returns>
    /// <exception cref="ArgumentNullException">An argument is <c>null</c>.</exception>
    public static LatticeReplicationStatusGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(callInvoker);
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeReplicationStatusGrpcClient(
            callInvoker,
            LatticeReplicationStatusGrpcMethods.FromServiceProvider(serializerProvider));
    }

    /// <inheritdoc />
    public async Task<ReplicationPeerStatusPage> GetPeerStatusAsync(
        ReplicationPeerStatusQuery query,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);

        using var call = _invoker.AsyncUnaryCall(
            _methods.GetPeerStatus,
            host: null,
            new CallOptions(cancellationToken: cancellationToken),
            query);

        return await call.ResponseAsync.ConfigureAwait(false);
    }
}
