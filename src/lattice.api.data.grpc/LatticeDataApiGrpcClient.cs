using Grpc.Core;

namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Strongly-typed client for the write-capable data-API gRPC surface. Wraps a
/// gRPC <see cref="CallInvoker"/> and the code-first method definitions, exposing
/// one method per RPC over the same public, Orleans-serialized request/response
/// records the server binds. A non-.NET client typically generates its own stub
/// from the wire contract; this client is the in-process / .NET consumer used by
/// tests and .NET edge services.
/// </summary>
/// <remarks>
/// The client carries no transport policy of its own: address, TLS, retries,
/// deadlines, and call credentials are configured on the
/// <see cref="CallInvoker"/> / <c>GrpcChannel</c> the caller supplies. Build one
/// with <see cref="Create(CallInvoker, IServiceProvider)"/>, passing a service
/// provider that has Orleans serialization registered (<c>AddSerializer()</c>)
/// so the wire marshallers match the server exactly.
/// </remarks>
public sealed class LatticeDataApiGrpcClient
{
    private readonly CallInvoker _invoker;
    private readonly LatticeDataApiGrpcMethods _methods;

    internal LatticeDataApiGrpcClient(CallInvoker invoker, LatticeDataApiGrpcMethods methods)
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
    public static LatticeDataApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(callInvoker);
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeDataApiGrpcClient(
            callInvoker,
            LatticeDataApiGrpcMethods.FromServiceProvider(serializerProvider));
    }

    /// <summary>Writes a value at a key. Throws a <c>PermissionDenied</c> <see cref="RpcException"/> when the caller may not write the key.</summary>
    public Task<DataSetResponse> SetAsync(DataSetRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.Set, request, cancellationToken);

    /// <summary>Deletes a key. Throws a <c>PermissionDenied</c> <see cref="RpcException"/> when the caller may not delete the key.</summary>
    public Task<DataDeleteResponse> DeleteAsync(DataDeleteRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.Delete, request, cancellationToken);

    /// <summary>Commits a single-tree atomic batch all-or-nothing. A denied leg aborts the whole batch with a <c>PermissionDenied</c> status.</summary>
    public Task<DataAtomicResponse> SetManyAtomicAsync(DataAtomicRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.SetManyAtomic, request, cancellationToken);

    /// <summary>Commits a cross-tree atomic batch all-or-nothing. A denied leg aborts the whole batch with a <c>PermissionDenied</c> status.</summary>
    public Task<DataCrossTreeResponse> SetManyAtomicCrossTreeAsync(DataCrossTreeRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.SetManyAtomicCrossTree, request, cancellationToken);

    /// <summary>Reads a value at a key. A key the caller may not read reports absent, never a value.</summary>
    public Task<DataReadResult> GetAsync(DataGetRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.Get, request, cancellationToken);

    /// <summary>Reads one page of a bounded range, pruned to the caller's authorized key subset.</summary>
    public Task<DataRangePage> ReadRangeAsync(DataRangeRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.ReadRange, request, cancellationToken);

    /// <summary>Deletes a bounded range of keys, draining to completion with transparent reconnect. Throws a <c>PermissionDenied</c> <see cref="RpcException"/> when the caller may not delete the whole range.</summary>
    public Task<DataRangeDeleteResult> DeleteRangeAsync(DataRangeDeleteRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.DeleteRange, request, cancellationToken);

    /// <summary>Commits a non-atomic, upsert-only bulk write. A denied key aborts before any write with a <c>PermissionDenied</c> status.</summary>
    public Task<DataSetManyResponse> SetManyAsync(DataSetManyRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.SetMany, request, cancellationToken);

    /// <summary>Applies a typed CRDT write delta. A denied key aborts with a <c>PermissionDenied</c> status.</summary>
    public Task<CrdtWriteResponse> CrdtWriteAsync(CrdtWriteRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.CrdtWrite, request, cancellationToken);

    /// <summary>Reads a typed CRDT logical value. An unreadable key yields the empty value for its kind.</summary>
    public Task<CrdtReadResponse> CrdtReadAsync(CrdtReadRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.CrdtRead, request, cancellationToken);

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
