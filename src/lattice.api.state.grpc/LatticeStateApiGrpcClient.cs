using System.Runtime.CompilerServices;
using Grpc.Core;

namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Strongly-typed client for the read-only cluster state-API gRPC surface.
/// Wraps a gRPC <see cref="CallInvoker"/> and the code-first method
/// definitions, exposing one method per RPC over the same public,
/// Orleans-serialized request/response records the server binds. A dashboard,
/// CLI explorer, or a future MCP bridge consumes the API through this client
/// rather than hand-rolling channel calls.
/// </summary>
/// <remarks>
/// The client carries no transport policy of its own: address, TLS, retries,
/// deadlines, and call credentials are configured on the
/// <see cref="CallInvoker"/> / <c>GrpcChannel</c> the caller supplies. Build
/// one with <see cref="Create(CallInvoker, IServiceProvider)"/>, passing a
/// service provider that has Orleans serialization registered
/// (<c>AddSerializer()</c>) so the wire marshallers match the server exactly.
/// </remarks>
public sealed class LatticeStateApiGrpcClient
{
    private readonly CallInvoker _invoker;
    private readonly LatticeStateGrpcMethods _methods;

    internal LatticeStateApiGrpcClient(CallInvoker invoker, LatticeStateGrpcMethods methods)
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
    public static LatticeStateApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(callInvoker);
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeStateApiGrpcClient(
            callInvoker,
            LatticeStateGrpcMethods.FromServiceProvider(serializerProvider));
    }

    /// <summary>Lists the registered trees as a deterministic, paged catalog.</summary>
    public Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.ListTrees, request, cancellationToken);

    /// <summary>Lists the materialised views as a deterministic, paged catalog.</summary>
    public Task<ViewCatalogPage> ListViewsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.ListViews, request, cancellationToken);

    /// <summary>Lists the tag-index membership trees as a deterministic, paged catalog.</summary>
    public Task<TagIndexCatalogPage> ListTagIndexesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.ListTagIndexes, request, cancellationToken);

    /// <summary>Lists the distinct tag values of one tag index as a deterministic, paged catalog.</summary>
    public Task<TagValueCatalogPage> ListTagValuesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.ListTagValues, request, cancellationToken);

    /// <summary>Lists the subject trees a tag index covers as a deterministic, paged catalog.</summary>
    public Task<CoveredTreeCatalogPage> ListCoveredTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.ListCoveredTrees, request, cancellationToken);

    /// <summary>Lists a tag index's distinct tags across every covered tree as a deterministic, paged catalog.</summary>
    public Task<TagValueCatalogPage> ListIndexTagsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.ListIndexTags, request, cancellationToken);

    /// <summary>Scans the live members of a tag across a tag index as a deterministic, paged result.</summary>
    public Task<TagMemberScanPage> ScanTagMembersAsync(TagMemberScanRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.ScanTagMembers, request, cancellationToken);

    /// <summary>Returns the structural node graph of a tree.</summary>
    public Task<StructureResponse> GetTreeStructureAsync(StructureRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.GetTreeStructure, request, cancellationToken);

    /// <summary>Scans a key-ordered page of entries under a snapshot-isolated cursor.</summary>
    public Task<EntryScanResponse> ScanEntriesAsync(EntryScanRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.ScanEntries, request, cancellationToken);

    /// <summary>Returns the full record for a single key.</summary>
    public Task<EntryGetResponse> GetEntryAsync(EntryGetRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.GetEntry, request, cancellationToken);

    /// <summary>Returns a page of a single key's change-history timeline.</summary>
    public Task<EntryHistoryResponse> GetEntryHistoryAsync(EntryHistoryRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.GetEntryHistory, request, cancellationToken);

    /// <summary>Releases a snapshot scan cursor named by a continuation token.</summary>
    public Task<EntryScanCancelResponse> CancelScanAsync(EntryScanCancelRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.CancelScan, request, cancellationToken);

    /// <summary>Returns a single live metrics snapshot.</summary>
    public Task<TreeMetricsSnapshot> GetMetricsSnapshotAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.GetMetricsSnapshot, request, cancellationToken);

    /// <summary>Returns identity and metadata for the connected cluster.</summary>
    public Task<ClusterInfo> GetClusterInfoAsync(ClusterInfoRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.GetClusterInfo, request, cancellationToken);

    /// <summary>
    /// Returns the endpoint's advertised auth schemes. This RPC is
    /// unauthenticated: it can be called before any credential is acquired, so a
    /// client can discover how to sign in.
    /// </summary>
    public Task<AuthSchemeAdvertisement> GetAuthSchemeAsync(AuthSchemeAdvertisementRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.GetAuthScheme, request, cancellationToken);

    /// <summary>
    /// Subscribes to live change notifications for a tree, yielding each
    /// notification until the call is cancelled or the server ends the stream.
    /// </summary>
    public IAsyncEnumerable<StateChangeNotification> ObserveChangesAsync(
        StateObserveRequest request,
        CancellationToken cancellationToken = default)
        => ServerStreamingAsync(_methods.ObserveChanges, request, cancellationToken);

    /// <summary>
    /// Subscribes to live metric snapshots, yielding the initial full snapshot
    /// then delta snapshots until the call is cancelled or the server ends the
    /// stream.
    /// </summary>
    public IAsyncEnumerable<TreeMetricsSnapshot> ObserveMetricsAsync(
        TreeMetricsRequest request,
        CancellationToken cancellationToken = default)
        => ServerStreamingAsync(_methods.ObserveMetrics, request, cancellationToken);

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

    private async IAsyncEnumerable<TResponse> ServerStreamingAsync<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where TRequest : class
        where TResponse : class
    {
        ArgumentNullException.ThrowIfNull(request);

        using var call = _invoker.AsyncServerStreamingCall(
            method,
            host: null,
            new CallOptions(cancellationToken: cancellationToken),
            request);

        while (await call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
        {
            yield return call.ResponseStream.Current;
        }
    }
}
