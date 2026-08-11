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

    /// <summary>
    /// Reads a per-shard read/write hotness report for <paramref name="treeId"/>,
    /// with no side effects. Requires whole-tree read authority.
    /// </summary>
    /// <param name="treeId">The tree to sample. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The whole-tree hotness report.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<TreeHotnessReport> GetShardHotnessAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.GetShardHotness,
            new TreeAdminTreeRequest { TreeId = treeId },
            cancellationToken);
    }

    /// <summary>
    /// Reads a whole-tree diagnostic report for <paramref name="treeId"/>. When
    /// <paramref name="deep"/> is <see langword="true"/> the counts are taken from a
    /// more expensive leaf walk. Requires whole-tree read authority.
    /// </summary>
    /// <param name="treeId">The tree to diagnose. Must not be <c>null</c> or empty.</param>
    /// <param name="deep">Walk leaf state for authoritative counts; defaults to the cheap projection.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The whole-tree diagnostic report.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<TreeAdminDiagnosticReport> GetDiagnosticsAsync(
        string treeId, bool deep = false, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.GetDiagnostics,
            new TreeAdminDiagnosticsRequest { TreeId = treeId, Deep = deep },
            cancellationToken);
    }

    /// <summary>
    /// Inspects the shard-map topology for <paramref name="treeId"/>, with no side
    /// effects. Requires whole-tree read authority.
    /// </summary>
    /// <param name="treeId">The tree to inspect. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The shard-map inspection.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<ShardMapInspection> InspectShardMapAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.InspectShardMap,
            new TreeAdminTreeRequest { TreeId = treeId },
            cancellationToken);
    }

    /// <summary>
    /// Reads a leaf-projection digest for a single physical shard of
    /// <paramref name="treeId"/>, with no side effects. Requires whole-tree read
    /// authority.
    /// </summary>
    /// <param name="treeId">The tree the shard belongs to. Must not be <c>null</c> or empty.</param>
    /// <param name="shardIndex">The zero-based physical shard index. Must not be negative.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The shard's projection digest.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="shardIndex"/> is negative.</exception>
    public Task<ShardProjectionDigestReport> GetProjectionDigestAsync(
        string treeId, int shardIndex, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentOutOfRangeException.ThrowIfNegative(shardIndex);
        return UnaryAsync(
            _methods.GetProjectionDigest,
            new TreeAdminShardRequest { TreeId = treeId, ShardIndex = shardIndex },
            cancellationToken);
    }

    /// <summary>
    /// Reads a rolled-up statistics snapshot for <paramref name="treeId"/>, with no
    /// side effects. Requires whole-tree read authority.
    /// </summary>
    /// <param name="treeId">The tree to summarize. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree statistics snapshot.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<TreeStatsReport> GetTreeStatsAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.GetTreeStats,
            new TreeAdminTreeRequest { TreeId = treeId },
            cancellationToken);
    }

    /// <summary>
    /// Reads a cluster-wide storage accounting summary. When <paramref name="deep"/>
    /// is <see langword="true"/> a fresh leaf-walk re-measures every shard. Requires
    /// cluster telemetry authority.
    /// </summary>
    /// <param name="deep">Force a fresh leaf-walk re-measure; defaults to the cheap cached aggregate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The cluster-wide storage usage summary.</returns>
    public Task<ClusterStorageUsageSummary> GetStorageUsageAsync(
        bool deep = false, CancellationToken cancellationToken = default)
    {
        return UnaryAsync(
            _methods.GetStorageUsage,
            new TreeAdminStorageUsageRequest { Deep = deep },
            cancellationToken);
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
