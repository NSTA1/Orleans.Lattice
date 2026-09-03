using Grpc.Core;
using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Strongly-typed client for the replication control-API gRPC surface. Wraps a
/// gRPC <see cref="CallInvoker"/> and the code-first method definitions,
/// re-exposing the transport-agnostic
/// <see cref="ILatticeReplicationControl"/> facade surface over the wire: enable
/// replication for a tree, disable it, and read the runtime replicated-tree set.
/// A dashboard, CLI, or an MCP server consumes the API through this client
/// rather than hand-rolling channel calls.
/// </summary>
/// <remarks>
/// The client carries no transport policy of its own: address, TLS, retries,
/// deadlines, and call credentials are configured on the
/// <see cref="CallInvoker"/> / <c>GrpcChannel</c> the caller supplies. Build one
/// with <see cref="Create(CallInvoker, IServiceProvider)"/>, passing a service
/// provider that has Orleans serialization registered (<c>AddSerializer()</c>)
/// so the wire marshallers match the server exactly.
/// </remarks>
public sealed class LatticeReplicationApiGrpcClient
{
    private readonly CallInvoker _invoker;
    private readonly LatticeReplicationGrpcMethods _methods;

    internal LatticeReplicationApiGrpcClient(CallInvoker invoker, LatticeReplicationGrpcMethods methods)
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
    public static LatticeReplicationApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(callInvoker);
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeReplicationApiGrpcClient(
            callInvoker,
            LatticeReplicationGrpcMethods.FromServiceProvider(serializerProvider));
    }

    /// <summary>
    /// Enables replication for the tree <paramref name="treeId"/> under the merge
    /// mode <paramref name="mode"/>, optionally requesting a one-shot bootstrap
    /// from <paramref name="bootstrapSourceClusterId"/>.
    /// </summary>
    /// <param name="treeId">The tree to enable. Must not be <c>null</c> or empty.</param>
    /// <param name="mode">The merge mode replication should run under.</param>
    /// <param name="bootstrapSourceClusterId">
    /// The cluster to bootstrap the tree's state from, or <c>null</c> for no
    /// bootstrap.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the enable operation.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public async Task<ReplicationEnableResult> EnableReplicationAsync(
        string treeId,
        LatticeMergeMode mode,
        string? bootstrapSourceClusterId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var response = await UnaryAsync(
            _methods.EnableReplication,
            new ReplicationEnableRequestMessage
            {
                TreeId = treeId,
                Mode = mode,
                BootstrapSourceClusterId = bootstrapSourceClusterId,
            },
            cancellationToken).ConfigureAwait(false);

        return new ReplicationEnableResult(
            response.TreeId,
            response.Mode,
            response.AlreadyEnabled,
            response.BootstrapRequested);
    }

    /// <summary>
    /// Disables replication for the tree <paramref name="treeId"/>.
    /// </summary>
    /// <param name="treeId">The tree to disable. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the disable operation.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public async Task<ReplicationDisableResult> DisableReplicationAsync(
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var response = await UnaryAsync(
            _methods.DisableReplication,
            new ReplicationDisableRequestMessage { TreeId = treeId },
            cancellationToken).ConfigureAwait(false);

        return new ReplicationDisableResult(response.TreeId, response.AlreadyDisabled);
    }

    /// <summary>
    /// Reports the effective replicated-tree set visible to the calling
    /// credential, reconciling runtime and static enrollment.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The permission-scoped replication configuration report.</returns>
    public async Task<ReplicationConfigReport> GetReplicationConfigAsync(
        CancellationToken cancellationToken = default)
    {
        var response = await UnaryAsync(
            _methods.GetReplicationConfig,
            new ReplicationGetConfigRequest(),
            cancellationToken).ConfigureAwait(false);

        var entries = new List<ReplicationTreeConfigEntry>(response.Trees.Count);
        foreach (var tree in response.Trees)
        {
            entries.Add(new ReplicationTreeConfigEntry(
                tree.TreeId,
                tree.Enabled,
                tree.HasMode ? tree.Mode : null,
                tree.Ambiguous)
            {
                Source = tree.Source,
            });
        }

        return new ReplicationConfigReport(entries);
    }

    /// <summary>
    /// Returns the endpoint's advertised auth schemes. This RPC is
    /// unauthenticated: it can be called before any credential is acquired, so a
    /// client can discover how to sign in.
    /// </summary>
    /// <param name="request">The (empty) advertisement request.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The endpoint's auth-scheme advertisement.</returns>
    public Task<AuthSchemeAdvertisement> GetAuthSchemeAsync(
        AuthSchemeAdvertisementRequest request,
        CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.GetAuthScheme, request, cancellationToken);

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
