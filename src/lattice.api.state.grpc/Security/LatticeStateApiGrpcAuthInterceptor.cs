using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Server-side gRPC interceptor that enforces the configured
/// <see cref="ILatticeStateApiAuthorizer"/> on every inbound state-API call.
/// Calls that the authorizer rejects are failed with
/// <see cref="StatusCode.PermissionDenied"/>. Enforcement is scoped to the
/// state-API service by matching on the service-name prefix, so unrelated
/// gRPC services hosted in the same ASP.NET Core pipeline are unaffected.
/// </summary>
/// <remarks>
/// Registered globally via
/// <c>AddGrpc(o =&gt; o.Interceptors.Add&lt;LatticeStateApiGrpcAuthInterceptor&gt;())</c>
/// inside
/// <see cref="LatticeStateApiGrpcServiceCollectionExtensions.AddLatticeStateApiGrpc"/>.
/// With the default <see cref="DenyAllStateApiAuthorizer"/> and
/// <see cref="LatticeStateApiGrpcOptions.RequireAuthorization"/> left at its
/// <see langword="true"/> default, every state-API call is rejected until a
/// host opts in - the default-deny posture for a read surface that exposes
/// cluster state.
/// </remarks>
internal sealed class LatticeStateApiGrpcAuthInterceptor : Interceptor
{
    private readonly ILatticeStateApiAuthorizer _authorizer;
    private readonly IOptionsMonitor<LatticeStateApiGrpcOptions> _options;
    private readonly ILogger<LatticeStateApiGrpcAuthInterceptor> _logger;

    /// <summary>Initialises the interceptor.</summary>
    public LatticeStateApiGrpcAuthInterceptor(
        ILatticeStateApiAuthorizer authorizer,
        IOptionsMonitor<LatticeStateApiGrpcOptions> options,
        ILogger<LatticeStateApiGrpcAuthInterceptor> logger)
    {
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _authorizer = authorizer;
        _options = options;
        _logger = logger;
    }

    /// <inheritdoc />
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(continuation);

        if (!IsLatticeStateApiMethod(context.Method))
        {
            return await continuation(request, context).ConfigureAwait(false);
        }

        await EnforceAuthAsync(request, context).ConfigureAwait(false);
        return await continuation(request, context).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override async Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(responseStream);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(continuation);

        if (!IsLatticeStateApiMethod(context.Method))
        {
            await continuation(request, responseStream, context).ConfigureAwait(false);
            return;
        }

        await EnforceAuthAsync(request, context).ConfigureAwait(false);
        await continuation(request, responseStream, context).ConfigureAwait(false);
    }

    private async Task EnforceAuthAsync<TRequest>(TRequest request, ServerCallContext context)
    {
        if (!_options.CurrentValue.RequireAuthorization)
        {
            return;
        }

        var (operation, targetTreeId) = DescribeCall(context.Method, request);
        var authorizationContext = new LatticeStateApiAuthorizationContext(context, operation, targetTreeId);

        bool authorized;
        try
        {
            authorized = await _authorizer
                .IsAuthorizedAsync(authorizationContext, context.CancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(
                StatusCode.Cancelled,
                "State-API authorization check was cancelled."));
        }

        if (!authorized)
        {
            _logger.LogWarning(
                "Api.State: rejected inbound gRPC call to {Method} - authorizer denied the request.",
                context.Method);
            throw new RpcException(new Status(
                StatusCode.PermissionDenied,
                "Caller is not authorized to read cluster state through the Lattice state API. "
                + "Register a permissive ILatticeStateApiAuthorizer (or AllowAllStateApiAuthorizer) to opt in, "
                + "or set LatticeStateApiGrpcOptions.RequireAuthorization=false when an outer boundary guards the endpoint."));
        }
    }

    /// <summary>
    /// Decodes the inbound call's operation (from the gRPC method name) and the
    /// tree it targets (from the request payload), so the authorizer receives a
    /// faithful per-operation, per-tree description of every state-API RPC.
    /// Operations that are not scoped to a single tree - the cluster-wide
    /// catalog discovery RPCs, <c>GetClusterInfo</c>, and a multi-tree or
    /// unscoped metrics request - carry a <see langword="null"/> target. An
    /// unrecognised method maps to <see cref="LatticeStateApiOperation.Unknown"/>
    /// (never a permissive default) so a deny-by-default policy refuses it.
    /// </summary>
    /// <remarks>Exposed as <c>internal</c> so the operation/target mapping can be
    /// asserted directly in unit tests without standing up a gRPC server.</remarks>
    internal static (LatticeStateApiOperation Operation, string? TargetTreeId) DescribeCall<TRequest>(string fullMethodName, TRequest request)
    {
        var methodName = fullMethodName[(fullMethodName.LastIndexOf('/') + 1)..];
        var operation = methodName switch
        {
            LatticeStateGrpcMethods.ListTreesMethodName => LatticeStateApiOperation.ListTrees,
            LatticeStateGrpcMethods.ListViewsMethodName => LatticeStateApiOperation.ListViews,
            LatticeStateGrpcMethods.ListTagIndexesMethodName => LatticeStateApiOperation.ListTagIndexes,
            LatticeStateGrpcMethods.ListTagValuesMethodName => LatticeStateApiOperation.ListTagValues,
            LatticeStateGrpcMethods.ListCoveredTreesMethodName => LatticeStateApiOperation.ListCoveredTrees,
            LatticeStateGrpcMethods.ListIndexTagsMethodName => LatticeStateApiOperation.ListIndexTags,
            LatticeStateGrpcMethods.ScanTagMembersMethodName => LatticeStateApiOperation.ScanTagMembers,
            LatticeStateGrpcMethods.GetTreeStructureMethodName => LatticeStateApiOperation.GetTreeStructure,
            LatticeStateGrpcMethods.ScanEntriesMethodName => LatticeStateApiOperation.ScanEntries,
            LatticeStateGrpcMethods.GetEntryMethodName => LatticeStateApiOperation.GetEntry,
            LatticeStateGrpcMethods.GetEntryHistoryMethodName => LatticeStateApiOperation.GetEntryHistory,
            LatticeStateGrpcMethods.CancelScanMethodName => LatticeStateApiOperation.CancelScan,
            LatticeStateGrpcMethods.ObserveChangesMethodName => LatticeStateApiOperation.ObserveChanges,
            LatticeStateGrpcMethods.ObserveMetricsMethodName => LatticeStateApiOperation.ObserveMetrics,
            LatticeStateGrpcMethods.GetMetricsSnapshotMethodName => LatticeStateApiOperation.GetMetricsSnapshot,
            LatticeStateGrpcMethods.GetClusterInfoMethodName => LatticeStateApiOperation.GetClusterInfo,
            _ => LatticeStateApiOperation.Unknown,
        };

        var targetTreeId = request switch
        {
            StructureRequest s => s.TreeId,
            EntryScanRequest s => s.TreeId,
            EntryGetRequest g => g.TreeId,
            EntryHistoryRequest h => h.TreeId,
            EntryScanCancelRequest c => c.TreeId,
            StateObserveRequest o => o.TreeId,
            // The tag-catalog RPCs scope to a subject tree via SourceTreeId; the
            // cluster-wide ListTrees / ListViews leave it null.
            CatalogRequest cat => cat.SourceTreeId,
            // A metrics request can name many trees (or none); present a target
            // only when it is scoped to exactly one.
            TreeMetricsRequest m => m.TreeIds is { Count: 1 } ? m.TreeIds[0] : null,
            _ => null,
        };

        return (operation, targetTreeId);
    }

    private static bool IsLatticeStateApiMethod(string fullMethodName)
    {
        const string ServicePrefix = "/" + LatticeStateGrpcMethods.ServiceName + "/";
        return fullMethodName.StartsWith(ServicePrefix, StringComparison.Ordinal);
    }
}
