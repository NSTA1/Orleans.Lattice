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
/// <para>
/// Before the authorizer runs, a call that presents a credential the cluster
/// refuses - one a registered authenticator recognises and rejects, such as a
/// wrong password - is failed with <see cref="StatusCode.Unauthenticated"/>,
/// whether or not <see cref="LatticeStateApiGrpcOptions.RequireAuthorization"/>
/// is set. Without it the refused caller would resolve to the anonymous subject
/// and be answered exactly like a caller who presented no credential, or like a
/// signed-in caller who holds no grants, so a client could not tell a bad
/// password from a missing grant. A valid caller without grants is unaffected and
/// still meets the authorizer and the access gate. A credential nothing
/// recognises, or a host with no membership context, is left as it resolves
/// today.
/// </para>
/// </remarks>
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
    /// <summary>The status detail a call presenting a refused credential fails with.</summary>
    internal const string CredentialRefusedMessage =
        "The cluster did not accept the presented credential. Sign in again with a valid credential.";

    private readonly ILatticeStateApiAuthorizer _authorizer;
    private readonly IOptionsMonitor<LatticeStateApiGrpcOptions> _options;
    private readonly ILogger<LatticeStateApiGrpcAuthInterceptor> _logger;
    private readonly ILatticeStateApiCredentialBridge? _credentialBridge;
    private readonly ILatticeMembershipContext? _membership;

    /// <summary>Initialises the interceptor.</summary>
    /// <param name="authorizer">The transport-level authorizer.</param>
    /// <param name="options">The binding options.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="credentialBridge">
    /// Lifts the caller's credential off the call, so a refused credential can be
    /// failed as unauthenticated. <see langword="null"/> skips that check.
    /// </param>
    /// <param name="membership">
    /// The membership context that judges the credential. <see langword="null"/>
    /// (no membership registered on this host) skips that check.
    /// </param>
    public LatticeStateApiGrpcAuthInterceptor(
        ILatticeStateApiAuthorizer authorizer,
        IOptionsMonitor<LatticeStateApiGrpcOptions> options,
        ILogger<LatticeStateApiGrpcAuthInterceptor> logger,
        ILatticeStateApiCredentialBridge? credentialBridge = null,
        ILatticeMembershipContext? membership = null)
    {
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _authorizer = authorizer;
        _options = options;
        _logger = logger;
        _credentialBridge = credentialBridge;
        _membership = membership;
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

        var method = GrpcRoutedMethod.Resolve(context);
        if (!IsLatticeStateApiMethod(method) || IsUnauthenticatedMethod(method))
        {
            return await continuation(request, context).ConfigureAwait(false);
        }

        await RefuseRejectedCredentialAsync(method, context).ConfigureAwait(false);
        await EnforceAuthAsync(method, request, context).ConfigureAwait(false);
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

        var method = GrpcRoutedMethod.Resolve(context);
        if (!IsLatticeStateApiMethod(method))
        {
            await continuation(request, responseStream, context).ConfigureAwait(false);
            return;
        }

        await RefuseRejectedCredentialAsync(method, context).ConfigureAwait(false);
        await EnforceAuthAsync(method, request, context).ConfigureAwait(false);
        await continuation(request, responseStream, context).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override async Task<TResponse> ClientStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        ServerCallContext context,
        ClientStreamingServerMethod<TRequest, TResponse> continuation)
    {
        ArgumentNullException.ThrowIfNull(requestStream);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(continuation);

        var method = GrpcRoutedMethod.Resolve(context);
        if (!IsLatticeStateApiMethod(method))
        {
            return await continuation(requestStream, context).ConfigureAwait(false);
        }

        // A streaming call has no single request message to describe, so the call
        // is identified by its method name alone. The authorization decision is
        // made before the first client message is read.
        await RefuseRejectedCredentialAsync(method, context).ConfigureAwait(false);
        await EnforceAuthAsync<TRequest>(method, default!, context).ConfigureAwait(false);
        return await continuation(requestStream, context).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override async Task DuplexStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        DuplexStreamingServerMethod<TRequest, TResponse> continuation)
    {
        ArgumentNullException.ThrowIfNull(requestStream);
        ArgumentNullException.ThrowIfNull(responseStream);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(continuation);

        var method = GrpcRoutedMethod.Resolve(context);
        if (!IsLatticeStateApiMethod(method))
        {
            await continuation(requestStream, responseStream, context).ConfigureAwait(false);
            return;
        }

        await RefuseRejectedCredentialAsync(method, context).ConfigureAwait(false);
        await EnforceAuthAsync<TRequest>(method, default!, context).ConfigureAwait(false);
        await continuation(requestStream, responseStream, context).ConfigureAwait(false);
    }

    /// <summary>
    /// Fails the call with <see cref="StatusCode.Unauthenticated"/> when it presents
    /// a credential the cluster refuses. Runs ahead of the authorizer and regardless
    /// of <see cref="LatticeStateApiGrpcOptions.RequireAuthorization"/>, so a refused
    /// credential is never answered as an anonymous or an ungranted caller. A call
    /// with no credential, an accepted one, or one nothing recognises passes through.
    /// The credential itself is never logged.
    /// </summary>
    private async ValueTask RefuseRejectedCredentialAsync(string method, ServerCallContext context)
    {
        if (_credentialBridge is null || _membership is null)
        {
            return;
        }

        var credential = _credentialBridge.Resolve(context);
        if (credential is null)
        {
            return;
        }

        LatticeCredentialVerdict verdict;
        using (LatticeCredentialContext.With(credential))
        {
            try
            {
                verdict = await LatticeCredentialVerification
                    .VerifyCurrentAsync(_membership, context.CancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw new RpcException(new Status(
                    StatusCode.Cancelled,
                    "State-API credential verification was cancelled."));
            }
        }

        if (verdict == LatticeCredentialVerdict.Refused)
        {
            _logger.LogWarning(
                "Api.State: rejected inbound gRPC call to {Method} - the presented credential was refused.",
                method);
            throw new RpcException(new Status(StatusCode.Unauthenticated, CredentialRefusedMessage));
        }
    }

    private async Task EnforceAuthAsync<TRequest>(string method, TRequest request, ServerCallContext context)
    {
        if (!_options.CurrentValue.RequireAuthorization)
        {
            return;
        }

        var (operation, targetTreeId) = DescribeCall(method, request);
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
                method);
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
            LatticeStateGrpcMethods.GetDeadLetterCountMethodName => LatticeStateApiOperation.GetDeadLetterCount,
            LatticeStateGrpcMethods.ListDeadLettersMethodName => LatticeStateApiOperation.ListDeadLetters,
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
            DeadLetterCountRequest dc => dc.TreeId,
            DeadLetterQueueRequest dq => dq.TreeId,
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

    /// <summary>
    /// Whether the call targets a method exempt from authorization. The
    /// auth-scheme advertisement RPC must be reachable without a credential so a
    /// client can discover how to sign in before it holds one; every other
    /// state-API method is enforced.
    /// </summary>
    /// <remarks>Exposed as <c>internal</c> so the exemption can be asserted
    /// directly in unit tests without standing up a gRPC server.</remarks>
    internal static bool IsUnauthenticatedMethod(string fullMethodName)
    {
        var methodName = fullMethodName[(fullMethodName.LastIndexOf('/') + 1)..];
        return string.Equals(methodName, LatticeStateGrpcMethods.GetAuthSchemeMethodName, StringComparison.Ordinal);
    }
}
