using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Server-side gRPC interceptor that enforces the configured
/// <see cref="ILatticeTreeAdminApiAuthorizer"/> on every inbound
/// tree-administration control-API call. Calls that the authorizer rejects are
/// failed with <see cref="StatusCode.PermissionDenied"/>. Enforcement is scoped to
/// the tree-administration control-API service by matching on the service-name
/// prefix, so unrelated gRPC services hosted in the same ASP.NET Core pipeline are
/// unaffected. The unauthenticated <c>GetAuthScheme</c> discovery RPC is exempt so
/// a client can learn how to sign in before it holds any credential.
/// </summary>
/// <remarks>
/// Registered globally via
/// <c>AddGrpc(o =&gt; o.Interceptors.Add&lt;LatticeTreeAdminApiGrpcAuthInterceptor&gt;())</c>
/// inside
/// <see cref="LatticeTreeAdminApiGrpcServiceCollectionExtensions.AddLatticeTreeAdminApiGrpc"/>.
/// With the default <see cref="DenyTreeAdminApiAuthorizer"/> and
/// <see cref="LatticeTreeAdminApiGrpcOptions.RequireAuthorization"/> left at its
/// <see langword="true"/> default, every tree-administration control-API call is
/// rejected until a host opts in - the default-deny posture for a surface that
/// drives whole-tree administration operations.
/// </remarks>
internal sealed class LatticeTreeAdminApiGrpcAuthInterceptor : Interceptor
{
    private readonly ILatticeTreeAdminApiAuthorizer _authorizer;
    private readonly IOptionsMonitor<LatticeTreeAdminApiGrpcOptions> _options;
    private readonly ILogger<LatticeTreeAdminApiGrpcAuthInterceptor> _logger;

    /// <summary>Initialises the interceptor.</summary>
    public LatticeTreeAdminApiGrpcAuthInterceptor(
        ILatticeTreeAdminApiAuthorizer authorizer,
        IOptionsMonitor<LatticeTreeAdminApiGrpcOptions> options,
        ILogger<LatticeTreeAdminApiGrpcAuthInterceptor> logger)
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

        if (!IsLatticeTreeAdminApiMethod(context.Method) || IsUnauthenticatedMethod(context.Method))
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

        if (!IsLatticeTreeAdminApiMethod(context.Method))
        {
            await continuation(request, responseStream, context).ConfigureAwait(false);
            return;
        }

        await EnforceAuthAsync(request, context).ConfigureAwait(false);
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

        if (!IsLatticeTreeAdminApiMethod(context.Method))
        {
            return await continuation(requestStream, context).ConfigureAwait(false);
        }

        // A streaming call has no single request message to describe, so the call
        // is identified by its method name alone. The authorization decision is
        // made before the first client message is read.
        await EnforceAuthAsync<TRequest>(default!, context).ConfigureAwait(false);
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

        if (!IsLatticeTreeAdminApiMethod(context.Method))
        {
            await continuation(requestStream, responseStream, context).ConfigureAwait(false);
            return;
        }

        await EnforceAuthAsync<TRequest>(default!, context).ConfigureAwait(false);
        await continuation(requestStream, responseStream, context).ConfigureAwait(false);
    }
    private async Task EnforceAuthAsync<TRequest>(TRequest request, ServerCallContext context)
    {
        if (!_options.CurrentValue.RequireAuthorization)
        {
            return;
        }

        var (operation, targetId) = DescribeCall(context.Method, request);
        await AuthorizeTargetAsync(context, operation, targetId).ConfigureAwait(false);

        // A request may name a SECOND caller-supplied tree the operation writes to
        // or redirects at, which the primary target id does not cover: the
        // destination of a snapshot capture, and the physical tree an alias
        // resolves to. Authorizing only the primary would let a caller authorized
        // for one tree snapshot over - or silently re-point its own logical name
        // at - a tree it holds no grant for, so every tree the call can reach is
        // put to the authorizer under the same operation. Null when the request
        // names no second tree, or names the same one twice, so the overwhelmingly
        // common single-tree call still costs exactly one authorizer round-trip.
        var secondaryTargetId = DescribeSecondaryTarget(request, targetId);
        if (secondaryTargetId is not null)
        {
            await AuthorizeTargetAsync(context, operation, secondaryTargetId).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Puts one target tree to the configured authorizer for the given operation
    /// and throws <see cref="StatusCode.PermissionDenied"/> when it is refused.
    /// Every tree an inbound call can reach is authorized through this one seam, so
    /// a request carrying more than one caller-supplied tree id is adjudicated once
    /// per tree rather than once per call.
    /// </summary>
    private async Task AuthorizeTargetAsync(
        ServerCallContext context,
        LatticeTreeAdminApiOperation operation,
        string? targetId)
    {
        var authorizationContext = new LatticeTreeAdminApiAuthorizationContext(context, operation, targetId);

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
                "Tree-administration control-API authorization check was cancelled."));
        }

        if (!authorized)
        {
            _logger.LogWarning(
                "Api.TreeAdmin: rejected inbound gRPC call to {Method} - authorizer denied the request.",
                context.Method);
            throw new RpcException(new Status(
                StatusCode.PermissionDenied,
                "Caller is not authorized to drive the Lattice tree-administration control API. "
                + "Register a permissive ILatticeTreeAdminApiAuthorizer (or AllowAllTreeAdminApiAuthorizer) to opt in, "
                + "or set LatticeTreeAdminApiGrpcOptions.RequireAuthorization=false when an outer boundary guards the endpoint."));
        }
    }

    /// <summary>
    /// Decodes the inbound call's operation (from the gRPC method name) and the
    /// tree it targets (from the request payload), so the authorizer receives a
    /// faithful per-operation description of every tree-administration control-API
    /// RPC. An unrecognised method maps to
    /// <see cref="LatticeTreeAdminApiOperation.Unknown"/> (never a permissive
    /// default) so a deny-by-default policy refuses it.
    /// </summary>
    /// <remarks>Exposed as <c>internal</c> so the operation/target mapping can be
    /// asserted directly in unit tests without standing up a gRPC server.</remarks>
    internal static (LatticeTreeAdminApiOperation Operation, string? TargetId) DescribeCall<TRequest>(string fullMethodName, TRequest request)
    {
        var methodName = fullMethodName[(fullMethodName.LastIndexOf('/') + 1)..];
        var operation = methodName switch
        {
            LatticeTreeAdminGrpcMethods.ProbeCapabilitiesMethodName => LatticeTreeAdminApiOperation.ProbeCapabilities,
            LatticeTreeAdminGrpcMethods.GetShardHotnessMethodName => LatticeTreeAdminApiOperation.GetShardHotness,
            LatticeTreeAdminGrpcMethods.GetDiagnosticsMethodName => LatticeTreeAdminApiOperation.GetDiagnostics,
            LatticeTreeAdminGrpcMethods.InspectShardMapMethodName => LatticeTreeAdminApiOperation.InspectShardMap,
            LatticeTreeAdminGrpcMethods.GetProjectionDigestMethodName => LatticeTreeAdminApiOperation.GetProjectionDigest,
            LatticeTreeAdminGrpcMethods.GetTreeStatsMethodName => LatticeTreeAdminApiOperation.GetTreeStats,
            LatticeTreeAdminGrpcMethods.GetStorageUsageMethodName => LatticeTreeAdminApiOperation.GetStorageUsage,
            LatticeTreeAdminGrpcMethods.CreateTreeMethodName => LatticeTreeAdminApiOperation.CreateTree,
            LatticeTreeAdminGrpcMethods.CheckTreeExistsMethodName => LatticeTreeAdminApiOperation.CheckTreeExists,
            LatticeTreeAdminGrpcMethods.SetTreeAliasMethodName => LatticeTreeAdminApiOperation.SetTreeAlias,
            LatticeTreeAdminGrpcMethods.ResolveTreeAliasMethodName => LatticeTreeAdminApiOperation.ResolveTreeAlias,
            LatticeTreeAdminGrpcMethods.GetTreeConfigMethodName => LatticeTreeAdminApiOperation.GetTreeConfig,
            LatticeTreeAdminGrpcMethods.SetTreeConfigMethodName => LatticeTreeAdminApiOperation.SetTreeConfig,
            LatticeTreeAdminGrpcMethods.GetShardMapMethodName => LatticeTreeAdminApiOperation.GetShardMap,
            LatticeTreeAdminGrpcMethods.CreateViewMethodName => LatticeTreeAdminApiOperation.CreateView,
            _ => LatticeTreeAdminApiOperation.Unknown,
        };

        var targetId = request switch
        {
            TreeAdminTreeRequest t => t.TreeId,
            TreeAdminShardRequest s => s.TreeId,
            TreeAdminDiagnosticsRequest d => d.TreeId,
            TreeAdminCreateRequest c => c.TreeId,
            TreeAdminSetAliasRequest a => a.TreeId,
            TreeAdminSetConfigRequest cfg => cfg.TreeId,
            TreeAdminPurgeRequest p => p.TreeId,
            TreeAdminBulkLoadSessionRequest bls => bls.TreeId,
            TreeAdminBulkLoadAppendRequest bla => bla.TreeId,
            TreeAdminRestoreRequest r => r.TreeId,
            TreeAdminReshardRequest rsh => rsh.TreeId,
            TreeAdminResizeRequest rsz => rsz.TreeId,
            TreeAdminSnapshotRequest snap => snap.TreeId,
            TreeAdminWalMovePlanRequest wpl => wpl.TreeId,
            TreeAdminWalMoveExecuteRequest wmv => wmv.TreeId,
            TreeAdminWalReclaimRequest wrc => wrc.TreeId,
            TreeAdminCreateViewRequest view => view.SourceTreeId,
            TreeAdminSetRetentionRequest ret => ret.TreeId,
            TreeRestoreResult rev => rev.TargetTreeId,
            _ => null,
        };

        return (operation, targetId);
    }

    /// <summary>
    /// Decodes the <em>second</em> caller-supplied tree id a request carries, when
    /// it names one: the destination of a snapshot capture
    /// (<see cref="TreeAdminSnapshotRequest.DestinationTreeId"/>) and the physical
    /// tree a logical alias resolves to
    /// (<see cref="TreeAdminSetAliasRequest.PhysicalTreeId"/>). Both are trees the
    /// operation reaches but which <see cref="DescribeCall{TRequest}"/>'s single
    /// <c>TargetId</c> never shows the authorizer, so without this the grant on the
    /// primary tree alone would carry the call into a second, unvetted one.
    /// </summary>
    /// <returns>
    /// The second tree id to authorize, or <see langword="null"/> when the request
    /// names no second tree, when that id is absent or blank, or when it is the
    /// same tree as <paramref name="primaryTargetId"/> and so has already been
    /// adjudicated.
    /// </returns>
    /// <remarks>Exposed as <c>internal</c> so the mapping can be asserted directly
    /// in unit tests without standing up a gRPC server.</remarks>
    internal static string? DescribeSecondaryTarget<TRequest>(TRequest request, string? primaryTargetId)
    {
        var secondary = request switch
        {
            TreeAdminSnapshotRequest snap => snap.DestinationTreeId,
            TreeAdminSetAliasRequest alias => alias.PhysicalTreeId,
            _ => null,
        };

        if (string.IsNullOrWhiteSpace(secondary)
            || string.Equals(secondary, primaryTargetId, StringComparison.Ordinal))
        {
            return null;
        }

        return secondary;
    }

    private static bool IsLatticeTreeAdminApiMethod(string fullMethodName)
    {
        const string ServicePrefix = "/" + LatticeTreeAdminGrpcMethods.ServiceName + "/";
        return fullMethodName.StartsWith(ServicePrefix, StringComparison.Ordinal);
    }

    /// <summary>
    /// Whether the call targets a method exempt from authorization. The auth-scheme
    /// advertisement RPC must be reachable without a credential so a client can
    /// discover how to sign in before it holds one; every other tree-administration
    /// control-API method is enforced.
    /// </summary>
    /// <remarks>Exposed as <c>internal</c> so the exemption can be asserted directly
    /// in unit tests without standing up a gRPC server.</remarks>
    internal static bool IsUnauthenticatedMethod(string fullMethodName)
    {
        var methodName = fullMethodName[(fullMethodName.LastIndexOf('/') + 1)..];
        return string.Equals(methodName, LatticeTreeAdminGrpcMethods.GetAuthSchemeMethodName, StringComparison.Ordinal);
    }
}
