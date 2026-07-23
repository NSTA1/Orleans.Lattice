using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Server-side gRPC interceptor that enforces the configured
/// <see cref="ILatticeReplicationApiAuthorizer"/> on every inbound replication
/// control-API call. Calls that the authorizer rejects are failed with
/// <see cref="StatusCode.PermissionDenied"/>. Enforcement is scoped to the
/// replication control-API service by matching on the service-name prefix, so
/// unrelated gRPC services hosted in the same ASP.NET Core pipeline are
/// unaffected. The unauthenticated <c>GetAuthScheme</c> discovery RPC is exempt
/// so a client can learn how to sign in before it holds any credential.
/// </summary>
/// <remarks>
/// Registered globally via
/// <c>AddGrpc(o =&gt; o.Interceptors.Add&lt;LatticeReplicationApiGrpcAuthInterceptor&gt;())</c>
/// inside
/// <see cref="LatticeReplicationApiGrpcServiceCollectionExtensions.AddLatticeReplicationApiGrpc"/>.
/// With the default <see cref="DenyAllReplicationApiAuthorizer"/> and
/// <see cref="LatticeReplicationApiGrpcOptions.RequireAuthorization"/> left at
/// its <see langword="true"/> default, every replication control-API call is
/// rejected until a host opts in - the default-deny posture for a surface that
/// drives cross-cluster data egress.
/// </remarks>
internal sealed class LatticeReplicationApiGrpcAuthInterceptor : Interceptor
{
    private readonly ILatticeReplicationApiAuthorizer _authorizer;
    private readonly IOptionsMonitor<LatticeReplicationApiGrpcOptions> _options;
    private readonly ILogger<LatticeReplicationApiGrpcAuthInterceptor> _logger;

    /// <summary>Initialises the interceptor.</summary>
    public LatticeReplicationApiGrpcAuthInterceptor(
        ILatticeReplicationApiAuthorizer authorizer,
        IOptionsMonitor<LatticeReplicationApiGrpcOptions> options,
        ILogger<LatticeReplicationApiGrpcAuthInterceptor> logger)
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

        if (!IsLatticeReplicationApiMethod(context.Method) || IsUnauthenticatedMethod(context.Method))
        {
            return await continuation(request, context).ConfigureAwait(false);
        }

        await EnforceAuthAsync(request, context).ConfigureAwait(false);
        return await continuation(request, context).ConfigureAwait(false);
    }

    private async Task EnforceAuthAsync<TRequest>(TRequest request, ServerCallContext context)
    {
        if (!_options.CurrentValue.RequireAuthorization)
        {
            return;
        }

        var (operation, targetId) = DescribeCall(context.Method, request);
        var authorizationContext = new LatticeReplicationApiAuthorizationContext(context, operation, targetId);

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
                "Replication control-API authorization check was cancelled."));
        }

        if (!authorized)
        {
            _logger.LogWarning(
                "Api.Replication: rejected inbound gRPC call to {Method} - authorizer denied the request.",
                context.Method);
            throw new RpcException(new Status(
                StatusCode.PermissionDenied,
                "Caller is not authorized to drive the Lattice replication control API. "
                + "Register a permissive ILatticeReplicationApiAuthorizer (or AllowAllReplicationApiAuthorizer) to opt in, "
                + "or set LatticeReplicationApiGrpcOptions.RequireAuthorization=false when an outer boundary guards the endpoint."));
        }
    }

    /// <summary>
    /// Decodes the inbound call's operation (from the gRPC method name) and the
    /// tree it targets (from the request payload), so the authorizer receives a
    /// faithful per-operation description of every replication control-API RPC.
    /// Operations that are not scoped to a single tree - the whole-estate config
    /// read - carry a <see langword="null"/> target. An unrecognised method maps
    /// to <see cref="LatticeReplicationApiOperation.Unknown"/> (never a permissive
    /// default) so a deny-by-default policy refuses it.
    /// </summary>
    /// <remarks>Exposed as <c>internal</c> so the operation/target mapping can be
    /// asserted directly in unit tests without standing up a gRPC server.</remarks>
    internal static (LatticeReplicationApiOperation Operation, string? TargetId) DescribeCall<TRequest>(string fullMethodName, TRequest request)
    {
        var methodName = fullMethodName[(fullMethodName.LastIndexOf('/') + 1)..];
        var operation = methodName switch
        {
            LatticeReplicationGrpcMethods.EnableReplicationMethodName => LatticeReplicationApiOperation.EnableReplication,
            LatticeReplicationGrpcMethods.DisableReplicationMethodName => LatticeReplicationApiOperation.DisableReplication,
            LatticeReplicationGrpcMethods.GetReplicationConfigMethodName => LatticeReplicationApiOperation.GetReplicationConfig,
            _ => LatticeReplicationApiOperation.Unknown,
        };

        var targetId = request switch
        {
            ReplicationEnableRequestMessage e => e.TreeId,
            ReplicationDisableRequestMessage d => d.TreeId,
            _ => null,
        };

        return (operation, targetId);
    }

    private static bool IsLatticeReplicationApiMethod(string fullMethodName)
    {
        const string ServicePrefix = "/" + LatticeReplicationGrpcMethods.ServiceName + "/";
        return fullMethodName.StartsWith(ServicePrefix, StringComparison.Ordinal);
    }

    /// <summary>
    /// Whether the call targets a method exempt from authorization. The
    /// auth-scheme advertisement RPC must be reachable without a credential so a
    /// client can discover how to sign in before it holds one; every other
    /// replication control-API method is enforced.
    /// </summary>
    /// <remarks>Exposed as <c>internal</c> so the exemption can be asserted
    /// directly in unit tests without standing up a gRPC server.</remarks>
    internal static bool IsUnauthenticatedMethod(string fullMethodName)
    {
        var methodName = fullMethodName[(fullMethodName.LastIndexOf('/') + 1)..];
        return string.Equals(methodName, LatticeReplicationGrpcMethods.GetAuthSchemeMethodName, StringComparison.Ordinal);
    }
}
