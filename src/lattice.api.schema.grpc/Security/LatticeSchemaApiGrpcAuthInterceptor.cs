using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Server-side gRPC interceptor that enforces the configured
/// <see cref="ILatticeSchemaApiAuthorizer"/> on every inbound schema control-API
/// call. Calls that the authorizer rejects are failed with
/// <see cref="StatusCode.PermissionDenied"/>. Enforcement is scoped to the
/// schema control-API service by matching on the service-name prefix, so
/// unrelated gRPC services hosted in the same ASP.NET Core pipeline are
/// unaffected. The unauthenticated <c>GetAuthScheme</c> discovery RPC is exempt
/// so a client can learn how to sign in before it holds any credential.
/// </summary>
/// <remarks>
/// Registered globally via
/// <c>AddGrpc(o =&gt; o.Interceptors.Add&lt;LatticeSchemaApiGrpcAuthInterceptor&gt;())</c>
/// inside
/// <see cref="LatticeSchemaApiGrpcServiceCollectionExtensions.AddLatticeSchemaApiGrpc"/>.
/// With the default <see cref="DenySchemaApiAuthorizer"/> and
/// <see cref="LatticeSchemaApiGrpcOptions.RequireAuthorization"/> left at its
/// <see langword="true"/> default, every schema control-API call is rejected
/// until a host opts in - the default-deny posture for a surface that drives
/// schema-management operations.
/// </remarks>
internal sealed class LatticeSchemaApiGrpcAuthInterceptor : Interceptor
{
    private readonly ILatticeSchemaApiAuthorizer _authorizer;
    private readonly IOptionsMonitor<LatticeSchemaApiGrpcOptions> _options;
    private readonly ILogger<LatticeSchemaApiGrpcAuthInterceptor> _logger;

    /// <summary>Initialises the interceptor.</summary>
    public LatticeSchemaApiGrpcAuthInterceptor(
        ILatticeSchemaApiAuthorizer authorizer,
        IOptionsMonitor<LatticeSchemaApiGrpcOptions> options,
        ILogger<LatticeSchemaApiGrpcAuthInterceptor> logger)
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

        if (!IsLatticeSchemaApiMethod(context.Method) || IsUnauthenticatedMethod(context.Method))
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

        if (!IsLatticeSchemaApiMethod(context.Method))
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

        var (operation, targetId) = DescribeCall(context.Method, request);
        var authorizationContext = new LatticeSchemaApiAuthorizationContext(context, operation, targetId);

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
                "Schema control-API authorization check was cancelled."));
        }

        if (!authorized)
        {
            _logger.LogWarning(
                "Api.Schema: rejected inbound gRPC call to {Method} - authorizer denied the request.",
                context.Method);
            throw new RpcException(new Status(
                StatusCode.PermissionDenied,
                "Caller is not authorized to drive the Lattice schema control API. "
                + "Register a permissive ILatticeSchemaApiAuthorizer (or AllowAllSchemaApiAuthorizer) to opt in, "
                + "or set LatticeSchemaApiGrpcOptions.RequireAuthorization=false when an outer boundary guards the endpoint."));
        }
    }

    /// <summary>
    /// Decodes the inbound call's operation (from the gRPC method name) and the
    /// tree it targets (from the request payload), so the authorizer receives a
    /// faithful per-operation description of every schema control-API RPC. An
    /// unrecognised method maps to <see cref="LatticeSchemaApiOperation.Unknown"/>
    /// (never a permissive default) so a deny-by-default policy refuses it.
    /// </summary>
    /// <remarks>Exposed as <c>internal</c> so the operation/target mapping can be
    /// asserted directly in unit tests without standing up a gRPC server.</remarks>
    internal static (LatticeSchemaApiOperation Operation, string? TargetId) DescribeCall<TRequest>(string fullMethodName, TRequest request)
    {
        var methodName = fullMethodName[(fullMethodName.LastIndexOf('/') + 1)..];
        var operation = methodName switch
        {
            LatticeSchemaGrpcMethods.SetPolicyMethodName => LatticeSchemaApiOperation.SetPolicy,
            LatticeSchemaGrpcMethods.ClearPolicyMethodName => LatticeSchemaApiOperation.ClearPolicy,
            LatticeSchemaGrpcMethods.GetPolicyMethodName => LatticeSchemaApiOperation.GetPolicy,
            LatticeSchemaGrpcMethods.StreamDeadLettersMethodName => LatticeSchemaApiOperation.StreamDeadLetters,
            LatticeSchemaGrpcMethods.CountDeadLettersMethodName => LatticeSchemaApiOperation.CountDeadLetters,
            LatticeSchemaGrpcMethods.SetVersionConfigMethodName => LatticeSchemaApiOperation.SetVersionConfig,
            LatticeSchemaGrpcMethods.GetVersionConfigMethodName => LatticeSchemaApiOperation.GetVersionConfig,
            LatticeSchemaGrpcMethods.AdvanceTargetVersionMethodName => LatticeSchemaApiOperation.AdvanceTargetVersion,
            LatticeSchemaGrpcMethods.AdvanceAndMigrateMethodName => LatticeSchemaApiOperation.AdvanceAndMigrate,
            LatticeSchemaGrpcMethods.MigrateToTargetVersionMethodName => LatticeSchemaApiOperation.MigrateToTargetVersion,
            LatticeSchemaGrpcMethods.ClearVersionConfigMethodName => LatticeSchemaApiOperation.ClearVersionConfig,
            LatticeSchemaGrpcMethods.RemediateMethodName => LatticeSchemaApiOperation.Remediate,
            LatticeSchemaGrpcMethods.GetRemediationStatusMethodName => LatticeSchemaApiOperation.GetRemediationStatus,
            LatticeSchemaGrpcMethods.ScanComplianceMethodName => LatticeSchemaApiOperation.ScanCompliance,
            LatticeSchemaGrpcMethods.ProbeCapabilitiesMethodName => LatticeSchemaApiOperation.ProbeCapabilities,
            _ => LatticeSchemaApiOperation.Unknown,
        };

        var targetId = request switch
        {
            SetPolicyRequest p => p.TreeId,
            SetVersionConfigRequest v => v.TreeId,
            AdvanceVersionRequest a => a.TreeId,
            RemediateRequest r => r.TreeId,
            SchemaTreeRequest t => t.TreeId,
            _ => null,
        };

        return (operation, targetId);
    }

    private static bool IsLatticeSchemaApiMethod(string fullMethodName)
    {
        const string ServicePrefix = "/" + LatticeSchemaGrpcMethods.ServiceName + "/";
        return fullMethodName.StartsWith(ServicePrefix, StringComparison.Ordinal);
    }

    /// <summary>
    /// Whether the call targets a method exempt from authorization. The
    /// auth-scheme advertisement RPC must be reachable without a credential so a
    /// client can discover how to sign in before it holds one; every other
    /// schema control-API method is enforced.
    /// </summary>
    /// <remarks>Exposed as <c>internal</c> so the exemption can be asserted
    /// directly in unit tests without standing up a gRPC server.</remarks>
    internal static bool IsUnauthenticatedMethod(string fullMethodName)
    {
        var methodName = fullMethodName[(fullMethodName.LastIndexOf('/') + 1)..];
        return string.Equals(methodName, LatticeSchemaGrpcMethods.GetAuthSchemeMethodName, StringComparison.Ordinal);
    }
}
