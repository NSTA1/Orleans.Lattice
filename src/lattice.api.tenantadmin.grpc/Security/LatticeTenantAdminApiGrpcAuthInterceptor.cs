using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Server-side gRPC interceptor that enforces the configured
/// <see cref="ILatticeTenantAdminApiAuthorizer"/> on every inbound
/// tenant-administration control-API call. Calls that the authorizer rejects are
/// failed with <see cref="StatusCode.PermissionDenied"/>. Enforcement is scoped to
/// the tenant-administration control-API service by matching on the service-name
/// prefix, so unrelated gRPC services hosted in the same ASP.NET Core pipeline are
/// unaffected. The unauthenticated <c>GetAuthScheme</c> discovery RPC is exempt so
/// a client can learn how to sign in before it holds any credential, and the three
/// read-only self-service RPCs (<c>GetCurrentTenant</c>,
/// <c>ListAccessibleTenants</c>, <c>GetTenant</c>) are exempt because they must be
/// reachable by any read-capable caller and enforce their own fail-closed per-caller
/// scoping at the facade rather than through this default-deny admin gate.
/// </summary>
/// <remarks>
/// Registered globally via
/// <c>AddGrpc(o =&gt; o.Interceptors.Add&lt;LatticeTenantAdminApiGrpcAuthInterceptor&gt;())</c>
/// inside
/// <see cref="LatticeTenantAdminApiGrpcServiceCollectionExtensions.AddLatticeTenantAdminApiGrpc"/>.
/// With the default <see cref="DenyTenantAdminApiAuthorizer"/> and
/// <see cref="LatticeTenantAdminApiGrpcOptions.RequireAuthorization"/> left at its
/// <see langword="true"/> default, every tenant-administration control-API call is
/// rejected until a host opts in - the default-deny posture for a surface that
/// drives destructive tenant lifecycle operations.
/// </remarks>
internal sealed class LatticeTenantAdminApiGrpcAuthInterceptor : Interceptor
{
    private readonly ILatticeTenantAdminApiAuthorizer _authorizer;
    private readonly IOptionsMonitor<LatticeTenantAdminApiGrpcOptions> _options;
    private readonly ILogger<LatticeTenantAdminApiGrpcAuthInterceptor> _logger;

    /// <summary>Initialises the interceptor.</summary>
    public LatticeTenantAdminApiGrpcAuthInterceptor(
        ILatticeTenantAdminApiAuthorizer authorizer,
        IOptionsMonitor<LatticeTenantAdminApiGrpcOptions> options,
        ILogger<LatticeTenantAdminApiGrpcAuthInterceptor> logger)
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

        if (!IsLatticeTenantAdminApiMethod(context.Method)
            || IsUnauthenticatedMethod(context.Method)
            || IsSelfServiceMethod(context.Method))
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
        var authorizationContext = new LatticeTenantAdminApiAuthorizationContext(context, operation, targetId);

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
                "Tenant-administration control-API authorization check was cancelled."));
        }

        if (!authorized)
        {
            _logger.LogWarning(
                "Api.TenantAdmin: rejected inbound gRPC call to {Method} - authorizer denied the request.",
                context.Method);
            throw new RpcException(new Status(
                StatusCode.PermissionDenied,
                "Caller is not authorized to drive the Lattice tenant-administration control API. "
                + "Register a permissive ILatticeTenantAdminApiAuthorizer (or AllowAllTenantAdminApiAuthorizer) to opt in, "
                + "or set LatticeTenantAdminApiGrpcOptions.RequireAuthorization=false when an outer boundary guards the endpoint."));
        }
    }

    /// <summary>
    /// Decodes the inbound call's operation (from the gRPC method name) and the
    /// tenant it targets (from the request payload), so the authorizer receives a
    /// faithful per-operation description of every tenant-administration
    /// control-API RPC. An unrecognised method maps to
    /// <see cref="LatticeTenantAdminApiOperation.Unknown"/> (never a permissive
    /// default) so a deny-by-default policy refuses it.
    /// </summary>
    /// <remarks>Exposed as <c>internal</c> so the operation/target mapping can be
    /// asserted directly in unit tests without standing up a gRPC server.</remarks>
    internal static (LatticeTenantAdminApiOperation Operation, string? TargetId) DescribeCall<TRequest>(string fullMethodName, TRequest request)
    {
        var methodName = fullMethodName[(fullMethodName.LastIndexOf('/') + 1)..];
        var operation = methodName switch
        {
            LatticeTenantAdminGrpcMethods.CreateTenantMethodName => LatticeTenantAdminApiOperation.CreateTenant,
            LatticeTenantAdminGrpcMethods.SuspendTenantMethodName => LatticeTenantAdminApiOperation.SuspendTenant,
            LatticeTenantAdminGrpcMethods.ResumeTenantMethodName => LatticeTenantAdminApiOperation.ResumeTenant,
            LatticeTenantAdminGrpcMethods.DeleteTenantMethodName => LatticeTenantAdminApiOperation.DeleteTenant,
            LatticeTenantAdminGrpcMethods.SetTenantQuotasMethodName => LatticeTenantAdminApiOperation.SetTenantQuotas,
            LatticeTenantAdminGrpcMethods.AuthorizeAllowedRegionsMethodName => LatticeTenantAdminApiOperation.AuthorizeAllowedRegions,
            LatticeTenantAdminGrpcMethods.SetTenantResidencyMethodName => LatticeTenantAdminApiOperation.SetTenantResidency,
            LatticeTenantAdminGrpcMethods.GetTenantRegionStatusMethodName => LatticeTenantAdminApiOperation.GetTenantRegionStatus,
            _ => LatticeTenantAdminApiOperation.Unknown,
        };

        var targetId = request switch
        {
            TenantAdminTenantRequest t => t.TenantId,
            TenantAdminCreateRequest c => c.TenantId,
            TenantAdminSetQuotasRequest q => q.TenantId,
            TenantAdminRegionSetRequest r => r.TenantId,
            _ => null,
        };

        return (operation, targetId);
    }

    private static bool IsLatticeTenantAdminApiMethod(string fullMethodName)
    {
        const string ServicePrefix = "/" + LatticeTenantAdminGrpcMethods.ServiceName + "/";
        return fullMethodName.StartsWith(ServicePrefix, StringComparison.Ordinal);
    }

    /// <summary>
    /// Whether the call targets a method exempt from authorization. The auth-scheme
    /// advertisement RPC must be reachable without a credential so a client can
    /// discover how to sign in before it holds one; every other tenant-administration
    /// control-API method is enforced.
    /// </summary>
    /// <remarks>Exposed as <c>internal</c> so the exemption can be asserted directly
    /// in unit tests without standing up a gRPC server.</remarks>
    internal static bool IsUnauthenticatedMethod(string fullMethodName)
    {
        var methodName = fullMethodName[(fullMethodName.LastIndexOf('/') + 1)..];
        return string.Equals(methodName, LatticeTenantAdminGrpcMethods.GetAuthSchemeMethodName, StringComparison.Ordinal);
    }

    /// <summary>
    /// Whether the call targets a read-only tenant self-service method
    /// (<c>GetCurrentTenant</c>, <c>ListAccessibleTenants</c>, <c>GetTenant</c>).
    /// These are exempt from the tenant-admin authorizer: the tenant-admin gate is
    /// default-deny and gates the destructive lifecycle surface, whereas self-service
    /// must be reachable by any read-capable (even anonymous) caller. The caller
    /// credential is still bridged into the ambient context by the service, so the
    /// <see cref="ILatticeTenantSelfService"/> facade resolves the caller's subject
    /// and scopes enumeration and inspection fail-closed - enforcement lives at that
    /// single narrowest seam, not at this transport gate, exactly as it does for the
    /// co-hosted MCP self-awareness tools.
    /// </summary>
    /// <remarks>Exposed as <c>internal</c> so the exemption can be asserted directly
    /// in unit tests without standing up a gRPC server.</remarks>
    internal static bool IsSelfServiceMethod(string fullMethodName)
    {
        var methodName = fullMethodName[(fullMethodName.LastIndexOf('/') + 1)..];
        return string.Equals(methodName, LatticeTenantAdminGrpcMethods.GetCurrentTenantMethodName, StringComparison.Ordinal)
            || string.Equals(methodName, LatticeTenantAdminGrpcMethods.ListAccessibleTenantsMethodName, StringComparison.Ordinal)
            || string.Equals(methodName, LatticeTenantAdminGrpcMethods.GetTenantMethodName, StringComparison.Ordinal);
    }
}
