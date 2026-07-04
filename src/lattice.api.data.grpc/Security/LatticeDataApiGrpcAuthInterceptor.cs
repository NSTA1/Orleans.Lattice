using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Server-side gRPC interceptor that enforces the configured
/// <see cref="ILatticeDataApiAuthorizer"/> on every inbound data-API call.
/// Calls that the authorizer rejects are failed with
/// <see cref="StatusCode.PermissionDenied"/>. Enforcement is scoped to the
/// data-API service by matching on the service-name prefix, so unrelated gRPC
/// services hosted in the same ASP.NET Core pipeline are unaffected.
/// </summary>
/// <remarks>
/// Registered globally via
/// <c>AddGrpc(o =&gt; o.Interceptors.Add&lt;LatticeDataApiGrpcAuthInterceptor&gt;())</c>
/// inside
/// <see cref="LatticeDataApiGrpcServiceCollectionExtensions.AddLatticeDataApiGrpc"/>.
/// With the default <see cref="DenyAllDataApiAuthorizer"/> and
/// <see cref="LatticeDataApiGrpcOptions.RequireAuthorization"/> left at its
/// <see langword="true"/> default, every data-API call is rejected until a host
/// opts in - the default-deny posture for a write-capable data plane. This
/// coarse gate is orthogonal to, and runs before, the per-tree / per-key
/// enforcement the gated <see cref="ILattice"/> surface applies using the
/// caller's resolved subject.
/// </remarks>
internal sealed class LatticeDataApiGrpcAuthInterceptor : Interceptor
{
    private readonly ILatticeDataApiAuthorizer _authorizer;
    private readonly IOptionsMonitor<LatticeDataApiGrpcOptions> _options;
    private readonly ILogger<LatticeDataApiGrpcAuthInterceptor> _logger;

    /// <summary>Initialises the interceptor.</summary>
    public LatticeDataApiGrpcAuthInterceptor(
        ILatticeDataApiAuthorizer authorizer,
        IOptionsMonitor<LatticeDataApiGrpcOptions> options,
        ILogger<LatticeDataApiGrpcAuthInterceptor> logger)
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

        if (!IsLatticeDataApiMethod(context.Method))
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

        var (operation, targetTreeId) = DescribeCall(context.Method, request);
        var authorizationContext = new LatticeDataApiAuthorizationContext(context, operation, targetTreeId);

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
                "Data-API authorization check was cancelled."));
        }

        if (!authorized)
        {
            _logger.LogWarning(
                "Api.Data: rejected inbound gRPC call to {Method} - authorizer denied the request.",
                context.Method);
            throw new RpcException(new Status(
                StatusCode.PermissionDenied,
                "Caller is not authorized to reach the Lattice data API. "
                + "Register a permissive ILatticeDataApiAuthorizer (or AllowAllDataApiAuthorizer) to opt in, "
                + "or set LatticeDataApiGrpcOptions.RequireAuthorization=false when an outer boundary guards the endpoint."));
        }
    }

    /// <summary>
    /// Decodes the inbound call's operation (from the gRPC method name) and the
    /// tree it targets (from the request payload), so the authorizer receives a
    /// faithful per-operation, per-tree description of every data-API RPC. The
    /// cross-tree atomic batch spans several trees and carries a
    /// <see langword="null"/> target. An unrecognised method maps to
    /// <see cref="LatticeDataApiOperation.Unknown"/> (never a permissive default)
    /// so a deny-by-default policy refuses it.
    /// </summary>
    /// <remarks>Exposed as <c>internal</c> so the operation/target mapping can be
    /// asserted directly in unit tests without standing up a gRPC server.</remarks>
    internal static (LatticeDataApiOperation Operation, string? TargetTreeId) DescribeCall<TRequest>(string fullMethodName, TRequest request)
    {
        var methodName = fullMethodName[(fullMethodName.LastIndexOf('/') + 1)..];
        var operation = methodName switch
        {
            LatticeDataApiGrpcMethods.SetMethodName => LatticeDataApiOperation.SetPoint,
            LatticeDataApiGrpcMethods.DeleteMethodName => LatticeDataApiOperation.DeletePoint,
            LatticeDataApiGrpcMethods.SetManyAtomicMethodName => LatticeDataApiOperation.SetManyAtomic,
            LatticeDataApiGrpcMethods.SetManyAtomicCrossTreeMethodName => LatticeDataApiOperation.SetManyAtomicCrossTree,
            LatticeDataApiGrpcMethods.GetMethodName => LatticeDataApiOperation.GetPoint,
            LatticeDataApiGrpcMethods.ReadRangeMethodName => LatticeDataApiOperation.ReadRange,
            _ => LatticeDataApiOperation.Unknown,
        };

        var targetTreeId = request switch
        {
            DataSetRequest s => s.TreeId,
            DataDeleteRequest d => d.TreeId,
            DataAtomicRequest a => a.TreeId,
            DataGetRequest g => g.TreeId,
            DataRangeRequest r => r.TreeId,
            // The cross-tree batch spans several trees; present no single target.
            _ => null,
        };

        return (operation, targetTreeId);
    }

    private static bool IsLatticeDataApiMethod(string fullMethodName)
    {
        const string ServicePrefix = "/" + LatticeDataApiGrpcMethods.ServiceName + "/";
        return fullMethodName.StartsWith(ServicePrefix, StringComparison.Ordinal);
    }
}
