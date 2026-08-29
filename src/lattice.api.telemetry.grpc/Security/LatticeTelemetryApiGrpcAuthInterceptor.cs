using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Telemetry.Grpc;

/// <summary>
/// Server-side gRPC interceptor that enforces the configured
/// <see cref="ILatticeTelemetryApiAuthorizer"/> on every inbound telemetry call.
/// Calls that the authorizer rejects are failed with
/// <see cref="StatusCode.PermissionDenied"/>. Enforcement is scoped to the
/// telemetry service by matching on the service-name prefix, so unrelated gRPC
/// services hosted in the same ASP.NET Core pipeline are unaffected. The
/// unauthenticated <c>GetAuthScheme</c> discovery RPC is exempt so a client can
/// learn how to sign in before it holds any credential.
/// </summary>
/// <remarks>
/// <para>
/// Registered globally via
/// <c>AddGrpc(o =&gt; o.Interceptors.Add&lt;LatticeTelemetryApiGrpcAuthInterceptor&gt;())</c>
/// inside
/// <see cref="LatticeTelemetryApiGrpcServiceCollectionExtensions.AddLatticeTelemetryApiGrpc"/>.
/// With the default <see cref="DenyTelemetryApiAuthorizer"/> and
/// <see cref="LatticeTelemetryApiGrpcOptions.RequireAuthorization"/> left at its
/// <see langword="true"/> default, every telemetry call is rejected until a host
/// opts in.
/// </para>
/// <para>
/// This gate is coarse. It never decides what a caller may see: the catalogue is
/// scoped and the effective tenant derived server-side by the facade, which stays
/// the single enforcement point even when a host turns this gate off.
/// </para>
/// </remarks>
internal sealed class LatticeTelemetryApiGrpcAuthInterceptor : Interceptor
{
    private readonly ILatticeTelemetryApiAuthorizer _authorizer;
    private readonly IOptionsMonitor<LatticeTelemetryApiGrpcOptions> _options;
    private readonly ILogger<LatticeTelemetryApiGrpcAuthInterceptor> _logger;

    /// <summary>Initialises the interceptor.</summary>
    /// <param name="authorizer">The configured transport authorizer.</param>
    /// <param name="options">The binding options monitor.</param>
    /// <param name="logger">The logger.</param>
    /// <exception cref="ArgumentNullException">Any argument is <see langword="null"/>.</exception>
    public LatticeTelemetryApiGrpcAuthInterceptor(
        ILatticeTelemetryApiAuthorizer authorizer,
        IOptionsMonitor<LatticeTelemetryApiGrpcOptions> options,
        ILogger<LatticeTelemetryApiGrpcAuthInterceptor> logger)
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

        if (!IsLatticeTelemetryApiMethod(context.Method)
            || IsUnauthenticatedMethod(context.Method))
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
        var authorizationContext = new LatticeTelemetryApiAuthorizationContext(context, operation, targetId);

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
                "Telemetry authorization check was cancelled."));
        }

        if (!authorized)
        {
            _logger.LogWarning(
                "Api.Telemetry: rejected inbound gRPC call to {Method} - authorizer denied the request.",
                context.Method);
            throw new RpcException(new Status(
                StatusCode.PermissionDenied,
                "Caller is not authorized to read the Lattice telemetry API. "
                + "Register a permissive ILatticeTelemetryApiAuthorizer (or AllowAllTelemetryApiAuthorizer) to opt in, "
                + "or set LatticeTelemetryApiGrpcOptions.RequireAuthorization=false when an outer boundary guards the endpoint."));
        }
    }

    /// <summary>
    /// Decodes the inbound call's operation (from the gRPC method name) and the
    /// curated query id it selects (from the request payload), so the authorizer
    /// receives a faithful per-operation description of every telemetry RPC. An
    /// unrecognised method maps to
    /// <see cref="LatticeTelemetryApiOperation.Unknown"/> (never a permissive
    /// default) so a deny-by-default policy refuses it.
    /// </summary>
    /// <remarks>
    /// The target is the selected <b>query id</b>, never a tenant. The binding
    /// derives no tenant of its own, and deliberately does not surface the
    /// request's <c>RequestedVisibility</c> / <c>RequestedTenantId</c> here: those
    /// are wire-supplied requests the facade validates server-side, and letting a
    /// transport policy read them would invite a decision made on a value the
    /// caller controls.
    /// </remarks>
    /// <typeparam name="TRequest">The inbound request message type.</typeparam>
    /// <param name="fullMethodName">The full gRPC method name.</param>
    /// <param name="request">The inbound request payload.</param>
    /// <returns>The decoded operation and its selected query id, if any.</returns>
    internal static (LatticeTelemetryApiOperation Operation, string? TargetId) DescribeCall<TRequest>(string fullMethodName, TRequest request)
    {
        // Slice rather than substring: this runs on every gated call, and the
        // comparison never needs a heap-allocated copy of the method name.
        var methodName = MethodNameOf(fullMethodName);
        var operation = methodName switch
        {
            _ when methodName.SequenceEqual(LatticeTelemetryGrpcMethods.GetCatalogMethodName) =>
                LatticeTelemetryApiOperation.GetCatalog,
            _ when methodName.SequenceEqual(LatticeTelemetryGrpcMethods.QueryMethodName) =>
                LatticeTelemetryApiOperation.Query,
            _ => LatticeTelemetryApiOperation.Unknown,
        };

        var targetId = request switch
        {
            TelemetryQueryRequest q => q.QueryId,
            _ => null,
        };

        return (operation, targetId);
    }

    private static bool IsLatticeTelemetryApiMethod(string fullMethodName)
    {
        const string ServicePrefix = "/" + LatticeTelemetryGrpcMethods.ServiceName + "/";
        return fullMethodName.StartsWith(ServicePrefix, StringComparison.Ordinal);
    }

    /// <summary>
    /// The bare method name of a full gRPC method name, as a slice over the
    /// original string so the per-call path allocates nothing.
    /// </summary>
    private static ReadOnlySpan<char> MethodNameOf(string fullMethodName)
        => fullMethodName.AsSpan(fullMethodName.LastIndexOf('/') + 1);

    /// <summary>
    /// Whether the call targets a method exempt from authorization. The auth-scheme
    /// advertisement RPC must be reachable without a credential so a client can
    /// discover how to sign in before it holds one; every other telemetry method is
    /// enforced.
    /// </summary>
    /// <remarks>Exposed as <c>internal</c> so the exemption can be asserted directly
    /// in unit tests without standing up a gRPC server.</remarks>
    /// <param name="fullMethodName">The full gRPC method name.</param>
    /// <returns><see langword="true"/> when the method is exempt.</returns>
    internal static bool IsUnauthenticatedMethod(string fullMethodName)
        => MethodNameOf(fullMethodName).SequenceEqual(LatticeTelemetryGrpcMethods.GetAuthSchemeMethodName);
}
