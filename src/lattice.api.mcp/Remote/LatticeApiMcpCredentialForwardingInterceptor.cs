using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Client-side gRPC interceptor that forwards the resolved caller credential and
/// the caller's ambient active tenant to the remote cluster as request headers on
/// every outbound unary and server-streaming call, so the remote cluster enforces
/// the same fail-closed, permission-scoped, per-tenant behaviour as an in-silo
/// binding. The credential header name and scheme mirror the gRPC bindings'
/// server-side credential bridge (<c>authorization</c> / <c>Bearer</c> by
/// default); the active-tenant header mirrors the data binding's active-tenant
/// bridge (<c>lattice-active-tenant</c> by default), which re-validates the
/// asserted tenant against the caller's membership.
/// </summary>
/// <remarks>
/// The interceptor is a stateless singleton: it reads the ambient credential per
/// call via <see cref="ILatticeApiMcpRemoteCredentialSource"/> and the ambient
/// active tenant via <see cref="LatticeActiveTenantContext"/> (stamped by the tool
/// invocation seam this interceptor runs inside), so one interceptor-wrapped call
/// invoker serves every session and every group. When no credential resolves, the
/// call is left unauthenticated and the remote cluster fails closed; when no
/// active tenant is asserted, no tenant header is added and the outbound call is
/// byte-for-byte unchanged.
/// </remarks>
internal sealed class LatticeApiMcpCredentialForwardingInterceptor : Interceptor
{
    private readonly ILatticeApiMcpRemoteCredentialSource _credentialSource;
    private readonly IOptionsMonitor<LatticeApiMcpRemoteOptions> _options;

    /// <summary>Initialises the interceptor from the credential source and remote options.</summary>
    public LatticeApiMcpCredentialForwardingInterceptor(
        ILatticeApiMcpRemoteCredentialSource credentialSource,
        IOptionsMonitor<LatticeApiMcpRemoteOptions> options)
    {
        _credentialSource = credentialSource ?? throw new ArgumentNullException(nameof(credentialSource));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <inheritdoc />
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
        => continuation(request, WithForwardedContext(context));

    /// <inheritdoc />
    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncServerStreamingCallContinuation<TRequest, TResponse> continuation)
        => continuation(request, WithForwardedContext(context));

    private ClientInterceptorContext<TRequest, TResponse> WithForwardedContext<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context)
        where TRequest : class
        where TResponse : class
    {
        var options = _options.CurrentValue;
        Grpc.Core.Metadata? headers = null;

        AddCredentialHeader(options, ref headers);
        AddActiveTenantHeader(options, ref headers);

        if (headers is null)
        {
            return context;
        }

        // Merge into any caller-supplied headers rather than replacing them.
        var merged = context.Options.Headers ?? new Grpc.Core.Metadata();
        foreach (var entry in headers)
        {
            merged.Add(entry);
        }

        return new ClientInterceptorContext<TRequest, TResponse>(
            context.Method,
            context.Host,
            context.Options.WithHeaders(merged));
    }

    private void AddCredentialHeader(LatticeApiMcpRemoteOptions options, ref Grpc.Core.Metadata? headers)
    {
        var credential = _credentialSource.ResolveOutbound();
        if (credential is null || string.IsNullOrEmpty(credential.Value.Token))
        {
            return;
        }

        var headerName = options.CredentialHeaderName;
        if (string.IsNullOrEmpty(headerName))
        {
            return;
        }

        var scheme = options.CredentialScheme;
        var headerValue = string.IsNullOrEmpty(scheme)
            ? credential.Value.Token
            : scheme + " " + credential.Value.Token;

        (headers ??= new Grpc.Core.Metadata()).Add(headerName, headerValue);
    }

    private static void AddActiveTenantHeader(LatticeApiMcpRemoteOptions options, ref Grpc.Core.Metadata? headers)
    {
        // The ambient active tenant was stamped by the tool invocation seam
        // (CredentialStampingTool) and this interceptor runs within that scope, so
        // the outbound call carries the caller's asserted tenant to the silo-side
        // bridge, reaching the same per-tenant enforcement an in-silo head reaches
        // through the Orleans request context. Cold path (no active tenant): no
        // header is added and the outbound call is byte-for-byte unchanged.
        if (LatticeActiveTenantContext.Current is not { Value: { Length: > 0 } value })
        {
            return;
        }

        var headerName = options.ActiveTenantHeaderName;
        if (string.IsNullOrEmpty(headerName))
        {
            return;
        }

        (headers ??= new Grpc.Core.Metadata()).Add(headerName, value);
    }
}
