using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Client-side gRPC interceptor that forwards the resolved caller credential to
/// the remote cluster as a request header on every outbound unary and
/// server-streaming call, so the remote cluster enforces the same fail-closed,
/// permission-scoped behaviour as an in-silo binding. The header name and scheme
/// mirror the gRPC bindings' server-side credential bridge
/// (<c>authorization</c> / <c>Bearer</c> by default), which strips the scheme
/// prefix and resolves the remaining token to the caller's subject.
/// </summary>
/// <remarks>
/// The interceptor is a stateless singleton: it reads the ambient credential per
/// call via <see cref="ILatticeApiMcpRemoteCredentialSource"/>, so one
/// interceptor-wrapped call invoker serves every session and every group. When no
/// credential resolves, the call is left unmodified (anonymous) and the remote
/// cluster fails closed.
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
        => continuation(request, WithCredential(context));

    /// <inheritdoc />
    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncServerStreamingCallContinuation<TRequest, TResponse> continuation)
        => continuation(request, WithCredential(context));

    private ClientInterceptorContext<TRequest, TResponse> WithCredential<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context)
        where TRequest : class
        where TResponse : class
    {
        var credential = _credentialSource.ResolveOutbound();
        if (credential is null || string.IsNullOrEmpty(credential.Value.Token))
        {
            return context;
        }

        var options = _options.CurrentValue;
        var headerName = options.CredentialHeaderName;
        if (string.IsNullOrEmpty(headerName))
        {
            return context;
        }

        var scheme = options.CredentialScheme;
        var headerValue = string.IsNullOrEmpty(scheme)
            ? credential.Value.Token
            : scheme + " " + credential.Value.Token;

        var headers = context.Options.Headers ?? new Grpc.Core.Metadata();
        headers.Add(headerName, headerValue);
        return new ClientInterceptorContext<TRequest, TResponse>(
            context.Method,
            context.Host,
            context.Options.WithHeaders(headers));
    }
}
