using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Orleans.Lattice.Explorer.Core.Connection;

/// <summary>
/// A client interceptor that attaches per-call <see cref="CallCredentials"/> to
/// every outgoing RPC by composing them onto the call options. The credentials
/// are resolved asynchronously by gRPC at call time, which lets a bearer-token
/// provider refresh a near-expiry token before the <c>authorization</c> header
/// is written.
/// </summary>
internal sealed class CallCredentialsInterceptor : Interceptor
{
    private readonly CallCredentials _credentials;

    /// <summary>Creates the interceptor over the supplied call credentials.</summary>
    /// <param name="credentials">The call credentials to attach to every call.</param>
    public CallCredentialsInterceptor(CallCredentials credentials)
    {
        _credentials = credentials ?? throw new ArgumentNullException(nameof(credentials));
    }

    /// <inheritdoc />
    public override TResponse BlockingUnaryCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        BlockingUnaryCallContinuation<TRequest, TResponse> continuation)
        => continuation(request, WithCredentials(context));

    /// <inheritdoc />
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
        => continuation(request, WithCredentials(context));

    /// <inheritdoc />
    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncServerStreamingCallContinuation<TRequest, TResponse> continuation)
        => continuation(request, WithCredentials(context));

    private ClientInterceptorContext<TRequest, TResponse> WithCredentials<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context)
        where TRequest : class
        where TResponse : class
        => new(context.Method, context.Host, context.Options.WithCredentials(_credentials));
}
