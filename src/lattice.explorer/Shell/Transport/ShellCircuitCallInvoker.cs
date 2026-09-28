using Grpc.Core;

namespace Orleans.Lattice.Explorer.Shell.Transport;

/// <summary>
/// A <see cref="CallInvoker"/> that resolves the circuit's current invoker on
/// every call and forwards to it. A typed gRPC client is built once per circuit
/// over this invoker and still follows every endpoint change and every sign-in,
/// because the channel, and the credential attached to it, are chosen at call
/// time rather than captured when the client was built.
/// </summary>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellCircuitCallInvoker(ShellTransportChannel channel) : CallInvoker
{
    /// <inheritdoc />
    public override TResponse BlockingUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options,
        TRequest request) =>
        channel.ResolveInvoker().BlockingUnaryCall(method, host, options, request);

    /// <inheritdoc />
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options,
        TRequest request) =>
        channel.ResolveInvoker().AsyncUnaryCall(method, host, options, request);

    /// <inheritdoc />
    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options,
        TRequest request) =>
        channel.ResolveInvoker().AsyncServerStreamingCall(method, host, options, request);

    /// <inheritdoc />
    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options) =>
        channel.ResolveInvoker().AsyncClientStreamingCall(method, host, options);

    /// <inheritdoc />
    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options) =>
        channel.ResolveInvoker().AsyncDuplexStreamingCall(method, host, options);
}
