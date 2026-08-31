using Grpc.Core;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// A <see cref="CallInvoker"/> test double whose unary calls complete
/// synchronously with a caller-supplied response object, so the client's
/// request-shaping and response-mapping logic can be exercised deterministically
/// without a live gRPC server. Only the unary path is supported; the streaming
/// paths throw, so a test that reaches them fails loudly.
/// </summary>
internal sealed class UnaryResponseCallInvoker : CallInvoker
{
    private readonly object _response;

    public UnaryResponseCallInvoker(object response) => _response = response;

    /// <summary>The most recent unary request observed, for assertion.</summary>
    public object? LastRequest { get; private set; }

    /// <summary>The most recent unary method name observed, for assertion.</summary>
    public string? LastMethodName { get; private set; }

    /// <summary>The cancellation token the client threaded onto the call options.</summary>
    public CancellationToken LastCancellationToken { get; private set; }

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options,
        TRequest request)
    {
        LastRequest = request;
        LastMethodName = method.Name;
        LastCancellationToken = options.CancellationToken;
        return new AsyncUnaryCall<TResponse>(
            Task.FromResult((TResponse)_response),
            Task.FromResult(new global::Grpc.Core.Metadata()),
            static () => Status.DefaultSuccess,
            static () => new global::Grpc.Core.Metadata(),
            static () => { });
    }

    public override TResponse BlockingUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options,
        TRequest request) => throw new NotSupportedException();

    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options) => throw new NotSupportedException();

    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options,
        TRequest request) => throw new NotSupportedException();

    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options) => throw new NotSupportedException();
}
