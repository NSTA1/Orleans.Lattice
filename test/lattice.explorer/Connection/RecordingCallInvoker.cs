using Grpc.Core;
using NSubstitute;
using GrpcMetadata = Grpc.Core.Metadata;

namespace Orleans.Lattice.Explorer.Tests.Connection;

/// <summary>
/// The bottom of a call pipeline under test: records the metadata each call
/// reached it with and answers every call shape with an empty success, so an
/// interceptor above it can be exercised without a channel.
/// </summary>
internal sealed class RecordingCallInvoker : CallInvoker
{
    /// <summary>The metadata each call carried, in call order; <see langword="null"/> when it carried none.</summary>
    public List<GrpcMetadata?> Headers { get; } = [];

    /// <inheritdoc />
    public override TResponse BlockingUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options,
        TRequest request)
    {
        Headers.Add(options.Headers);
        return default!;
    }

    /// <inheritdoc />
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options,
        TRequest request)
    {
        Headers.Add(options.Headers);
        return new AsyncUnaryCall<TResponse>(
            Task.FromResult(default(TResponse)!),
            Task.FromResult(new GrpcMetadata()),
            static () => Status.DefaultSuccess,
            static () => [],
            static () => { });
    }

    /// <inheritdoc />
    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options,
        TRequest request)
    {
        Headers.Add(options.Headers);
        return new AsyncServerStreamingCall<TResponse>(
            Substitute.For<IAsyncStreamReader<TResponse>>(),
            Task.FromResult(new GrpcMetadata()),
            static () => Status.DefaultSuccess,
            static () => [],
            static () => { });
    }

    /// <inheritdoc />
    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options)
    {
        Headers.Add(options.Headers);
        return new AsyncClientStreamingCall<TRequest, TResponse>(
            Substitute.For<IClientStreamWriter<TRequest>>(),
            Task.FromResult(default(TResponse)!),
            Task.FromResult(new GrpcMetadata()),
            static () => Status.DefaultSuccess,
            static () => [],
            static () => { });
    }

    /// <inheritdoc />
    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options)
    {
        Headers.Add(options.Headers);
        return new AsyncDuplexStreamingCall<TRequest, TResponse>(
            Substitute.For<IClientStreamWriter<TRequest>>(),
            Substitute.For<IAsyncStreamReader<TResponse>>(),
            Task.FromResult(new GrpcMetadata()),
            static () => Status.DefaultSuccess,
            static () => [],
            static () => { });
    }
}
