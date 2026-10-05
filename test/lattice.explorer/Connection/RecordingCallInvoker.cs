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

    /// <summary>The whole options each call reached the bottom with, in call order.</summary>
    public List<CallOptions> Options { get; } = [];

    /// <summary>The method each call named, in call order.</summary>
    public List<IMethod> Methods { get; } = [];

    /// <summary>The host each call named, in call order; <see langword="null"/> for the channel's own.</summary>
    public List<string?> Hosts { get; } = [];

    /// <summary>The request each call carried, in call order; <see langword="null"/> for a streaming-request shape.</summary>
    public List<object?> Requests { get; } = [];

    /// <inheritdoc />
    public override TResponse BlockingUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options,
        TRequest request)
    {
        Record(method, host, options, request);
        return default!;
    }

    /// <inheritdoc />
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options,
        TRequest request)
    {
        Record(method, host, options, request);
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
        Record(method, host, options, request);
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
        Record(method, host, options, request: null);
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
        Record(method, host, options, request: null);
        return new AsyncDuplexStreamingCall<TRequest, TResponse>(
            Substitute.For<IClientStreamWriter<TRequest>>(),
            Substitute.For<IAsyncStreamReader<TResponse>>(),
            Task.FromResult(new GrpcMetadata()),
            static () => Status.DefaultSuccess,
            static () => [],
            static () => { });
    }

    private void Record(IMethod method, string? host, CallOptions options, object? request)
    {
        Headers.Add(options.Headers);
        Options.Add(options);
        Methods.Add(method);
        Hosts.Add(host);
        Requests.Add(request);
    }
}
