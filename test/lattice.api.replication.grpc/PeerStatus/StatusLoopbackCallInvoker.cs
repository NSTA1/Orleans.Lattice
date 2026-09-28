using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Replication.Grpc.Tests.PeerStatus;

/// <summary>
/// In-memory <see cref="CallInvoker"/> that closes the loop between the
/// <see cref="LatticeReplicationStatusGrpcClient"/> and the
/// <see cref="LatticeReplicationStatusGrpcServiceBase"/> without a network or a
/// host. Every request and response is serialized and deserialized with the same
/// Orleans serializer the production marshaller uses, so a round trip exercises
/// the full wire encoding in both directions.
/// </summary>
internal sealed class StatusLoopbackCallInvoker(
    LatticeReplicationStatusGrpcServiceBase service,
    IServiceProvider serializers) : CallInvoker
{
    /// <summary>The call options the most recent call carried.</summary>
    public CallOptions LastOptions { get; private set; }

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options,
        TRequest request)
    {
        LastOptions = options;
        var responseTask = DispatchAsync(method, request, options.CancellationToken);
        return new AsyncUnaryCall<TResponse>(
            responseTask,
            Task.FromResult(new global::Grpc.Core.Metadata()),
            () => Status.DefaultSuccess,
            () => new global::Grpc.Core.Metadata(),
            () => { });
    }

    private async Task<TResponse> DispatchAsync<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        CancellationToken cancellationToken)
        where TRequest : class
        where TResponse : class
    {
        var wireRequest = RoundTrip(request);
        var context = new FakeServerCallContext(method.FullName, cancellationToken: cancellationToken);

        object response = method.Name switch
        {
            LatticeReplicationStatusGrpcMethods.GetPeerStatusMethodName =>
                await service.GetPeerStatus((ReplicationPeerStatusQuery)(object)wireRequest, context),
            _ => throw new NotSupportedException($"Unmapped loopback method '{method.Name}'."),
        };

        return RoundTrip((TResponse)response);
    }

    private T RoundTrip<T>(T value)
    {
        var serializer = serializers.GetRequiredService<Serializer<T>>();
        return serializer.Deserialize(serializer.SerializeToArray(value));
    }

    public override TResponse BlockingUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
        throw new NotSupportedException();

    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
        throw new NotSupportedException();

    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options) =>
        throw new NotSupportedException();

    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options) =>
        throw new NotSupportedException();
}
