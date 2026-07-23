using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Replication.Grpc.Tests;

/// <summary>
/// In-memory <see cref="CallInvoker"/> that closes the loop between the
/// <see cref="LatticeReplicationApiGrpcClient"/> and the
/// <see cref="LatticeReplicationGrpcService"/> without a network or a host. Every
/// request and response is serialized and deserialized with the same Orleans
/// serializer the production gRPC marshaller uses, so a round-trip through this
/// invoker exercises the full client-mapping -> wire-encoding -> service ->
/// wire-encoding -> client-mapping path deterministically.
/// </summary>
internal sealed class LoopbackCallInvoker : CallInvoker
{
    private readonly LatticeReplicationGrpcServiceBase _service;
    private readonly IServiceProvider _serializers;

    public LoopbackCallInvoker(LatticeReplicationGrpcServiceBase service, IServiceProvider serializers)
    {
        _service = service;
        _serializers = serializers;
    }

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options,
        TRequest request)
    {
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
            "EnableReplication" => await _service.EnableReplication((ReplicationEnableRequestMessage)(object)wireRequest, context),
            "DisableReplication" => await _service.DisableReplication((ReplicationDisableRequestMessage)(object)wireRequest, context),
            "GetReplicationConfig" => await _service.GetReplicationConfig((ReplicationGetConfigRequest)(object)wireRequest, context),
            "GetAuthScheme" => await _service.GetAuthScheme((AuthSchemeAdvertisementRequest)(object)wireRequest, context),
            _ => throw new NotSupportedException($"Unmapped loopback method '{method.Name}'."),
        };

        return RoundTrip((TResponse)response);
    }

    private T RoundTrip<T>(T value)
    {
        var serializer = _serializers.GetRequiredService<Serializer<T>>();
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
