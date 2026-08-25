using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Configurable in-memory <see cref="ILatticeTenantAdmin"/> facade for the gRPC
/// service tests. Either returns a canned result per operation or throws a
/// pre-seeded exception, so the service's result-mapping and its
/// exception-to-<see cref="StatusCode"/> translation can both be exercised without
/// a real tenancy engine.
/// </summary>
internal sealed class FakeTenantAdmin : ILatticeTenantAdmin
{
    public Exception? Throw { get; set; }

    public string? LastTenantId { get; private set; }

    public Task<TenantCreationResult> CreateTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        return Throw is not null
            ? Task.FromException<TenantCreationResult>(Throw)
            : Task.FromResult(new TenantCreationResult { TenantId = tenantId, Status = TenantLifecycleStatus.Active });
    }

    public Task<TenantStatusChangeResult> SuspendTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        return Throw is not null
            ? Task.FromException<TenantStatusChangeResult>(Throw)
            : Task.FromResult(new TenantStatusChangeResult
            {
                TenantId = tenantId,
                PreviousStatus = TenantLifecycleStatus.Active,
                NewStatus = TenantLifecycleStatus.Suspended,
                Changed = true,
            });
    }

    public Task<TenantStatusChangeResult> ResumeTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        return Throw is not null
            ? Task.FromException<TenantStatusChangeResult>(Throw)
            : Task.FromResult(new TenantStatusChangeResult
            {
                TenantId = tenantId,
                PreviousStatus = TenantLifecycleStatus.Suspended,
                NewStatus = TenantLifecycleStatus.Active,
                Changed = true,
            });
    }

    public Task<TenantDeletionResult> DeleteTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        return Throw is not null
            ? Task.FromException<TenantDeletionResult>(Throw)
            : Task.FromResult(new TenantDeletionResult { TenantId = tenantId, CascadedTreeCount = 2 });
    }
}

/// <summary>A no-op credential bridge that resolves no credential (anonymous).</summary>
internal sealed class NullCredentialBridge : ILatticeTenantAdminApiCredentialBridge
{
    public LatticeCredential? Resolve(ServerCallContext context) => null;
}

/// <summary>A fixed auth-scheme source returning a pre-built advertisement.</summary>
internal sealed class FixedAuthSchemeSource(AuthSchemeAdvertisement advertisement) : ILatticeTenantAdminApiAuthSchemeSource
{
    public AuthSchemeAdvertisement GetAdvertisement() => advertisement;
}

/// <summary>
/// In-memory <see cref="CallInvoker"/> that closes the loop between the
/// <see cref="LatticeTenantAdminApiGrpcClient"/> and the
/// <see cref="LatticeTenantAdminGrpcService"/> without a network or a host. Every
/// request and response is serialized and deserialized with the same Orleans
/// serializer the production gRPC marshaller uses, so a round-trip through this
/// invoker exercises the full client-mapping -> wire-encoding -> service ->
/// wire-encoding -> client-mapping path deterministically.
/// </summary>
internal sealed class LoopbackCallInvoker(LatticeTenantAdminGrpcServiceBase service, IServiceProvider serializers)
    : CallInvoker
{
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
            "CreateTenant" => await service.CreateTenant((TenantAdminTenantRequest)(object)wireRequest, context),
            "SuspendTenant" => await service.SuspendTenant((TenantAdminTenantRequest)(object)wireRequest, context),
            "ResumeTenant" => await service.ResumeTenant((TenantAdminTenantRequest)(object)wireRequest, context),
            "DeleteTenant" => await service.DeleteTenant((TenantAdminTenantRequest)(object)wireRequest, context),
            "GetAuthScheme" => await service.GetAuthScheme((AuthSchemeAdvertisementRequest)(object)wireRequest, context),
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
