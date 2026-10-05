using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>
/// Builds the unary gRPC <see cref="Method{TRequest, TResponse}"/> descriptors
/// the installable-apps services expose, resolving each message's Orleans
/// <see cref="Serializer{T}"/> from the supplied provider and wrapping it in the
/// matching <see cref="LatticeAppsGrpcMarshallers"/> marshaller. Shared by every
/// apps method table so a service differs from another only in its name and
/// its per-method message bounds.
/// </summary>
internal static class LatticeAppsGrpcUnaryMethods
{
    /// <summary>Builds a unary method whose messages ride the service and channel limits.</summary>
    /// <typeparam name="TRequest">The request message type.</typeparam>
    /// <typeparam name="TResponse">The response message type.</typeparam>
    /// <param name="serviceName">The gRPC service name.</param>
    /// <param name="name">The method name.</param>
    /// <param name="serializers">The provider the Orleans serializers are resolved from.</param>
    /// <returns>The method descriptor.</returns>
    public static Method<TRequest, TResponse> Unary<TRequest, TResponse>(
        string serviceName, string name, IServiceProvider serializers)
        where TRequest : class where TResponse : class
        => new(MethodType.Unary, serviceName, name,
            LatticeAppsGrpcMarshallers.Create(serializers.GetRequiredService<Serializer<TRequest>>()),
            LatticeAppsGrpcMarshallers.Create(serializers.GetRequiredService<Serializer<TResponse>>()));

    /// <summary>
    /// Builds a unary method whose request and response are each bounded per
    /// method, never by raising the service or channel limit.
    /// </summary>
    /// <typeparam name="TRequest">The request message type.</typeparam>
    /// <typeparam name="TResponse">The response message type.</typeparam>
    /// <param name="serviceName">The gRPC service name.</param>
    /// <param name="name">The method name.</param>
    /// <param name="serializers">The provider the Orleans serializers are resolved from.</param>
    /// <param name="maxRequestBytes">The largest request accepted, in bytes.</param>
    /// <param name="maxResponseBytes">The largest response accepted, in bytes.</param>
    /// <returns>The method descriptor.</returns>
    public static Method<TRequest, TResponse> BoundedUnary<TRequest, TResponse>(
        string serviceName, string name, IServiceProvider serializers, int maxRequestBytes, int maxResponseBytes)
        where TRequest : class where TResponse : class
        => new(MethodType.Unary, serviceName, name,
            LatticeAppsGrpcMarshallers.CreateBounded(serializers.GetRequiredService<Serializer<TRequest>>(), maxRequestBytes),
            LatticeAppsGrpcMarshallers.CreateBounded(serializers.GetRequiredService<Serializer<TResponse>>(), maxResponseBytes));
}
