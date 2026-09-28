using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Apps.Grpc;

internal sealed class LatticeAppBridgeGrpcMethods
{
    public const string ServiceName = "orleans.lattice.api.apps.bridge";
    public const string ServicePrefix = "/" + ServiceName + "/";

    public Method<AppsBridgeKeyRequest, AppsBridgeGetResponse> Get { get; }
    public Method<AppsBridgeScanRequest, AppBridgePage> Scan { get; }
    public Method<AppsBridgeSetRequest, AppsEmptyRequest> Set { get; }
    public Method<AppsBridgeKeyRequest, AppsBridgeDeleteResponse> Delete { get; }

    public LatticeAppBridgeGrpcMethods(IServiceProvider serializers)
    {
        ArgumentNullException.ThrowIfNull(serializers);
        Get = Create<AppsBridgeKeyRequest, AppsBridgeGetResponse>(nameof(Get), serializers);
        Scan = Create<AppsBridgeScanRequest, AppBridgePage>(nameof(Scan), serializers);
        Set = Create<AppsBridgeSetRequest, AppsEmptyRequest>(nameof(Set), serializers);
        Delete = Create<AppsBridgeKeyRequest, AppsBridgeDeleteResponse>(nameof(Delete), serializers);
    }

    // Every bridge method is bounded in both directions, never by raising the service or channel limit.
    private static Method<TRequest, TResponse> Create<TRequest, TResponse>(string name, IServiceProvider serializers)
        where TRequest : class where TResponse : class
        => new(MethodType.Unary, ServiceName, name,
            LatticeAppsGrpcMarshallers.CreateBounded(
                serializers.GetRequiredService<Serializer<TRequest>>(), LatticeAppsGrpcMarshallers.MaxBridgeRequestBytes),
            LatticeAppsGrpcMarshallers.CreateBounded(
                serializers.GetRequiredService<Serializer<TResponse>>(), LatticeAppsGrpcMarshallers.MaxBridgeResponseBytes));
}
