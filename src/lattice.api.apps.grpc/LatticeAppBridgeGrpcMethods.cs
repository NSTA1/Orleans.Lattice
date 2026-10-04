using Grpc.Core;
using static Orleans.Lattice.Api.Apps.Grpc.LatticeAppsGrpcUnaryMethods;

namespace Orleans.Lattice.Api.Apps.Grpc;

internal sealed class LatticeAppBridgeGrpcMethods
{
    public const string ServiceName = "orleans.lattice.api.apps.bridge";
    public const string ServicePrefix = "/" + ServiceName + "/";

    // Every bridge method is bounded in both directions, never by raising the service or channel limit.
    private const int MaxRequestBytes = LatticeAppsGrpcMarshallers.MaxBridgeRequestBytes;
    private const int MaxResponseBytes = LatticeAppsGrpcMarshallers.MaxBridgeResponseBytes;

    public Method<AppsBridgeKeyRequest, AppsBridgeGetResponse> Get { get; }
    public Method<AppsBridgeScanRequest, AppBridgePage> Scan { get; }
    public Method<AppsBridgeSetRequest, AppsEmptyRequest> Set { get; }
    public Method<AppsBridgeKeyRequest, AppsBridgeDeleteResponse> Delete { get; }

    public LatticeAppBridgeGrpcMethods(IServiceProvider serializers)
    {
        ArgumentNullException.ThrowIfNull(serializers);
        Get = BoundedUnary<AppsBridgeKeyRequest, AppsBridgeGetResponse>(ServiceName, nameof(Get), serializers, MaxRequestBytes, MaxResponseBytes);
        Scan = BoundedUnary<AppsBridgeScanRequest, AppBridgePage>(ServiceName, nameof(Scan), serializers, MaxRequestBytes, MaxResponseBytes);
        Set = BoundedUnary<AppsBridgeSetRequest, AppsEmptyRequest>(ServiceName, nameof(Set), serializers, MaxRequestBytes, MaxResponseBytes);
        Delete = BoundedUnary<AppsBridgeKeyRequest, AppsBridgeDeleteResponse>(ServiceName, nameof(Delete), serializers, MaxRequestBytes, MaxResponseBytes);
    }
}
