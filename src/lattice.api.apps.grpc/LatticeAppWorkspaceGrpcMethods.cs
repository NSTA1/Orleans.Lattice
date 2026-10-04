using Grpc.Core;
using static Orleans.Lattice.Api.Apps.Grpc.LatticeAppsGrpcUnaryMethods;

namespace Orleans.Lattice.Api.Apps.Grpc;

internal sealed class LatticeAppWorkspaceGrpcMethods
{
    public const string ServiceName = "orleans.lattice.api.apps.workspace";
    public const string ServicePrefix = "/" + ServiceName + "/";

    // The asset-carrying methods are bounded in both directions; the rest ride the service limit.
    private const int MaxAssetBytes = LatticeAppsGrpcMarshallers.MaxAssetMessageBytes;

    public Method<AppsEmptyRequest, AppsWorkspaceListResponse> ListMyApps { get; }
    public Method<AppsSlugRequest, AppsWorkspaceDescribeResponse> DescribeMyApp { get; }
    public Method<AppsSlugRequest, AppsIconResponse> GetIcon { get; }
    public Method<AppsUiAssetRequest, AppsUiAssetResponse> GetUiAsset { get; }

    public LatticeAppWorkspaceGrpcMethods(IServiceProvider serializers)
    {
        ArgumentNullException.ThrowIfNull(serializers);
        ListMyApps = Unary<AppsEmptyRequest, AppsWorkspaceListResponse>(ServiceName, nameof(ListMyApps), serializers);
        DescribeMyApp = Unary<AppsSlugRequest, AppsWorkspaceDescribeResponse>(ServiceName, nameof(DescribeMyApp), serializers);
        GetIcon = BoundedUnary<AppsSlugRequest, AppsIconResponse>(ServiceName, nameof(GetIcon), serializers, MaxAssetBytes, MaxAssetBytes);
        GetUiAsset = BoundedUnary<AppsUiAssetRequest, AppsUiAssetResponse>(ServiceName, nameof(GetUiAsset), serializers, MaxAssetBytes, MaxAssetBytes);
    }
}
