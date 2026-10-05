using Grpc.Core;
using static Orleans.Lattice.Api.Apps.Grpc.LatticeAppsGrpcUnaryMethods;

namespace Orleans.Lattice.Api.Apps.Grpc;

internal sealed class LatticeAppCatalogGrpcMethods
{
    public const string ServiceName = "orleans.lattice.api.apps.catalog";
    public const string ServicePrefix = "/" + ServiceName + "/";

    // The asset-carrying methods are bounded in both directions; the rest ride the service limit.
    private const int MaxAssetBytes = LatticeAppsGrpcMarshallers.MaxAssetMessageBytes;

    public Method<AppsEmptyRequest, AppsSourcesResponse> ListSources { get; }
    public Method<AvailableAppQuery, AvailableAppPage> ListAvailable { get; }
    public Method<AppsSourceAppRequest, AppsDescribeResponse> DescribeFromSource { get; }
    public Method<AppsSourceAppRequest, AppsIconResponse> GetIcon { get; }
    public Method<AppsEmptyRequest, LatticeAppCatalogCapabilities> GetCapabilities { get; }

    public LatticeAppCatalogGrpcMethods(IServiceProvider serializers)
    {
        ArgumentNullException.ThrowIfNull(serializers);
        ListSources = Unary<AppsEmptyRequest, AppsSourcesResponse>(ServiceName, nameof(ListSources), serializers);
        ListAvailable = Unary<AvailableAppQuery, AvailableAppPage>(ServiceName, nameof(ListAvailable), serializers);
        DescribeFromSource = Unary<AppsSourceAppRequest, AppsDescribeResponse>(ServiceName, nameof(DescribeFromSource), serializers);
        GetIcon = BoundedUnary<AppsSourceAppRequest, AppsIconResponse>(ServiceName, nameof(GetIcon), serializers, MaxAssetBytes, MaxAssetBytes);
        GetCapabilities = Unary<AppsEmptyRequest, LatticeAppCatalogCapabilities>(ServiceName, nameof(GetCapabilities), serializers);
    }
}
