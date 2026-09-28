using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Apps.Grpc;

internal sealed class LatticeAppWorkspaceGrpcMethods
{
    public const string ServiceName = "orleans.lattice.api.apps.workspace";
    public const string ServicePrefix = "/" + ServiceName + "/";

    public Method<AppsEmptyRequest, AppsWorkspaceListResponse> ListMyApps { get; }
    public Method<AppsSlugRequest, AppsWorkspaceDescribeResponse> DescribeMyApp { get; }
    public Method<AppsSlugRequest, AppsIconResponse> GetIcon { get; }
    public Method<AppsUiAssetRequest, AppsUiAssetResponse> GetUiAsset { get; }

    public LatticeAppWorkspaceGrpcMethods(IServiceProvider serializers)
    {
        ArgumentNullException.ThrowIfNull(serializers);
        ListMyApps = Create<AppsEmptyRequest, AppsWorkspaceListResponse>(nameof(ListMyApps), serializers);
        DescribeMyApp = Create<AppsSlugRequest, AppsWorkspaceDescribeResponse>(nameof(DescribeMyApp), serializers);
        GetIcon = Create<AppsSlugRequest, AppsIconResponse>(nameof(GetIcon), serializers, LatticeAppsGrpcMarshallers.MaxAssetMessageBytes);
        GetUiAsset = Create<AppsUiAssetRequest, AppsUiAssetResponse>(nameof(GetUiAsset), serializers, LatticeAppsGrpcMarshallers.MaxAssetMessageBytes);
    }

    private static Method<TRequest, TResponse> Create<TRequest, TResponse>(
        string name, IServiceProvider serializers, int? maxMessageBytes = null)
        where TRequest : class where TResponse : class
    {
        var request = serializers.GetRequiredService<Serializer<TRequest>>();
        var response = serializers.GetRequiredService<Serializer<TResponse>>();
        return maxMessageBytes is { } max
            ? new(MethodType.Unary, ServiceName, name,
                LatticeAppsGrpcMarshallers.CreateBounded(request, max), LatticeAppsGrpcMarshallers.CreateBounded(response, max))
            : new(MethodType.Unary, ServiceName, name,
                LatticeAppsGrpcMarshallers.Create(request), LatticeAppsGrpcMarshallers.Create(response));
    }
}
