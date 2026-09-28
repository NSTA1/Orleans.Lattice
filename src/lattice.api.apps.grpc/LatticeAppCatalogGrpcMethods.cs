using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Apps.Grpc;

internal sealed class LatticeAppCatalogGrpcMethods
{
    public const string ServiceName = "orleans.lattice.api.apps.catalog";
    public const string ServicePrefix = "/" + ServiceName + "/";

    public Method<AppsEmptyRequest, AppsSourcesResponse> ListSources { get; }
    public Method<AvailableAppQuery, AvailableAppPage> ListAvailable { get; }
    public Method<AppsSourceAppRequest, AppsDescribeResponse> DescribeFromSource { get; }
    public Method<AppsSourceAppRequest, AppsIconResponse> GetIcon { get; }
    public Method<AppsEmptyRequest, LatticeAppCatalogCapabilities> GetCapabilities { get; }

    public LatticeAppCatalogGrpcMethods(IServiceProvider serializers)
    {
        ArgumentNullException.ThrowIfNull(serializers);
        ListSources = Create<AppsEmptyRequest, AppsSourcesResponse>(nameof(ListSources), serializers);
        ListAvailable = Create<AvailableAppQuery, AvailableAppPage>(nameof(ListAvailable), serializers);
        DescribeFromSource = Create<AppsSourceAppRequest, AppsDescribeResponse>(nameof(DescribeFromSource), serializers);
        GetIcon = Create<AppsSourceAppRequest, AppsIconResponse>(nameof(GetIcon), serializers, LatticeAppsGrpcMarshallers.MaxAssetMessageBytes);
        GetCapabilities = Create<AppsEmptyRequest, LatticeAppCatalogCapabilities>(nameof(GetCapabilities), serializers);
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
