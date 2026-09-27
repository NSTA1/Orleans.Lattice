using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Apps.Grpc;

internal sealed class LatticeAppsGrpcMethods
{
    public const string ServiceName = "orleans.lattice.api.apps";
    public const string ServicePrefix = "/" + ServiceName + "/";

    public Method<AppInstallRequest, AppLifecycleResult> Install { get; }
    public Method<AppsSlugRequest, AppLifecycleResult> Enable { get; }
    public Method<AppsSlugRequest, AppLifecycleResult> Disable { get; }
    public Method<AppsSlugRequest, AppLifecycleResult> Uninstall { get; }
    public Method<AppsEmptyRequest, AppCatalog> List { get; }
    public Method<AppsDescribeRequest, AppsDescribeResponse> Describe { get; }
    public Method<AppsSlugRequest, AppsConsentResponse> GetConsent { get; }
    public Method<AppConsentUpdate, AppConsentReport> UpdateConsent { get; }
    public Method<AppsEmptyRequest, LatticeAppsCapabilities> GetCapabilities { get; }
    public Method<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement> GetAuthScheme { get; }

    public LatticeAppsGrpcMethods(IServiceProvider serializers)
    {
        ArgumentNullException.ThrowIfNull(serializers);
        Install = Create<AppInstallRequest, AppLifecycleResult>(nameof(Install), serializers);
        Enable = Create<AppsSlugRequest, AppLifecycleResult>(nameof(Enable), serializers);
        Disable = Create<AppsSlugRequest, AppLifecycleResult>(nameof(Disable), serializers);
        Uninstall = Create<AppsSlugRequest, AppLifecycleResult>(nameof(Uninstall), serializers);
        List = Create<AppsEmptyRequest, AppCatalog>(nameof(List), serializers);
        Describe = Create<AppsDescribeRequest, AppsDescribeResponse>(nameof(Describe), serializers);
        GetConsent = Create<AppsSlugRequest, AppsConsentResponse>(nameof(GetConsent), serializers);
        UpdateConsent = Create<AppConsentUpdate, AppConsentReport>(nameof(UpdateConsent), serializers);
        GetCapabilities = Create<AppsEmptyRequest, LatticeAppsCapabilities>(nameof(GetCapabilities), serializers);
        GetAuthScheme = Create<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>(nameof(GetAuthScheme), serializers);
    }

    private static Method<TRequest, TResponse> Create<TRequest, TResponse>(string name, IServiceProvider serializers)
        where TRequest : class where TResponse : class
        => new(MethodType.Unary, ServiceName, name,
            LatticeAppsGrpcMarshallers.Create(serializers.GetRequiredService<Serializer<TRequest>>()),
            LatticeAppsGrpcMarshallers.Create(serializers.GetRequiredService<Serializer<TResponse>>()));
}
