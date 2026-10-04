using Grpc.Core;
using static Orleans.Lattice.Api.Apps.Grpc.LatticeAppsGrpcUnaryMethods;

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
    public Method<AppRoleBindingsUpdate, AppRoleBindingsReport> UpdateRoleBindings { get; }

    public LatticeAppsGrpcMethods(IServiceProvider serializers)
    {
        ArgumentNullException.ThrowIfNull(serializers);
        Install = Unary<AppInstallRequest, AppLifecycleResult>(ServiceName, nameof(Install), serializers);
        Enable = Unary<AppsSlugRequest, AppLifecycleResult>(ServiceName, nameof(Enable), serializers);
        Disable = Unary<AppsSlugRequest, AppLifecycleResult>(ServiceName, nameof(Disable), serializers);
        Uninstall = Unary<AppsSlugRequest, AppLifecycleResult>(ServiceName, nameof(Uninstall), serializers);
        List = Unary<AppsEmptyRequest, AppCatalog>(ServiceName, nameof(List), serializers);
        Describe = Unary<AppsDescribeRequest, AppsDescribeResponse>(ServiceName, nameof(Describe), serializers);
        GetConsent = Unary<AppsSlugRequest, AppsConsentResponse>(ServiceName, nameof(GetConsent), serializers);
        UpdateConsent = Unary<AppConsentUpdate, AppConsentReport>(ServiceName, nameof(UpdateConsent), serializers);
        GetCapabilities = Unary<AppsEmptyRequest, LatticeAppsCapabilities>(ServiceName, nameof(GetCapabilities), serializers);
        GetAuthScheme = Unary<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>(ServiceName, nameof(GetAuthScheme), serializers);
        UpdateRoleBindings = Unary<AppRoleBindingsUpdate, AppRoleBindingsReport>(ServiceName, nameof(UpdateRoleBindings), serializers);
    }
}
