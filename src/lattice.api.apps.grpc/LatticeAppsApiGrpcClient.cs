using Grpc.Core;

namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Strongly typed app-control client. The supplied invoker owns routing, credentials, TLS, and deadlines.</summary>
public sealed class LatticeAppsApiGrpcClient : ILatticeAppsControl
{
    private static readonly AppsEmptyRequest EmptyRequest = new();
    private static readonly AuthSchemeAdvertisementRequest AdvertisementRequest = new();
    private readonly CallInvoker _invoker;
    private readonly LatticeAppsGrpcMethods _methods;

    private LatticeAppsApiGrpcClient(CallInvoker invoker, LatticeAppsGrpcMethods methods)
    {
        _invoker = invoker;
        _methods = methods;
    }

    /// <summary>Creates a client using Orleans serializers from the supplied provider; does not own either argument.</summary>
    public static LatticeAppsApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(callInvoker);
        ArgumentNullException.ThrowIfNull(serializerProvider);
        return new(callInvoker, new LatticeAppsGrpcMethods(serializerProvider));
    }

    /// <inheritdoc />
    public Task<AppLifecycleResult> InstallAsync(AppInstallRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return UnaryAsync(_methods.Install, request, cancellationToken);
    }

    /// <inheritdoc />
    public Task<AppLifecycleResult> EnableAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(appSlug);
        return UnaryAsync(_methods.Enable, new AppsSlugRequest { Slug = appSlug }, cancellationToken);
    }

    /// <inheritdoc />
    public Task<AppLifecycleResult> DisableAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(appSlug);
        return UnaryAsync(_methods.Disable, new AppsSlugRequest { Slug = appSlug }, cancellationToken);
    }

    /// <inheritdoc />
    public Task<AppLifecycleResult> UninstallAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(appSlug);
        return UnaryAsync(_methods.Uninstall, new AppsSlugRequest { Slug = appSlug }, cancellationToken);
    }

    /// <inheritdoc />
    public Task<AppCatalog> ListAsync(CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.List, EmptyRequest, cancellationToken);

    /// <inheritdoc />
    public async Task<AppDescriptor?> DescribeAsync(string appSlug, string? version = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(appSlug);
        return (await UnaryAsync(_methods.Describe,
            new AppsDescribeRequest { Slug = appSlug, Version = version }, cancellationToken).ConfigureAwait(false)).Descriptor;
    }

    /// <inheritdoc />
    public async Task<AppConsentReport?> GetConsentAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(appSlug);
        return (await UnaryAsync(_methods.GetConsent,
            new AppsSlugRequest { Slug = appSlug }, cancellationToken).ConfigureAwait(false)).Consent;
    }

    /// <inheritdoc />
    public Task<AppConsentReport> UpdateConsentAsync(AppConsentUpdate request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return UnaryAsync(_methods.UpdateConsent, request, cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeAppsCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.GetCapabilities, EmptyRequest, cancellationToken);

    /// <summary>Discovers public sign-in schemes without requiring a credential.</summary>
    public async Task<IReadOnlyList<AuthSchemeDescriptor>> GetAuthSchemeAsync(CancellationToken cancellationToken = default)
        => (await UnaryAsync(_methods.GetAuthScheme, AdvertisementRequest, cancellationToken).ConfigureAwait(false)).Schemes;

    private async Task<TResponse> UnaryAsync<TRequest, TResponse>(
        Method<TRequest, TResponse> method, TRequest request, CancellationToken cancellationToken)
        where TRequest : class where TResponse : class
    {
        using var call = _invoker.AsyncUnaryCall(method, null, new CallOptions(cancellationToken: cancellationToken), request);
        return await call.ResponseAsync.ConfigureAwait(false);
    }
}
