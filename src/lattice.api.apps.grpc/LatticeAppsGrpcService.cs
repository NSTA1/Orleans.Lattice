using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Apps.Grpc;

internal sealed class LatticeAppsGrpcService(
    ILatticeAppsControl control,
    ILatticeAppsApiCredentialBridge credentialBridge,
    ILatticeAppsApiAuthSchemeSource authSchemeSource,
    IOptions<LatticeAppsApiGrpcOptions> options,
    ILogger<LatticeAppsGrpcService> logger)
{
    public Task<AppLifecycleResult> Install(AppInstallRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (c, r, ct) => c.InstallAsync(r, ct));

    public Task<AppLifecycleResult> Enable(AppsSlugRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (c, r, ct) => c.EnableAsync(r.Slug, ct));

    public Task<AppLifecycleResult> Disable(AppsSlugRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (c, r, ct) => c.DisableAsync(r.Slug, ct));

    public Task<AppLifecycleResult> Uninstall(AppsSlugRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (c, r, ct) => c.UninstallAsync(r.Slug, ct));

    public Task<AppCatalog> List(AppsEmptyRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (c, _, ct) => c.ListAsync(ct));

    public async Task<AppsDescribeResponse> Describe(AppsDescribeRequest request, ServerCallContext context)
        => new() { Descriptor = await InvokeAsync(request, context,
            static (c, r, ct) => c.DescribeAsync(r.Slug, r.Version, ct)).ConfigureAwait(false) };

    public async Task<AppsConsentResponse> GetConsent(AppsSlugRequest request, ServerCallContext context)
        => new() { Consent = await InvokeAsync(request, context,
            static (c, r, ct) => c.GetConsentAsync(r.Slug, ct)).ConfigureAwait(false) };

    public Task<AppConsentReport> UpdateConsent(AppConsentUpdate request, ServerCallContext context)
        => InvokeAsync(request, context, static (c, r, ct) => c.UpdateConsentAsync(r, ct));

    public Task<LatticeAppsCapabilities> GetCapabilities(AppsEmptyRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (c, _, ct) => c.GetCapabilitiesAsync(ct));

    public Task<AuthSchemeAdvertisement> GetAuthScheme(AuthSchemeAdvertisementRequest request, ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);
        try
        {
            return Task.FromResult(authSchemeSource.GetAdvertisement());
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Api.Apps: auth-scheme discovery failed.");
            throw new RpcException(new Status(StatusCode.Internal, "App-control auth-scheme discovery failed."));
        }
    }

    private IDisposable? StampActiveTenant(ServerCallContext context)
        => LatticeActiveTenantAssertion.Stamp(
            context, static (c, name) => c.RequestHeaders.GetValue(name), options.Value.ActiveTenantHeaderName);

    private async Task<TResponse> InvokeAsync<TRequest, TResponse>(
        TRequest request, ServerCallContext context,
        Func<ILatticeAppsControl, TRequest, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);
        try
        {
            using var credentialScope = LatticeCredentialContext.With(credentialBridge.Resolve(context));
            using var tenantScope = StampActiveTenant(context);
            return await handler(control, request, context.CancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(StatusCode.Cancelled, "The app-control request was cancelled."));
        }
        catch (LatticeAuthorizationDeniedException)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, "App-control permission was denied."));
        }
        catch (LatticeTenantAccessDeniedException)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, "The asserted tenant is not permitted."));
        }
        catch (ArgumentException ex)
        {
            logger.LogWarning(ex, "Api.Apps: invalid request to {Method}.", context.Method);
            throw new RpcException(new Status(StatusCode.InvalidArgument, "The app-control request is invalid."));
        }
        catch (KeyNotFoundException)
        {
            throw new RpcException(new Status(StatusCode.NotFound, "The requested app or version was not found."));
        }
        catch (InvalidOperationException ex)
        {
            logger.LogWarning(ex, "Api.Apps: precondition failed for {Method}.", context.Method);
            throw new RpcException(new Status(StatusCode.FailedPrecondition, "The app-control precondition was not met."));
        }
        catch (Exception ex)
        {
            // Even custom facade failures must not disclose composed physical tree ids.
            logger.LogError(ex, "Api.Apps: gRPC call to {Method} failed.", context.Method);
            throw new RpcException(new Status(StatusCode.Internal, "The app-control request failed."));
        }
    }
}
