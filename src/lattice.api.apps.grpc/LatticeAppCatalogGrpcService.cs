using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Apps.Grpc;

internal sealed class LatticeAppCatalogGrpcService(
    ILatticeAppCatalog catalog,
    ILatticeAppsApiCredentialBridge credentialBridge,
    IOptions<LatticeAppsApiGrpcOptions> options,
    ILogger<LatticeAppCatalogGrpcService> logger)
{
    public async Task<AppsSourcesResponse> ListSources(AppsEmptyRequest request, ServerCallContext context)
        => new() { Sources = await InvokeAsync(request, context, static (c, _, ct) => c.ListSourcesAsync(ct)).ConfigureAwait(false) };

    public Task<AvailableAppPage> ListAvailable(AvailableAppQuery request, ServerCallContext context)
        => InvokeAsync(request, context, static (c, r, ct) => c.ListAvailableAsync(r, ct));

    public async Task<AppsDescribeResponse> DescribeFromSource(AppsSourceAppRequest request, ServerCallContext context)
        => new() { Descriptor = await InvokeAsync(request, context,
            static (c, r, ct) => c.DescribeFromSourceAsync(r.SourceKey, r.Slug, r.Version, ct)).ConfigureAwait(false) };

    public async Task<AppsIconResponse> GetIcon(AppsSourceAppRequest request, ServerCallContext context)
        => new() { Icon = await InvokeAsync(request, context,
            static (c, r, ct) => c.GetIconAsync(r.SourceKey, r.Slug, r.Version, ct)).ConfigureAwait(false) };

    public Task<LatticeAppCatalogCapabilities> GetCapabilities(AppsEmptyRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (c, _, ct) => c.GetCapabilitiesAsync(ct));

    private IDisposable? StampActiveTenant(ServerCallContext context)
        => LatticeActiveTenantAssertion.Stamp(
            context, static (c, name) => c.RequestHeaders.GetValue(name), options.Value.ActiveTenantHeaderName);

    // Every failure maps to a fixed, sanitised status exactly as the app-control service does, so no facade
    // message (which could name an app, a source or a tree) reaches the wire.
    private async Task<TResponse> InvokeAsync<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        Func<ILatticeAppCatalog, TRequest, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);
        try
        {
            using var credentialScope = LatticeCredentialContext.With(credentialBridge.Resolve(context));
            using var tenantScope = StampActiveTenant(context);
            return await handler(catalog, request, context.CancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(StatusCode.Cancelled, "The app request was cancelled."));
        }
        catch (LatticeAuthorizationDeniedException)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, "App permission was denied."));
        }
        catch (LatticeTenantAccessDeniedException)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, "The asserted tenant is not permitted."));
        }
        catch (ArgumentException ex)
        {
            logger.LogWarning(ex, "Api.Apps: invalid request to {Method}.", context.Method);
            throw new RpcException(new Status(StatusCode.InvalidArgument, "The app request is invalid."));
        }
        catch (KeyNotFoundException)
        {
            throw new RpcException(new Status(StatusCode.NotFound, "The requested app or version was not found."));
        }
        catch (InvalidOperationException ex)
        {
            logger.LogWarning(ex, "Api.Apps: precondition failed for {Method}.", context.Method);
            throw new RpcException(new Status(StatusCode.FailedPrecondition, "The app request precondition was not met."));
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Api.Apps: gRPC call to {Method} failed.", context.Method);
            throw new RpcException(new Status(StatusCode.Internal, "The app request failed."));
        }
    }
}
