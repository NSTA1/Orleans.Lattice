using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Apps.Grpc;

internal sealed class LatticeAppsApiGrpcAuthInterceptor(
    ILatticeAppsApiAuthorizer authorizer,
    IOptionsMonitor<LatticeAppsApiGrpcOptions> options,
    ILogger<LatticeAppsApiGrpcAuthInterceptor> logger) : Interceptor
{
    private readonly ILatticeAppsApiAuthorizer _authorizer =
        authorizer ?? throw new ArgumentNullException(nameof(authorizer));
    private readonly IOptionsMonitor<LatticeAppsApiGrpcOptions> _options =
        options ?? throw new ArgumentNullException(nameof(options));
    private readonly ILogger<LatticeAppsApiGrpcAuthInterceptor> _logger =
        logger ?? throw new ArgumentNullException(nameof(logger));

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(continuation);
        if (!IsAppsMethod(context.Method))
            return await continuation(request, context).ConfigureAwait(false);

        if (context.Method == LatticeAppsGrpcMethods.ServicePrefix + nameof(LatticeAppsGrpcMethods.GetAuthScheme)
            && request is AuthSchemeAdvertisementRequest)
            return await continuation(request, context).ConfigureAwait(false);

        var (operation, slug) = DescribeCall(context.Method, request);
        if (operation == LatticeAppsApiOperation.Unknown)
            throw Denied(context);

        if (_options.CurrentValue.RequireAuthorization)
        {
            bool allowed;
            try
            {
                allowed = await _authorizer.IsAuthorizedAsync(
                    new(context, operation, slug), context.CancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw new RpcException(new Status(StatusCode.Cancelled, "App-control authorization was cancelled."));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Api.Apps: authorization failed for {Method}.", context.Method);
                throw new RpcException(new Status(StatusCode.Internal, "App-control authorization failed."));
            }
            if (!allowed)
                throw Denied(context);
        }
        return await continuation(request, context).ConfigureAwait(false);
    }

    public override Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request, IServerStreamWriter<TResponse> responseStream, ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (IsAppsMethod(context.Method))
            throw Denied(context);
        return base.ServerStreamingServerHandler(request, responseStream, context, continuation);
    }

    public override Task<TResponse> ClientStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream, ServerCallContext context,
        ClientStreamingServerMethod<TRequest, TResponse> continuation)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (IsAppsMethod(context.Method))
            throw Denied(context);
        return base.ClientStreamingServerHandler(requestStream, context, continuation);
    }

    public override Task DuplexStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream, IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context, DuplexStreamingServerMethod<TRequest, TResponse> continuation)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (IsAppsMethod(context.Method))
            throw Denied(context);
        return base.DuplexStreamingServerHandler(requestStream, responseStream, context, continuation);
    }

    private RpcException Denied(ServerCallContext context)
    {
        _logger.LogWarning("Api.Apps: rejected inbound gRPC call to {Method}.", context.Method);
        return new RpcException(new Status(StatusCode.PermissionDenied, "Caller is not authorized for this app-control operation."));
    }

    private static bool IsAppsMethod(string method)
        => method.StartsWith(LatticeAppsGrpcMethods.ServicePrefix, StringComparison.Ordinal)
            || method.StartsWith(LatticeAppCatalogGrpcMethods.ServicePrefix, StringComparison.Ordinal)
            || method.StartsWith(LatticeAppWorkspaceGrpcMethods.ServicePrefix, StringComparison.Ordinal)
            || method.StartsWith(LatticeAppBridgeGrpcMethods.ServicePrefix, StringComparison.Ordinal);

    internal static (LatticeAppsApiOperation Operation, string? Slug) DescribeCall<TRequest>(string method, TRequest request)
    {
        // Both the bound method and its expected request shape must agree.
        return (method, request) switch
        {
            (LatticeAppsGrpcMethods.ServicePrefix + "Install", AppInstallRequest r) => (LatticeAppsApiOperation.Install, r.Slug),
            (LatticeAppsGrpcMethods.ServicePrefix + "Enable", AppsSlugRequest r) => (LatticeAppsApiOperation.Enable, r.Slug),
            (LatticeAppsGrpcMethods.ServicePrefix + "Disable", AppsSlugRequest r) => (LatticeAppsApiOperation.Disable, r.Slug),
            (LatticeAppsGrpcMethods.ServicePrefix + "Uninstall", AppsSlugRequest r) => (LatticeAppsApiOperation.Uninstall, r.Slug),
            (LatticeAppsGrpcMethods.ServicePrefix + "List", AppsEmptyRequest) => (LatticeAppsApiOperation.List, null),
            (LatticeAppsGrpcMethods.ServicePrefix + "Describe", AppsDescribeRequest r) => (LatticeAppsApiOperation.Describe, r.Slug),
            (LatticeAppsGrpcMethods.ServicePrefix + "GetConsent", AppsSlugRequest r) => (LatticeAppsApiOperation.GetConsent, r.Slug),
            (LatticeAppsGrpcMethods.ServicePrefix + "UpdateConsent", AppConsentUpdate r) => (LatticeAppsApiOperation.UpdateConsent, r.Slug),
            (LatticeAppsGrpcMethods.ServicePrefix + "GetCapabilities", AppsEmptyRequest) => (LatticeAppsApiOperation.GetCapabilities, null),
            (LatticeAppsGrpcMethods.ServicePrefix + "UpdateRoleBindings", AppRoleBindingsUpdate r) => (LatticeAppsApiOperation.UpdateRoleBindings, r.Slug),
            (LatticeAppCatalogGrpcMethods.ServicePrefix + "ListSources", AppsEmptyRequest) => (LatticeAppsApiOperation.ListSources, null),
            (LatticeAppCatalogGrpcMethods.ServicePrefix + "ListAvailable", AvailableAppQuery) => (LatticeAppsApiOperation.ListAvailable, null),
            (LatticeAppCatalogGrpcMethods.ServicePrefix + "DescribeFromSource", AppsSourceAppRequest r) => (LatticeAppsApiOperation.DescribeFromSource, r.Slug),
            (LatticeAppCatalogGrpcMethods.ServicePrefix + "GetIcon", AppsSourceAppRequest r) => (LatticeAppsApiOperation.GetSourceIcon, r.Slug),
            (LatticeAppCatalogGrpcMethods.ServicePrefix + "GetCapabilities", AppsEmptyRequest) => (LatticeAppsApiOperation.GetCatalogCapabilities, null),
            (LatticeAppWorkspaceGrpcMethods.ServicePrefix + "ListMyApps", AppsEmptyRequest) => (LatticeAppsApiOperation.ListMyApps, null),
            (LatticeAppWorkspaceGrpcMethods.ServicePrefix + "DescribeMyApp", AppsSlugRequest r) => (LatticeAppsApiOperation.DescribeMyApp, r.Slug),
            (LatticeAppWorkspaceGrpcMethods.ServicePrefix + "GetIcon", AppsSlugRequest r) => (LatticeAppsApiOperation.GetMyAppIcon, r.Slug),
            (LatticeAppWorkspaceGrpcMethods.ServicePrefix + "GetUiAsset", AppsUiAssetRequest r) => (LatticeAppsApiOperation.GetUiAsset, r.Slug),
            (LatticeAppBridgeGrpcMethods.ServicePrefix + "Get", AppsBridgeKeyRequest r) => (LatticeAppsApiOperation.BridgeGet, r.Target?.AppSlug),
            (LatticeAppBridgeGrpcMethods.ServicePrefix + "Scan", AppsBridgeScanRequest r) => (LatticeAppsApiOperation.BridgeScan, r.Target?.AppSlug),
            (LatticeAppBridgeGrpcMethods.ServicePrefix + "Set", AppsBridgeSetRequest r) => (LatticeAppsApiOperation.BridgeSet, r.Target?.AppSlug),
            (LatticeAppBridgeGrpcMethods.ServicePrefix + "Delete", AppsBridgeKeyRequest r) => (LatticeAppsApiOperation.BridgeDelete, r.Target?.AppSlug),
            _ => (LatticeAppsApiOperation.Unknown, null),
        };
    }
}
