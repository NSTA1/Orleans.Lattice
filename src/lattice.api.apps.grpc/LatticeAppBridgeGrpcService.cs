using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Apps.Grpc;

internal sealed class LatticeAppBridgeGrpcService(
    ILatticeAppBridge bridge,
    ILatticeAppsApiCredentialBridge credentialBridge,
    IOptions<LatticeAppsApiGrpcOptions> options,
    ILogger<LatticeAppBridgeGrpcService> logger)
{
    private static readonly AppsEmptyRequest Acknowledged = new();

    public async Task<AppsBridgeGetResponse> Get(AppsBridgeKeyRequest request, ServerCallContext context)
        => new() { Value = await InvokeAsync(request, context,
            static (b, r, ct) => b.GetAsync(r.Target, r.Key, ct)).ConfigureAwait(false) };

    public Task<AppBridgePage> Scan(AppsBridgeScanRequest request, ServerCallContext context)
        => InvokeAsync(request, context,
            static (b, r, ct) => b.ScanAsync(r.Target, r.Prefix, r.PageSize, r.Continuation, ct));

    public async Task<AppsEmptyRequest> Set(AppsBridgeSetRequest request, ServerCallContext context)
    {
        await InvokeAsync(request, context, static (b, r, ct) => b.SetAsync(r.Target, r.Key, r.Value, ct)).ConfigureAwait(false);
        return Acknowledged;
    }

    public async Task<AppsBridgeDeleteResponse> Delete(AppsBridgeKeyRequest request, ServerCallContext context)
        => new() { Deleted = await InvokeAsync(request, context,
            static (b, r, ct) => b.DeleteAsync(r.Target, r.Key, ct)).ConfigureAwait(false) };

    // The write verb returns no value; this twin keeps its catch ladder identical to the one below without a
    // second async state machine per write.
    private async Task InvokeAsync<TRequest>(
        TRequest request,
        ServerCallContext context,
        Func<ILatticeAppBridge, TRequest, CancellationToken, Task> handler)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);
        try
        {
            using var credentialScope = LatticeCredentialContext.With(credentialBridge.Resolve(context));
            using var tenantScope = StampActiveTenant(context);
            await handler(bridge, request, context.CancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(StatusCode.Cancelled, "The app bridge request was cancelled."));
        }
        catch (AppBridgeException ex)
        {
            throw new RpcException(AppBridgeGrpcStatus.ToStatus(ex.Failure));
        }
        catch (LatticeAuthorizationDeniedException)
        {
            throw new RpcException(AppBridgeGrpcStatus.ToStatus(AppBridgeFailure.Denied));
        }
        catch (LatticeTenantAccessDeniedException)
        {
            throw new RpcException(AppBridgeGrpcStatus.ToStatus(AppBridgeFailure.Denied));
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Api.Apps: gRPC call to {Method} failed.", context.Method);
            throw new RpcException(AppBridgeGrpcStatus.ToStatus(AppBridgeFailure.Unavailable));
        }
    }
    private IDisposable? StampActiveTenant(ServerCallContext context)
        => LatticeActiveTenantAssertion.Stamp(
            context, static (c, name) => c.RequestHeaders.GetValue(name), options.Value.ActiveTenantHeaderName);

    // A bridge failure is sent as its status code with the fixed message for that code, and anything else maps
    // to a fixed status, so no facade or data-path message reaches the wire.
    private async Task<TResponse> InvokeAsync<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        Func<ILatticeAppBridge, TRequest, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);
        try
        {
            using var credentialScope = LatticeCredentialContext.With(credentialBridge.Resolve(context));
            using var tenantScope = StampActiveTenant(context);
            return await handler(bridge, request, context.CancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(StatusCode.Cancelled, "The app bridge request was cancelled."));
        }
        catch (AppBridgeException ex)
        {
            throw new RpcException(AppBridgeGrpcStatus.ToStatus(ex.Failure));
        }
        catch (LatticeAuthorizationDeniedException)
        {
            throw new RpcException(AppBridgeGrpcStatus.ToStatus(AppBridgeFailure.Denied));
        }
        catch (LatticeTenantAccessDeniedException)
        {
            throw new RpcException(AppBridgeGrpcStatus.ToStatus(AppBridgeFailure.Denied));
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Api.Apps: gRPC call to {Method} failed.", context.Method);
            throw new RpcException(AppBridgeGrpcStatus.ToStatus(AppBridgeFailure.Unavailable));
        }
    }
}
