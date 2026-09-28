using Grpc.Core;

namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>
/// Strongly typed client for the app bridge: the cluster-side seam an app UI's data requests are relayed to.
/// The supplied invoker owns routing, credentials, TLS and deadlines; the caller's credential is what the bridge
/// authorizes. Every failure the service reports is rethrown as an <see cref="AppBridgeException"/> carrying the
/// matching <see cref="AppBridgeFailure"/> and its fixed, sanitised message.
/// </summary>
public sealed class LatticeAppBridgeApiGrpcClient : ILatticeAppBridge
{
    private readonly CallInvoker _invoker;
    private readonly LatticeAppBridgeGrpcMethods _methods;

    private LatticeAppBridgeApiGrpcClient(CallInvoker invoker, LatticeAppBridgeGrpcMethods methods)
    {
        _invoker = invoker;
        _methods = methods;
    }

    /// <summary>Creates a client using Orleans serializers from the supplied provider; does not own either argument.</summary>
    /// <param name="callInvoker">The call invoker the client sends through.</param>
    /// <param name="serializerProvider">The provider supplying Orleans serializers for the bridge messages.</param>
    /// <returns>The client.</returns>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public static LatticeAppBridgeApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(callInvoker);
        ArgumentNullException.ThrowIfNull(serializerProvider);
        return new(callInvoker, new LatticeAppBridgeGrpcMethods(serializerProvider));
    }

    /// <inheritdoc />
    /// <exception cref="ArgumentNullException"><paramref name="target"/> or <paramref name="key"/> is null.</exception>
    public async Task<AppBridgeValue?> GetAsync(AppBridgeTarget target, string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(target);
        ArgumentNullException.ThrowIfNull(key);
        return (await UnaryAsync(_methods.Get,
            new AppsBridgeKeyRequest { Target = target, Key = key }, cancellationToken).ConfigureAwait(false)).Value;
    }

    /// <inheritdoc />
    /// <exception cref="ArgumentNullException"><paramref name="target"/> or <paramref name="prefix"/> is null.</exception>
    public Task<AppBridgePage> ScanAsync(
        AppBridgeTarget target,
        string prefix,
        int pageSize,
        string? continuation = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(target);
        ArgumentNullException.ThrowIfNull(prefix);
        return UnaryAsync(_methods.Scan, new AppsBridgeScanRequest
        {
            Target = target,
            Prefix = prefix,
            PageSize = pageSize,
            Continuation = continuation,
        }, cancellationToken);
    }

    /// <inheritdoc />
    /// <exception cref="ArgumentNullException"><paramref name="target"/> or <paramref name="key"/> is null.</exception>
    public async Task SetAsync(
        AppBridgeTarget target,
        string key,
        ReadOnlyMemory<byte> value,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(target);
        ArgumentNullException.ThrowIfNull(key);
        await UnaryAsync(_methods.Set,
            new AppsBridgeSetRequest { Target = target, Key = key, Value = value }, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    /// <exception cref="ArgumentNullException"><paramref name="target"/> or <paramref name="key"/> is null.</exception>
    public async Task<bool> DeleteAsync(AppBridgeTarget target, string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(target);
        ArgumentNullException.ThrowIfNull(key);
        return (await UnaryAsync(_methods.Delete,
            new AppsBridgeKeyRequest { Target = target, Key = key }, cancellationToken).ConfigureAwait(false)).Deleted;
    }

    private async Task<TResponse> UnaryAsync<TRequest, TResponse>(
        Method<TRequest, TResponse> method, TRequest request, CancellationToken cancellationToken)
        where TRequest : class where TResponse : class
    {
        try
        {
            using var call = _invoker.AsyncUnaryCall(method, null, new CallOptions(cancellationToken: cancellationToken), request);
            return await call.ResponseAsync.ConfigureAwait(false);
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled && cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException("The app bridge request was cancelled.", ex, cancellationToken);
        }
        catch (RpcException ex)
        {
            throw new AppBridgeException(AppBridgeGrpcStatus.ToFailure(ex.StatusCode));
        }
    }
}
