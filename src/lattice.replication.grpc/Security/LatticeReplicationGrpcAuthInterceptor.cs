using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// Server-side gRPC interceptor that enforces the
/// <c>Orleans.Lattice.Replication</c> shared-secret authenticator on
/// every inbound unary call. Reads the
/// <see cref="LatticeReplicationGrpcMetadataNames.SecretHeader"/>
/// metadata entry, validates it against
/// <see cref="IReplicationSecretProvider.IsAcceptedAsync"/>, and rejects
/// the call with <see cref="StatusCode.Unauthenticated"/> when the
/// header is absent and with <see cref="StatusCode.PermissionDenied"/>
/// when the header is present but does not match any accepted secret.
/// </summary>
/// <remarks>
/// The interceptor is registered globally via
/// <c>AddGrpc(o =&gt; o.Interceptors.Add&lt;LatticeReplicationGrpcAuthInterceptor&gt;())</c>
/// inside <see cref="LatticeReplicationGrpcServiceCollectionExtensions.AddLatticeReplicationGrpc"/>.
/// Hosts that wire their own gRPC services into the same ASP.NET Core
/// pipeline are unaffected: this interceptor's enforcement is scoped
/// to the receiver-side services registered by this package.
/// </remarks>
internal sealed class LatticeReplicationGrpcAuthInterceptor : Interceptor
{
    private readonly IReplicationSecretProvider _secrets;
    private readonly IOptionsMonitor<LatticeReplicationSecurityOptions> _options;
    private readonly ILogger<LatticeReplicationGrpcAuthInterceptor> _logger;

    /// <summary>Initialises the interceptor.</summary>
    public LatticeReplicationGrpcAuthInterceptor(
        IReplicationSecretProvider secrets,
        IOptionsMonitor<LatticeReplicationSecurityOptions> options,
        ILogger<LatticeReplicationGrpcAuthInterceptor> logger)
    {
        ArgumentNullException.ThrowIfNull(secrets);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _secrets = secrets;
        _options = options;
        _logger = logger;
    }

    /// <inheritdoc />
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(continuation);

        // Scope the auth check to the Lattice replication services so a
        // host that mounts other gRPC services in the same pipeline is
        // unaffected. The full method name is "/{service}/{method}";
        // we match by service-name prefix to keep the interceptor
        // resilient to future RPC additions on the same services.
        if (!IsLatticeReplicationMethod(context.Method))
        {
            return await continuation(request, context).ConfigureAwait(false);
        }

        await EnforceAuthAsync(context).ConfigureAwait(false);
        return await continuation(request, context).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override async Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(responseStream);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(continuation);

        if (!IsLatticeReplicationMethod(context.Method))
        {
            await continuation(request, responseStream, context).ConfigureAwait(false);
            return;
        }

        await EnforceAuthAsync(context).ConfigureAwait(false);
        await continuation(request, responseStream, context).ConfigureAwait(false);
    }

    private async Task EnforceAuthAsync(ServerCallContext context)
    {
        if (!_options.CurrentValue.RequireAuthentication)
        {
            return;
        }

        var presented = ReadHeader(context, LatticeReplicationGrpcMetadataNames.SecretHeader);
        if (string.IsNullOrEmpty(presented))
        {
            _logger.LogWarning(
                "Replication: rejected inbound gRPC call to {Method} - missing {Header}.",
                context.Method, LatticeReplicationGrpcMetadataNames.SecretHeader);
            throw new RpcException(new Status(
                StatusCode.Unauthenticated,
                "Replication batch is missing the shared-secret credential. "
                + "Configure LATTICE_REPLICATION_SECRET (or a custom ILatticeReplicationSecretSource) on the sender."));
        }

        var ok = await _secrets.IsAcceptedAsync(presented, context.CancellationToken).ConfigureAwait(false);
        if (!ok)
        {
            _logger.LogWarning(
                "Replication: rejected inbound gRPC call to {Method} - credential did not match the accepted-set.",
                context.Method);
            throw new RpcException(new Status(
                StatusCode.PermissionDenied,
                "Replication batch credential did not match any accepted secret on this cluster. "
                + "Rotate by publishing the next-generation secret in LATTICE_REPLICATION_ACCEPTED_SECRETS on every peer before flipping LATTICE_REPLICATION_SECRET on the sender."));
        }
    }

    /// <summary>
    /// Returns <see langword="true"/> when the call targets a method
    /// hosted by one of this package's gRPC services (the live push
    /// transport, the cross-cluster snapshot transport, or the saga
    /// control channel). Matching by the service-id segment of the
    /// method-name keeps the interceptor from inspecting headers on
    /// unrelated services in a shared ASP.NET Core pipeline.
    /// </summary>
    private static bool IsLatticeReplicationMethod(string fullMethodName)
    {
        // Method-name format is "/{ServiceId}/{MethodName}".
        const string PushServicePrefix = "/" + LatticeReplicationGrpcMethod.ServiceName + "/";
        const string SnapshotServicePrefix = "/" + LatticeRemoteSnapshotGrpcMethods.ServiceName + "/";
        const string SagaServicePrefix = "/" + LatticeSagaGrpcMethods.ServiceName + "/";
        return fullMethodName.StartsWith(PushServicePrefix, StringComparison.Ordinal)
            || fullMethodName.StartsWith(SnapshotServicePrefix, StringComparison.Ordinal)
            || fullMethodName.StartsWith(SagaServicePrefix, StringComparison.Ordinal);
    }

    private static string? ReadHeader(ServerCallContext context, string key)
    {
        foreach (var entry in context.RequestHeaders)
        {
            if (string.Equals(entry.Key, key, StringComparison.OrdinalIgnoreCase))
            {
                return entry.Value;
            }
        }
        return null;
    }
}
