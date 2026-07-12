using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Server-side gRPC interceptor that enforces the configured
/// <see cref="ILatticeBackupApiAuthorizer"/> on every inbound backup control-API
/// call. Calls that the authorizer rejects are failed with
/// <see cref="StatusCode.PermissionDenied"/>. Enforcement is scoped to the
/// backup control-API service by matching on the service-name prefix, so
/// unrelated gRPC services hosted in the same ASP.NET Core pipeline are
/// unaffected. The unauthenticated <c>GetAuthScheme</c> discovery RPC is exempt
/// so a client can learn how to sign in before it holds any credential.
/// </summary>
/// <remarks>
/// Registered globally via
/// <c>AddGrpc(o =&gt; o.Interceptors.Add&lt;LatticeBackupApiGrpcAuthInterceptor&gt;())</c>
/// inside
/// <see cref="LatticeBackupApiGrpcServiceCollectionExtensions.AddLatticeBackupApiGrpc"/>.
/// With the default <see cref="DenyAllBackupApiAuthorizer"/> and
/// <see cref="LatticeBackupApiGrpcOptions.RequireAuthorization"/> left at its
/// <see langword="true"/> default, every backup control-API call is rejected
/// until a host opts in - the default-deny posture for a surface that drives
/// destructive backup operations.
/// </remarks>
internal sealed class LatticeBackupApiGrpcAuthInterceptor : Interceptor
{
    private readonly ILatticeBackupApiAuthorizer _authorizer;
    private readonly IOptionsMonitor<LatticeBackupApiGrpcOptions> _options;
    private readonly ILogger<LatticeBackupApiGrpcAuthInterceptor> _logger;

    /// <summary>Initialises the interceptor.</summary>
    public LatticeBackupApiGrpcAuthInterceptor(
        ILatticeBackupApiAuthorizer authorizer,
        IOptionsMonitor<LatticeBackupApiGrpcOptions> options,
        ILogger<LatticeBackupApiGrpcAuthInterceptor> logger)
    {
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _authorizer = authorizer;
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

        if (!IsLatticeBackupApiMethod(context.Method) || IsUnauthenticatedMethod(context.Method))
        {
            return await continuation(request, context).ConfigureAwait(false);
        }

        await EnforceAuthAsync(request, context).ConfigureAwait(false);
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

        if (!IsLatticeBackupApiMethod(context.Method))
        {
            await continuation(request, responseStream, context).ConfigureAwait(false);
            return;
        }

        await EnforceAuthAsync(request, context).ConfigureAwait(false);
        await continuation(request, responseStream, context).ConfigureAwait(false);
    }

    private async Task EnforceAuthAsync<TRequest>(TRequest request, ServerCallContext context)
    {
        if (!_options.CurrentValue.RequireAuthorization)
        {
            return;
        }

        var (operation, targetId) = DescribeCall(context.Method, request);
        var authorizationContext = new LatticeBackupApiAuthorizationContext(context, operation, targetId);

        bool authorized;
        try
        {
            authorized = await _authorizer
                .IsAuthorizedAsync(authorizationContext, context.CancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(
                StatusCode.Cancelled,
                "Backup control-API authorization check was cancelled."));
        }

        if (!authorized)
        {
            _logger.LogWarning(
                "Api.Backup: rejected inbound gRPC call to {Method} - authorizer denied the request.",
                context.Method);
            throw new RpcException(new Status(
                StatusCode.PermissionDenied,
                "Caller is not authorized to drive the Lattice backup control API. "
                + "Register a permissive ILatticeBackupApiAuthorizer (or AllowAllBackupApiAuthorizer) to opt in, "
                + "or set LatticeBackupApiGrpcOptions.RequireAuthorization=false when an outer boundary guards the endpoint."));
        }
    }

    /// <summary>
    /// Decodes the inbound call's operation (from the gRPC method name) and the
    /// backup or tree it targets (from the request payload), so the authorizer
    /// receives a faithful per-operation description of every backup control-API
    /// RPC. Operations that are not scoped to a single backup or tree - the
    /// whole-catalog listing and drain RPCs - carry a <see langword="null"/>
    /// target. An unrecognised method maps to
    /// <see cref="LatticeBackupApiOperation.Unknown"/> (never a permissive
    /// default) so a deny-by-default policy refuses it.
    /// </summary>
    /// <remarks>Exposed as <c>internal</c> so the operation/target mapping can be
    /// asserted directly in unit tests without standing up a gRPC server.</remarks>
    internal static (LatticeBackupApiOperation Operation, string? TargetId) DescribeCall<TRequest>(string fullMethodName, TRequest request)
    {
        var methodName = fullMethodName[(fullMethodName.LastIndexOf('/') + 1)..];
        var operation = methodName switch
        {
            LatticeBackupGrpcMethods.CreateBackupMethodName => LatticeBackupApiOperation.CreateBackup,
            LatticeBackupGrpcMethods.CreateIncrementalBackupMethodName => LatticeBackupApiOperation.CreateIncrementalBackup,
            LatticeBackupGrpcMethods.CreateBackupSetMethodName => LatticeBackupApiOperation.CreateBackupSet,
            LatticeBackupGrpcMethods.ListBackupsMethodName => LatticeBackupApiOperation.ListBackups,
            LatticeBackupGrpcMethods.StreamBackupsMethodName => LatticeBackupApiOperation.StreamBackups,
            LatticeBackupGrpcMethods.DescribeBackupMethodName => LatticeBackupApiOperation.DescribeBackup,
            LatticeBackupGrpcMethods.DeleteBackupMethodName => LatticeBackupApiOperation.DeleteBackup,
            LatticeBackupGrpcMethods.RestoreBackupMethodName => LatticeBackupApiOperation.RestoreBackup,
            LatticeBackupGrpcMethods.RevertRestoreMethodName => LatticeBackupApiOperation.RevertRestore,
            LatticeBackupGrpcMethods.ExportArtifactMethodName => LatticeBackupApiOperation.ExportArtifact,
            LatticeBackupGrpcMethods.ScheduleBackupMethodName => LatticeBackupApiOperation.ScheduleBackup,
            _ => LatticeBackupApiOperation.Unknown,
        };

        var targetId = request switch
        {
            BackupCaptureRequestMessage c => c.Scope?.TreeId,
            BackupIncrementalCaptureRequestMessage i => i.Scope?.TreeId,
            BackupDescribeRequest d => d.BackupId,
            BackupDeleteRequest d => d.BackupId,
            RestoreRequestMessage r => r.BackupId,
            // The revert RPC carries the prior restore result as its request.
            RestoreResponse r => r.BackupId,
            ArtifactExportRequest a => a.BackupId,
            BackupScheduleRequestMessage s => s.Scope?.TreeId,
            _ => null,
        };

        return (operation, targetId);
    }

    private static bool IsLatticeBackupApiMethod(string fullMethodName)
    {
        const string ServicePrefix = "/" + LatticeBackupGrpcMethods.ServiceName + "/";
        return fullMethodName.StartsWith(ServicePrefix, StringComparison.Ordinal);
    }

    /// <summary>
    /// Whether the call targets a method exempt from authorization. The
    /// auth-scheme advertisement RPC must be reachable without a credential so a
    /// client can discover how to sign in before it holds one; every other
    /// backup control-API method is enforced.
    /// </summary>
    /// <remarks>Exposed as <c>internal</c> so the exemption can be asserted
    /// directly in unit tests without standing up a gRPC server.</remarks>
    internal static bool IsUnauthenticatedMethod(string fullMethodName)
    {
        var methodName = fullMethodName[(fullMethodName.LastIndexOf('/') + 1)..];
        return string.Equals(methodName, LatticeBackupGrpcMethods.GetAuthSchemeMethodName, StringComparison.Ordinal);
    }
}
