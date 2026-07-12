using Grpc.Core;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Abstract base for the backup control-API gRPC service. Carries the
/// <see cref="BindServiceMethodAttribute"/> that <c>Grpc.AspNetCore</c> reflects
/// against to discover and register the unary RPCs (<c>CreateBackup</c>,
/// <c>CreateIncrementalBackup</c>, <c>ListBackups</c>, <c>DescribeBackup</c>,
/// <c>DeleteBackup</c>, <c>RestoreBackup</c>, <c>RevertRestore</c>,
/// <c>GetAuthScheme</c>) and the server-streaming RPCs (<c>StreamBackups</c>,
/// <c>ExportArtifact</c>).
/// </summary>
/// <remarks>
/// The base/derived split mirrors the codegen shape <c>Grpc.Tools</c> produces
/// for a <c>.proto</c> service: the base type bears the binding metadata the
/// binder discovers, and the derived type is the concrete implementation
/// resolved from DI per request. <c>Grpc.AspNetCore</c> calls
/// <see cref="LatticeBackupGrpcServiceBase.BindService"/> once at startup with a
/// <see langword="null"/> instance to record method metadata, then resolves the
/// actual instance per request.
/// </remarks>
[BindServiceMethod(typeof(LatticeBackupGrpcServiceBase), nameof(BindService))]
internal abstract class LatticeBackupGrpcServiceBase
{
    /// <summary>Captures a full backup. Implemented in <see cref="LatticeBackupGrpcService"/>.</summary>
    public abstract Task<BackupCaptureResponse> CreateBackup(BackupCaptureRequestMessage request, ServerCallContext context);

    /// <summary>Captures an incremental backup. Implemented in <see cref="LatticeBackupGrpcService"/>.</summary>
    public abstract Task<BackupCaptureResponse> CreateIncrementalBackup(BackupIncrementalCaptureRequestMessage request, ServerCallContext context);

    /// <summary>Captures a backup set. Implemented in <see cref="LatticeBackupGrpcService"/>.</summary>
    public abstract Task<BackupSetCaptureResponse> CreateBackupSet(BackupSetCaptureRequestMessage request, ServerCallContext context);

    /// <summary>Lists a cursor-resumable page of the catalog. Implemented in <see cref="LatticeBackupGrpcService"/>.</summary>
    public abstract Task<BackupCatalogPage> ListBackups(BackupCatalogRequest request, ServerCallContext context);

    /// <summary>Streams every catalogued backup, in id order. Implemented in <see cref="LatticeBackupGrpcService"/>.</summary>
    public abstract Task StreamBackups(
        BackupStreamRequest request,
        IServerStreamWriter<BackupManifest> responseStream,
        ServerCallContext context);

    /// <summary>Describes a backup and its restore chain. Implemented in <see cref="LatticeBackupGrpcService"/>.</summary>
    public abstract Task<BackupChainResponse> DescribeBackup(BackupDescribeRequest request, ServerCallContext context);

    /// <summary>Deletes a backup. Implemented in <see cref="LatticeBackupGrpcService"/>.</summary>
    public abstract Task<BackupDeleteResponse> DeleteBackup(BackupDeleteRequest request, ServerCallContext context);

    /// <summary>Restores a backup into its target tree. Implemented in <see cref="LatticeBackupGrpcService"/>.</summary>
    public abstract Task<RestoreResponse> RestoreBackup(RestoreRequestMessage request, ServerCallContext context);

    /// <summary>Reverts a shadow-cutover restore. Implemented in <see cref="LatticeBackupGrpcService"/>.</summary>
    public abstract Task<RevertRestoreResponse> RevertRestore(RestoreResponse request, ServerCallContext context);

    /// <summary>Streams one artifact's bytes chunk-wise. Implemented in <see cref="LatticeBackupGrpcService"/>.</summary>
    public abstract Task ExportArtifact(
        ArtifactExportRequest request,
        IServerStreamWriter<ArtifactChunk> responseStream,
        ServerCallContext context);

    /// <summary>
    /// Returns the endpoint's advertised auth schemes. Unauthenticated: this RPC
    /// is exempt from the authorization interceptor so a client can learn how to
    /// sign in before it holds any credential. Implemented in
    /// <see cref="LatticeBackupGrpcService"/>.
    /// </summary>
    public abstract Task<AuthSchemeAdvertisement> GetAuthScheme(AuthSchemeAdvertisementRequest request, ServerCallContext context);

    /// <summary>Probes the caller's backup / restore capabilities for a scope. Implemented in <see cref="LatticeBackupGrpcService"/>.</summary>
    public abstract Task<BackupScopeCapabilities> ProbeCapabilities(BackupCapabilityProbeRequest request, ServerCallContext context);

    /// <summary>Registers a recurring backup schedule. Implemented in <see cref="LatticeBackupGrpcService"/>.</summary>
    public abstract Task<BackupScheduleResponse> ScheduleBackup(BackupScheduleRequestMessage request, ServerCallContext context);

    /// <summary>
    /// gRPC binding hook invoked by <c>Grpc.AspNetCore</c>. Called once at
    /// startup with <paramref name="serviceImpl"/> set to
    /// <see langword="null"/> to record method metadata; the actual service
    /// instance is resolved per request from DI.
    /// </summary>
    public static void BindService(ServiceBinderBase binder, LatticeBackupGrpcServiceBase? serviceImpl)
    {
        ArgumentNullException.ThrowIfNull(binder);

        var methods = LatticeBackupGrpcMethodsHolder.Current
            ?? throw new InvalidOperationException(
                "LatticeBackupGrpcMethodsHolder.Current was not initialised before BindService. "
                + $"Ensure {nameof(LatticeBackupApiGrpcServiceCollectionExtensions.AddLatticeBackupApiGrpc)} ran and that "
                + $"{nameof(LatticeBackupApiGrpcServiceCollectionExtensions.MapLatticeBackupApiGrpc)} pre-resolved "
                + "LatticeBackupGrpcMethods before Grpc.AspNetCore reflected on the service type.");

        if (serviceImpl is null)
        {
            binder.AddMethod(methods.CreateBackup, (UnaryServerMethod<BackupCaptureRequestMessage, BackupCaptureResponse>?)null);
            binder.AddMethod(methods.CreateIncrementalBackup, (UnaryServerMethod<BackupIncrementalCaptureRequestMessage, BackupCaptureResponse>?)null);
            binder.AddMethod(methods.CreateBackupSet, (UnaryServerMethod<BackupSetCaptureRequestMessage, BackupSetCaptureResponse>?)null);
            binder.AddMethod(methods.ListBackups, (UnaryServerMethod<BackupCatalogRequest, BackupCatalogPage>?)null);
            binder.AddMethod(methods.StreamBackups, (ServerStreamingServerMethod<BackupStreamRequest, BackupManifest>?)null);
            binder.AddMethod(methods.DescribeBackup, (UnaryServerMethod<BackupDescribeRequest, BackupChainResponse>?)null);
            binder.AddMethod(methods.DeleteBackup, (UnaryServerMethod<BackupDeleteRequest, BackupDeleteResponse>?)null);
            binder.AddMethod(methods.RestoreBackup, (UnaryServerMethod<RestoreRequestMessage, RestoreResponse>?)null);
            binder.AddMethod(methods.RevertRestore, (UnaryServerMethod<RestoreResponse, RevertRestoreResponse>?)null);
            binder.AddMethod(methods.ExportArtifact, (ServerStreamingServerMethod<ArtifactExportRequest, ArtifactChunk>?)null);
            binder.AddMethod(methods.GetAuthScheme, (UnaryServerMethod<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>?)null);
            binder.AddMethod(methods.ProbeCapabilities, (UnaryServerMethod<BackupCapabilityProbeRequest, BackupScopeCapabilities>?)null);
            binder.AddMethod(methods.ScheduleBackup, (UnaryServerMethod<BackupScheduleRequestMessage, BackupScheduleResponse>?)null);
            return;
        }

        binder.AddMethod(methods.CreateBackup, new UnaryServerMethod<BackupCaptureRequestMessage, BackupCaptureResponse>(serviceImpl.CreateBackup));
        binder.AddMethod(methods.CreateIncrementalBackup, new UnaryServerMethod<BackupIncrementalCaptureRequestMessage, BackupCaptureResponse>(serviceImpl.CreateIncrementalBackup));
        binder.AddMethod(methods.CreateBackupSet, new UnaryServerMethod<BackupSetCaptureRequestMessage, BackupSetCaptureResponse>(serviceImpl.CreateBackupSet));
        binder.AddMethod(methods.ListBackups, new UnaryServerMethod<BackupCatalogRequest, BackupCatalogPage>(serviceImpl.ListBackups));
        binder.AddMethod(methods.StreamBackups, new ServerStreamingServerMethod<BackupStreamRequest, BackupManifest>(serviceImpl.StreamBackups));
        binder.AddMethod(methods.DescribeBackup, new UnaryServerMethod<BackupDescribeRequest, BackupChainResponse>(serviceImpl.DescribeBackup));
        binder.AddMethod(methods.DeleteBackup, new UnaryServerMethod<BackupDeleteRequest, BackupDeleteResponse>(serviceImpl.DeleteBackup));
        binder.AddMethod(methods.RestoreBackup, new UnaryServerMethod<RestoreRequestMessage, RestoreResponse>(serviceImpl.RestoreBackup));
        binder.AddMethod(methods.RevertRestore, new UnaryServerMethod<RestoreResponse, RevertRestoreResponse>(serviceImpl.RevertRestore));
        binder.AddMethod(methods.ExportArtifact, new ServerStreamingServerMethod<ArtifactExportRequest, ArtifactChunk>(serviceImpl.ExportArtifact));
        binder.AddMethod(methods.GetAuthScheme, new UnaryServerMethod<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>(serviceImpl.GetAuthScheme));
        binder.AddMethod(methods.ProbeCapabilities, new UnaryServerMethod<BackupCapabilityProbeRequest, BackupScopeCapabilities>(serviceImpl.ProbeCapabilities));
        binder.AddMethod(methods.ScheduleBackup, new UnaryServerMethod<BackupScheduleRequestMessage, BackupScheduleResponse>(serviceImpl.ScheduleBackup));
    }
}

/// <summary>
/// Server-side gRPC service for the backup control API. Adapts each RPC onto the
/// transport-agnostic <see cref="ILatticeBackupControl"/> facade, mapping the
/// facade's plain result records onto the serializable wire responses and
/// translating typed not-founds, argument failures, and authorization denials
/// onto gRPC status codes. Streaming RPCs drain the facade's
/// <see cref="IAsyncEnumerable{T}"/> straight to the response stream with bounded
/// memory.
/// </summary>
internal sealed class LatticeBackupGrpcService : LatticeBackupGrpcServiceBase
{
    private readonly ILatticeBackupControl _control;
    private readonly ILatticeBackupApiCredentialBridge _credentialBridge;
    private readonly ILatticeBackupApiAuthSchemeSource _authSchemeSource;
    private readonly ILogger<LatticeBackupGrpcService> _logger;

    /// <summary>
    /// Initialises the service. The <paramref name="methods"/> parameter is
    /// unused in the body but load-bearing on the constructor: resolving it
    /// forces the DI container to build the <see cref="LatticeBackupGrpcMethods"/>
    /// singleton (whose factory populates
    /// <see cref="LatticeBackupGrpcMethodsHolder.Current"/>) before this service
    /// resolves, so the static <see cref="LatticeBackupGrpcServiceBase.BindService"/>
    /// hook always observes a populated holder.
    /// </summary>
    public LatticeBackupGrpcService(
        LatticeBackupGrpcMethods methods,
        ILatticeBackupControl control,
        ILatticeBackupApiCredentialBridge credentialBridge,
        ILatticeBackupApiAuthSchemeSource authSchemeSource,
        ILogger<LatticeBackupGrpcService> logger)
    {
        ArgumentNullException.ThrowIfNull(methods);
        ArgumentNullException.ThrowIfNull(control);
        ArgumentNullException.ThrowIfNull(credentialBridge);
        ArgumentNullException.ThrowIfNull(authSchemeSource);
        ArgumentNullException.ThrowIfNull(logger);

        _control = control;
        _credentialBridge = credentialBridge;
        _authSchemeSource = authSchemeSource;
        _logger = logger;
    }

    /// <summary>
    /// Bridges the caller identity on <paramref name="context"/> into the ambient
    /// <see cref="LatticeCredentialContext"/> for the duration of the returned
    /// scope, so the backup engine's own fail-closed access gate resolves the
    /// caller's subject. Returns <see langword="null"/> (no scope) when the call
    /// carries no credential, leaving the caller anonymous. This is orthogonal
    /// to, and runs after, the transport-level
    /// <see cref="ILatticeBackupApiAuthorizer"/> gate.
    /// </summary>
    private IDisposable? StampCallerCredential(ServerCallContext context)
    {
        var credential = _credentialBridge.Resolve(context);
        return credential is null ? null : LatticeCredentialContext.With(credential);
    }

    /// <inheritdoc />
    public override Task<BackupCaptureResponse> CreateBackup(BackupCaptureRequestMessage request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            var result = await control
                .CreateBackupAsync(new LatticeBackupCaptureRequest(req.Name, req.Scope, req.PageSize), ct)
                .ConfigureAwait(false);
            return ToCaptureResponse(result);
        });

    /// <inheritdoc />
    public override Task<BackupCaptureResponse> CreateIncrementalBackup(BackupIncrementalCaptureRequestMessage request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            var result = await control
                .CreateIncrementalBackupAsync(
                    new LatticeBackupIncrementalCaptureRequest(req.Name, req.Scope, req.BaseBackupId, req.PageSize),
                    ct)
                .ConfigureAwait(false);
            return ToCaptureResponse(result);
        });

    /// <inheritdoc />
    public override Task<BackupSetCaptureResponse> CreateBackupSet(BackupSetCaptureRequestMessage request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            var result = await control
                .CreateBackupSetAsync(
                    new LatticeBackupSetCaptureRequest(req.Name, req.Scopes, req.CrossTreeConsistent, req.PageSize),
                    ct)
                .ConfigureAwait(false);
            return ToSetCaptureResponse(result);
        });

    /// <inheritdoc />
    public override Task<BackupScheduleResponse> ScheduleBackup(BackupScheduleRequestMessage request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            var interval = TimeSpan.FromTicks(req.IntervalTicks);
            await control
                .ScheduleBackupAsync(new LatticeBackupScheduleRequest(req.Scope, req.Incremental, interval), ct)
                .ConfigureAwait(false);

            // Mirror the scheduler's clamp so the caller learns the cadence that
            // was actually registered when a sub-minimum interval was rounded up.
            var effective = interval < LatticeBackupScheduleOptions.MinimumInterval
                ? LatticeBackupScheduleOptions.MinimumInterval
                : interval;
            return new BackupScheduleResponse { Scheduled = true, EffectiveIntervalTicks = effective.Ticks };
        });

    /// <inheritdoc />
    public override Task<BackupCatalogPage> ListBackups(BackupCatalogRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (control, req, ct) => control.ListBackupsAsync(req, ct));

    /// <inheritdoc />
    public override async Task StreamBackups(
        BackupStreamRequest request,
        IServerStreamWriter<BackupManifest> responseStream,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(responseStream);
        ArgumentNullException.ThrowIfNull(context);

        using var credentialScope = StampCallerCredential(context);

        try
        {
            await foreach (var manifest in _control
                .StreamBackupsAsync(context.CancellationToken)
                .ConfigureAwait(false))
            {
                await responseStream.WriteAsync(manifest).ConfigureAwait(false);
            }
        }
        catch (RpcException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            // Client tore down the stream; a clean return ends it.
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Api.Backup: gRPC catalog stream failed.");
            throw new RpcException(new Status(StatusCode.Internal, "The backup catalog stream failed."));
        }
    }

    /// <inheritdoc />
    public override Task<BackupChainResponse> DescribeBackup(BackupDescribeRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            var description = await control.DescribeBackupAsync(req.BackupId, ct).ConfigureAwait(false);
            if (description is null)
            {
                return new BackupChainResponse { Found = false };
            }

            return new BackupChainResponse
            {
                Found = true,
                Manifest = description.Manifest,
                ChainBackupIds = description.ChainBackupIds,
            };
        });

    /// <inheritdoc />
    public override Task<BackupDeleteResponse> DeleteBackup(BackupDeleteRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            var deleted = await control.DeleteBackupAsync(req.BackupId, ct).ConfigureAwait(false);
            return new BackupDeleteResponse { Deleted = deleted };
        });

    /// <inheritdoc />
    public override Task<RestoreResponse> RestoreBackup(RestoreRequestMessage request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            var result = await control
                .RestoreBackupAsync(
                    new LatticeRestoreRequest(req.BackupId, req.TargetTreeId, req.Scope, req.Mode, req.OperationId, req.ApplyBatchSize),
                    ct)
                .ConfigureAwait(false);
            return ToRestoreResponse(result);
        });

    /// <inheritdoc />
    public override Task<RevertRestoreResponse> RevertRestore(RestoreResponse request, ServerCallContext context)
        => InvokeAsync(request, context, static async (control, req, ct) =>
        {
            await control.RevertRestoreAsync(ToRestoreResult(req), ct).ConfigureAwait(false);
            return new RevertRestoreResponse();
        });

    /// <inheritdoc />
    public override async Task ExportArtifact(
        ArtifactExportRequest request,
        IServerStreamWriter<ArtifactChunk> responseStream,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(responseStream);
        ArgumentNullException.ThrowIfNull(context);

        using var credentialScope = StampCallerCredential(context);

        try
        {
            await foreach (var chunk in _control
                .ExportArtifactAsync(request.BackupId, request.ArtifactId, context.CancellationToken)
                .ConfigureAwait(false))
            {
                // One bounded copy per chunk: the wire message carries a byte[]
                // (Orleans' array codec needs an array) while the facade yields
                // ReadOnlyMemory<byte>. Only a single chunk is materialised at a
                // time, so peak memory stays bounded regardless of artifact size.
                await responseStream.WriteAsync(new ArtifactChunk { Data = chunk.ToArray() }).ConfigureAwait(false);
            }
        }
        catch (RpcException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            // Client tore down the stream; a clean return ends it.
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
        }
        catch (KeyNotFoundException ex)
        {
            throw new RpcException(new Status(StatusCode.NotFound, ex.Message));
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Api.Backup: gRPC artifact export for backup {BackupId} failed.", request.BackupId);
            throw new RpcException(new Status(StatusCode.Internal, "The backup artifact export failed."));
        }
    }

    /// <inheritdoc />
    public override Task<AuthSchemeAdvertisement> GetAuthScheme(AuthSchemeAdvertisementRequest request, ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

        // Unauthenticated by design (the interceptor exempts this method), so no
        // credential is bridged and only the public advertisement is returned.
        return Task.FromResult(_authSchemeSource.GetAdvertisement());
    }

    /// <inheritdoc />
    public override Task<BackupScopeCapabilities> ProbeCapabilities(BackupCapabilityProbeRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (control, req, ct) => control.ProbeCapabilitiesAsync(req.Scope, ct));

    private async Task<TResponse> InvokeAsync<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        Func<ILatticeBackupControl, TRequest, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

        using var credentialScope = StampCallerCredential(context);

        try
        {
            return await handler(_control, request, context.CancellationToken).ConfigureAwait(false);
        }
        catch (RpcException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(StatusCode.Cancelled, "The backup control-API request was cancelled."));
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
        }
        catch (KeyNotFoundException ex)
        {
            throw new RpcException(new Status(StatusCode.NotFound, ex.Message));
        }
        catch (LatticeRestoreValidationException ex)
        {
            // A restore failed its pre-apply trust-boundary validation (a missing
            // manifest or artifact, a digest mismatch, an out-of-scope request, or
            // a coordinated saga that aborted because a peer could not prepare).
            // It is a precondition failure, not an internal fault, and its message
            // is safe and actionable (it names backups / trees, no secrets), so
            // surface it as FailedPrecondition instead of the opaque Internal
            // below - the operator UI turns this into a clear, fixable message.
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Api.Backup: gRPC call to {Method} failed.", context.Method);
            throw new RpcException(new Status(StatusCode.Internal, "The backup control-API request failed."));
        }
    }

    private static BackupCaptureResponse ToCaptureResponse(LatticeBackupCaptureResult result) =>
        new() { BackupId = result.BackupId, Manifest = result.Manifest };

    private static BackupSetCaptureResponse ToSetCaptureResponse(LatticeBackupSetCaptureResult result) =>
        new()
        {
            SetManifest = result.SetManifest,
            Members = result.Members.Select(ToCaptureResponse).ToList(),
        };

    private static RestoreResponse ToRestoreResponse(LatticeRestoreResult result) =>
        new()
        {
            BackupId = result.BackupId,
            TargetTreeId = result.TargetTreeId,
            Mode = result.Mode,
            OperationId = result.OperationId,
            ManifestChain = result.ManifestChain,
            EntriesApplied = result.EntriesApplied,
            ShadowPhysicalTreeId = result.ShadowPhysicalTreeId,
            PreviousPhysicalTreeId = result.PreviousPhysicalTreeId,
        };

    private static LatticeRestoreResult ToRestoreResult(RestoreResponse response) =>
        new(
            response.BackupId,
            response.TargetTreeId,
            response.Mode,
            response.OperationId,
            response.ManifestChain,
            response.EntriesApplied,
            response.ShadowPhysicalTreeId,
            response.PreviousPhysicalTreeId);
}
