using System.Runtime.CompilerServices;
using Grpc.Core;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Strongly-typed client for the backup control-API gRPC surface. Wraps a gRPC
/// <see cref="CallInvoker"/> and the code-first method definitions, re-exposing
/// the transport-agnostic <see cref="ILatticeBackupControl"/> facade surface
/// over the wire: capture, catalog listing / streaming, describe, delete,
/// restore, revert, and artifact export. A dashboard, CLI, or the
/// Orleans.Lattice.Api.Mcp MCP server consumes the API through this client
/// rather than hand-rolling channel calls.
/// </summary>
/// <remarks>
/// The client carries no transport policy of its own: address, TLS, retries,
/// deadlines, and call credentials are configured on the
/// <see cref="CallInvoker"/> / <c>GrpcChannel</c> the caller supplies. Build one
/// with <see cref="Create(CallInvoker, IServiceProvider)"/>, passing a service
/// provider that has Orleans serialization registered (<c>AddSerializer()</c>)
/// so the wire marshallers match the server exactly.
/// </remarks>
public sealed class LatticeBackupApiGrpcClient
{
    private readonly CallInvoker _invoker;
    private readonly LatticeBackupGrpcMethods _methods;

    internal LatticeBackupApiGrpcClient(CallInvoker invoker, LatticeBackupGrpcMethods methods)
    {
        _invoker = invoker ?? throw new ArgumentNullException(nameof(invoker));
        _methods = methods ?? throw new ArgumentNullException(nameof(methods));
    }

    /// <summary>
    /// Creates a client over <paramref name="callInvoker"/>, building the wire
    /// marshallers from the Orleans serializers resolved out of
    /// <paramref name="serializerProvider"/>.
    /// </summary>
    /// <param name="callInvoker">
    /// The gRPC call invoker, typically <c>channel.CreateCallInvoker()</c>.
    /// </param>
    /// <param name="serializerProvider">
    /// A service provider with Orleans serialization registered
    /// (<c>AddSerializer()</c>), used to resolve the per-message serializers.
    /// </param>
    public static LatticeBackupApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(callInvoker);
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeBackupApiGrpcClient(
            callInvoker,
            LatticeBackupGrpcMethods.FromServiceProvider(serializerProvider));
    }

    /// <summary>Captures a full backup of the request's scope.</summary>
    public async Task<LatticeBackupCaptureResult> CreateBackupAsync(
        LatticeBackupCaptureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var response = await UnaryAsync(
            _methods.CreateBackup,
            new BackupCaptureRequestMessage { Name = request.Name, Scope = request.Scope, PageSize = request.PageSize },
            cancellationToken).ConfigureAwait(false);

        return new LatticeBackupCaptureResult(response.BackupId, response.Manifest);
    }

    /// <summary>Captures an incremental backup layered on a base backup.</summary>
    public async Task<LatticeBackupCaptureResult> CreateIncrementalBackupAsync(
        LatticeBackupIncrementalCaptureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var response = await UnaryAsync(
            _methods.CreateIncrementalBackup,
            new BackupIncrementalCaptureRequestMessage
            {
                Name = request.Name,
                Scope = request.Scope,
                BaseBackupId = request.BaseBackupId,
                PageSize = request.PageSize,
            },
            cancellationToken).ConfigureAwait(false);

        return new LatticeBackupCaptureResult(response.BackupId, response.Manifest);
    }

    /// <summary>
    /// Captures a backup set - one full backup per scope, grouped under a single
    /// set manifest - optionally at a single cross-tree causal fence.
    /// </summary>
    public async Task<LatticeBackupSetCaptureResult> CreateBackupSetAsync(
        LatticeBackupSetCaptureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var response = await UnaryAsync(
            _methods.CreateBackupSet,
            new BackupSetCaptureRequestMessage
            {
                Name = request.Name,
                Scopes = request.Scopes,
                CrossTreeConsistent = request.CrossTreeConsistent,
                PageSize = request.PageSize,
            },
            cancellationToken).ConfigureAwait(false);

        var members = response.Members
            .Select(m => new LatticeBackupCaptureResult(m.BackupId, m.Manifest))
            .ToList();
        return new LatticeBackupSetCaptureResult(response.SetManifest, members);
    }

    /// <summary>Lists the catalogued backups as a deterministic, cursor-resumable page.</summary>
    public Task<BackupCatalogPage> ListBackupsAsync(
        BackupCatalogRequest request,
        CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.ListBackups, request, cancellationToken);

    /// <summary>
    /// Streams every catalogued backup the caller may read, in backup-id order,
    /// with bounded memory. The raw-enumeration analog of
    /// <see cref="ListBackupsAsync"/>.
    /// </summary>
    public IAsyncEnumerable<BackupManifest> StreamBackupsAsync(CancellationToken cancellationToken = default)
        => ServerStreamingAsync(_methods.StreamBackups, new BackupStreamRequest(), cancellationToken);

    /// <summary>
    /// Describes a single backup and its base-first restore chain, or
    /// <see langword="null"/> when no backup with the id exists.
    /// </summary>
    public async Task<BackupChainDescription?> DescribeBackupAsync(
        string backupId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);

        var response = await UnaryAsync(
            _methods.DescribeBackup,
            new BackupDescribeRequest { BackupId = backupId },
            cancellationToken).ConfigureAwait(false);

        if (!response.Found || response.Manifest is null)
        {
            return null;
        }

        return new BackupChainDescription(response.Manifest, response.ChainBackupIds);
    }

    /// <summary>
    /// Deletes a backup. Returns <see langword="true"/> when a backup was
    /// removed, <see langword="false"/> when none existed.
    /// </summary>
    public async Task<bool> DeleteBackupAsync(
        string backupId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);

        var response = await UnaryAsync(
            _methods.DeleteBackup,
            new BackupDeleteRequest { BackupId = backupId },
            cancellationToken).ConfigureAwait(false);

        return response.Deleted;
    }

    /// <summary>Restores a backup into its target tree.</summary>
    public async Task<LatticeRestoreResult> RestoreBackupAsync(
        LatticeRestoreRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var response = await UnaryAsync(
            _methods.RestoreBackup,
            new RestoreRequestMessage
            {
                BackupId = request.BackupId,
                TargetTreeId = request.TargetTreeId,
                Scope = request.Scope,
                Mode = request.Mode,
                OperationId = request.OperationId,
                ApplyBatchSize = request.ApplyBatchSize,
            },
            cancellationToken).ConfigureAwait(false);

        return ToRestoreResult(response);
    }

    /// <summary>Reverts a shadow-cutover restore. Idempotent.</summary>
    public async Task RevertRestoreAsync(
        LatticeRestoreResult restore,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(restore);

        await UnaryAsync(
            _methods.RevertRestore,
            new RestoreResponse
            {
                BackupId = restore.BackupId,
                TargetTreeId = restore.TargetTreeId,
                Mode = restore.Mode,
                OperationId = restore.OperationId,
                ManifestChain = restore.ManifestChain,
                EntriesApplied = restore.EntriesApplied,
                ShadowPhysicalTreeId = restore.ShadowPhysicalTreeId,
                PreviousPhysicalTreeId = restore.PreviousPhysicalTreeId,
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Streams one of a backup's content-addressed artifacts back chunk-wise,
    /// with bounded memory, yielding each chunk until the server ends the stream.
    /// </summary>
    public async IAsyncEnumerable<ReadOnlyMemory<byte>> ExportArtifactAsync(
        string backupId,
        string artifactId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        ArgumentException.ThrowIfNullOrEmpty(artifactId);

        var request = new ArtifactExportRequest { BackupId = backupId, ArtifactId = artifactId };
        await foreach (var chunk in ServerStreamingAsync(_methods.ExportArtifact, request, cancellationToken)
            .ConfigureAwait(false))
        {
            yield return chunk.Data;
        }
    }

    /// <summary>
    /// Returns the endpoint's advertised auth schemes. This RPC is
    /// unauthenticated: it can be called before any credential is acquired, so a
    /// client can discover how to sign in.
    /// </summary>
    public Task<AuthSchemeAdvertisement> GetAuthSchemeAsync(
        AuthSchemeAdvertisementRequest request,
        CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.GetAuthScheme, request, cancellationToken);

    /// <summary>
    /// Probes, with no side effects, which backup / restore operations the calling
    /// credential may perform over <paramref name="scope"/>. Never fails on a
    /// permission denial: each capability is reported as an allowed / denied flag,
    /// default-deny. The flags are advisory (a UX affordance); the server still
    /// authorizes each real operation fail-closed on attempt.
    /// </summary>
    /// <param name="scope">The scope to probe. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The caller's allowed-operation set for <paramref name="scope"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <c>null</c>.</exception>
    public Task<BackupScopeCapabilities> ProbeCapabilitiesAsync(
        BackupScopeSelector scope,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return UnaryAsync(
            _methods.ProbeCapabilities,
            new BackupCapabilityProbeRequest { Scope = scope },
            cancellationToken);
    }

    /// <summary>
    /// Registers (or updates) a recurring backup schedule for
    /// <paramref name="scope"/> that fires every <paramref name="interval"/>,
    /// capturing an incremental backup when <paramref name="incremental"/> is
    /// <see langword="true"/> or a full backup otherwise. An interval below the
    /// scheduler minimum is clamped up; the returned effective interval reports
    /// the cadence actually registered.
    /// </summary>
    /// <param name="scope">The scope to schedule. Must not be <c>null</c>.</param>
    /// <param name="incremental"><c>true</c> to schedule incremental captures, <c>false</c> for full captures.</param>
    /// <param name="interval">The requested cadence between captures. Must be strictly positive.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The cadence actually registered (clamped up to the scheduler minimum when smaller).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="interval"/> is not strictly positive.</exception>
    public async Task<TimeSpan> ScheduleBackupAsync(
        BackupScopeSelector scope,
        bool incremental,
        TimeSpan interval,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(interval.Ticks);

        var response = await UnaryAsync(
            _methods.ScheduleBackup,
            new BackupScheduleRequestMessage
            {
                Scope = scope,
                Incremental = incremental,
                IntervalTicks = interval.Ticks,
            },
            cancellationToken).ConfigureAwait(false);

        return TimeSpan.FromTicks(response.EffectiveIntervalTicks);
    }

    /// <summary>
    /// Removes the runtime recurring backup schedule for <paramref name="scope"/>.
    /// Idempotent.
    /// </summary>
    /// <param name="scope">The scope whose schedule should be removed. Must not be <c>null</c>.</param>
    /// <param name="incremental"><c>true</c> to remove the incremental schedule, <c>false</c> for the full schedule.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <c>null</c>.</exception>
    public async Task CancelScheduleAsync(
        BackupScopeSelector scope,
        bool incremental,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        await UnaryAsync(
            _methods.CancelSchedule,
            new BackupCancelScheduleRequestMessage { Scope = scope, Incremental = incremental },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads a scope's schedule and last-run status, or returns
    /// <see langword="null"/> when the scope is unknown.
    /// </summary>
    /// <param name="scope">The scope to describe. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <c>null</c>.</exception>
    public async Task<BackupScopeStatus?> GetScopeStatusAsync(
        BackupScopeSelector scope,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        var response = await UnaryAsync(
            _methods.GetScopeStatus,
            new BackupScopeStatusRequestMessage { Scope = scope },
            cancellationToken).ConfigureAwait(false);

        return ToScopeStatus(response);
    }

    /// <summary>
    /// Reports whether periodic backup-health monitoring is available on the
    /// server (true only when the configured backup sink is durable and external).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true"/> when monitoring applies; otherwise <see langword="false"/>.</returns>
    public async Task<bool> IsHealthMonitoringAvailableAsync(CancellationToken cancellationToken = default)
    {
        var response = await UnaryAsync(
            _methods.IsHealthMonitoringAvailable,
            new BackupHealthAvailabilityRequest(),
            cancellationToken).ConfigureAwait(false);

        return response.Available;
    }

    /// <summary>
    /// Runs a fresh health verification of the backup identified by
    /// <paramref name="backupId"/>, persists the report server-side, and returns it.
    /// </summary>
    /// <param name="backupId">The backup id to verify. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The fresh health report.</returns>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    public async Task<BackupHealthReport> CheckBackupHealthAsync(
        string backupId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        var response = await UnaryAsync(
            _methods.CheckBackupHealth,
            new BackupHealthCheckRequestMessage { BackupId = backupId },
            cancellationToken).ConfigureAwait(false);

        return response.Report!;
    }

    /// <summary>
    /// Reads the latest stored health report for the backup identified by
    /// <paramref name="backupId"/>, or <see langword="null"/> when no verification
    /// has run for it yet.
    /// </summary>
    /// <param name="backupId">The backup id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The latest stored report, or <see langword="null"/> when none exists.</returns>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    public async Task<BackupHealthReport?> GetBackupHealthAsync(
        string backupId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        var response = await UnaryAsync(
            _methods.GetBackupHealth,
            new BackupHealthGetRequestMessage { BackupId = backupId },
            cancellationToken).ConfigureAwait(false);

        return response.Found ? response.Report : null;
    }

    /// <summary>
    /// Overrides the per-backup health-monitor configuration for the backup
    /// identified by <paramref name="backupId"/>.
    /// </summary>
    /// <param name="backupId">The backup id. Must not be <c>null</c> or empty.</param>
    /// <param name="config">The per-backup monitor configuration. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="config"/> is <c>null</c>.</exception>
    public async Task ConfigureBackupHealthAsync(
        string backupId,
        BackupHealthConfig config,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        ArgumentNullException.ThrowIfNull(config);
        await UnaryAsync(
            _methods.ConfigureBackupHealth,
            new BackupHealthConfigureRequestMessage
            {
                BackupId = backupId,
                MonitoringEnabled = config.MonitoringEnabled,
                IntervalTicks = config.Interval.Ticks,
            },
            cancellationToken).ConfigureAwait(false);
    }

    private async Task<TResponse> UnaryAsync<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        CancellationToken cancellationToken)
        where TRequest : class
        where TResponse : class
    {
        ArgumentNullException.ThrowIfNull(request);

        using var call = _invoker.AsyncUnaryCall(
            method,
            host: null,
            new CallOptions(cancellationToken: cancellationToken),
            request);

        return await call.ResponseAsync.ConfigureAwait(false);
    }

    private async IAsyncEnumerable<TResponse> ServerStreamingAsync<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where TRequest : class
        where TResponse : class
    {
        ArgumentNullException.ThrowIfNull(request);

        using var call = _invoker.AsyncServerStreamingCall(
            method,
            host: null,
            new CallOptions(cancellationToken: cancellationToken),
            request);

        while (await call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
        {
            yield return call.ResponseStream.Current;
        }
    }

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

    private static BackupScopeStatus? ToScopeStatus(BackupScopeStatusResponse response)
    {
        if (!response.Found || response.Scope is null)
        {
            return null;
        }

        return new BackupScopeStatus(
            response.Scope,
            response.FullScheduleRegistered,
            response.IncrementalScheduleRegistered,
            response.LastFullRunUtc,
            response.LastFullSuccessUtc,
            response.LastIncrementalRunUtc,
            response.LastIncrementalSuccessUtc,
            response.LastRunOutcome,
            response.ChainDepth,
            response.RuntimeFullBackupIntervalTicks is { } fullTicks ? TimeSpan.FromTicks(fullTicks) : null,
            response.RuntimeIncrementalBackupIntervalTicks is { } incrementalTicks ? TimeSpan.FromTicks(incrementalTicks) : null);
    }
}
