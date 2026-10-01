using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Api.Backup;

/// <summary>
/// The accept-then-poll half of the backup facade (#4122): the
/// <see cref="ILatticeBackupOperations"/> verbs, and the start cores the
/// deprecated blocking verbs wrap. Every start authorizes exactly as the blocking
/// verb always has - the same tenant composition and the same gate, on the same
/// composed scope - before the work is handed to the shared coordinator.
/// </summary>
internal sealed partial class LatticeBackupControl
{
    /// <inheritdoc />
    public async Task<LatticeOperationHandle> StartBackupAsync(
        LatticeBackupCaptureRequest request,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var (_, launch) = await StartCaptureCoreAsync(request, ResolveOperationId(operationId), cancellationToken)
            .ConfigureAwait(false);
        return ToHandle(launch);
    }

    /// <inheritdoc />
    public async Task<LatticeOperationHandle> StartIncrementalBackupAsync(
        LatticeBackupIncrementalCaptureRequest request,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var (_, launch) = await StartIncrementalCaptureCoreAsync(
            request, ResolveOperationId(operationId), cancellationToken).ConfigureAwait(false);
        return ToHandle(launch);
    }

    /// <inheritdoc />
    public async Task<LatticeOperationHandle> StartBackupSetAsync(
        LatticeBackupSetCaptureRequest request,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var (_, launch) = await StartSetCaptureCoreAsync(request, ResolveOperationId(operationId), cancellationToken)
            .ConfigureAwait(false);
        return ToHandle(launch);
    }

    /// <inheritdoc />
    public async Task<LatticeOperationHandle> StartRestoreAsync(
        LatticeRestoreRequest request,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var (_, launch) = await StartRestoreCoreAsync(request, ResolveOperationId(operationId), cancellationToken)
            .ConfigureAwait(false);
        return ToHandle(launch);
    }

    /// <inheritdoc />
    public async Task<LatticeOperationHandle> StartColdRestoreAsync(
        LatticeRestoreRequest request,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var (_, launch) = await StartColdRestoreCoreAsync(request, ResolveOperationId(operationId), cancellationToken)
            .ConfigureAwait(false);
        return ToHandle(launch);
    }

    /// <inheritdoc />
    public async Task<LatticeOperationHandle> StartBackupHealthCheckAsync(
        string backupId,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        var (_, launch) = await StartHealthCheckCoreAsync(backupId, ResolveOperationId(operationId), cancellationToken)
            .ConfigureAwait(false);
        return ToHandle(launch);
    }

    /// <inheritdoc />
    public async Task<LatticeOperationHandle> StartCatalogRebuildAsync(
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        var (_, launch) = await StartCatalogRebuildCoreAsync(ResolveOperationId(operationId), cancellationToken)
            .ConfigureAwait(false);
        return ToHandle(launch);
    }

    /// <inheritdoc />
    public async Task<LatticeOperationHandle> StartCatalogScrubAsync(
        bool pruneOrphans = false,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        var (_, launch) = await StartCatalogScrubCoreAsync(pruneOrphans, ResolveOperationId(operationId), cancellationToken)
            .ConfigureAwait(false);
        return ToHandle(launch);
    }

    /// <inheritdoc />
    public async Task<LatticeOperationStatus?> GetOperationStatusAsync(
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);

        var tenant = await ResolveActiveTenantAsync(cancellationToken).ConfigureAwait(false);
        var record = await _operations.Runner.GetAsync(tenant.Value, operationId).ConfigureAwait(false);
        return record is not null && await IsVisibleAsync(record, cancellationToken).ConfigureAwait(false)
            ? LatticeOperationMapping.ToStatus(record)
            : null;
    }

    /// <inheritdoc />
    public async Task<LatticeOperationPage> ListOperationsAsync(
        LatticeOperationListRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var tenant = await ResolveActiveTenantAsync(cancellationToken).ConfigureAwait(false);
        var (records, next) = await _operations.Runner
            .ListAsync(tenant.Value, BackupOperationKinds.Prefix, request.PageToken, request.EffectivePageSize)
            .ConfigureAwait(false);

        var visible = new List<LatticeOperationStatus>(records.Count);
        foreach (var record in records)
        {
            if (await IsVisibleAsync(record, cancellationToken).ConfigureAwait(false))
            {
                visible.Add(LatticeOperationMapping.ToStatus(record));
            }
        }

        return new LatticeOperationPage { Operations = visible, NextPageToken = next };
    }

    /// <inheritdoc />
    public async Task<LatticeOperationStatus?> CancelOperationAsync(
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);

        var tenant = await ResolveActiveTenantAsync(cancellationToken).ConfigureAwait(false);
        var record = await _operations.Runner.GetAsync(tenant.Value, operationId).ConfigureAwait(false);
        if (record is null || !await IsVisibleAsync(record, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        // Visible, so its existence is no secret; cancelling needs the grant that
        // starting it needed, and a caller without it is refused outright.
        foreach (var scope in BackupOperationScopes.FromOperation(record.TreeIds, record.Attributes)!)
        {
            if (RequiresRestoreGrant(record.Kind))
            {
                await _authorizer.AuthorizeRestoreAsync(scope, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await _authorizer.AuthorizeBackupAsync(scope, cancellationToken).ConfigureAwait(false);
            }
        }

        var cancelled = await _operations.Runner.RequestCancelAsync(tenant.Value, operationId).ConfigureAwait(false);
        return cancelled is null ? null : LatticeOperationMapping.ToStatus(cancelled);
    }

    private async Task<(string TenantId, LatticeOperationLaunch<LatticeBackupCaptureResult> Launch)> StartCaptureCoreAsync(
        LatticeBackupCaptureRequest request,
        string operationId,
        CancellationToken cancellationToken)
    {
        // Caller-supplied scope: composed once under the active tenant, then used
        // for both the gate and the capture so the authorized tree and the
        // captured tree can never diverge.
        var scope = await ResolveEffectiveScopeAsync(request.Scope, cancellationToken).ConfigureAwait(false);
        if (!ReferenceEquals(scope, request.Scope))
        {
            request = request with { Scope = scope };
        }

        await _authorizer.AuthorizeBackupAsync(scope, cancellationToken).ConfigureAwait(false);
        var tenantId = (await ResolveActiveTenantAsync(cancellationToken).ConfigureAwait(false)).Value;
        var launch = await _operations.StartCaptureAsync(tenantId, operationId, request, [scope]).ConfigureAwait(false);
        return (tenantId, launch);
    }

    private async Task<(string TenantId, LatticeOperationLaunch<LatticeBackupCaptureResult> Launch)> StartIncrementalCaptureCoreAsync(
        LatticeBackupIncrementalCaptureRequest request,
        string operationId,
        CancellationToken cancellationToken)
    {
        // Caller-supplied scope: composed once, then used for both the gate and
        // the incremental capture.
        var scope = await ResolveEffectiveScopeAsync(request.Scope, cancellationToken).ConfigureAwait(false);
        if (!ReferenceEquals(scope, request.Scope))
        {
            request = request with { Scope = scope };
        }

        await _authorizer.AuthorizeBackupAsync(scope, cancellationToken).ConfigureAwait(false);
        var tenantId = (await ResolveActiveTenantAsync(cancellationToken).ConfigureAwait(false)).Value;
        var launch = await _operations.StartIncrementalCaptureAsync(tenantId, operationId, request, [scope])
            .ConfigureAwait(false);
        return (tenantId, launch);
    }

    private async Task<(string TenantId, LatticeOperationLaunch<LatticeBackupSetCaptureResult> Launch)> StartSetCaptureCoreAsync(
        LatticeBackupSetCaptureRequest request,
        string operationId,
        CancellationToken cancellationToken)
    {
        // Every member scope is caller-supplied, so each is composed under the
        // active tenant before anything is authorized. Rebuilt through the primary
        // constructor rather than a `with` clone so the "one scope per distinct
        // tree" guard re-runs over the composed ids.
        request = await ResolveEffectiveSetRequestAsync(request, cancellationToken).ConfigureAwait(false);

        // Authorize every member scope fail-closed before any tree is touched, so
        // a set that includes even one forbidden scope is rejected in full rather
        // than partially captured.
        foreach (var scope in request.Scopes)
        {
            await _authorizer.AuthorizeBackupAsync(scope, cancellationToken).ConfigureAwait(false);
        }

        var tenantId = (await ResolveActiveTenantAsync(cancellationToken).ConfigureAwait(false)).Value;
        var launch = await _operations.StartSetCaptureAsync(tenantId, operationId, request, request.Scopes)
            .ConfigureAwait(false);
        return (tenantId, launch);
    }

    private async Task<(string TenantId, LatticeOperationLaunch<LatticeRestoreResult> Launch)> StartRestoreCoreAsync(
        LatticeRestoreRequest request,
        string operationId,
        CancellationToken cancellationToken)
    {
        // Derive the target scope to authorize: the explicit target tree when
        // supplied, else the tree the backup was captured from. When neither can
        // be resolved the gate is NOT skipped - see
        // ResolveRestoreAuthorizationScope, which falls back to the reserved
        // catalog tree so the check stays total.
        //
        // MIXED SITE - the two branches are NOT equivalent. An explicit
        // TargetTreeId is a caller-supplied, tenant-local name and is composed
        // under the active tenant (and written back onto the request, so the gate
        // and the restore engine target the same tree). A target falling back to
        // the manifest's captured scope is already the effective id the capture
        // recorded, so it is used verbatim: composing it would double-scope it, or
        // silently re-attribute another tenant's backup to this caller.
        var manifest = await _catalog.GetAsync(request.BackupId, cancellationToken).ConfigureAwait(false);
        string? targetTreeId;
        if (request.TargetTreeId is { } requestedTarget)
        {
            targetTreeId = await ResolveEffectiveTreeIdAsync(requestedTarget, cancellationToken)
                .ConfigureAwait(false);
            if (!ReferenceEquals(targetTreeId, requestedTarget))
            {
                request = request with { TargetTreeId = targetTreeId };
            }
        }
        else
        {
            targetTreeId = manifest?.Scope.TreeId;
        }

        var authorizedScope = ResolveRestoreAuthorizationScope(targetTreeId, request.Scope);
        await _authorizer.AuthorizeRestoreAsync(authorizedScope, cancellationToken).ConfigureAwait(false);

        var tenantId = (await ResolveActiveTenantAsync(cancellationToken).ConfigureAwait(false)).Value;
        var launch = await _operations.StartRestoreAsync(tenantId, operationId, request, [authorizedScope])
            .ConfigureAwait(false);
        return (tenantId, launch);
    }

    private async Task<(string TenantId, LatticeOperationLaunch<LatticeRestoreResult> Launch)> StartColdRestoreCoreAsync(
        LatticeRestoreRequest request,
        string operationId,
        CancellationToken cancellationToken)
    {
        // Derive the target scope to authorize from the SINK, not the catalog: a
        // cold restore runs precisely when the catalog may be gone, so the target
        // tree is resolved from the explicit request or the sink-held manifest. When
        // neither resolves the gate is NOT skipped - see
        // ResolveRestoreAuthorizationScope, which falls back to the reserved catalog
        // tree so the check stays total.
        //
        // MIXED SITE, exactly as StartRestoreCoreAsync: the caller-supplied target is
        // composed, the sink-manifest-derived one is already effective and is left
        // alone.
        var manifest = await _sink.ReadManifestAsync(request.BackupId, cancellationToken).ConfigureAwait(false);
        string? targetTreeId;
        if (request.TargetTreeId is { } requestedTarget)
        {
            targetTreeId = await ResolveEffectiveTreeIdAsync(requestedTarget, cancellationToken)
                .ConfigureAwait(false);
            if (!ReferenceEquals(targetTreeId, requestedTarget))
            {
                request = request with { TargetTreeId = targetTreeId };
            }
        }
        else
        {
            targetTreeId = manifest?.Scope.TreeId;
        }

        var authorizedScope = ResolveRestoreAuthorizationScope(targetTreeId, request.Scope);
        await _authorizer.AuthorizeRestoreAsync(authorizedScope, cancellationToken).ConfigureAwait(false);

        var tenantId = (await ResolveActiveTenantAsync(cancellationToken).ConfigureAwait(false)).Value;
        var launch = await _operations.StartColdRestoreAsync(tenantId, operationId, request, [authorizedScope])
            .ConfigureAwait(false);
        return (tenantId, launch);
    }

    private async Task<(string TenantId, LatticeOperationLaunch<BackupHealthReport> Launch)> StartHealthCheckCoreAsync(
        string backupId,
        string operationId,
        CancellationToken cancellationToken)
    {
        var manifest = await _catalog.GetAsync(backupId, cancellationToken).ConfigureAwait(false)
            ?? throw new KeyNotFoundException($"No backup with id '{backupId}' exists in the catalog.");

        // Manifest-derived scope: already effective, never re-composed. The
        // operation is recorded over that same scope, so its visibility and cancel
        // are authorized against the backup's own tree.
        await _authorizer.AuthorizeBackupAsync(manifest.Scope, cancellationToken).ConfigureAwait(false);
        var tenantId = (await ResolveActiveTenantAsync(cancellationToken).ConfigureAwait(false)).Value;
        var launch = await _operations.StartHealthCheckAsync(tenantId, operationId, backupId, [manifest.Scope])
            .ConfigureAwait(false);
        return (tenantId, launch);
    }

    private async Task<(string TenantId, LatticeOperationLaunch<BackupCatalogRebuildReport> Launch)> StartCatalogRebuildCoreAsync(
        string operationId,
        CancellationToken cancellationToken)
    {
        // Cluster-wide administrative action: authorized fail-closed at the reserved
        // catalog tree with the Restore authority, exactly as the blocking verb is.
        // The catalog tree is a platform-owned constant, never tenant-composed.
        var catalogScope = BackupScopeSelector.WholeTree(BackupConstants.CatalogTree);
        await _authorizer.AuthorizeRestoreAsync(catalogScope, cancellationToken).ConfigureAwait(false);
        var tenantId = (await ResolveActiveTenantAsync(cancellationToken).ConfigureAwait(false)).Value;
        var launch = await _operations.StartCatalogRebuildAsync(tenantId, operationId, [catalogScope])
            .ConfigureAwait(false);
        return (tenantId, launch);
    }

    private async Task<(string TenantId, LatticeOperationLaunch<BackupCatalogScrubReport> Launch)> StartCatalogScrubCoreAsync(
        bool pruneOrphans,
        string operationId,
        CancellationToken cancellationToken)
    {
        // As StartCatalogRebuildCoreAsync: the catalog tree, the Restore authority.
        var catalogScope = BackupScopeSelector.WholeTree(BackupConstants.CatalogTree);
        await _authorizer.AuthorizeRestoreAsync(catalogScope, cancellationToken).ConfigureAwait(false);
        var tenantId = (await ResolveActiveTenantAsync(cancellationToken).ConfigureAwait(false)).Value;
        var launch = await _operations.StartCatalogScrubAsync(tenantId, operationId, pruneOrphans, [catalogScope])
            .ConfigureAwait(false);
        return (tenantId, launch);
    }

    /// <summary>
    /// The deprecated blocking verbs' wait: awaits the in-process work this call
    /// started and returns the engine's own result or rethrows its own exception.
    /// Cancelling <paramref name="cancellationToken"/> cancels the operation, as
    /// cancelling a blocking call always cancelled its work.
    /// </summary>
    private async Task<TResult> AwaitOperationAsync<TResult>(
        string tenantId,
        LatticeOperationLaunch<TResult> launch,
        CancellationToken cancellationToken)
    {
        // A wrapper always starts under a freshly generated id, so it always owns
        // the work; a launch without it would mean the id collided.
        var completion = launch.Completion ?? throw new InvalidOperationException(
            $"Operation '{launch.Record.OperationId}' already existed, so there is no work to wait for.");

        try
        {
            return await completion.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested && !completion.IsCompleted)
        {
            await _operations.Runner.RequestCancelAsync(tenantId, launch.Record.OperationId).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Whether the caller may see an operation: it must be a backup kind, its
    /// recorded scopes must decode, and the caller must hold the read grant - or,
    /// for a restore, the restore grant - over every one of them. Anything else is
    /// reported as not found, so an operation's existence is never disclosed.
    /// </summary>
    private async ValueTask<bool> IsVisibleAsync(LatticeOperationRecord record, CancellationToken cancellationToken)
    {
        if (!record.Kind.StartsWith(BackupOperationKinds.Prefix, StringComparison.Ordinal)
            || BackupOperationScopes.FromOperation(record.TreeIds, record.Attributes) is not { } scopes)
        {
            return false;
        }

        var restoreKind = RequiresRestoreGrant(record.Kind);
        foreach (var scope in scopes)
        {
            if (!await IsReadAuthorizedAsync(scope, cancellationToken).ConfigureAwait(false)
                && !(restoreKind && await IsRestoreAuthorizedAsync(scope, cancellationToken).ConfigureAwait(false)))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Whether starting (and so cancelling) an operation of <paramref name="kind"/>
    /// needs the restore grant rather than the backup grant: a restore, or a
    /// cluster-wide catalog rebuild or scrub, which is authorized at the reserved
    /// catalog tree with the restore authority.
    /// </summary>
    private static bool RequiresRestoreGrant(string kind) =>
        string.Equals(kind, BackupOperationKinds.Restore, StringComparison.Ordinal)
        || string.Equals(kind, BackupOperationKinds.ColdRestore, StringComparison.Ordinal)
        || string.Equals(kind, BackupOperationKinds.CatalogRebuild, StringComparison.Ordinal)
        || string.Equals(kind, BackupOperationKinds.CatalogScrub, StringComparison.Ordinal);

    private static string ResolveOperationId(string? operationId)
    {
        if (operationId is null)
        {
            return LatticeOperationKey.NewId();
        }

        LatticeOperationKey.ThrowIfInvalid(operationId, nameof(operationId));
        return operationId;
    }

    private static LatticeOperationHandle ToHandle<TResult>(LatticeOperationLaunch<TResult> launch) =>
        LatticeOperationMapping.ToHandle(launch.Record, created: launch.Completion is not null);
}
