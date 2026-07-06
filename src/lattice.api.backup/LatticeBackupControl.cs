using System.Runtime.CompilerServices;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup;

/// <summary>
/// Default <see cref="ILatticeBackupControl"/> implementation. Registered as a
/// silo singleton by <c>AddLatticeBackupApi</c>; it drives the backup engine's
/// capture, incremental-capture, restore, catalog, and sink seams and gates
/// every operation through the shared <see cref="BackupAccessAuthorizer"/>
/// fail-closed before touching data.
/// </summary>
internal sealed class LatticeBackupControl : ILatticeBackupControl
{
    private readonly ILatticeBackupCaptureService _capture;
    private readonly ILatticeBackupIncrementalCaptureService _incremental;
    private readonly ILatticeBackupCatalogStore _catalog;
    private readonly ILatticeBackupSink _sink;
    private readonly ILatticeBackupRestoreService _restore;
    private readonly BackupAccessAuthorizer _authorizer;
    private readonly IGrainFactory _grainFactory;
    private readonly BackupInventoryRegistry _inventory;
    private readonly LatticeApiBackupOptions _options;

    /// <summary>Initializes a new <see cref="LatticeBackupControl"/>.</summary>
    /// <param name="capture">The full-capture engine. Must not be <c>null</c>.</param>
    /// <param name="incremental">The incremental-capture engine. Must not be <c>null</c>.</param>
    /// <param name="catalog">The backup catalog store. Must not be <c>null</c>.</param>
    /// <param name="sink">The backup storage sink. Must not be <c>null</c>.</param>
    /// <param name="restore">The restore engine. Must not be <c>null</c>.</param>
    /// <param name="authorizer">The fail-closed backup authorization seam. Must not be <c>null</c>.</param>
    /// <param name="grainFactory">The grain factory used to reach the per-scope scheduler. Must not be <c>null</c>.</param>
    /// <param name="inventory">The in-memory backup metric / inventory registry. Must not be <c>null</c>.</param>
    /// <param name="options">The facade options. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">A required dependency is <c>null</c>.</exception>
    public LatticeBackupControl(
        ILatticeBackupCaptureService capture,
        ILatticeBackupIncrementalCaptureService incremental,
        ILatticeBackupCatalogStore catalog,
        ILatticeBackupSink sink,
        ILatticeBackupRestoreService restore,
        BackupAccessAuthorizer authorizer,
        IGrainFactory grainFactory,
        BackupInventoryRegistry inventory,
        IOptions<LatticeApiBackupOptions> options)
    {
        ArgumentNullException.ThrowIfNull(capture);
        ArgumentNullException.ThrowIfNull(incremental);
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(sink);
        ArgumentNullException.ThrowIfNull(restore);
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(inventory);
        ArgumentNullException.ThrowIfNull(options);

        _capture = capture;
        _incremental = incremental;
        _catalog = catalog;
        _sink = sink;
        _restore = restore;
        _authorizer = authorizer;
        _grainFactory = grainFactory;
        _inventory = inventory;
        _options = options.Value;
    }

    /// <inheritdoc />
    public async Task<LatticeBackupCaptureResult> CreateBackupAsync(
        LatticeBackupCaptureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await _authorizer.AuthorizeBackupAsync(request.Scope, cancellationToken).ConfigureAwait(false);
        return await _capture.CaptureAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<LatticeBackupCaptureResult> CreateIncrementalBackupAsync(
        LatticeBackupIncrementalCaptureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await _authorizer.AuthorizeBackupAsync(request.Scope, cancellationToken).ConfigureAwait(false);
        return await _incremental.CaptureIncrementalAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<BackupCatalogPage> ListBackupsAsync(
        BackupCatalogRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var pageSize = request.PageSize <= 0
            ? _options.DefaultListPageSize
            : Math.Min(request.PageSize, _options.MaxListPageSize);

        var token = request.PageToken;
        var entries = new List<BackupManifest>(pageSize);
        string? nextPageToken = null;

        await foreach (var manifest in _catalog.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            // Exclusive cursor: skip everything at or before the caller's token.
            if (token is not null && string.CompareOrdinal(manifest.Id, token) <= 0)
            {
                continue;
            }

            if (!await IsReadAuthorizedAsync(manifest.Scope, cancellationToken).ConfigureAwait(false))
            {
                continue;
            }

            // The first authorized manifest beyond a full page is the signal
            // that more remain; stop and hand back the last page id as the
            // exclusive continuation cursor.
            if (entries.Count == pageSize)
            {
                nextPageToken = entries[^1].Id;
                break;
            }

            entries.Add(manifest);
        }

        return new BackupCatalogPage { Entries = entries, NextPageToken = nextPageToken };
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<BackupManifest> StreamBackupsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var manifest in _catalog.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            if (await IsReadAuthorizedAsync(manifest.Scope, cancellationToken).ConfigureAwait(false))
            {
                yield return manifest;
            }
        }
    }

    /// <inheritdoc />
    public async Task<BackupChainDescription?> DescribeBackupAsync(
        string backupId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);

        var manifest = await _catalog.GetAsync(backupId, cancellationToken).ConfigureAwait(false);
        if (manifest is null)
        {
            return null;
        }

        await _authorizer.AuthorizeBackupAsync(manifest.Scope, cancellationToken).ConfigureAwait(false);

        // Walk the base chain tip-first, guarding against a malformed cycle, then
        // reverse so the chain is reported base-first (the replay order).
        var chain = new List<string>();
        var visited = new HashSet<string>(StringComparer.Ordinal);
        var current = manifest;
        while (current is not null && visited.Add(current.Id))
        {
            chain.Add(current.Id);
            if (current.BaseBackupId is null)
            {
                break;
            }

            current = await _catalog.GetAsync(current.BaseBackupId, cancellationToken).ConfigureAwait(false);
        }

        chain.Reverse();
        return new BackupChainDescription(manifest, chain);
    }

    /// <inheritdoc />
    public async Task<bool> DeleteBackupAsync(
        string backupId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);

        var manifest = await _catalog.GetAsync(backupId, cancellationToken).ConfigureAwait(false);
        if (manifest is null)
        {
            return false;
        }

        await _authorizer.AuthorizeBackupAsync(manifest.Scope, cancellationToken).ConfigureAwait(false);

        // Collect every artifact still referenced by another retained manifest so
        // a shared artifact (a base backup an increment still depends on, or a
        // content-addressed artifact reused across backups) is never deleted.
        var stillReferenced = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var other in _catalog.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            if (string.Equals(other.Id, backupId, StringComparison.Ordinal))
            {
                continue;
            }

            foreach (var descriptor in other.ContentDescriptors)
            {
                stillReferenced.Add(descriptor.ArtifactId);
            }
        }

        // Remove the manifest first so no catalog / sink entry can outlive the
        // artifacts it points at, then delete only this backup's own unshared
        // artifacts.
        await _catalog.RemoveAsync(backupId, cancellationToken).ConfigureAwait(false);
        await _sink.DeleteManifestAsync(backupId, cancellationToken).ConfigureAwait(false);

        foreach (var descriptor in manifest.ContentDescriptors)
        {
            if (!stillReferenced.Contains(descriptor.ArtifactId))
            {
                await _sink.DeleteArtifactAsync(descriptor.ArtifactId, cancellationToken).ConfigureAwait(false);
            }
        }

        return true;
    }

    /// <inheritdoc />
    public async Task<LatticeRestoreResult> RestoreBackupAsync(
        LatticeRestoreRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        // Derive the target scope to authorize: the explicit target tree when
        // supplied, else the tree the backup was captured from. When neither can
        // be resolved (an unknown backup id and no target) the restore engine's
        // own fail-closed validation refuses it without touching data.
        var manifest = await _catalog.GetAsync(request.BackupId, cancellationToken).ConfigureAwait(false);
        var targetTreeId = request.TargetTreeId ?? manifest?.Scope.TreeId;
        if (targetTreeId is not null)
        {
            var scope = request.Scope is { } sub
                ? new BackupScopeSelector(sub.Kind, targetTreeId, sub.KeyOrPrefix)
                : BackupScopeSelector.WholeTree(targetTreeId);
            await _authorizer.AuthorizeRestoreAsync(scope, cancellationToken).ConfigureAwait(false);
        }

        return await _restore.RestoreAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RevertRestoreAsync(
        LatticeRestoreResult restore,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(restore);
        await _authorizer.AuthorizeRestoreAsync(
            BackupScopeSelector.WholeTree(restore.TargetTreeId), cancellationToken).ConfigureAwait(false);
        await _restore.RevertRestoreAsync(restore, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ReadOnlyMemory<byte>> ExportArtifactAsync(
        string backupId,
        string artifactId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        ArgumentException.ThrowIfNullOrEmpty(artifactId);

        var manifest = await _catalog.GetAsync(backupId, cancellationToken).ConfigureAwait(false)
            ?? throw new KeyNotFoundException($"No backup with id '{backupId}' exists.");

        await _authorizer.AuthorizeBackupAsync(manifest.Scope, cancellationToken).ConfigureAwait(false);

        var owned = false;
        foreach (var descriptor in manifest.ContentDescriptors)
        {
            if (string.Equals(descriptor.ArtifactId, artifactId, StringComparison.Ordinal))
            {
                owned = true;
                break;
            }
        }

        if (!owned)
        {
            throw new KeyNotFoundException(
                $"Backup '{backupId}' does not reference artifact '{artifactId}'.");
        }

        await foreach (var chunk in _sink.ReadArtifactAsync(artifactId, cancellationToken).ConfigureAwait(false))
        {
            yield return chunk;
        }
    }

    /// <inheritdoc />
    public async Task<BackupInventoryReport> GetInventoryAsync(
        CancellationToken cancellationToken = default)
    {
        long total = 0;
        long totalBytes = 0;
        long fullCount = 0;
        long incrementalCount = 0;
        DateTimeOffset? oldest = null;
        DateTimeOffset? newest = null;

        await foreach (var manifest in _catalog.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            if (!await IsReadAuthorizedAsync(manifest.Scope, cancellationToken).ConfigureAwait(false))
            {
                continue;
            }

            total++;
            foreach (var descriptor in manifest.ContentDescriptors)
            {
                totalBytes += descriptor.ByteLength;
            }

            if (manifest.Kind == BackupKind.Incremental)
            {
                incrementalCount++;
            }
            else
            {
                fullCount++;
            }

            if (oldest is null || manifest.CreatedAtUtc < oldest)
            {
                oldest = manifest.CreatedAtUtc;
            }

            if (newest is null || manifest.CreatedAtUtc > newest)
            {
                newest = manifest.CreatedAtUtc;
            }
        }

        return new BackupInventoryReport(
            total,
            totalBytes,
            fullCount,
            incrementalCount,
            oldest,
            newest,
            _inventory.CaptureFailureCount,
            _inventory.RestoreFailureCount,
            _inventory.BytesReclaimed);
    }

    /// <inheritdoc />
    public async Task<BackupScopeStatus?> GetScopeStatusAsync(
        BackupScopeSelector scope,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);

        // Fail-closed read authorization before any scope state is revealed.
        await _authorizer.AuthorizeBackupAsync(scope, cancellationToken).ConfigureAwait(false);

        var runtime = await _grainFactory
            .GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(scope))
            .GetScopeRuntimeStatusAsync()
            .ConfigureAwait(false);

        var chainDepth = await ComputeScopeChainDepthAsync(scope, cancellationToken).ConfigureAwait(false);

        // An unknown scope - no schedule of either kind, no recorded run, and no
        // catalogued backup - is reported as absent rather than an empty status.
        if (!runtime.FullScheduleRegistered
            && !runtime.IncrementalScheduleRegistered
            && runtime.LastRunOutcome == BackupScopeRunOutcome.None
            && chainDepth == 0)
        {
            return null;
        }

        return new BackupScopeStatus(
            scope,
            runtime.FullScheduleRegistered,
            runtime.IncrementalScheduleRegistered,
            runtime.LastFullRunUtc,
            runtime.LastFullSuccessUtc,
            runtime.LastIncrementalRunUtc,
            runtime.LastIncrementalSuccessUtc,
            runtime.LastRunOutcome,
            chainDepth);
    }

    /// <summary>
    /// Computes the base-chain depth of a scope's latest backup by finding the
    /// newest manifest whose scope matches and walking its
    /// <see cref="BackupManifest.BaseBackupId"/> ancestry, guarding against a
    /// malformed cycle. Returns 0 when the scope has no catalogued backup.
    /// </summary>
    private async Task<int> ComputeScopeChainDepthAsync(
        BackupScopeSelector scope,
        CancellationToken cancellationToken)
    {
        BackupManifest? latest = null;
        var byId = new Dictionary<string, BackupManifest>(StringComparer.Ordinal);
        await foreach (var manifest in _catalog.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            byId[manifest.Id] = manifest;
            if (!ScopeMatches(manifest.Scope, scope))
            {
                continue;
            }

            if (latest is null
                || manifest.CreatedAtUtc > latest.CreatedAtUtc
                || (manifest.CreatedAtUtc == latest.CreatedAtUtc
                    && string.CompareOrdinal(manifest.Id, latest.Id) > 0))
            {
                latest = manifest;
            }
        }

        if (latest is null)
        {
            return 0;
        }

        var depth = 0;
        var visited = new HashSet<string>(StringComparer.Ordinal);
        var current = latest;
        while (current is not null && visited.Add(current.Id))
        {
            depth++;
            if (current.BaseBackupId is not { } baseId || !byId.TryGetValue(baseId, out var baseManifest))
            {
                break;
            }

            current = baseManifest;
        }

        return depth;
    }

    private static bool ScopeMatches(BackupScopeSelector a, BackupScopeSelector b) =>
        a.Kind == b.Kind
        && string.Equals(a.TreeId, b.TreeId, StringComparison.Ordinal)
        && string.Equals(a.KeyOrPrefix, b.KeyOrPrefix, StringComparison.Ordinal);

    /// <summary>
    /// Applies the fail-closed read gate to a manifest's scope, returning
    /// <see langword="true"/> when the caller may read it and
    /// <see langword="false"/> when the gate denies it, so a listing hides
    /// manifests the caller has no read grant for rather than faulting the whole
    /// enumeration.
    /// </summary>
    private async ValueTask<bool> IsReadAuthorizedAsync(
        BackupScopeSelector scope,
        CancellationToken cancellationToken)
    {
        try
        {
            await _authorizer.AuthorizeBackupAsync(scope, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return false;
        }
    }
}
