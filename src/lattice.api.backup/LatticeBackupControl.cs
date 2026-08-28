using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
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
/// <remarks>
/// <para>
/// <b>Tenant scoping.</b> This facade is an external control surface, so a tree
/// name it receives from a caller is an unqualified, <i>tenant-local</i> name.
/// Every such name is composed into the caller's effective, tenant-scoped tree
/// id through <see cref="ITenantContextResolver"/> once, at method entry, and the
/// composed scope is then used for <em>both</em> the authorization call and the
/// operation - authorizing one tree and acting on another would be a security
/// bug. Two tenants that pick the same unqualified name therefore reach two
/// different trees.
/// </para>
/// <para>
/// A tree id that did <i>not</i> come from the caller is never composed. A scope
/// read back off a stored <see cref="BackupManifest"/> was written with the
/// already-effective id, so re-composing it would either double-scope it or,
/// worse, re-attribute another tenant's backup to the current tenant; the
/// reserved catalog tree is platform-owned and likewise left alone. The
/// per-method comments below record which of the two each authorization site is.
/// </para>
/// <para>
/// <b>Zero-cost when tenancy is off.</b> The core no-op resolver resolves the
/// reserved default tenant synchronously and hands back the caller's bare name -
/// the same <see cref="string"/> reference - so
/// <see cref="ResolveEffectiveScopeAsync"/> returns the caller's own selector
/// instance with no allocation and no <c>await</c>, and a cluster with no tenancy
/// add-on behaves byte-for-byte as it did before.
/// </para>
/// </remarks>
internal sealed class LatticeBackupControl : ILatticeBackupControl
{
    private readonly ILatticeBackupCaptureService _capture;
    private readonly ILatticeBackupIncrementalCaptureService _incremental;
    private readonly ILatticeBackupCatalogStore _catalog;
    private readonly ILatticeBackupCatalogRebuildService _catalogRebuild;
    private readonly ILatticeBackupCatalogScrubService _catalogScrub;
    private readonly ILatticeBackupSink _sink;
    private readonly ILatticeBackupRestoreService _restore;
    private readonly ILatticeBackupColdRestoreService _coldRestore;
    private readonly ILatticeBackupHealthService _health;
    private readonly ILatticeBackupHealthStore _healthStore;
    private readonly BackupAccessAuthorizer _authorizer;
    private readonly IGrainFactory _grainFactory;
    private readonly BackupInventoryRegistry _inventory;
    private readonly ITenantContextResolver _tenantResolver;
    private readonly LatticeApiBackupOptions _options;
    private readonly ILatticeViewFactory? _viewFactory;

    /// <summary>Initializes a new <see cref="LatticeBackupControl"/>.</summary>
    /// <param name="capture">The full-capture engine. Must not be <c>null</c>.</param>
    /// <param name="incremental">The incremental-capture engine. Must not be <c>null</c>.</param>
    /// <param name="catalog">The backup catalog store. Must not be <c>null</c>.</param>
    /// <param name="catalogRebuild">The catalog rebuild-from-sink engine. Must not be <c>null</c>.</param>
    /// <param name="catalogScrub">The catalog reconcile / scrub engine. Must not be <c>null</c>.</param>
    /// <param name="sink">The backup storage sink. Must not be <c>null</c>.</param>
    /// <param name="restore">The restore engine. Must not be <c>null</c>.</param>
    /// <param name="coldRestore">The cold, catalog-free disaster-restore engine. Must not be <c>null</c>.</param>
    /// <param name="health">The backup-health verification engine. Must not be <c>null</c>.</param>
    /// <param name="healthStore">The per-backup health-state store. Must not be <c>null</c>.</param>
    /// <param name="authorizer">The fail-closed backup authorization seam. Must not be <c>null</c>.</param>
    /// <param name="grainFactory">The grain factory used to reach the per-scope scheduler. Must not be <c>null</c>.</param>
    /// <param name="inventory">The in-memory backup metric / inventory registry. Must not be <c>null</c>.</param>
    /// <param name="options">The facade options. Must not be <c>null</c>.</param>
    /// <param name="tenantResolver">
    /// The active-tenant context resolver used to compose a caller-supplied,
    /// tenant-local tree name into its effective, tenant-scoped id. Must not be
    /// <c>null</c>. With no tenancy add-on registered this is the core no-op
    /// resolver, which returns the bare name unchanged at zero cost.
    /// </param>
    /// <param name="services">The silo service provider, used to resolve the optional materialised-view factory. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">A required dependency is <c>null</c>.</exception>
    public LatticeBackupControl(
        ILatticeBackupCaptureService capture,
        ILatticeBackupIncrementalCaptureService incremental,
        ILatticeBackupCatalogStore catalog,
        ILatticeBackupCatalogRebuildService catalogRebuild,
        ILatticeBackupCatalogScrubService catalogScrub,
        ILatticeBackupSink sink,
        ILatticeBackupRestoreService restore,
        ILatticeBackupColdRestoreService coldRestore,
        ILatticeBackupHealthService health,
        ILatticeBackupHealthStore healthStore,
        BackupAccessAuthorizer authorizer,
        IGrainFactory grainFactory,
        BackupInventoryRegistry inventory,
        IOptions<LatticeApiBackupOptions> options,
        ITenantContextResolver tenantResolver,
        IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(capture);
        ArgumentNullException.ThrowIfNull(incremental);
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(catalogRebuild);
        ArgumentNullException.ThrowIfNull(catalogScrub);
        ArgumentNullException.ThrowIfNull(sink);
        ArgumentNullException.ThrowIfNull(restore);
        ArgumentNullException.ThrowIfNull(coldRestore);
        ArgumentNullException.ThrowIfNull(health);
        ArgumentNullException.ThrowIfNull(healthStore);
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(inventory);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(tenantResolver);
        ArgumentNullException.ThrowIfNull(services);

        _capture = capture;
        _incremental = incremental;
        _catalog = catalog;
        _catalogRebuild = catalogRebuild;
        _catalogScrub = catalogScrub;
        _sink = sink;
        _restore = restore;
        _coldRestore = coldRestore;
        _health = health;
        _healthStore = healthStore;
        _authorizer = authorizer;
        _grainFactory = grainFactory;
        _inventory = inventory;
        _tenantResolver = tenantResolver;
        _options = options.Value;
        _viewFactory = services.GetService<ILatticeViewFactory>();
    }

    /// <inheritdoc />
    public async Task<LatticeBackupCaptureResult> CreateBackupAsync(
        LatticeBackupCaptureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        // Caller-supplied scope: composed once under the active tenant, then used
        // for both the gate and the capture so the authorized tree and the
        // captured tree can never diverge.
        var scope = await ResolveEffectiveScopeAsync(request.Scope, cancellationToken).ConfigureAwait(false);
        if (!ReferenceEquals(scope, request.Scope))
        {
            request = request with { Scope = scope };
        }

        await _authorizer.AuthorizeBackupAsync(scope, cancellationToken).ConfigureAwait(false);
        return await _capture.CaptureAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<LatticeBackupCaptureResult> CreateIncrementalBackupAsync(
        LatticeBackupIncrementalCaptureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        // Caller-supplied scope: composed once, then used for both the gate and
        // the incremental capture.
        var scope = await ResolveEffectiveScopeAsync(request.Scope, cancellationToken).ConfigureAwait(false);
        if (!ReferenceEquals(scope, request.Scope))
        {
            request = request with { Scope = scope };
        }

        await _authorizer.AuthorizeBackupAsync(scope, cancellationToken).ConfigureAwait(false);
        return await _incremental.CaptureIncrementalAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<LatticeBackupSetCaptureResult> CreateBackupSetAsync(
        LatticeBackupSetCaptureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

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

        return await _capture.CaptureSetAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task ScheduleBackupAsync(
        LatticeBackupScheduleRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        // Caller-supplied scope: composed once and then used for the gate, the
        // scheduler grain key, and the scope persisted on the schedule, so every
        // later cycle captures the tenant's own tree.
        var scope = await ResolveEffectiveScopeAsync(request.Scope, cancellationToken).ConfigureAwait(false);

        await _authorizer.AuthorizeBackupAsync(scope, cancellationToken).ConfigureAwait(false);
        await _grainFactory
            .GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(scope))
            .ScheduleRecurringAsync(scope, request.Incremental, request.Interval)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task CancelScheduleAsync(
        BackupScopeSelector scope,
        bool incremental,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);

        // Caller-supplied scope: composed once so the cancel reaches the same
        // per-tenant scheduler grain the schedule was registered on.
        scope = await ResolveEffectiveScopeAsync(scope, cancellationToken).ConfigureAwait(false);

        await _authorizer.AuthorizeBackupAsync(scope, cancellationToken).ConfigureAwait(false);
        await _grainFactory
            .GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(scope))
            .CancelScheduleAsync(incremental)
            .ConfigureAwait(false);
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

        // Newest-first / filtered listing is served from the backup-catalog index
        // (with a full-scan fallback); the default listing keeps the legacy
        // ascending-by-backup-id order and streams the catalog directly.
        if (request.OrderByCreatedDescending)
        {
            // The tree-id predicate is a caller-supplied, tenant-local name that is
            // matched verbatim against the effective ids recorded on the indexed
            // manifests, so it is composed under the active tenant - otherwise a
            // tenant could never filter by a name it is able to type. The listing
            // stays gated per row by the manifest-scope read check below.
            if (request.TreeId is { Length: > 0 } treeIdFilter)
            {
                var effectiveFilter = await ResolveEffectiveTreeIdAsync(treeIdFilter, cancellationToken)
                    .ConfigureAwait(false);
                if (!ReferenceEquals(effectiveFilter, treeIdFilter))
                {
                    request = request with { TreeId = effectiveFilter };
                }
            }

            var query = new BackupCatalogIndexQuery(_catalog, _sink, _viewFactory);
            return await query
                .QueryAsync(request, pageSize, IsReadAuthorizedAsync, cancellationToken)
                .ConfigureAwait(false);
        }

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

            // Manifest-derived scope: the stored id is already effective, so it is
            // gated as-is and never re-composed (re-composing would re-attribute
            // another tenant's manifest to the current caller).
            if (!await IsReadAuthorizedAsync(manifest.Scope, cancellationToken).ConfigureAwait(false))
            {
                continue;
            }

            // Selection-time liveness: a catalog row whose sink manifest is gone
            // (store drift after a non-clean restart) is unresolvable, so it must
            // not be offered as a restore point. A cheap manifest-presence probe.
            if (!await _sink.ManifestExistsAsync(manifest.Id, cancellationToken).ConfigureAwait(false))
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
            // Manifest-derived scope: already effective, gated as-is.
            if (!await IsReadAuthorizedAsync(manifest.Scope, cancellationToken).ConfigureAwait(false))
            {
                continue;
            }

            // Skip a catalog row the sink can no longer resolve, so a drained
            // enumeration never surfaces an unresolvable backup.
            if (await _sink.ManifestExistsAsync(manifest.Id, cancellationToken).ConfigureAwait(false))
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

        // Manifest-derived scope: already effective, never re-composed.
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

        // Manifest-derived scope: already effective, never re-composed.
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

        // Discard the deleted backup's health state so a stale report never lingers
        // for a backup id that no longer exists.
        await _healthStore.RemoveAsync(backupId, cancellationToken).ConfigureAwait(false);

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

        if (targetTreeId is not null)
        {
            // Only the sub-region shape (kind and key / prefix) is taken from the
            // caller's scope; its own tree id is ignored by the restore engine, so
            // the authorized scope is always rooted at the resolved target.
            var scope = request.Scope is { } sub
                ? new BackupScopeSelector(sub.Kind, targetTreeId, sub.KeyOrPrefix)
                : BackupScopeSelector.WholeTree(targetTreeId);
            await _authorizer.AuthorizeRestoreAsync(scope, cancellationToken).ConfigureAwait(false);
        }

        return await _restore.RestoreAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<LatticeRestoreResult> ColdRestoreAsync(
        LatticeRestoreRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        // Derive the target scope to authorize from the SINK, not the catalog: a
        // cold restore runs precisely when the catalog may be gone, so the target
        // tree is resolved from the explicit request or the sink-held manifest. When
        // neither resolves (an unknown backup id and no target) the cold-restore
        // engine's own fail-closed validation refuses it without touching data.
        //
        // MIXED SITE, exactly as RestoreBackupAsync: the caller-supplied target is
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

        if (targetTreeId is not null)
        {
            var scope = request.Scope is { } sub
                ? new BackupScopeSelector(sub.Kind, targetTreeId, sub.KeyOrPrefix)
                : BackupScopeSelector.WholeTree(targetTreeId);
            await _authorizer.AuthorizeRestoreAsync(scope, cancellationToken).ConfigureAwait(false);
        }

        return await _coldRestore.ColdRestoreAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RevertRestoreAsync(
        LatticeRestoreResult restore,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(restore);

        // The result is handed back in over the wire, so its target tree is a
        // caller-supplied name and is composed under the active tenant. A result
        // returned by RestoreBackupAsync already carries the effective id, which
        // composition leaves untouched; a caller that instead names its own
        // tenant-local tree resolves to the same place. The composed id is written
        // back onto the result before it reaches the restore engine, because the
        // engine re-authorizes and then acts on that same field - gating one tree
        // and reverting another would be a security bug.
        var targetTreeId = await ResolveEffectiveTreeIdAsync(restore.TargetTreeId, cancellationToken)
            .ConfigureAwait(false);
        if (!ReferenceEquals(targetTreeId, restore.TargetTreeId))
        {
            restore = restore with { TargetTreeId = targetTreeId };
        }

        await _authorizer.AuthorizeRestoreAsync(
            BackupScopeSelector.WholeTree(targetTreeId), cancellationToken).ConfigureAwait(false);
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

        // Manifest-derived scope: already effective, never re-composed.
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
            // Manifest-derived scope: already effective, gated as-is.
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
    public async Task<BackupCatalogRebuildReport> RebuildCatalogFromSinkAsync(
        CancellationToken cancellationToken = default)
    {
        // Rebuilding the catalog re-registers manifests of every scope from the
        // sink, so it is a cluster-wide administrative action rather than a
        // per-scope one. Authorize it fail-closed at the reserved catalog tree
        // with the high-privilege Restore (author / bulk-load) authority - the
        // same grant a bulk restore into a tree requires - before any catalog
        // write happens. Under the default no-op gate this short-circuits to
        // allow at zero cost; a bootstrap administrator is always permitted.
        // The catalog tree is a platform-owned constant, not a caller-supplied
        // name, so it is never tenant-composed.
        await _authorizer
            .AuthorizeRestoreAsync(BackupScopeSelector.WholeTree(BackupConstants.CatalogTree), cancellationToken)
            .ConfigureAwait(false);

        return await _catalogRebuild.RebuildFromSinkAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<BackupCatalogScrubReport> ScrubCatalogAgainstSinkAsync(
        bool pruneOrphans = false,
        CancellationToken cancellationToken = default)
    {
        // Scrubbing reconciles rows of every scope against the sink and, when
        // pruning, removes rows from the reserved catalog tree, so it is a
        // cluster-wide administrative action rather than a per-scope one. Authorize
        // it fail-closed at the catalog tree with the high-privilege Restore
        // (author / bulk-load) authority - the same grant rebuild-from-sink requires
        // - before any probe or delete happens. Under the default no-op gate this
        // short-circuits to allow at zero cost; a bootstrap administrator is always
        // permitted. The catalog tree is a platform-owned constant, not a
        // caller-supplied name, so it is never tenant-composed.
        await _authorizer
            .AuthorizeRestoreAsync(BackupScopeSelector.WholeTree(BackupConstants.CatalogTree), cancellationToken)
            .ConfigureAwait(false);

        return await _catalogScrub.ScrubAsync(pruneOrphans, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<BackupScopeStatus?> GetScopeStatusAsync(
        BackupScopeSelector scope,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);

        // Caller-supplied scope: composed once and used for the gate, the
        // scheduler grain key, and the chain-depth match against catalogued
        // manifests (whose scopes already carry effective ids). The status is
        // reported against the composed scope so it lines up with the scope on a
        // manifest returned by ListBackupsAsync.
        scope = await ResolveEffectiveScopeAsync(scope, cancellationToken).ConfigureAwait(false);

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
            && runtime.RuntimeFullBackupInterval is null
            && runtime.RuntimeIncrementalBackupInterval is null
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
            chainDepth,
            runtime.RuntimeFullBackupInterval,
            runtime.RuntimeIncrementalBackupInterval);
    }

    /// <inheritdoc />
    public async Task<BackupScopeCapabilities> ProbeCapabilitiesAsync(
        BackupScopeSelector scope,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);

        // Caller-supplied scope: composed once, so both probes and the reported
        // scope describe the tenant's own tree.
        scope = await ResolveEffectiveScopeAsync(scope, cancellationToken).ConfigureAwait(false);

        // The gate exposes two backup capabilities for a scope: the capture / read
        // authority (Backup) and the author / bulk-load authority (Restore). Probe
        // both with no side effects; list, capture, incremental, and delete all
        // require the Backup authority, restore requires the Restore authority.
        var canBackup = await IsBackupAuthorizedAsync(scope, cancellationToken).ConfigureAwait(false);
        var canRestore = await IsRestoreAuthorizedAsync(scope, cancellationToken).ConfigureAwait(false);

        return new BackupScopeCapabilities
        {
            Scope = scope,
            CanList = canBackup,
            CanCapture = canBackup,
            CanCaptureIncremental = canBackup,
            CanDelete = canBackup,
            CanRestore = canRestore,
        };
    }

    /// <inheritdoc />
    public Task<bool> IsHealthMonitoringAvailableAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(_sink.IsDurable);

    /// <inheritdoc />
    public async Task<BackupHealthReport> CheckBackupHealthAsync(
        string backupId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);

        var manifest = await _catalog.GetAsync(backupId, cancellationToken).ConfigureAwait(false)
            ?? throw new KeyNotFoundException($"No backup with id '{backupId}' exists in the catalog.");

        // Manifest-derived scope: already effective, never re-composed.
        await _authorizer.AuthorizeBackupAsync(manifest.Scope, cancellationToken).ConfigureAwait(false);

        var report = await _health.VerifyAsync(backupId, cancellationToken).ConfigureAwait(false);
        await _healthStore.SetReportAsync(report, cancellationToken).ConfigureAwait(false);
        return report;
    }

    /// <inheritdoc />
    public async Task<BackupHealthReport?> GetBackupHealthAsync(
        string backupId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);

        var manifest = await _catalog.GetAsync(backupId, cancellationToken).ConfigureAwait(false);
        if (manifest is null)
        {
            return null;
        }

        // Manifest-derived scope: already effective, never re-composed.
        await _authorizer.AuthorizeBackupAsync(manifest.Scope, cancellationToken).ConfigureAwait(false);
        return await _healthStore.GetReportAsync(backupId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task ConfigureBackupHealthAsync(
        string backupId,
        BackupHealthConfig config,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        ArgumentNullException.ThrowIfNull(config);

        var manifest = await _catalog.GetAsync(backupId, cancellationToken).ConfigureAwait(false)
            ?? throw new KeyNotFoundException($"No backup with id '{backupId}' exists in the catalog.");

        // Manifest-derived scope: already effective, never re-composed.
        await _authorizer.AuthorizeBackupAsync(manifest.Scope, cancellationToken).ConfigureAwait(false);
        await _healthStore.SetConfigAsync(backupId, config, cancellationToken).ConfigureAwait(false);
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
    /// Composes a caller-supplied, tenant-local tree name into the effective,
    /// tenant-scoped id for the caller's active tenant.
    /// </summary>
    /// <remarks>
    /// Only ever applied to a name that came from the caller. A tree id read back
    /// off a stored manifest is already effective and must never be passed through
    /// here: re-composing it would double-scope it, or re-attribute a manifest
    /// captured by a different tenant to the current one.
    /// </remarks>
    /// <param name="treeName">The caller-supplied, tenant-local tree name.</param>
    /// <param name="cancellationToken">Cancels an asynchronous tenant resolution.</param>
    /// <returns>
    /// The effective tree id. With tenancy off this is the caller's own
    /// <see cref="string"/> instance, returned synchronously.
    /// </returns>
    private ValueTask<string> ResolveEffectiveTreeIdAsync(
        string treeName,
        CancellationToken cancellationToken) =>
        _tenantResolver.ResolveEffectiveTreeIdAsync(treeName, cancellationToken);

    /// <summary>
    /// Composes a caller-supplied scope into the equivalent scope over the
    /// caller's effective, tenant-scoped tree, preserving its kind and its key or
    /// prefix. <see cref="BackupScopeSelector"/> is immutable, so a composed scope
    /// is a new instance; when the tree id is unchanged (tenancy off, or an
    /// already-qualified or reserved name) the caller's own instance is returned
    /// so the warm path allocates nothing.
    /// </summary>
    /// <param name="scope">The caller-supplied scope.</param>
    /// <param name="cancellationToken">Cancels an asynchronous tenant resolution.</param>
    /// <returns>The scope to both authorize and operate on.</returns>
    private ValueTask<BackupScopeSelector> ResolveEffectiveScopeAsync(
        BackupScopeSelector scope,
        CancellationToken cancellationToken)
    {
        var pending = _tenantResolver.ResolveEffectiveTreeIdAsync(scope.TreeId, cancellationToken);

        // Warm path: the no-op resolver always completes synchronously, so no async
        // state machine is entered and no scope is rebuilt.
        if (pending.IsCompletedSuccessfully)
        {
            return new ValueTask<BackupScopeSelector>(WithTreeId(scope, pending.Result));
        }

        return AwaitEffectiveScopeAsync(scope, pending);
    }

    private static async ValueTask<BackupScopeSelector> AwaitEffectiveScopeAsync(
        BackupScopeSelector scope,
        ValueTask<string> pending)
    {
        var effectiveTreeId = await pending.ConfigureAwait(false);
        return WithTreeId(scope, effectiveTreeId);
    }

    private static BackupScopeSelector WithTreeId(BackupScopeSelector scope, string effectiveTreeId) =>
        ReferenceEquals(scope.TreeId, effectiveTreeId)
            ? scope
            : new BackupScopeSelector(scope.Kind, effectiveTreeId, scope.KeyOrPrefix);

    /// <summary>
    /// Composes every member scope of a backup-set request under the active
    /// tenant, returning the caller's own request instance when nothing changed.
    /// The request is rebuilt through its primary constructor rather than a
    /// <c>with</c> clone so its "one scope per distinct tree" guard re-runs over
    /// the composed ids.
    /// </summary>
    private async ValueTask<LatticeBackupSetCaptureRequest> ResolveEffectiveSetRequestAsync(
        LatticeBackupSetCaptureRequest request,
        CancellationToken cancellationToken)
    {
        List<BackupScopeSelector>? composed = null;
        for (var i = 0; i < request.Scopes.Count; i++)
        {
            var scope = request.Scopes[i];
            var effective = await ResolveEffectiveScopeAsync(scope, cancellationToken).ConfigureAwait(false);
            if (composed is null && ReferenceEquals(effective, scope))
            {
                continue;
            }

            if (composed is null)
            {
                composed = new List<BackupScopeSelector>(request.Scopes.Count);
                for (var j = 0; j < i; j++)
                {
                    composed.Add(request.Scopes[j]);
                }
            }

            composed.Add(effective);
        }

        return composed is null
            ? request
            : new LatticeBackupSetCaptureRequest(
                request.Name, composed, request.CrossTreeConsistent, request.PageSize);
    }

    /// <summary>
    /// Applies the fail-closed capture / read gate to a scope, returning
    /// <see langword="true"/> when the caller holds the backup (capture / read)
    /// authority and <see langword="false"/> when the gate denies it, so a listing
    /// hides manifests the caller has no read grant for rather than faulting the
    /// whole enumeration - and so a capability probe reports a clean allow / deny.
    /// </summary>
    /// <remarks>
    /// Takes the scope exactly as given and never tenant-composes it: its callers
    /// pass either a manifest-derived scope (already effective) or a scope already
    /// composed at method entry.
    /// </remarks>
    private ValueTask<bool> IsReadAuthorizedAsync(
        BackupScopeSelector scope,
        CancellationToken cancellationToken) =>
        IsBackupAuthorizedAsync(scope, cancellationToken);

    /// <summary>
    /// Probes the fail-closed <see cref="LatticeOperation.Backup"/> (capture /
    /// read) authority over a scope with no side effects, translating the gate's
    /// throw-on-deny into a boolean.
    /// </summary>
    /// <remarks>
    /// Both denial types must be translated, not just the authorization one. The
    /// tenancy scope refuses a tree outside the active tenant's namespace with
    /// <see cref="LatticeBackupTenantIsolationException"/>, and letting that escape
    /// turns a filter into a fault: a single catalogue entry belonging to another
    /// tenant would fail the caller's entire listing, and the refusal message would
    /// disclose a tree id the caller is not entitled to see.
    /// </remarks>
    private async ValueTask<bool> IsBackupAuthorizedAsync(
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
        catch (LatticeBackupTenantIsolationException)
        {
            return false;
        }
    }

    /// <summary>
    /// Probes the fail-closed <see cref="LatticeOperation.Restore"/> (author /
    /// bulk-load) authority over a scope with no side effects, translating the
    /// gate's throw-on-deny into a boolean.
    /// </summary>
    /// <remarks>
    /// Translates the tenancy refusal as well, for the same reason as
    /// <see cref="IsBackupAuthorizedAsync"/>: this probe reports whether a scope is
    /// available to the caller, so a tenancy denial is an answer, not a fault.
    /// </remarks>
    private async ValueTask<bool> IsRestoreAuthorizedAsync(
        BackupScopeSelector scope,
        CancellationToken cancellationToken)
    {
        try
        {
            await _authorizer.AuthorizeRestoreAsync(scope, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return false;
        }
        catch (LatticeBackupTenantIsolationException)
        {
            return false;
        }
    }
}
