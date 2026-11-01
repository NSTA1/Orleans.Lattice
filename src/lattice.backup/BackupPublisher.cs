namespace Orleans.Lattice.Backup;

/// <summary>
/// The publish and retire steps of the backup catalog and sink, without any
/// serialisation of their own. <see cref="BackupSchedulerGrain"/> runs each one
/// under its per-id hold (issue #5418); <see cref="DirectBackupPublicationGate"/>
/// runs them unheld, for hosts and tests with no grain runtime.
/// </summary>
/// <param name="catalog">The catalog store.</param>
/// <param name="sink">The backup sink.</param>
/// <param name="grainFactory">
/// Reaches the per-id <see cref="IBackupIdClaimGrain"/> that arbitrates a bare
/// content id between scopes; <see langword="null"/> reads ownership directly,
/// which is unserialised across scopes.
/// </param>
internal sealed class BackupPublisher(
    ILatticeBackupCatalogStore catalog,
    ILatticeBackupSink sink,
    IGrainFactory? grainFactory = null)
{
    /// <summary>
    /// Resolves the id a full backup publishes under (issue #5620). It is the bare
    /// content address carried in <paramref name="manifest"/> unless that id belongs
    /// to a backup of another scope, in which case it is the scope-qualified id.
    /// A scope that already holds the scope-qualified id keeps it, so a same-scope
    /// retry re-derives the id it took before.
    /// </summary>
    /// <param name="manifest">A full manifest whose id is its bare content address.</param>
    /// <param name="scopeKey">Its scope's key.</param>
    /// <returns>The resolved id, and whether the bare id was claimed (and must be released).</returns>
    public async Task<(string Id, bool ClaimedBare)> ResolveFullIdAsync(BackupManifest manifest, string scopeKey)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        var bare = manifest.Id;
        var qualified = BackupIdentity.ScopeQualifiedFull(scopeKey, ContentHashOf(manifest));
        var qualifiedOwner = await BackupIdOwnership.OwnerScopeKeyAsync(catalog, sink, qualified).ConfigureAwait(false);
        if (string.Equals(qualifiedOwner, scopeKey, StringComparison.Ordinal))
        {
            return (qualified, false);
        }

        if (grainFactory is not null)
        {
            var claimed = await grainFactory.GetGrain<IBackupIdClaimGrain>(bare).TryClaimAsync(scopeKey).ConfigureAwait(false);
            return claimed ? (bare, true) : (qualified, false);
        }

        var owner = await BackupIdOwnership.OwnerScopeKeyAsync(catalog, sink, bare).ConfigureAwait(false);
        return owner is null || string.Equals(owner, scopeKey, StringComparison.Ordinal)
            ? (bare, false)
            : (qualified, false);
    }

    /// <summary>Releases a bare-id claim taken by <see cref="ResolveFullIdAsync"/>.</summary>
    /// <param name="bareId">The claimed bare id.</param>
    /// <param name="scopeKey">The claiming scope's key.</param>
    public Task ReleaseClaimAsync(string bareId, string scopeKey) =>
        grainFactory is null
            ? Task.CompletedTask
            : grainFactory.GetGrain<IBackupIdClaimGrain>(bareId).ReleaseAsync(scopeKey);

    /// <summary>
    /// Publishes <paramref name="manifest"/>, whose id is final, as
    /// <paramref name="mode"/> directs. The caller holds the id.
    /// </summary>
    /// <param name="manifest">The manifest, under its final id.</param>
    /// <param name="mode">How to publish it.</param>
    /// <param name="register">For a full backup: register now, or defer to a set stamp.</param>
    /// <returns>The outcome and, when published, the manifest as published.</returns>
    public async Task<BackupPublishResult> PublishAsync(BackupManifest manifest, BackupPublishMode mode, bool register)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        switch (mode)
        {
            case BackupPublishMode.Full:
            case BackupPublishMode.Register:
            {
                var existing = await catalog.GetAsync(manifest.Id).ConfigureAwait(false);
                var published = BackupManifestRegistration.Reconcile(existing, manifest);
                await sink.WriteManifestAsync(published).ConfigureAwait(false);
                if (mode == BackupPublishMode.Register || register)
                {
                    await catalog.RegisterAsync(published).ConfigureAwait(false);
                }

                return new BackupPublishResult(BackupPublishOutcome.Published, published);
            }

            case BackupPublishMode.Increment:
                return await PublishIncrementAsync(manifest).ConfigureAwait(false);

            case BackupPublishMode.Rebuild:
            {
                // Re-read under the hold and registered only while it is still in the
                // sink: a pass or a delete that removed it first is never undone by a
                // rebuild, and a capture that republished it since is registered as
                // it now stands, not as the rebuild's walk read it.
                var current = await sink.ReadManifestAsync(manifest.Id).ConfigureAwait(false);
                if (current is null)
                {
                    return new BackupPublishResult(BackupPublishOutcome.Skipped, null);
                }

                await catalog.RegisterAsync(current).ConfigureAwait(false);
                return new BackupPublishResult(BackupPublishOutcome.Published, current);
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown publish mode.");
        }
    }

    /// <summary>
    /// Removes a backup as <paramref name="kind"/> directs. The caller holds the id.
    /// </summary>
    /// <param name="backupId">The backup to remove.</param>
    /// <param name="kind">An operator delete or a scrub of an orphan row.</param>
    /// <returns>Whether anything was removed.</returns>
    public async Task<BackupRetireOutcome> RetireAsync(string backupId, BackupRetireKind kind)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        if (kind == BackupRetireKind.OrphanRow)
        {
            if (await catalog.GetAsync(backupId).ConfigureAwait(false) is null)
            {
                return BackupRetireOutcome.Unchanged;
            }

            // Re-probed under the hold: a capture that republished the id since the
            // scrub's read pass made the row resolvable again, and it stays.
            var resolution = await sink.ProbeAsync(backupId).ConfigureAwait(false);
            if (resolution.IsResolvable)
            {
                return BackupRetireOutcome.Unchanged;
            }

            return await catalog.RemoveAsync(backupId).ConfigureAwait(false)
                ? BackupRetireOutcome.Removed
                : BackupRetireOutcome.Unchanged;
        }

        var reserved = await BeginDeleteAsync(backupId).ConfigureAwait(false);
        if (reserved is null)
        {
            return BackupRetireOutcome.Unchanged;
        }

        await CompleteDeleteAsync(reserved).ConfigureAwait(false);
        return BackupRetireOutcome.Removed;
    }

    /// <summary>
    /// The short first step of an operator delete, which the caller runs under the
    /// id's hold (issue #5628): it removes the catalog row and returns the manifest
    /// the rest of the delete works from. With the row already gone - a delete
    /// interrupted after this step (issue #5334) - it returns the sink manifest,
    /// so a retry finishes the delete. Returns <see langword="null"/> when neither
    /// remains: there is nothing left to delete.
    /// </summary>
    /// <param name="backupId">The backup to delete.</param>
    /// <returns>The backup's manifest, or <see langword="null"/>.</returns>
    public async Task<BackupManifest?> BeginDeleteAsync(string backupId)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        var row = await catalog.GetAsync(backupId).ConfigureAwait(false);
        if (row is null)
        {
            return await sink.ReadManifestAsync(backupId).ConfigureAwait(false);
        }

        await catalog.RemoveAsync(backupId).ConfigureAwait(false);
        return row;
    }

    /// <summary>
    /// The rest of an operator delete, run outside any grain call so a large
    /// backup or catalog cannot outlast the grain response timeout (issue #5628):
    /// it deletes the backup's unshared artifacts and then its sink manifest. The
    /// sink manifest goes last, so an interrupted delete leaves it behind and a
    /// retry finds it (<see cref="BeginDeleteAsync"/>) and finishes. Idempotent.
    /// </summary>
    /// <param name="manifest">The manifest <see cref="BeginDeleteAsync"/> returned.</param>
    public async Task CompleteDeleteAsync(BackupManifest manifest)
    {
        ArgumentNullException.ThrowIfNull(manifest);

        // Collect every artifact still referenced by another catalogued manifest so
        // a shared artifact is never deleted.
        var stillReferenced = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var other in catalog.ListAsync().ConfigureAwait(false))
        {
            if (string.Equals(other.Id, manifest.Id, StringComparison.Ordinal))
            {
                continue;
            }

            foreach (var descriptor in other.ContentDescriptors)
            {
                stillReferenced.Add(descriptor.ArtifactId);
            }
        }

        foreach (var descriptor in manifest.ContentDescriptors)
        {
            if (!stillReferenced.Contains(descriptor.ArtifactId))
            {
                await sink.DeleteArtifactAsync(descriptor.ArtifactId).ConfigureAwait(false);
            }
        }

        await sink.DeleteManifestAsync(manifest.Id).ConfigureAwait(false);
    }

    private async Task<BackupPublishResult> PublishIncrementAsync(BackupManifest manifest)
    {
        var existing = await catalog.GetAsync(manifest.Id).ConfigureAwait(false);
        if (existing is not null || await sink.ManifestExistsAsync(manifest.Id).ConfigureAwait(false))
        {
            // The id already has a sink manifest: an identical delta on the same
            // base republishes it in place. Its base is checked first, and a base
            // whose row is gone abandons the republish without touching the
            // existing manifest; otherwise that manifest already makes any pass
            // that dooms the base keep it.
            var baseRow = await ReadBaseRowAsync(manifest).ConfigureAwait(false);
            if (!BackupRetentionPlanner.MayPublishIncrement(manifest, baseRow))
            {
                return new BackupPublishResult(BackupPublishOutcome.Abandoned, null);
            }

            var published = BackupManifestRegistration.Reconcile(existing, manifest);
            await sink.WriteManifestAsync(published).ConfigureAwait(false);
            await catalog.RegisterAsync(published).ConfigureAwait(false);
            return new BackupPublishResult(BackupPublishOutcome.Published, published);
        }

        // A new id goes through the fence against a retention pass (issue #5357).
        // The base was read once, before the drain, and a pass may have begun to
        // prune it since. A pass removes a doomed base's catalog row before it
        // scans the sink for increments built on it, so with this manifest already
        // in the sink either the row is still here - and a pass that removes it
        // later finds this manifest and keeps the base - or a pass is pruning the
        // base and this increment must not be published over it, so its manifest
        // is withdrawn.
        await sink.WriteManifestAsync(manifest).ConfigureAwait(false);
        var row = await ReadBaseRowAsync(manifest).ConfigureAwait(false);
        if (BackupRetentionPlanner.MayPublishIncrement(manifest, row))
        {
            await catalog.RegisterAsync(manifest).ConfigureAwait(false);
            return new BackupPublishResult(BackupPublishOutcome.Published, manifest);
        }

        await sink.DeleteManifestAsync(manifest.Id).ConfigureAwait(false);
        return new BackupPublishResult(BackupPublishOutcome.Abandoned, null);
    }

    private Task<BackupManifest?> ReadBaseRowAsync(BackupManifest increment) =>
        increment.BaseBackupId is { } baseId
            ? catalog.GetAsync(baseId)
            : Task.FromResult<BackupManifest?>(null);

    private static string ContentHashOf(BackupManifest manifest) =>
        manifest.ContentDescriptors.Count == 1
            ? manifest.ContentDescriptors[0].ContentHash
            : throw new ArgumentException(
                "A full backup's manifest carries exactly one content descriptor.", nameof(manifest));
}
