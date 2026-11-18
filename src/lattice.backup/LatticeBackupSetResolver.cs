namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="ILatticeBackupSetResolver"/>. A set captured by a
/// backup-set schedule resolves to the exact membership the schedule recorded when
/// it captured the set (issue #6067); any other set id is expanded by scanning the
/// backup catalog for the member manifests stamped with that
/// <see cref="BackupManifest.SetId"/> (every set member is catalogued as an
/// ordinary manifest carrying the owning set's id). The recorded membership comes
/// first because the stamp is not durable for a content-addressed member: a later
/// set that captures an unchanged tree reproduces the same backup id and moves its
/// stamp, so a stamp scan of the earlier set would miss that tree. Members are
/// returned in tree-id order so a caller fences and commits them in a
/// deterministic order across every cluster.
/// </summary>
internal sealed class LatticeBackupSetResolver(ILatticeBackupCatalogStore catalog, IGrainFactory? grainFactory = null)
    : ILatticeBackupSetResolver
{
    /// <inheritdoc />
    public async Task<IReadOnlyList<BackupSetMember>> ResolveMembersAsync(
        string setId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(setId);

        if (grainFactory is not null)
        {
            var scheduled = await ResolveScheduledAsync(grainFactory, setId).ConfigureAwait(false);
            if (scheduled is not null)
            {
                return scheduled;
            }
        }

        var members = new List<BackupSetMember>();
        await foreach (var manifest in catalog.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            if (string.Equals(manifest.SetId, setId, StringComparison.Ordinal))
            {
                members.Add(new BackupSetMember(manifest.Id, manifest.Scope.TreeId));
            }
        }

        members.Sort(static (left, right) =>
            string.CompareOrdinal(left.TreeId, right.TreeId));
        return members;
    }

    private static async Task<IReadOnlyList<BackupSetMember>?> ResolveScheduledAsync(IGrainFactory grainFactory, string setId)
    {
        var registry = grainFactory.GetGrain<IBackupSetScheduleRegistryGrain>(IBackupSetScheduleRegistryGrain.SingletonKey);
        foreach (var scheduleKey in await registry.FindSchedulesForSetAsync(setId).ConfigureAwait(false))
        {
            var set = await grainFactory.GetGrain<ILatticeBackupSetSchedulerGrain>(scheduleKey)
                .GetSetAsync(setId).ConfigureAwait(false);
            if (set is null)
            {
                continue;
            }

            var members = new List<BackupSetMember>(set.Members.Count);
            foreach (var member in set.Members)
            {
                members.Add(new BackupSetMember(member.BackupId, member.TreeId));
            }

            members.Sort(static (left, right) => string.CompareOrdinal(left.TreeId, right.TreeId));
            return members;
        }

        return null;
    }
}
