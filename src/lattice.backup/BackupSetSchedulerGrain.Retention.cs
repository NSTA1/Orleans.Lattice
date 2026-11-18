using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Retention for a backup-set schedule (issue #6067): sets are kept and pruned
/// whole. The members of every retained set are held from per-scope retention on
/// the member scopes' schedulers, and a pruned set's member backup is deleted only
/// when no retained set still names it, no other schedule holds it, and no other
/// capture has since taken it over.
/// </summary>
internal sealed partial class BackupSetSchedulerGrain
{
    /// <summary>
    /// The sets of <paramref name="sets"/> (ordered newest first) the policy keeps
    /// at <paramref name="now"/>: always the newest, the newest
    /// <paramref name="keepLast"/>, and every set last captured within
    /// <paramref name="maxAge"/>.
    /// </summary>
    internal static List<ScheduledBackupSet> Retained(
        IReadOnlyList<ScheduledBackupSet> sets, int? keepLast, TimeSpan? maxAge, DateTimeOffset now)
    {
        var keep = new List<ScheduledBackupSet>(sets.Count);
        var cutoff = maxAge is { } window && window < now - DateTimeOffset.MinValue
            ? now - window
            : (DateTimeOffset?)null;
        for (var i = 0; i < sets.Count; i++)
        {
            var set = sets[i];
            if (i == 0
                || (keepLast is { } count && i < count)
                || (maxAge is not null && (cutoff is null || set.LastCapturedAtUtc >= cutoff.Value)))
            {
                keep.Add(set);
            }
        }

        return keep;
    }

    private async Task ApplyRetentionAsync(LatticeBackupSetScheduleRequest request)
    {
        var sets = state.State.Sets;
        var retained = Retained(sets, request.RetentionKeepLast, request.RetentionMaxAge, DateTimeOffset.UtcNow);
        var doomed = sets.Where(set => !retained.Contains(set)).ToList();

        // Hold what stays before deleting anything, so no member a retained set
        // names is ever left without its hold.
        await ApplyHoldsAsync(retained);
        if (doomed.Count == 0)
        {
            return;
        }

        state.State.Sets = retained;
        await state.WriteStateAsync();
        await Registry.SetRetainedSetIdsAsync(ScheduleKey, retained.Select(set => set.SetId).ToList());
        var deleted = await DeleteSetsAsync(doomed, retained);
        logger.LogInformation(
            "Backup-set schedule {Schedule}: retention pruned {Pruned} set(s) ({Deleted} backup(s) deleted) and retained {Retained}.",
            ScheduleKey, doomed.Count, deleted, retained.Count);
    }

    /// <summary>
    /// Places this schedule's retention hold on exactly the member backups of
    /// <paramref name="retained"/>, scope by scope, and lifts it from every scope
    /// it no longer touches.
    /// </summary>
    private async Task ApplyHoldsAsync(IReadOnlyList<ScheduledBackupSet> retained)
    {
        var byScopeKey = new Dictionary<string, (BackupScopeSelector Scope, HashSet<string> Ids)>(StringComparer.Ordinal);
        foreach (var member in retained.SelectMany(set => set.Members))
        {
            var key = BackupScopeKey.For(member.Scope);
            if (!byScopeKey.TryGetValue(key, out var entry))
            {
                entry = (member.Scope, new HashSet<string>(StringComparer.Ordinal));
                byScopeKey[key] = entry;
            }

            entry.Ids.Add(member.BackupId);
        }

        foreach (var (key, entry) in byScopeKey)
        {
            await grainFactory.GetGrain<ILatticeBackupSchedulerGrain>(key)
                .SetSetScheduleHoldsAsync(ScheduleKey, entry.Ids.ToList());
        }

        var lifted = state.State.HeldScopeKeys.Where(key => !byScopeKey.ContainsKey(key)).ToList();
        foreach (var key in lifted)
        {
            await grainFactory.GetGrain<ILatticeBackupSchedulerGrain>(key)
                .SetSetScheduleHoldsAsync(ScheduleKey, Array.Empty<string>());
        }

        var heldKeys = byScopeKey.Keys.Order(StringComparer.Ordinal).ToList();
        if (!heldKeys.SequenceEqual(state.State.HeldScopeKeys, StringComparer.Ordinal))
        {
            state.State.HeldScopeKeys = heldKeys;
            await state.WriteStateAsync();
        }
    }

    /// <summary>
    /// Deletes the member backups of <paramref name="doomed"/> that no set of
    /// <paramref name="retained"/> names, that this schedule still owns - the
    /// backup's catalog row still records the doomed set's capture, so no later
    /// capture of identical content has taken it over - and that no other
    /// schedule holds. Each delete runs through the backup's per-id hold on its
    /// scope's scheduler, like an operator delete. Best effort per backup: one
    /// failed delete does not stop the others, and a backup left behind is an
    /// ordinary catalogued backup that per-scope retention or an operator can
    /// remove. Returns how many backups were deleted.
    /// </summary>
    private async Task<int> DeleteSetsAsync(IReadOnlyList<ScheduledBackupSet> doomed, IReadOnlyList<ScheduledBackupSet> retained)
    {
        var keep = retained.SelectMany(set => set.Members).Select(member => member.BackupId)
            .ToHashSet(StringComparer.Ordinal);
        var visited = new HashSet<string>(StringComparer.Ordinal);
        var deleted = 0;
        foreach (var set in doomed)
        {
            foreach (var member in set.Members)
            {
                if (keep.Contains(member.BackupId) || !visited.Add(member.BackupId))
                {
                    continue;
                }

                try
                {
                    if (await TryDeleteMemberAsync(set, member))
                    {
                        deleted++;
                    }
                }
                catch (Exception ex)
                {
                    logger.LogWarning(
                        ex,
                        "Backup-set schedule {Schedule}: deleting backup {BackupId} of pruned set {SetId} failed; it stays listed.",
                        ScheduleKey, member.BackupId, set.SetId);
                }
            }
        }

        return deleted;
    }

    private async Task<bool> TryDeleteMemberAsync(ScheduledBackupSet set, ScheduledBackupSetMember member)
    {
        var row = await catalog.GetAsync(member.BackupId);
        if (row is null)
        {
            return false;
        }

        // A content-addressed backup is re-registered by every capture that
        // reproduces it, taking that capture's name and set stamp: a row that no
        // longer carries this set's capture belongs to a later one.
        var owned = string.Equals(row.SetId, set.SetId, StringComparison.OrdinalIgnoreCase)
            || (row.SetId is null && string.Equals(row.Name, set.Name, StringComparison.Ordinal));
        if (!owned)
        {
            return false;
        }

        var scopeGrain = grainFactory.GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(row.Scope));
        if (await scopeGrain.IsHeldBySetScheduleAsync(member.BackupId, ScheduleKey))
        {
            return false;
        }

        if (!await Gate.RetireAsync(row, BackupRetireKind.Delete, CancellationToken.None))
        {
            return false;
        }

        if (healthStore is not null)
        {
            await healthStore.RemoveAsync(member.BackupId);
        }

        return true;
    }
}
