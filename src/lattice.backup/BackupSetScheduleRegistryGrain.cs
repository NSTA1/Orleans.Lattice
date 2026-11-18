using Orleans.Runtime;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="IBackupSetScheduleRegistryGrain"/>. Every mutation is
/// rolled back in memory when its write fails, so a retry is never
/// short-circuited by a value storage does not hold (cf. #3471).
/// </summary>
internal sealed class BackupSetScheduleRegistryGrain(
    [PersistentState("backup-set-schedule-registry", LatticeOptions.StorageProviderName)]
    IPersistentState<BackupSetScheduleRegistryState> state)
    : Grain, IBackupSetScheduleRegistryGrain
{
    /// <inheritdoc />
    public async Task RegisterAsync(string scheduleKey, string effectiveName)
    {
        ArgumentException.ThrowIfNullOrEmpty(scheduleKey);
        ArgumentException.ThrowIfNullOrEmpty(effectiveName);
        var names = state.State.Names;
        if (names.TryGetValue(scheduleKey, out var existing)
            && string.Equals(existing, effectiveName, StringComparison.Ordinal))
        {
            return;
        }

        names[scheduleKey] = effectiveName;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            if (existing is null)
            {
                names.Remove(scheduleKey);
            }
            else
            {
                names[scheduleKey] = existing;
            }

            throw;
        }
    }

    /// <inheritdoc />
    public async Task UnregisterAsync(string scheduleKey)
    {
        ArgumentException.ThrowIfNullOrEmpty(scheduleKey);
        var hadName = state.State.Names.Remove(scheduleKey, out var name);
        var hadSets = state.State.RetainedSetIds.Remove(scheduleKey, out var sets);
        if (!hadName && !hadSets)
        {
            return;
        }

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            if (hadName)
            {
                state.State.Names[scheduleKey] = name!;
            }

            if (hadSets)
            {
                state.State.RetainedSetIds[scheduleKey] = sets!;
            }

            throw;
        }
    }

    /// <inheritdoc />
    public async Task SetRetainedSetIdsAsync(string scheduleKey, IReadOnlyList<string> setIds)
    {
        ArgumentException.ThrowIfNullOrEmpty(scheduleKey);
        ArgumentNullException.ThrowIfNull(setIds);
        var all = state.State.RetainedSetIds;
        all.TryGetValue(scheduleKey, out var previous);
        var next = setIds.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList();
        if (previous is not null && previous.SequenceEqual(next, StringComparer.Ordinal))
        {
            return;
        }

        all[scheduleKey] = next;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            if (previous is null)
            {
                all.Remove(scheduleKey);
            }
            else
            {
                all[scheduleKey] = previous;
            }

            throw;
        }
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<KeyValuePair<string, string>>> ListAsync()
    {
        IReadOnlyList<KeyValuePair<string, string>> list = state.State.Names
            .OrderBy(entry => entry.Value, StringComparer.Ordinal)
            .ToList();
        return Task.FromResult(list);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<string>> FindSchedulesForSetAsync(string setId)
    {
        ArgumentException.ThrowIfNullOrEmpty(setId);
        var keys = new List<string>();
        foreach (var (scheduleKey, setIds) in state.State.RetainedSetIds)
        {
            if (setIds.BinarySearch(setId, StringComparer.Ordinal) >= 0)
            {
                keys.Add(scheduleKey);
            }
        }

        keys.Sort(StringComparer.Ordinal);
        return Task.FromResult<IReadOnlyList<string>>(keys);
    }
}
