namespace Orleans.Lattice.Backup;

/// <summary>
/// The cluster-wide index of backup-set schedules (issue #6067): every
/// registered schedule's grain key and effective name, and the ids of the sets
/// each one retains, so the schedules can be listed and a scheduled set's id can
/// be resolved to the schedule that recorded its exact membership. A single
/// activation keyed <see cref="SingletonKey"/>. Holds only keys and ids - never
/// a set's membership, which stays on the schedule's own grain - so it stays
/// small however large the estate.
/// </summary>
[Alias(BackupTypeAliases.IBackupSetScheduleRegistryGrain)]
internal interface IBackupSetScheduleRegistryGrain : IGrainWithStringKey
{
    /// <summary>The registry's only grain key.</summary>
    const string SingletonKey = "backup-set-schedules";

    /// <summary>Adds (or keeps) a schedule in the index. Idempotent.</summary>
    /// <param name="scheduleKey">The schedule's grain key.</param>
    /// <param name="effectiveName">The schedule's effective (tenant-composed) name.</param>
    Task RegisterAsync(string scheduleKey, string effectiveName);

    /// <summary>Removes a schedule, and its retained set ids, from the index. Idempotent.</summary>
    /// <param name="scheduleKey">The schedule's grain key.</param>
    Task UnregisterAsync(string scheduleKey);

    /// <summary>Replaces the ids of the sets a schedule retains.</summary>
    /// <param name="scheduleKey">The schedule's grain key.</param>
    /// <param name="setIds">The retained set ids.</param>
    Task SetRetainedSetIdsAsync(string scheduleKey, IReadOnlyList<string> setIds);

    /// <summary>Lists every registered schedule as (grain key, effective name), in effective-name order.</summary>
    [Orleans.Concurrency.AlwaysInterleave]
    Task<IReadOnlyList<KeyValuePair<string, string>>> ListAsync();

    /// <summary>
    /// Returns the grain keys of the schedules that retain the set
    /// <paramref name="setId"/> (usually one; two schedules capturing identical
    /// content yield the same content-addressed set id), or an empty list.
    /// </summary>
    /// <param name="setId">The set id.</param>
    [Orleans.Concurrency.AlwaysInterleave]
    Task<IReadOnlyList<string>> FindSchedulesForSetAsync(string setId);
}
