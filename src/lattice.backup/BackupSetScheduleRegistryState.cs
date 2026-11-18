namespace Orleans.Lattice.Backup;

/// <summary>
/// Persistent state of the <see cref="IBackupSetScheduleRegistryGrain"/>: each
/// registered schedule's effective name and the ids of the sets it retains,
/// keyed by the schedule's grain key.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupSetScheduleRegistryState)]
internal sealed class BackupSetScheduleRegistryState
{
    /// <summary>Each registered schedule's effective name, keyed by its grain key.</summary>
    [Id(0)]
    public Dictionary<string, string> Names { get; set; } = new(StringComparer.Ordinal);

    /// <summary>The ids of the sets each schedule retains, keyed by its grain key.</summary>
    [Id(1)]
    public Dictionary<string, List<string>> RetainedSetIds { get; set; } = new(StringComparer.Ordinal);
}
