namespace Orleans.Lattice.Backup;

/// <summary>
/// What started a scheduled backup-set run (<see cref="BackupSetScheduleRun"/>).
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupSetScheduleTrigger)]
public enum BackupSetScheduleTrigger
{
    /// <summary>The schedule's reminder fired.</summary>
    Schedule = 0,

    /// <summary>A caller asked for a run now (<c>RunBackupSetScheduleNowAsync</c>).</summary>
    Manual = 1,
}
