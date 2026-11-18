namespace Orleans.Lattice.Backup;

/// <summary>
/// How a scheduled backup set (<see cref="LatticeBackupSetScheduleRequest"/>)
/// chooses the trees it captures.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupSetMembershipMode)]
public enum BackupSetMembershipMode
{
    /// <summary>
    /// The set captures exactly the scopes the schedule names, one per distinct
    /// tree (<see cref="LatticeBackupSetScheduleRequest.Scopes"/>).
    /// </summary>
    Explicit = 0,

    /// <summary>
    /// The set captures every user tree visible in the registering caller's
    /// tenant, whole, less any tree under
    /// <see cref="LatticeBackupSetScheduleRequest.ExcludedTreePrefixes"/>. System,
    /// materialised-view, tag-index and system-data trees are never included.
    /// <see cref="LatticeBackupSetScheduleRequest.IncludeNewTrees"/> decides
    /// whether the membership is re-resolved at every run or pinned when the
    /// schedule is registered.
    /// </summary>
    AllTrees = 1,
}
