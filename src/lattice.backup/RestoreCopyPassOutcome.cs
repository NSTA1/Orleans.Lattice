namespace Orleans.Lattice.Backup;

/// <summary>What one restore-copy retirement pass did (issue #6103).</summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.RestoreCopyPassOutcome)]
internal enum RestoreCopyPassOutcome
{
    /// <summary>Nothing is scheduled; the pass did nothing.</summary>
    Idle = 0,

    /// <summary>The pass retired one copy; another pass follows.</summary>
    Retired = 1,

    /// <summary>Nothing is retirable now; a later pass re-checks (a revert window is open, or a hold applies).</summary>
    Waiting = 2,

    /// <summary>Nothing is left to retire; the reminder is stopped.</summary>
    Completed = 3,

    /// <summary>A failure, or another alias operation, deferred the pass; it is retried with back-off.</summary>
    Deferred = 4,
}
