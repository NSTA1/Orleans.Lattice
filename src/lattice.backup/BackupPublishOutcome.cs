namespace Orleans.Lattice.Backup;

/// <summary>The outcome of a <see cref="BackupPublishRequest"/>.</summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupPublishOutcome)]
internal enum BackupPublishOutcome
{
    /// <summary>The manifest was published (and registered when requested).</summary>
    Published = 0,

    /// <summary>An increment whose base's row is gone was abandoned; nothing of it remains published.</summary>
    Abandoned = 1,

    /// <summary>A rebuild found the manifest no longer in the sink, and registered nothing.</summary>
    Skipped = 2,

    /// <summary>
    /// The id is held - a retention pass or a delete is retiring it, or another
    /// publish of it is running - beyond one call's wait. Nothing was written;
    /// the caller retries.
    /// </summary>
    Busy = 3,
}
