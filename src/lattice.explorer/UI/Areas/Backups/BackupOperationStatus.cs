namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>Where a staged backup operation is.</summary>
internal enum BackupOperationStatus
{
    /// <summary>A stage is under way.</summary>
    Running,

    /// <summary>Every stage completed.</summary>
    Succeeded,

    /// <summary>A stage failed; the operation stopped there.</summary>
    Failed,

    /// <summary>The circuit ended before the operation finished.</summary>
    Cancelled,
}
