namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>What a staged backup operation does.</summary>
internal enum BackupOperationKind
{
    /// <summary>Capture a full backup of one scope.</summary>
    FullCapture,

    /// <summary>Capture an incremental backup layered on a base.</summary>
    IncrementalCapture,

    /// <summary>Capture a backup set: one full backup per tree under one set manifest.</summary>
    SetCapture,

    /// <summary>Restore a catalogued backup.</summary>
    Restore,

    /// <summary>Restore from the backup store alone, into a cluster that may have lost its catalogue.</summary>
    ColdRestore,

    /// <summary>Revert a point-in-time restore.</summary>
    RevertRestore,

    /// <summary>Rebuild the catalogue from the backup store.</summary>
    RebuildCatalogue,

    /// <summary>Check the catalogue against the backup store, optionally removing orphan rows.</summary>
    ScrubCatalogue,
}
