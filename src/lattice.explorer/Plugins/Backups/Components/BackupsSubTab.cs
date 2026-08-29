namespace Orleans.Lattice.Explorer.Backup.Components;

/// <summary>
/// The Backups area's two sub-tabs: composing a new capture, and browsing the
/// existing catalogue. The active one is retained in the plugin's own
/// preference namespace, so the area reopens where it was left.
/// </summary>
internal enum BackupsSubTab
{
    /// <summary>Compose and trigger a new backup.</summary>
    New = 0,

    /// <summary>Browse, restore, schedule, and delete existing backups.</summary>
    Existing = 1,
}
