namespace Orleans.Lattice.Backup;

/// <summary>
/// The unit names a tracked backup operation reports as the unit name: what the completed and total unit counts measure in the current phase.
/// </summary>
public static class BackupOperationUnits
{
    /// <summary>Key-value entries read or written.</summary>
    public const string Entries = "entries";

    /// <summary>Physical shards loaded.</summary>
    public const string Shards = "shards";

    /// <summary>Backup-set members captured.</summary>
    public const string Members = "members";

    /// <summary>Manifests validated.</summary>
    public const string Manifests = "manifests";
}
