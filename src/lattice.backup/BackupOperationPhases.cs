namespace Orleans.Lattice.Backup;

/// <summary>
/// The phase names a backup or restore operation reports. The engine reports only
/// the phases that apply to the work in hand, so a restore that takes the merge
/// path never enters <see cref="Replaying"/>, for example.
/// </summary>
public static class BackupOperationPhases
{
    /// <summary>Streaming a scope's entries to the sink. Units: <see cref="BackupOperationUnits.Entries"/>.</summary>
    public const string Capturing = "Capturing";

    /// <summary>Capturing the members of a backup set. Units: <see cref="BackupOperationUnits.Members"/>.</summary>
    public const string CapturingMembers = "CapturingMembers";

    /// <summary>Writing the manifest to the sink and registering it in the catalog.</summary>
    public const string Cataloguing = "Cataloguing";

    /// <summary>Bootstrapping the reserved system trees ahead of a cold restore.</summary>
    public const string Bootstrapping = "Bootstrapping";

    /// <summary>Validating every manifest in a restore chain against its artifacts. Units: <see cref="BackupOperationUnits.Manifests"/>.</summary>
    public const string Validating = "Validating";

    /// <summary>Reading a restore chain's entries and applying them. Units: <see cref="BackupOperationUnits.Entries"/>.</summary>
    public const string Applying = "Applying";

    /// <summary>Bulk-loading the read entries into the target's physical shards. Units: <see cref="BackupOperationUnits.Shards"/>.</summary>
    public const string Replaying = "Replaying";
}
