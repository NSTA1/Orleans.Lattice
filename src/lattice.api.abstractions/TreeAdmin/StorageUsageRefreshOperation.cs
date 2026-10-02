namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The tracked long-running operation that re-measures every tree's storage usage
/// in the background (<see cref="ILatticeStorageUsageOperations.StartStorageUsageRefreshAsync"/>):
/// its kind, its phase and its unit. The result keys are on
/// <see cref="StorageUsageRefreshResults"/>.
/// </summary>
public static class StorageUsageRefreshOperation
{
    /// <summary>The operation kind. It records no result reference.</summary>
    public const string Kind = "treeadmin.storage-usage-refresh";

    /// <summary>The only phase: re-measuring every registered tree.</summary>
    public const string MeasuringPhase = "Measuring";

    /// <summary>The unit <see cref="MeasuringPhase"/> counts.</summary>
    public const string TreesUnit = "trees";
}
