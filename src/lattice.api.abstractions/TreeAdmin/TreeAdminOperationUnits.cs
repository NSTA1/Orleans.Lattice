using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The unit names a tree-administration operation reports in
/// <see cref="Operations.LatticeOperationStatus.UnitName"/>.
/// </summary>
public static class TreeAdminOperationUnits
{
    /// <summary>Source keys scanned or projected by a view rebuild or reconcile.</summary>
    public const string Keys = LatticeMaintenanceProgress.Keys;

    /// <summary>Covered trees probed or repaired by a tag-index reconcile.</summary>
    public const string Trees = LatticeMaintenanceProgress.Trees;

    /// <summary>WAL tail entries copied by a partition move.</summary>
    public const string Entries = LatticeMaintenanceProgress.Entries;

    /// <summary>Physical shards walked by an orphaned-leaf audit or repair.</summary>
    public const string Shards = LatticeMaintenanceProgress.Shards;
}
