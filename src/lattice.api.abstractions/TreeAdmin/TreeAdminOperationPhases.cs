using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The phase names a tree-administration operation reports in
/// <see cref="Operations.LatticeOperationStatus.Phase"/>. A kind reports only the
/// phases that apply to it (see <see cref="TreeAdminOperationKinds"/>), so the
/// status's phase index can skip. The values are the engine's own phase names.
/// </summary>
public static class TreeAdminOperationPhases
{
    /// <summary>Reading the view source tree's key set. Units: <see cref="TreeAdminOperationUnits.Keys"/>, total unknown.</summary>
    public const string Scanning = LatticeMaintenanceProgress.Scanning;

    /// <summary>Projecting each source key into the view. Units: <see cref="TreeAdminOperationUnits.Keys"/> of the scanned keys.</summary>
    public const string Projecting = LatticeMaintenanceProgress.Projecting;

    /// <summary>Digesting the live view before a reconcile. No units.</summary>
    public const string Digesting = LatticeMaintenanceProgress.Digesting;

    /// <summary>Comparing the rebuilt view against the live view. No units.</summary>
    public const string Comparing = LatticeMaintenanceProgress.Comparing;

    /// <summary>Flipping the view's active generation. No units.</summary>
    public const string Swapping = LatticeMaintenanceProgress.Swapping;

    /// <summary>Probing each covered tree's digest fingerprint. Units: <see cref="TreeAdminOperationUnits.Trees"/> of the covered trees.</summary>
    public const string Probing = LatticeMaintenanceProgress.Probing;

    /// <summary>Repairing each divergent covered tree. Units: <see cref="TreeAdminOperationUnits.Trees"/> of the divergent trees.</summary>
    public const string Repairing = LatticeMaintenanceProgress.Repairing;

    /// <summary>Copying the source WAL tail to the target. Units: <see cref="TreeAdminOperationUnits.Entries"/> of the live tail.</summary>
    public const string Copying = LatticeMaintenanceProgress.Copying;

    /// <summary>Verifying the target tail before the cutover. No units.</summary>
    public const string Verifying = LatticeMaintenanceProgress.Verifying;

    /// <summary>About to flip the WAL placement pin; the last point a cancel takes effect. No units.</summary>
    public const string Flipping = LatticeMaintenanceProgress.Flipping;

    /// <summary>Walking each physical shard's leaf chain. Units: <see cref="TreeAdminOperationUnits.Shards"/> of the physical shards.</summary>
    public const string Walking = LatticeMaintenanceProgress.Walking;
}
