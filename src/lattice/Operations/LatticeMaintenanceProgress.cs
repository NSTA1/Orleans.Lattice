namespace Orleans.Lattice.Operations;

/// <summary>
/// The phase and unit names the core engines report while a tree-maintenance
/// operation runs: a materialised-view rebuild or reconcile, a tag-index reconcile
/// sweep, a WAL partition move and an orphaned-leaf pass. The tree-administration
/// facade re-exports these as its public phase and unit constants, so the engine
/// and the contract can never disagree on a name.
/// </summary>
internal static class LatticeMaintenanceProgress
{
    /// <summary>
    /// The response timeout of a tracked grain call, in <see cref="TimeSpan.Parse(string)"/>
    /// syntax. Generous on purpose: a tracked call does one long piece of work whose
    /// duration scales with the data, and the operation's own heartbeat lease, not this
    /// deadline, is what detects a lost silo.
    /// </summary>
    internal const string TrackedCallResponseTimeout = "1.00:00:00";

    /// <summary>Reading the source tree's key set. Units: <see cref="Keys"/>, total unknown.</summary>
    internal const string Scanning = "Scanning";

    /// <summary>Projecting each source key into the view. Units: <see cref="Keys"/> of the scanned key count.</summary>
    internal const string Projecting = "Projecting";

    /// <summary>Digesting the live view before a reconcile. No units.</summary>
    internal const string Digesting = "Digesting";

    /// <summary>Comparing the rebuilt view against the live view. No units.</summary>
    internal const string Comparing = "Comparing";

    /// <summary>Flipping the view's active generation, or rewriting it in place. No units.</summary>
    internal const string Swapping = "Swapping";

    /// <summary>Probing each covered tree's digest fingerprint. Units: <see cref="Trees"/> of the covered count.</summary>
    internal const string Probing = "Probing";

    /// <summary>Repairing each divergent covered tree. Units: <see cref="Trees"/> of the divergent count.</summary>
    internal const string Repairing = "Repairing";

    /// <summary>Copying the source WAL tail to the target. Units: <see cref="Entries"/> of the live tail.</summary>
    internal const string Copying = "Copying";

    /// <summary>Verifying the target tail before the cutover. No units.</summary>
    internal const string Verifying = "Verifying";

    /// <summary>Flipping the WAL placement pin. No units.</summary>
    internal const string Flipping = "Flipping";

    /// <summary>Walking every physical shard's leaf chain. Units: <see cref="Shards"/> of the physical shard count.</summary>
    internal const string Walking = "Walking";

    /// <summary>The unit of a view scan or projection.</summary>
    internal const string Keys = "keys";

    /// <summary>The unit of a tag-index probe or repair.</summary>
    internal const string Trees = "trees";

    /// <summary>The unit of a WAL tail copy.</summary>
    internal const string Entries = "entries";

    /// <summary>The unit of an orphaned-leaf pass.</summary>
    internal const string Shards = "shards";
}
