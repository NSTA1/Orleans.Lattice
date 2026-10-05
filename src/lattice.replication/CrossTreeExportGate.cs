using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The cross-tree preconditions on serving a snapshot export to a peer (issue
/// #4684). While any silo of this cluster predates the cross-tree decision
/// purge hold, that silo can purge a cross-tree sub-saga's decision with no
/// regard for the peers of its sibling trees, so an export could carry a
/// participant as bare committed rows that a receiver's barrier never learns
/// of. Every export is therefore deferred - a transient refusal the receiver
/// retries - until every silo honours the hold. The same check keeps
/// <see cref="ReplicationCrossTreeDecisionHold"/> from releasing anything
/// meanwhile.
/// </summary>
internal sealed class CrossTreeExportGate(IServiceProvider services)
{
    /// <summary>Test seam: overrides the cluster-manifest check.</summary>
    internal Func<bool>? AllSilosHonourOverrideForTesting { get; set; }

    /// <summary>
    /// Throws <see cref="LatticeSnapshotExportDeferredException"/> while the
    /// export of <paramref name="treeName"/> must wait.
    /// </summary>
    public void EnsureMayExport(string treeName)
    {
        if (!AllSilosHonourCrossTreeHold())
        {
            ThrowDeferred(treeName);
        }
    }

    /// <summary>
    /// Whether every silo of this cluster honours the cross-tree decision purge
    /// hold. The hold itself releases nothing until it does.
    /// </summary>
    public bool AllSilosHonourCrossTreeHold() =>
        AllSilosHonourOverrideForTesting?.Invoke() ?? PurgeHoldSupport.AllSilosHonourCrossTreeHold(services);

    [DoesNotReturn]
    private static void ThrowDeferred(string treeName) =>
        throw new LatticeSnapshotExportDeferredException(
            $"Snapshot export of tree '{treeName}' is deferred: a silo of this cluster predates the cross-tree "
            + "decision purge hold, so an export could omit a cross-tree sub-saga's decision. Retry once every "
            + "silo is upgraded.");
}
