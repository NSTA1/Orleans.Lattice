using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// What the caller may do to one tree, from the facade's side-effect-free
/// capability probe. A denied probe is an all-deny answer, so every verb stays
/// hidden: the probe fails closed, and the cluster still authorizes every real
/// operation on attempt.
/// </summary>
internal static class ClusterTreeAccess
{
    /// <summary>The all-deny answer for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The tree.</param>
    /// <returns>Capabilities with every flag false.</returns>
    public static LatticeTreeAdminCapabilities None(string treeId) => new()
    {
        TreeId = treeId,
        Schema = new LatticeSchemaCapabilities { TreeId = treeId },
    };

    /// <summary>Probes <paramref name="treeId"/>, answering all-deny on a denial.</summary>
    /// <param name="admin">The tree administration facade.</param>
    /// <param name="treeId">The tree.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns>The caller's capabilities over the tree.</returns>
    public static async Task<LatticeTreeAdminCapabilities> ProbeAsync(ILatticeTreeAdmin admin, string treeId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(admin);

        try
        {
            return await admin.ProbeCapabilitiesAsync(treeId, cancellationToken).ConfigureAwait(false)
                ?? None(treeId);
        }
        catch (Exception exception) when (ClusterFaults.IsDenied(exception))
        {
            return None(treeId);
        }
    }

    /// <summary>Whether the caller may do anything at all to the tree.</summary>
    /// <param name="capabilities">The probe's answer.</param>
    /// <returns><see langword="true"/> when any tree-administration flag is set.</returns>
    public static bool Any(LatticeTreeAdminCapabilities capabilities) =>
        capabilities.CanViewDiagnostics
        || capabilities.CanAdministerTree
        || capabilities.CanManageTreeLifecycle
        || capabilities.CanBulkLoad;
}
