namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Internal per-tree probe that names the durable WAL materialiser pin holding a
/// tree's WAL floor, the leaf behind it, and that leaf's persisted checkpoint
/// (issue #4195). One activation per tree, keyed by the physical <c>treeId</c>.
/// Read-only: it reads the pin store and one leaf's durable state, and never
/// activates a leaf. Not part of the public API - callers use the tree-admin
/// facade's WAL reclamation read.
/// </summary>
[Alias(TypeAliases.ILatticeWalFloorHolderProbe)]
internal interface ILatticeWalFloorHolderProbe : IGrainWithStringKey
{
    /// <summary>
    /// Probes the tree's durable pins and classifies the one holding its WAL floor.
    /// </summary>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns>The probe report.</returns>
    Task<WalFloorHolderProbeReport> ProbeAsync(CancellationToken cancellationToken);
}
