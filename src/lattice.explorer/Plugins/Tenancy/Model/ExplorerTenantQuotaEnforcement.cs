namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// The scope a quota reading was taken and is enforced under. A per-cluster
/// reading is genuinely not a global total, so the two must stay
/// distinguishable all the way to the panel that captions the figure.
/// </summary>
public enum ExplorerTenantQuotaEnforcement
{
    /// <summary>
    /// The figures are a converged cross-cluster total, so the reading is the
    /// tenant's whole consumption wherever it runs.
    /// </summary>
    GlobalConverged = 0,

    /// <summary>
    /// The figures are this cluster's local view only. The tenant may be
    /// consuming more elsewhere, so a panel must caption the reading rather
    /// than present it as a global total.
    /// </summary>
    PerCluster = 1,
}
