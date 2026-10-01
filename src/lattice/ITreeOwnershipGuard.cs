namespace Orleans.Lattice;

/// <summary>
/// Optional control-plane seam that bounds alias changes by tree ownership.
/// The registry consults it for every alias assignment, including system-origin
/// maintenance, after namespace and target-control authorization.
/// Hosts without an ownership provider use an allow-all implementation.
/// </summary>
public interface ITreeOwnershipGuard
{
    /// <summary>
    /// Decides whether a logical tree may alias the specified physical tree.
    /// Implementations must use authoritative ownership state and return an
    /// explicit allow or denial. Failures propagate without writing the alias.
    /// </summary>
    /// <param name="logicalTreeId">The effective, tenant-composed logical tree id.</param>
    /// <param name="physicalTreeId">The effective, tenant-composed physical target id.</param>
    /// <param name="derivedFrom">The logical id recorded on the physical target at creation,
    /// or <see langword="null"/> for an independent or legacy tree. Read by the registry,
    /// never supplied by the alias caller.</param>
    /// <param name="cancellationToken">Cancels the ownership lookup.</param>
    /// <returns>An explicit ownership decision. A default decision denies.</returns>
    /// <remarks>
    /// This is an in-process service, not a grain. A synchronous decision should
    /// complete without allocation. The registry holds its mutation turn while
    /// awaiting the decision; implementations must not call registry mutations
    /// or range scans from this method, and must not read a tree that is not
    /// registered, because the read registers it - a registry mutation - and
    /// deadlocks behind the turn awaiting the decision. Check the tree's
    /// existence first and treat an unregistered tree as empty.
    /// Denial reasons must be safe to expose to the caller through API transports.
    /// </remarks>
    ValueTask<TreeOwnershipDecision> AuthorizeAliasAsync(
        string logicalTreeId,
        string physicalTreeId,
        string? derivedFrom,
        CancellationToken cancellationToken = default);
}
