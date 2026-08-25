namespace Orleans.Lattice;

/// <summary>
/// The per-tenant admission-control seam consulted before a tenant-scoped
/// operation on a tree is admitted (for example a tenant-local tree creation or
/// write). A registered controller can admit or refuse the operation. The
/// interface lives in core so a later tenant-aware choke point can consult it
/// without depending on the tenancy add-on; the core library ships only the
/// <see cref="NullTenantAdmissionController"/> (which admits everything and
/// reports itself inactive, so the choke point pays nothing when tenancy is
/// off), and the real, quota- or policy-evaluating implementation is contributed
/// by the tenancy package.
/// </summary>
public interface ITenantAdmissionController
{
    /// <summary>
    /// Returns <c>true</c> when this controller wants to evaluate admissions.
    /// Gates the seam so a cluster with no tenancy add-on never calls
    /// <see cref="IsAdmittedAsync"/>. The result is expected to be cheap and
    /// stable so a choke point can cache it. The null default is always
    /// <c>false</c>.
    /// </summary>
    bool IsActive { get; }

    /// <summary>
    /// Decides whether a tenant-scoped operation on <paramref name="treeId"/> is
    /// admitted for <paramref name="tenant"/>. Invoked only when
    /// <see cref="IsActive"/> is <c>true</c>.
    /// </summary>
    /// <param name="tenant">The tenant the operation runs under.</param>
    /// <param name="treeId">The fully-qualified tree id the operation targets. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the admission decision.</param>
    /// <returns><c>true</c> when the operation is admitted; <c>false</c> when it is refused.</returns>
    ValueTask<bool> IsAdmittedAsync(TenantId tenant, string treeId, CancellationToken cancellationToken = default);
}
