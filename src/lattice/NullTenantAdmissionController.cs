namespace Orleans.Lattice;

/// <summary>
/// The core no-op <see cref="ITenantAdmissionController"/>: admits every
/// operation and reports itself inactive. Registered by <c>AddLattice</c> as the
/// safe default so a consumer of the seam always resolves an instance even when
/// the tenancy add-on is not registered. Because <see cref="IsActive"/> is
/// always <c>false</c>, a tenant-aware choke point caches the inactive flag and
/// never calls <see cref="IsAdmittedAsync"/>, so an unregistered controller adds
/// no per-operation cost. The tenancy package replaces it with a real,
/// quota- or policy-evaluating controller.
/// </summary>
internal sealed class NullTenantAdmissionController : ITenantAdmissionController
{
    private static readonly ValueTask<bool> AdmittedResult = new(true);

    /// <inheritdoc />
    public bool IsActive => false;

    /// <inheritdoc />
    public ValueTask<bool> IsAdmittedAsync(
        TenantId tenant,
        string treeId,
        CancellationToken cancellationToken = default) =>
        AdmittedResult;
}
