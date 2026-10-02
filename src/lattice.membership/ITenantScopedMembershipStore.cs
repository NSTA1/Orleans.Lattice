namespace Orleans.Lattice.Membership;

/// <summary>
/// The tenant-scoped operations of the membership directory that the tenant tier
/// needs and the released <see cref="ILatticeMembershipDirectory"/> does not
/// carry: per-tenant counts for the tenant quota caps, an ordered, paged listing
/// of one tenant's groups, a cascading group removal, and the tenant purge run by
/// tenant deletion. Implemented by the default <c>LatticeMembershipDirectory</c>.
/// </summary>
/// <remarks>
/// <para>
/// Internal, exposed through <c>InternalsVisibleTo</c> to
/// <c>Orleans.Lattice.Tenancy</c> and <c>Orleans.Lattice.Api.TenantAdmin</c>, so
/// the released public interface stays byte-identical. Resolve it with
/// <see cref="TenantScopedMembershipStoreResolution.GetTenantScopedMembershipStore"/>.
/// </para>
/// <para>
/// Every operation addresses the <c>sys-membership-*</c> trees under system
/// origin, exactly as the rest of the directory does, and performs <b>no</b>
/// authorization: the calling facade is the single enforcement point.
/// </para>
/// <para>
/// A tenant's scope is every group id starting with <c>t/{tenant}/</c>, malformed
/// ones included, and every edge whose group or member is in that scope. Every
/// operation is a prefix scan or a range count over that scope, never a
/// whole-directory scan.
/// </para>
/// </remarks>
internal interface ITenantScopedMembershipStore
{
    /// <summary>Counts the group records in <paramref name="tenant"/>'s scope.</summary>
    /// <param name="tenant">The tenant. Must be initialised and not <see cref="TenantId.Default"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of group records whose id starts with <c>t/{tenant}/</c>.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> is uninitialised or the default tenant.</exception>
    Task<int> CountTenantGroupsAsync(TenantId tenant, CancellationToken cancellationToken = default);

    /// <summary>
    /// Counts the membership edges into <paramref name="tenant"/>'s groups: every
    /// edge whose parent group id starts with <c>t/{tenant}/</c>. Under the nesting
    /// invariant these are every edge the tenant tier can create.
    /// </summary>
    /// <param name="tenant">The tenant. Must be initialised and not <see cref="TenantId.Default"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of edges into the tenant's groups.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> is uninitialised or the default tenant.</exception>
    Task<int> CountTenantEdgesAsync(TenantId tenant, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads one page of <paramref name="tenant"/>'s group records in ascending
    /// group-id order with a prefix scan.
    /// </summary>
    /// <param name="tenant">The tenant. Must be initialised and not <see cref="TenantId.Default"/>.</param>
    /// <param name="afterGroupId">
    /// The previous page's <see cref="TenantGroupPage.ContinuationAfter"/>, or
    /// <c>null</c> for the first page. Must be in the tenant's scope when supplied.
    /// </param>
    /// <param name="pageSize">The maximum groups to return, 1 to <see cref="MaxTenantGroupPageSize"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The page.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="tenant"/> is uninitialised or the default tenant, or
    /// <paramref name="afterGroupId"/> is outside the tenant's scope.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="pageSize"/> is out of range.</exception>
    Task<TenantGroupPage> ListTenantGroupsAsync(
        TenantId tenant,
        string? afterGroupId,
        int pageSize,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes <paramref name="groupId"/>'s record and every edge it takes part in,
    /// in both directions (its members and its parents). Each edge's counterpart
    /// row is removed before the row that located it, so an interrupted call is
    /// completed by a re-run. Idempotent.
    /// </summary>
    /// <param name="groupId">The group to remove. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The edges removed by this call.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="groupId"/> is <c>null</c>.</exception>
    Task<IReadOnlyList<MembershipEdge>> RemoveGroupCascadeAsync(string groupId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes everything in <paramref name="tenant"/>'s scope: every edge whose
    /// member or group starts with <c>t/{tenant}/</c> (forward and reverse prefix
    /// scans, removing each counterpart row too) and then every group record in
    /// the scope. Idempotent and resumable: each counterpart row is removed before
    /// the row that located it, so a re-run after a partial purge completes it, and
    /// a re-run after a complete purge removes nothing.
    /// </summary>
    /// <param name="tenant">The tenant. Must be initialised and not <see cref="TenantId.Default"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>What this pass removed.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> is uninitialised or the default tenant.</exception>
    Task<TenantMembershipPurgeResult> PurgeTenantAsync(TenantId tenant, CancellationToken cancellationToken = default);

    /// <summary>The largest page <see cref="ListTenantGroupsAsync"/> accepts.</summary>
    const int MaxTenantGroupPageSize = 1000;
}
