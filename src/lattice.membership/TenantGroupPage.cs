namespace Orleans.Lattice.Membership;

/// <summary>
/// One page of a tenant's groups, in ascending group-id order, returned by
/// <see cref="ITenantScopedMembershipStore.ListTenantGroupsAsync"/>.
/// </summary>
/// <param name="Groups">The groups on this page, in ascending group-id order.</param>
/// <param name="ContinuationAfter">
/// The group id to pass as <c>afterGroupId</c> to read the next page, or
/// <c>null</c> when this is the last page.
/// </param>
internal sealed record TenantGroupPage(IReadOnlyList<MembershipGroup> Groups, string? ContinuationAfter);
