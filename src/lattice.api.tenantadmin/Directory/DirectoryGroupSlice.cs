using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// One page of a tenant's group records as read from the membership underlay, in
/// ascending ordinal order of full group id.
/// </summary>
/// <param name="Groups">The page's group records.</param>
/// <param name="ContinuationAfter">The full id of the page's last group when more remain; otherwise <c>null</c>.</param>
internal sealed record DirectoryGroupSlice(IReadOnlyList<MembershipGroup> Groups, string? ContinuationAfter);
