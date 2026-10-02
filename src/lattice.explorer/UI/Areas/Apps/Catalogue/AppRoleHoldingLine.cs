namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>One app role, the group it is bound to, and whether the caller is in that group.</summary>
/// <param name="Role">The role name.</param>
/// <param name="Group">The bound group, or <see langword="null"/> when the role is unbound.</param>
/// <param name="Membership">Whether the caller is in <paramref name="Group"/>.</param>
internal sealed record AppRoleHoldingLine(string Role, string? Group, AppGroupMembership Membership);
