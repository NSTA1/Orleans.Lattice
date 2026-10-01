using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// Whether the caller holds a role in an app, read from its role-to-group bindings and
/// the caller's groups alone - exactly the rule the cluster applies (issue #3902): a
/// role is held only through membership of a group it is bound to. Nothing else the
/// caller may do, administrator rights included, enters it.
/// </summary>
/// <param name="Lines">Every bound (or unbound) role, with the caller's membership of its group.</param>
/// <param name="MembershipKnown">Whether the caller's groups could be read.</param>
internal sealed record AppRoleHoldingAssessment(ImmutableArray<AppRoleHoldingLine> Lines, bool MembershipKnown)
{
    /// <summary>
    /// Whether the caller is in a group some role is bound to: <see langword="true"/> or
    /// <see langword="false"/> when that is known, <see langword="null"/> when the caller's
    /// groups could not be read and no line says otherwise.
    /// </summary>
    public bool? HoldsAny =>
        Lines.Any(line => line.Membership == AppGroupMembership.Member) ? true
        : MembershipKnown ? false
        : null;

    /// <summary>The roles the caller holds, in order, without repeats.</summary>
    public IReadOnlyList<string> HeldRoles =>
        [.. Lines.Where(line => line.Membership == AppGroupMembership.Member).Select(line => line.Role).Distinct(StringComparer.Ordinal)];

    /// <summary>The groups the caller is in that a role is bound to, without repeats.</summary>
    public IReadOnlyList<string> MemberGroups => GroupsWhere(AppGroupMembership.Member);

    /// <summary>The bound groups the caller is known not to be in, without repeats.</summary>
    public IReadOnlyList<string> MissingGroups => GroupsWhere(AppGroupMembership.NotMember);

    /// <summary>Every bound group, without repeats.</summary>
    public IReadOnlyList<string> BoundGroups =>
        [.. Lines.Where(line => line.Group is not null).Select(line => line.Group!).Distinct(StringComparer.Ordinal)];

    /// <summary>Each role and its group as text, for example <c>editor to operators</c>.</summary>
    public string BindingsText =>
        Lines.IsDefaultOrEmpty
            ? "no role"
            : string.Join(", ", Lines.Select(line => line.Group is null ? line.Role + " to no group" : line.Role + " to " + line.Group));

    /// <summary>Assesses role-to-group pairs, one or more per role, against the caller's groups.</summary>
    /// <param name="roleGroups">Each role and the group bound to it, or <see langword="null"/> when unbound.</param>
    /// <param name="caller">The caller's groups.</param>
    /// <returns>The assessment.</returns>
    public static AppRoleHoldingAssessment Assess(IEnumerable<(string Role, string? Group)> roleGroups, AppsCallerGroups caller)
    {
        ArgumentNullException.ThrowIfNull(roleGroups);
        ArgumentNullException.ThrowIfNull(caller);

        var lines = ImmutableArray.CreateBuilder<AppRoleHoldingLine>();
        foreach (var (role, group) in roleGroups)
        {
            lines.Add(new AppRoleHoldingLine(
                role,
                string.IsNullOrWhiteSpace(group) ? null : group,
                string.IsNullOrWhiteSpace(group) ? AppGroupMembership.Unbound : caller.Of(group)));
        }

        return new AppRoleHoldingAssessment(lines.ToImmutable(), caller.IsKnown);
    }

    /// <summary>
    /// Assesses an installed app's recorded bindings: every binding it records, and each
    /// declared role it records none for as unbound.
    /// </summary>
    /// <param name="app">The installed app's administrative description.</param>
    /// <param name="caller">The caller's groups.</param>
    /// <returns>The assessment.</returns>
    public static AppRoleHoldingAssessment Assess(AppDescriptor app, AppsCallerGroups caller)
    {
        ArgumentNullException.ThrowIfNull(app);
        return Assess(Pairs(app), caller);
    }

    private static IEnumerable<(string Role, string? Group)> Pairs(AppDescriptor app)
    {
        foreach (var role in app.Roles.IsDefault ? [] : app.Roles)
        {
            var bound = false;
            foreach (var binding in app.RoleBindings.IsDefault ? [] : app.RoleBindings)
            {
                if (string.Equals(binding.RoleName, role.Name, StringComparison.Ordinal))
                {
                    bound = true;
                    yield return (role.Name, binding.GroupId);
                }
            }

            if (!bound)
            {
                yield return (role.Name, null);
            }
        }
    }

    private IReadOnlyList<string> GroupsWhere(AppGroupMembership membership) =>
        [.. Lines.Where(line => line.Membership == membership && line.Group is not null).Select(line => line.Group!).Distinct(StringComparer.Ordinal)];
}
