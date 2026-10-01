namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// The role-to-group pairs the flow's role-holding advice reads (issue #4150): whether the
/// caller will hold a role is a matter of the groups the roles are bound to and nothing else.
/// </summary>
internal sealed partial class AppInstallFlow
{
    /// <summary>Every declared role and the group drafted for it, or <see langword="null"/> while it is unbound.</summary>
    public IEnumerable<(string Role, string? Group)> DraftedRoleGroups =>
        Descriptor is not { } descriptor
            ? []
            : descriptor.Roles.Select(role => (role.Name, (string?)_bindings.GetValueOrDefault(role.Name)));

    /// <summary>
    /// Every declared role and the group it ends up bound to: the drafted group, or - where
    /// nothing was drafted, as for a re-consent - the group the installed version records.
    /// </summary>
    public IEnumerable<(string Role, string? Group)> SettledRoleGroups =>
        Descriptor is not { } descriptor
            ? []
            : descriptor.Roles.Select(role => (role.Name, _bindings.GetValueOrDefault(role.Name) ?? RecordedGroup(role.Name)));
}
