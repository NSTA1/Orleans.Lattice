namespace Orleans.Lattice.Apps;

/// <summary>
/// Binds a manifest role to a membership group at install time. Bindings target groups only;
/// the compiler emits a group subject selector, never a user or inherited role.
/// </summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppRoleBinding), Immutable]
public sealed record AppRoleBinding
{
    /// <summary>The manifest role being bound.</summary>
    [Id(0)] public required string RoleName { get; init; }

    /// <summary>The membership group id that receives the compiled role.</summary>
    [Id(1)] public required string GroupId { get; init; }

    /// <summary>Creates a group binding after checking both identifiers are non-empty.</summary>
    /// <exception cref="ArgumentException">Either identifier is null or empty.</exception>
    public static AppRoleBinding Create(string roleName, string groupId)
    {
        ArgumentException.ThrowIfNullOrEmpty(roleName);
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        return new() { RoleName = roleName, GroupId = groupId };
    }
}
