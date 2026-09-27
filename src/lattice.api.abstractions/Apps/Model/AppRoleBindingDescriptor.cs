namespace Orleans.Lattice.Api.Apps;

/// <summary>An install-time binding from an app role to a membership group, never a user.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppRoleBindingDescriptor), Immutable]
public sealed record AppRoleBindingDescriptor
{
    /// <summary>The manifest's role name.</summary>
    [Id(0)] public required string RoleName { get; init; }
    /// <summary>The operator-selected membership group identifier.</summary>
    [Id(1)] public required string GroupId { get; init; }
}
