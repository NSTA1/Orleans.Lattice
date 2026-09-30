using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// A full replacement of an installed app's role-to-group bindings for an exact installed
/// version. Roles it leaves out end up bound to no group.
/// </summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppRoleBindingsUpdate), Immutable]
public sealed record AppRoleBindingsUpdate
{
    /// <summary>The installed app slug.</summary>
    [Id(0)] public required string Slug { get; init; }

    /// <summary>The expected installed version; a mismatch rejects the update.</summary>
    [Id(1)] public required string Version { get; init; }

    /// <summary>
    /// The complete replacement bindings: each names a role the installed manifest declares
    /// and a membership group, and no role appears twice.
    /// </summary>
    [Id(2)] public ImmutableArray<AppRoleBindingDescriptor> RoleBindings { get; init; } = [];
}
