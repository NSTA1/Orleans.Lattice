using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps;

/// <summary>An installed app's role-to-group bindings as read back after a re-binding.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppRoleBindingsReport), Immutable]
public sealed record AppRoleBindingsReport
{
    /// <summary>The installed app slug.</summary>
    [Id(0)] public required string Slug { get; init; }

    /// <summary>The installed version the bindings apply to.</summary>
    [Id(1)] public required string Version { get; init; }

    /// <summary>The recorded bindings, in the order they were supplied.</summary>
    [Id(2)] public ImmutableArray<AppRoleBindingDescriptor> RoleBindings { get; init; } = [];

    /// <summary>The app's lifecycle state, which a re-binding never changes.</summary>
    [Id(3)] public AppLifecycleState State { get; init; }
}
