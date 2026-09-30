using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// The sanitised, non-administrative description of an installed app that any of its
/// role holders may read.
/// </summary>
/// <remarks>
/// Deliberately excludes the capability ceiling, approved exception scopes, consent
/// history, role-to-group bindings and every physical or adopted tree id; those stay
/// behind <see cref="LatticeOperation.AppInstall"/> through <see cref="ILatticeAppsControl"/>.
/// </remarks>
[GenerateSerializer, Alias(ApiAppsTypeAliases.WorkspaceAppDescriptor), Immutable]
public sealed record WorkspaceAppDescriptor
{
    /// <summary>The app slug.</summary>
    [Id(0)] public required string Slug { get; init; }
    /// <summary>The installed version.</summary>
    [Id(1)] public required string Version { get; init; }
    /// <summary>The install revision a UI frame is bound to; it changes on every upgrade.</summary>
    [Id(2)] public long InstallRevision { get; init; }
    /// <summary>The key of the source the installed version came from, or null when not recorded.</summary>
    [Id(3)] public string? SourceKey { get; init; }
    /// <summary>The installation's lifecycle state.</summary>
    [Id(4)] public AppLifecycleState State { get; init; }
    /// <summary>The installed version's presentation, or null when it declares none.</summary>
    [Id(5)] public AppPresentationDescriptor? Presentation { get; init; }
    /// <summary>The app's trees, by app-local name only.</summary>
    [Id(6)] public ImmutableArray<WorkspaceTreeDescriptor> Trees { get; init; } = [];
    /// <summary>The roles the caller holds, with their declared operations and scope templates.</summary>
    [Id(7)] public ImmutableArray<AppRoleDescriptor> Roles { get; init; } = [];
    /// <summary>The app's MCP tools and the roles they require.</summary>
    [Id(8)] public ImmutableArray<AppMcpToolDescriptor> McpTools { get; init; } = [];
    /// <summary>The app's change-feed subscriptions.</summary>
    [Id(9)] public ImmutableArray<AppSubscriptionDescriptor> Subscriptions { get; init; } = [];
    /// <summary>The app's replication intent.</summary>
    [Id(10)] public ImmutableArray<AppReplicationDescriptor> Replication { get; init; } = [];
    /// <summary>
    /// The installed version's UI bundle, or null when it ships none. Its
    /// <see cref="AppUiDescriptor.Bridge"/> carries only the grants the operator consented to
    /// that the installed manifest still requests - what the bridge itself admits - never the
    /// bare request.
    /// </summary>
    [Id(11)] public AppUiDescriptor? Ui { get; init; }
}
