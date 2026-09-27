using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// A pre-code-load manifest review and matching installation state. Every tree
/// reference is app-local, except explicit operator-declared adoption ids;
/// composed physical identifiers must never be returned.
/// </summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppDescriptor), Immutable]
public sealed record AppDescriptor
{
    /// <summary>The described app slug.</summary>
    [Id(0)] public required string Slug { get; init; }
    /// <summary>The exact described source version.</summary>
    [Id(1)] public required string Version { get; init; }
    /// <summary>The source provenance metadata, never an authorization claim.</summary>
    [Id(2)] public required AppProvenanceDescriptor Provenance { get; init; }
    /// <summary>The lifecycle of this version; NotInstalled when only available from the source.</summary>
    [Id(3)] public AppLifecycleState State { get; init; }
    /// <summary>The consent pinned to this version; null when this version is not installed.</summary>
    [Id(4)] public AppCapabilityCeilingDescriptor? Ceiling { get; init; }
    /// <summary>The installed membership-group bindings; empty before installation.</summary>
    [Id(5)] public ImmutableArray<AppRoleBindingDescriptor> RoleBindings { get; init; } = [];
    /// <summary>The manifest's tree declarations, including rebuildability and explicit adoption.</summary>
    [Id(6)] public ImmutableArray<AppTreeDescriptor> Trees { get; init; } = [];
    /// <summary>The manifest's requested roles and scopes, before ceiling intersection.</summary>
    [Id(7)] public ImmutableArray<AppRoleDescriptor> Roles { get; init; } = [];
    /// <summary>The requested change-feed subscriptions, including cross-app sources.</summary>
    [Id(8)] public ImmutableArray<AppSubscriptionDescriptor> Subscriptions { get; init; } = [];
    /// <summary>The declared app-local MCP tool names, descriptions, and required roles.</summary>
    [Id(9)] public ImmutableArray<AppMcpToolDescriptor> McpTools { get; init; } = [];
    /// <summary>The optional replication declarations; empty when none are requested.</summary>
    [Id(10)] public ImmutableArray<AppReplicationDescriptor> Replication { get; init; } = [];
    /// <summary>The optional schema declarations; empty when none are requested.</summary>
    [Id(11)] public ImmutableArray<AppSchemaDescriptor> Schema { get; init; } = [];
}
