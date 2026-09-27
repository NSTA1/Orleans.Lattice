namespace Orleans.Lattice.Apps;

/// <summary>Inspectable app declarations. Contains no executable types, handlers or assembly names to load.</summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppManifest)]
public sealed record AppManifest
{
    /// <summary>Artifact identity and provenance.</summary>
    [Id(0)] public required AppIdentity Identity { get; init; }

    /// <summary>Owned trees, named relative to the app namespace.</summary>
    [Id(1)] public required AppTreeDeclaration[] Trees { get; init; }

    /// <summary>Flat roles bound to membership groups at installation, without inheritance.</summary>
    [Id(2)] public required AppRoleDeclaration[] Roles { get; init; }

    /// <summary>Optional per-tree replication enrolment and merge modes.</summary>
    [Id(3)] public AppReplicationDeclaration[]? Replication { get; init; }

    /// <summary>Optional per-tree schema-family and envelope-version declarations.</summary>
    [Id(4)] public AppSchemaDeclaration[]? Schema { get; init; }

    /// <summary>Change-feed observations of this app's or another app's trees.</summary>
    [Id(5)] public required AppSubscriptionDeclaration[] Subscriptions { get; init; }

    /// <summary>App-local MCP tools; dispatch prefixes each name with the app slug and underscore.</summary>
    [Id(6)] public required AppMcpToolDeclaration[] McpTools { get; init; }
}
