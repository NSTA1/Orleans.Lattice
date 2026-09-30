using System.Collections.Immutable;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// Builds the sanitised workspace projection of an installed app. It carries app-local tree names and the
/// held roles' declarations only: never the capability ceiling, an approved exception scope, a role-to-group
/// binding, a physical or adopted tree id, or a role the caller does not hold.
/// </summary>
internal static class AppsWorkspaceMapping
{
    /// <summary>Builds the workspace descriptor of an evaluated install.</summary>
    /// <param name="granted">The evaluation, which holds at least one role.</param>
    /// <param name="state">The install's wire lifecycle state.</param>
    /// <returns>The sanitised descriptor.</returns>
    public static WorkspaceAppDescriptor ToDescriptor(AppRoleGrantEvaluation granted, AppLifecycleState state)
    {
        var record = granted.Install.Record;
        var manifest = granted.Install.Manifest;
        var slug = record.Slug;
        return new WorkspaceAppDescriptor
        {
            Slug = slug.Value,
            Version = record.Version.Value,
            InstallRevision = record.Revision,
            SourceKey = record.Provenance.Source,
            State = state,
            Presentation = AppsPresentationMapping.ToWirePresentation(manifest.Presentation),
            Trees = AppsControlMapping.Map(manifest.Trees, static t => new WorkspaceTreeDescriptor
            {
                Name = t.Name,
                Rebuildable = t.Rebuildable,
                Adopted = t.AdoptedTreeId is not null,
                ShardCount = t.ShardCount,
                VirtualShardCount = t.VirtualShardCount,
                MaxLeafKeys = t.MaxLeafKeys,
                MaxInternalChildren = t.MaxInternalChildren,
                WalPartitions = t.WalPartitions,
                SoftDeleteDuration = t.SoftDeleteDuration,
            }),
            Roles = AppsControlMapping.MapRoles(HeldRoles(manifest.Roles, granted.HeldRoles), slug),
            McpTools = AppsControlMapping.Map(manifest.McpTools, static t => new AppMcpToolDescriptor
            {
                Name = t.Name,
                Description = t.Description,
                Role = t.Role,
            }),
            Subscriptions = AppsControlMapping.MapSubscriptions(manifest.Subscriptions, slug),
            Replication = AppsControlMapping.Map(manifest.Replication, static r => new AppReplicationDescriptor
            {
                Tree = r.Tree,
                MergeMode = r.MergeMode,
            }),
            Ui = AppsPresentationMapping.ToWireUi(manifest, record.ConsentedBridge),
        };
    }

    private static AppRoleDeclaration[] HeldRoles(AppRoleDeclaration[]? roles, ImmutableArray<string> held)
    {
        if (roles is null || roles.Length == 0 || held.IsDefaultOrEmpty)
        {
            return [];
        }

        var kept = new List<AppRoleDeclaration>(held.Length);
        foreach (var role in roles)
        {
            if (role is not null && held.Contains(role.Name, StringComparer.Ordinal))
            {
                kept.Add(role);
            }
        }

        return [.. kept];
    }
}
