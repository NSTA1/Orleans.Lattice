using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Server;
using Orleans.Lattice.Api.TenantAdmin;
using static Orleans.Lattice.Api.Mcp.McpHandlerToolFactory;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The delegated tenant access tool module: an <see cref="ILatticeApiMcpToolGroup"/>
/// for <see cref="LatticeApiMcpGroup.TenantAdmin"/> whose tools are thin adapters
/// over <see cref="ILatticeTenantDirectoryAdmin"/> (tenant groups, group members and
/// the tenant member set) and <see cref="ILatticeTenantPolicyAdmin"/> (tenant rules,
/// explain, effective permissions and the tenant access posture).
/// </summary>
/// <remarks>
/// <para>
/// <b>Gating.</b> The module sits beside <see cref="TenantAdminToolGroup"/> and
/// shares its single opt-in: it contributes nothing unless
/// <see cref="LatticeApiMcpOptions.EnableTenantAdminControlTools"/> is set, the same
/// switch that contributes the existing tenant-admin read tool
/// (<c>lattice_tenant_region_status</c>). With it set, the eight writes are
/// annotated destructive and the nine reads read-only.
/// </para>
/// <para>
/// <b>Optional registration.</b> Each facade's tools are contributed only when that
/// facade is registered in the host's service collection - checked once, without
/// resolving the facade, through <see cref="IServiceProviderIsService"/>. In-process
/// the tenant-admin API registers both; a remote (split) head contributes them once
/// it registers the tenant access gRPC client as both interfaces. Absent a facade,
/// its tools are absent, so the session's tool list and <c>lattice_capabilities</c>
/// are unchanged: the module advertises under the already-registered
/// <see cref="LatticeApiMcpGroup.TenantAdmin"/> group and adds no group of its own.
/// </para>
/// <para>
/// <b>Authorization.</b> The module adds no authorization path. The shared
/// credential-stamping seam stamps the caller credential for each call, and the
/// facades' own fail-closed tenant-admin checks, confinement rules, caps and
/// feature flag decide every request. A denial surfaces as a denial; the facades'
/// other typed failures are mapped by <see cref="TenantAccessToolFaults"/>.
/// </para>
/// </remarks>
internal sealed class TenantAccessToolGroup : ILatticeApiMcpToolGroup
{
    /// <summary>The directory tool names, in contribution order.</summary>
    internal static readonly string[] DirectoryToolNames =
    [
        "lattice_tenant_group_list",
        "lattice_tenant_group_get",
        "lattice_tenant_group_members",
        "lattice_tenant_member_list",
        "lattice_tenant_group_upsert",
        "lattice_tenant_group_remove",
        "lattice_tenant_group_member_add",
        "lattice_tenant_group_member_remove",
        "lattice_tenant_member_add",
        "lattice_tenant_member_remove",
    ];

    /// <summary>The policy tool names, in contribution order.</summary>
    internal static readonly string[] PolicyToolNames =
    [
        "lattice_tenant_rule_list",
        "lattice_tenant_rule_get",
        "lattice_tenant_explain",
        "lattice_tenant_effective_permissions",
        "lattice_tenant_access_posture",
        "lattice_tenant_rule_put",
        "lattice_tenant_rule_remove",
    ];

    private const string ControlSuffix =
        " Subject to the facade's fail-closed tenant-admin check. Requires tenant-admin control to be enabled on the "
        + "server, and delegated tenant access administration to be enabled on the cluster (see "
        + "lattice_tenant_access_posture).";

    /// <summary>
    /// Builds the tool set once from the resolved options and the registered
    /// facades.
    /// </summary>
    /// <param name="services">
    /// The root service provider: tells the MCP SDK which handler parameters are
    /// satisfied from dependency injection, and reports which facades are registered.
    /// Must not be <see langword="null"/>.
    /// </param>
    /// <param name="options">The resolved MCP binding options. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    public TenantAccessToolGroup(IServiceProvider services, IOptions<LatticeApiMcpOptions> options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        if (!options.Value.EnableTenantAdminControlTools)
        {
            Tools = [];
            return;
        }

        var registry = services.GetService<IServiceProviderIsService>();
        var tools = new List<McpServerTool>(DirectoryToolNames.Length + PolicyToolNames.Length);
        if (registry?.IsService(typeof(ILatticeTenantDirectoryAdmin)) == true)
        {
            AddDirectoryTools(services, tools);
        }

        if (registry?.IsService(typeof(ILatticeTenantPolicyAdmin)) == true)
        {
            AddPolicyTools(services, tools);
        }

        Tools = tools;
    }

    /// <inheritdoc />
    public LatticeApiMcpGroup Group => LatticeApiMcpGroup.TenantAdmin;

    /// <inheritdoc />
    public IReadOnlyList<McpServerTool> Tools { get; }

    private static void AddDirectoryTools(IServiceProvider services, List<McpServerTool> tools)
    {
        tools.Add(ReadOnlyTool(services, TenantAccessToolHandlers.ListGroupsAsync, "lattice_tenant_group_list",
            "List tenant groups",
            "Lists one page of the tenant's own groups in ascending name order, by tenant-local name. Another "
            + "tenant's groups are never listed. Pass the returned nextPageToken to continue. Read-only."));
        tools.Add(ReadOnlyTool(services, TenantAccessToolHandlers.GetGroupAsync, "lattice_tenant_group_get",
            "Get a tenant group",
            "Reads one of the tenant's groups by tenant-local name. A group that does not exist, including another "
            + "tenant's group, reports found=false. Read-only."));
        tools.Add(ReadOnlyTool(services, TenantAccessToolHandlers.ListGroupMembersAsync, "lattice_tenant_group_members",
            "List a tenant group's members",
            "Lists the direct members of one of the tenant's groups: users, cluster groups and the tenant's own "
            + "groups, each with its kind. Read-only."));
        tools.Add(ReadOnlyTool(services, TenantAccessToolHandlers.ListMembersAsync, "lattice_tenant_member_list",
            "List the tenant member set",
            "Lists one page of the tenant member set - the users and groups that are members of the tenant - each "
            + "with its kind. Pass the returned nextPageToken to continue. Read-only."));

        tools.Add(DestructiveTool(services, TenantAccessToolHandlers.UpsertGroupAsync, "lattice_tenant_group_upsert",
            "Create or update a tenant group",
            "Creates one of the tenant's groups, or updates its display name. The group is named by its tenant-local "
            + "name and is visible only within the tenant. Creating a group counts against the tenant's MaxGroups "
            + "cap." + ControlSuffix));
        tools.Add(DestructiveTool(services, TenantAccessToolHandlers.RemoveGroupAsync, "lattice_tenant_group_remove",
            "Remove a tenant group",
            "Removes one of the tenant's groups and cascades: its membership edges in both directions, its "
            + "member-set and admin-set entries, and the tenant rules that name it. Removing a group that does not "
            + "exist reports removed=false and changes nothing. Removing the tenant's last admin group is refused."
            + ControlSuffix));
        tools.Add(DestructiveTool(services, TenantAccessToolHandlers.AddGroupMemberAsync, "lattice_tenant_group_member_add",
            "Add a tenant group member",
            "Adds a direct member to one of the tenant's groups: a user, a cluster group, or another of the tenant's "
            + "own groups. A tenant group can never contain or be nested in another tenant's group, and is never "
            + "nested in a cluster group. Idempotent; counts against the MaxMembershipEdges cap." + ControlSuffix));
        tools.Add(DestructiveTool(services, TenantAccessToolHandlers.RemoveGroupMemberAsync, "lattice_tenant_group_member_remove",
            "Remove a tenant group member",
            "Removes a direct member from one of the tenant's groups. A no-op (changed=false) when the member is "
            + "not in the group." + ControlSuffix));
        tools.Add(DestructiveTool(services, TenantAccessToolHandlers.AddMemberAsync, "lattice_tenant_member_add",
            "Add a tenant member",
            "Adds a user, a cluster group or one of the tenant's own groups to the tenant member set. Idempotent; "
            + "counts against the MaxMemberSubjects cap." + ControlSuffix));
        tools.Add(DestructiveTool(services, TenantAccessToolHandlers.RemoveMemberAsync, "lattice_tenant_member_remove",
            "Remove a tenant member",
            "Removes an entry from the tenant member set. A no-op (changed=false) when the entry is not present."
            + ControlSuffix));
    }

    private static void AddPolicyTools(IServiceProvider services, List<McpServerTool> tools)
    {
        tools.Add(ReadOnlyTool(services, TenantAccessToolHandlers.ListRulesAsync, "lattice_tenant_rule_list",
            "List tenant rules",
            "Lists one page of the rules a tenant admin can see: the tenant's own tenant-tier rules (editable) and "
            + "the operator rules scoped to the tenant's own trees (read-only, layer Platform). Cluster-wide and "
            + "app rules are not listed. Pass the returned nextPageToken to continue. Read-only."));
        tools.Add(ReadOnlyTool(services, TenantAccessToolHandlers.GetRuleAsync, "lattice_tenant_rule_get",
            "Get a tenant rule",
            "Reads one of the tenant's tenant-tier rules by tenant-local id. A rule that does not exist reports "
            + "found=false. Read-only."));
        tools.Add(ReadOnlyTool(services, TenantAccessToolHandlers.ExplainAsync, "lattice_tenant_explain",
            "Explain a tenant access decision",
            "Explains whether a subject may perform an operation on one of the tenant's trees (optionally one key), "
            + "and which layer decided: an operator (Platform) rule always takes precedence over a tenant rule. When "
            + "a cluster-wide rule or an app role decided, only that rule's id and effect are reported. Read-only."));
        tools.Add(ReadOnlyTool(services, TenantAccessToolHandlers.EffectivePermissionsAsync, "lattice_tenant_effective_permissions",
            "List a subject's effective tenant permissions",
            "Lists the rules in effect for a subject within the tenant, optionally narrowed to one of the tenant's "
            + "trees, labelled by layer and origin. Cluster-wide and app-role rules carry only their id and effect. "
            + "Read-only."));
        tools.Add(ReadOnlyTool(services, TenantAccessToolHandlers.GetPostureAsync, "lattice_tenant_access_posture",
            "Read the tenant access posture",
            "Reports whether delegated tenant access administration is enabled on the cluster, whether the caller is "
            + "an admin of the tenant or a platform operator, and the tenant's group, membership-edge, member-set "
            + "and tenant-rule caps with their usage. The one tenant access read that answers while the feature is "
            + "off. Read-only."));

        tools.Add(DestructiveTool(services, TenantAccessToolHandlers.PutRuleAsync, "lattice_tenant_rule_put",
            "Create or replace a tenant rule",
            "Creates or replaces a tenant-tier authorization rule over one of the tenant's trees, a key, a prefix, or "
            + "every tree the tenant owns (TenantWide). A tenant rule sits beneath every operator rule: it can never "
            + "override an operator deny nor revoke an operator allow, never reaches another tenant's, app-owned or "
            + "system trees, and its subject may name only users, cluster groups and the tenant's own groups. "
            + "Counts against the MaxTenantRules cap." + ControlSuffix));
        tools.Add(DestructiveTool(services, TenantAccessToolHandlers.RemoveRuleAsync, "lattice_tenant_rule_remove",
            "Remove a tenant rule",
            "Removes one of the tenant's tenant-tier rules by tenant-local id, reporting removed=false when none "
            + "existed. Operator rules cannot be removed here." + ControlSuffix));
    }
}
