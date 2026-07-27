using Microsoft.Extensions.Options;
using ModelContextProtocol.Server;
using Orleans.Lattice.Api.Auth;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The auth-admin tool module: the <see cref="ILatticeApiMcpToolGroup"/> for
/// <see cref="LatticeApiMcpGroup.Auth"/>. It contributes MCP tools that adapt the
/// <see cref="ILatticeAuthAdmin"/> control-plane facade - policy and
/// membership <b>introspection</b> reads always, and the mutating
/// <b>administration</b> verbs only when the host opts them in through
/// <see cref="LatticeApiMcpOptions.EnableAuthAdministration"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Administrator-gated end to end.</b> The whole group maps to
/// <c>LatticeOperation.Admin</c> in the discovery core's capability map, so a
/// non-administrator session is offered <b>none</b> of these tools. Should a
/// caller reach an invocation regardless, the facade's administrator gate
/// refuses it fail-closed. The module itself adds no authorization logic.
/// </para>
/// <para>
/// <b>Built once.</b> The tool list is materialised a single time in the
/// constructor from the static <see cref="AuthToolHandlers"/> method groups. Each
/// tool resolves its <see cref="ILatticeAuthAdmin"/> collaborator from the
/// request service provider at call time, so the per-session discovery filter
/// selects from this prebuilt list and never re-materialises a tool per
/// <c>tools/list</c> or <c>tools/call</c>.
/// </para>
/// </remarks>
internal sealed class AuthToolGroup : ILatticeApiMcpToolGroup
{
    /// <summary>
    /// Builds the auth tool list once from the configured options. The supplied
    /// <paramref name="services"/> is used only to tell the MCP SDK which tool
    /// parameters (the <see cref="ILatticeAuthAdmin"/> facade) are satisfied from
    /// dependency injection rather than the tool-call arguments; the facade is
    /// resolved from the request service provider at invocation time.
    /// </summary>
    /// <param name="services">The service provider that reports the DI-satisfied tool parameters.</param>
    /// <param name="options">The MCP binding options, read for the administration opt-in.</param>
    public AuthToolGroup(IServiceProvider services, IOptions<LatticeApiMcpOptions> options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);
        Tools = Build(services, options.Value.EnableAuthAdministration);
    }

    /// <inheritdoc />
    public LatticeApiMcpGroup Group => LatticeApiMcpGroup.Auth;

    /// <inheritdoc />
    public IReadOnlyList<McpServerTool> Tools { get; }

    private static IReadOnlyList<McpServerTool> Build(IServiceProvider services, bool enableAdministration)
    {
        var tools = new List<McpServerTool>
        {
            // ----- Introspection (read-only) -----
            Read(services, AuthToolHandlers.ExplainAsync, "lattice_auth_explain", "Explain an authorization decision",
                "Explains whether a subject may perform an operation over a keyspace scope (whole tree, a key, or a "
                + "prefix), returning the access gate's verdict and the authored rules that apply. Set subjectKind to "
                + "Group to explain a group subject rather than a user. Under a default-allow posture a subject can be "
                + "allowed with an empty matchedRules list: no rule denied the operation, so the gate's implicit allow "
                + "stands - an empty matchedRules on an allow verdict means 'nothing objected', not 'nothing was "
                + "evaluated'. Read-only."),
            Read(services, AuthToolHandlers.EffectivePermissionsAsync, "lattice_auth_effective_permissions",
                "List a subject's effective permissions",
                "Returns the authorization rules currently in effect for a subject (grants and denies), resolved from "
                + "the live policy store and the subject's group closure. Set subjectKind to Group to resolve a group "
                + "subject rather than a user. Read-only."),
            Read(services, AuthToolHandlers.GetGroupAsync, "lattice_auth_get_group", "Get a group",
                "Reads a single group record by id, or null when no such group exists. Read-only."),
            Read(services, AuthToolHandlers.ListGroupsAsync, "lattice_auth_list_groups", "List groups",
                "Reads one page of the group catalog in ascending group-id order. Pass the returned next page token to "
                + "continue. Read-only."),
            Read(services, AuthToolHandlers.ListGroupMembersAsync, "lattice_auth_list_group_members", "List a group's members",
                "Returns the direct members (users and nested groups) of a group, in ascending ordinal order. Read-only."),
            Read(services, AuthToolHandlers.ListSubjectGroupsAsync, "lattice_auth_list_subject_groups",
                "List a subject's groups",
                "Returns the full transitive set of group ids a subject belongs to, walking nested groups. Read-only."),
            Read(services, AuthToolHandlers.GetRuleAsync, "lattice_auth_get_rule", "Get an authorization rule",
                "Reads a single rule by its governed tree id and rule id, or null when none exists. Read-only."),
            Read(services, AuthToolHandlers.ListRulesAsync, "lattice_auth_list_rules", "List all authorization rules",
                "Reads one page of every rule in the store, ordered by (governed tree id, rule id). Pass the returned "
                + "next page token to continue. Read-only."),
            Read(services, AuthToolHandlers.ListRulesForTreeAsync, "lattice_auth_list_rules_for_tree",
                "List a tree's authorization rules",
                "Reads one page of the rules governing a single tree, ordered by (governed tree id, rule id). Pass the "
                + "returned next page token to continue. Returns the tree's own rules AND the cluster-wide wildcard "
                + "rules (scope Tree:*, stored under the reserved '*' tree id) that effectively govern it; a wildcard "
                + "rule is recognisable by its scope tree id of '*'. Use lattice_auth_explain or "
                + "lattice_auth_effective_permissions for the resolved verdict a subject's access receives. Read-only."),
        };

        if (enableAdministration)
        {
            // ----- Administration (destructive) -----
            tools.Add(Write(services, AuthToolHandlers.UpsertGroupAsync, "lattice_auth_upsert_group", "Create or replace a group",
                "Creates or replaces a group record (id and optional display name), returning the written record. "
                + "Administrator-gated and destructive."));
            tools.Add(Write(services, AuthToolHandlers.RemoveGroupAsync, "lattice_auth_remove_group", "Remove a group",
                "Removes a group record by id. A no-op when no such group exists. Administrator-gated and destructive."));
            tools.Add(Write(services, AuthToolHandlers.AddMemberAsync, "lattice_auth_add_member", "Add a group member",
                "Adds a membership edge making a member (a user or a nested group) a direct member of a group. "
                + "Idempotent. Administrator-gated and destructive."));
            tools.Add(Write(services, AuthToolHandlers.RemoveMemberAsync, "lattice_auth_remove_member", "Remove a group member",
                "Removes a membership edge. A no-op when the edge does not exist. Administrator-gated and destructive."));
            tools.Add(Write(services, AuthToolHandlers.PutRuleAsync, "lattice_auth_put_rule", "Create or replace a rule",
                "Creates or replaces an authorization rule granting or denying a set of operations over a keyspace "
                + "scope to a user or group, returning the persisted rule. Administrator-gated and destructive."));
            tools.Add(Write(services, AuthToolHandlers.RemoveRuleAsync, "lattice_auth_remove_rule", "Remove a rule",
                "Removes a rule by its governed tree id and rule id, returning true when a rule was removed. "
                + "Administrator-gated and destructive."));
        }

        return tools;
    }

    private static McpServerTool Read(
        IServiceProvider services,
        Delegate handler,
        string name,
        string title,
        string description)
        => McpServerTool.Create(
            handler,
            new McpServerToolCreateOptions
            {
                Services = services,
                Name = name,
                Title = title,
                Description = description,
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool Write(
        IServiceProvider services,
        Delegate handler,
        string name,
        string title,
        string description)
        => McpServerTool.Create(
            handler,
            new McpServerToolCreateOptions
            {
                Services = services,
                Name = name,
                Title = title,
                Description = description,
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });
}
