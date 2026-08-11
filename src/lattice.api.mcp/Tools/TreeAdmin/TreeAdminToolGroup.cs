using Microsoft.Extensions.Options;
using ModelContextProtocol.Server;
using Orleans.Lattice.Api.Schema;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The tree-administration tool module: an <see cref="ILatticeApiMcpToolGroup"/>
/// for <see cref="LatticeApiMcpGroup.TreeAdmin"/>. Its first surface adapts the
/// transport-agnostic schema-management control facade
/// (<see cref="ILatticeSchemaControl"/>) onto MCP tools by delegation: the
/// read-only schema-inspection verbs (policy / version-config / dead-letter /
/// remediation-status / compliance / capability reads) are always contributed, and
/// the mutating schema-management verbs (set / clear policy, version-config change,
/// version advance / migrate, remediation) only when the host opts them in through
/// <see cref="LatticeApiMcpOptions.EnableTreeAdminSchemaControlTools"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Composition, not absorption.</b> The group holds the
/// <see cref="ILatticeSchemaControl"/> facade (resolved from the request service
/// provider at call time, exactly as the sibling groups resolve their facade) and
/// contributes tools that delegate straight to it. It does <b>not</b> add methods
/// to the tree-administration facade; the schema facade and its packages stay
/// byte-for-byte unchanged.
/// </para>
/// <para>
/// <b>Administrator-gated end to end.</b> The whole group maps to
/// <c>LatticeOperation.Admin</c> in the discovery core's capability map, so a
/// non-administrator session is offered <b>none</b> of these tools. Should a caller
/// reach an invocation regardless, the facade's own fail-closed schema access gate
/// refuses it - schema-admin authority for a mutation, read authority for an
/// inspect. The module itself adds no authorization logic.
/// </para>
/// <para>
/// <b>Built once.</b> The tool list is materialised a single time in the
/// constructor from the static <see cref="TreeAdminSchemaToolHandlers"/> method
/// groups. Each tool resolves its <see cref="ILatticeSchemaControl"/> collaborator
/// from the request service provider at call time, so the per-session discovery
/// filter selects from this prebuilt list and never re-materialises a tool per
/// <c>tools/list</c> or <c>tools/call</c>.
/// </para>
/// </remarks>
internal sealed class TreeAdminToolGroup : ILatticeApiMcpToolGroup
{
    /// <summary>
    /// Builds the tree-administration tool list once from the configured options.
    /// The supplied <paramref name="services"/> is used only to tell the MCP SDK
    /// which tool parameters (the <see cref="ILatticeSchemaControl"/> facade) are
    /// satisfied from dependency injection rather than the tool-call arguments; the
    /// facade is resolved from the request service provider at invocation time.
    /// </summary>
    /// <param name="services">The service provider that reports the DI-satisfied tool parameters.</param>
    /// <param name="options">The MCP binding options, read for the schema-management opt-in.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> or <paramref name="options"/> is <c>null</c>.</exception>
    public TreeAdminToolGroup(IServiceProvider services, IOptions<LatticeApiMcpOptions> options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);
        Tools = Build(services, options.Value.EnableTreeAdminSchemaControlTools);
    }

    /// <inheritdoc />
    public LatticeApiMcpGroup Group => LatticeApiMcpGroup.TreeAdmin;

    /// <inheritdoc />
    public IReadOnlyList<McpServerTool> Tools { get; }

    private static IReadOnlyList<McpServerTool> Build(IServiceProvider services, bool enableSchemaControl)
    {
        var tools = new List<McpServerTool>
        {
            // ----- Schema inspection (read-only) -----
            Read(services, TreeAdminSchemaToolHandlers.GetPolicyAsync, "lattice_treeadmin_schema_get_policy",
                "Get a tree's schema policy",
                "Reads the schema-enforcement policy for a tree - the ordered rule set an incoming value must "
                + "satisfy plus the strict-ingest flag - or null when the tree has no policy (it accepts every "
                + "value). Read-only."),
            Read(services, TreeAdminSchemaToolHandlers.ListDeadLettersAsync, "lattice_treeadmin_schema_list_dead_letters",
                "List a tree's schema dead letters",
                "Lists the strict-mode dead-letter entries retained for a tree: each entry carries the diverted "
                + "key, a bounded preview of the offending value's bytes, its original byte length, the validation "
                + "reason, the ingest source, and the UTC divert time. Empty when nothing was diverted. Read-only."),
            Read(services, TreeAdminSchemaToolHandlers.CountDeadLettersAsync, "lattice_treeadmin_schema_count_dead_letters",
                "Count a tree's schema dead letters",
                "Returns the number of strict-mode dead-letter entries retained for a tree, without materialising "
                + "the entries. Read-only."),
            Read(services, TreeAdminSchemaToolHandlers.GetVersionConfigAsync, "lattice_treeadmin_schema_get_version_config",
                "Get a tree's schema version config",
                "Reads a tree's envelope-versioning config - the stamped schema-family id, the monotonic target "
                + "version new writes are stamped at, and the strict-ingest flag - or null when the tree is "
                + "unversioned. Requires schema versioning to be registered on the server. Read-only."),
            Read(services, TreeAdminSchemaToolHandlers.GetRemediationStatusAsync, "lattice_treeadmin_schema_get_remediation_status",
                "Get a tree's schema remediation status",
                "Reads a tree's current or last-known background-remediation status: the phase, whether a build is "
                + "in flight, how many entries were scanned, and - on an abort - the first offending key, the "
                + "reason, and a bounded value preview. Read-only."),
            Read(services, TreeAdminSchemaToolHandlers.ScanComplianceAsync, "lattice_treeadmin_schema_scan_compliance",
                "Scan a tree for schema compliance",
                "Scans every current value of a tree against its compiled enforcement policy and returns a "
                + "compliance report: whether the tree has a policy, how many values are compliant / non-compliant, "
                + "the total scanned, and the non-compliant population grouped by failure reason. A pure read - it "
                + "never mutates data. Read-only."),
            Read(services, TreeAdminSchemaToolHandlers.ProbeCapabilitiesAsync, "lattice_treeadmin_schema_probe_capabilities",
                "Probe a caller's schema capabilities",
                "Probes which schema-management operations the current caller may perform over a tree, evaluated "
                + "through the same fail-closed schema access gate the real operations use but with no side effects. "
                + "Each flag is default-deny, so a management UI can grey out controls the caller cannot use. The "
                + "flags are advisory; the server still authorizes every real operation on attempt. Read-only."),
        };

        if (enableSchemaControl)
        {
            // ----- Schema management (destructive) -----
            tools.Add(Write(services, TreeAdminSchemaToolHandlers.SetPolicyAsync, "lattice_treeadmin_schema_set_policy",
                "Set a tree's schema policy",
                "Sets or replaces the schema-enforcement policy for a tree - the ordered rule set an incoming value "
                + "must satisfy plus the strict-ingest flag - returning the applied policy. Rejected for a reserved "
                + "tree id or an invalid rule. Schema-admin-gated and destructive."));
            tools.Add(Write(services, TreeAdminSchemaToolHandlers.ClearPolicyAsync, "lattice_treeadmin_schema_clear_policy",
                "Clear a tree's schema policy",
                "Clears the schema-enforcement policy for a tree, returning true when a policy was removed and false "
                + "when the tree already had none. Schema-admin-gated and destructive."));
            tools.Add(Write(services, TreeAdminSchemaToolHandlers.SetVersionConfigAsync, "lattice_treeadmin_schema_set_version_config",
                "Set a tree's schema version config",
                "Opts a tree in to envelope versioning (or replaces its existing config) from a schema-family id, a "
                + "target version (at least 1), and an optional strict-ingest flag, returning the installed config. "
                + "Requires schema versioning to be registered on the server. Schema-admin-gated and destructive."));
            tools.Add(Write(services, TreeAdminSchemaToolHandlers.ClearVersionConfigAsync, "lattice_treeadmin_schema_clear_version_config",
                "Clear a tree's schema version config",
                "Opts a tree back out of envelope versioning, returning true when a config was removed and false "
                + "when the tree was already unversioned. Requires schema versioning to be registered on the server. "
                + "Schema-admin-gated and destructive."));
            tools.Add(Write(services, TreeAdminSchemaToolHandlers.AdvanceTargetVersionAsync, "lattice_treeadmin_schema_advance_target_version",
                "Advance a tree's target schema version",
                "Advances a tree's monotonic target schema version to a strictly greater value, returning the "
                + "updated config. New writes stamp at the new target immediately; existing values upcast lazily on "
                + "read. Does not run an eager migration - use advance_and_migrate or migrate_to_target for that. "
                + "Schema-admin-gated and destructive."));
            tools.Add(Write(services, TreeAdminSchemaToolHandlers.AdvanceAndMigrateAsync, "lattice_treeadmin_schema_advance_and_migrate",
                "Advance and eagerly migrate a tree's schema",
                "Advances a tree's target schema version to a strictly greater value and kicks off a background "
                + "eager migration that re-stamps every existing value to the new target, returning the terminal "
                + "migration report. Schema-admin-gated and destructive."));
            tools.Add(Write(services, TreeAdminSchemaToolHandlers.MigrateToTargetVersionAsync, "lattice_treeadmin_schema_migrate_to_target",
                "Migrate a tree's values to its target schema version",
                "Runs (or idempotently resumes / no-ops) an eager migration that re-stamps every existing value of "
                + "a tree to the tree's current target version, returning the terminal migration report. "
                + "Schema-admin-gated and destructive."));
            tools.Add(Write(services, TreeAdminSchemaToolHandlers.RemediateAsync, "lattice_treeadmin_schema_remediate",
                "Remediate a tree's values against a target policy",
                "Starts (or idempotently resumes) a background remediation that rewrites every stored value of a "
                + "tree through a value transform and cuts the tree over once the transformed values satisfy a "
                + "target policy, returning the terminal report. Aborts without cutover on the first value the "
                + "transform cannot make compliant. Schema-admin-gated and destructive."));
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
