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
        Tools = Build(services, options.Value.EnableTreeAdminSchemaControlTools, options.Value.EnableTreeAdminLifecycleTools);
    }

    /// <inheritdoc />
    public LatticeApiMcpGroup Group => LatticeApiMcpGroup.TreeAdmin;

    /// <inheritdoc />
    public IReadOnlyList<McpServerTool> Tools { get; }

    private static IReadOnlyList<McpServerTool> Build(IServiceProvider services, bool enableSchemaControl, bool enableLifecycle)
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

            // ----- Diagnostics and storage accounting (read-only) -----
            Read(services, TreeAdminDiagnosticsToolHandlers.GetShardHotnessAsync, "lattice_treeadmin_shard_hotness",
                "Read a tree's per-shard hotness",
                "Reads a per-shard read/write hotness report for a tree: each physical shard's read and write "
                + "counters, observed operations-per-second, and the sampling window, plus tree-level totals. A "
                + "cheap, non-blocking sample used to spot skew (a few hot shards) before deciding on a reshard. "
                + "Requires whole-tree read authority. Read-only."),
            Read(services, TreeAdminDiagnosticsToolHandlers.GetDiagnosticsAsync, "lattice_treeadmin_shard_diagnostics",
                "Read a tree's shard diagnostics",
                "Reads a whole-tree diagnostic report: per-shard depth, root shape, live-key and tombstone counts, "
                + "tombstone ratio, activity counters, and in-flight maintenance flags, plus tree-level roll-ups. "
                + "The deep flag walks leaf state for authoritative counts (more expensive); the default reads the "
                + "cheap shard-root projection. Requires whole-tree read authority. Read-only."),
            Read(services, TreeAdminDiagnosticsToolHandlers.InspectShardMapAsync, "lattice_treeadmin_shard_map_inspect",
                "Inspect a tree's shard-map topology",
                "Inspects a tree's shard-map topology: the physical tree id it resolves to, the virtual routing "
                + "space size, the distinct physical shards the virtual slots map onto, and the map version (which "
                + "increments on every reshard). Requires whole-tree read authority. Read-only."),
            Read(services, TreeAdminDiagnosticsToolHandlers.GetProjectionDigestAsync, "lattice_treeadmin_projection_digest",
                "Read a shard's leaf-projection digest",
                "Reads a leaf-projection digest for a single physical shard of a tree: a lowercase-hex content hash "
                + "plus entry count, checkpoint offset, and version that identify the shard's committed state, for "
                + "cheap divergence detection without shipping the data. Requires whole-tree read authority. "
                + "Read-only."),
            Read(services, TreeAdminDiagnosticsToolHandlers.GetTreeStatsAsync, "lattice_treeadmin_tree_stats",
                "Read a tree's rolled-up statistics",
                "Reads a rolled-up statistics snapshot for a tree: shard and virtual-shard counts, live-key and "
                + "tombstone totals, and the storage byte breakdown (leaf state, snapshots, retained write-ahead "
                + "log, and total), in one call. Requires whole-tree read authority. Read-only."),
            Read(services, TreeAdminDiagnosticsToolHandlers.GetStorageUsageAsync, "lattice_treeadmin_storage_usage",
                "Read cluster-wide storage accounting",
                "Reads a cluster-wide storage accounting summary across every tree, split by surface (write-ahead "
                + "log, snapshots, leaf state) with per-tree breakdowns. The default returns the cheap cached "
                + "WAL-poll aggregate; the deep flag forces an expensive fresh leaf-walk that re-measures every "
                + "shard. Requires cluster telemetry authority. Read-only."),

            // ----- Tree lifecycle and registry config (read-only) -----
            Read(services, TreeAdminLifecycleToolHandlers.CheckTreeExistsAsync, "lattice_treeadmin_tree_exists",
                "Check whether a tree exists",
                "Reports whether a tree is registered in the tree registry. A pure existence check with no side "
                + "effects. Requires whole-tree read authority. Read-only."),
            Read(services, TreeAdminLifecycleToolHandlers.ResolveTreeAliasAsync, "lattice_treeadmin_tree_resolve_alias",
                "Resolve a tree's alias target",
                "Resolves the physical tree id a logical tree currently maps to, returning the logical id itself "
                + "when no alias is in effect. A pure read with no side effects. Requires whole-tree read authority. "
                + "Read-only."),
            Read(services, TreeAdminLifecycleToolHandlers.GetTreeConfigAsync, "lattice_treeadmin_tree_get_config",
                "Read a tree's registry configuration",
                "Reads a tree's registry-backed configuration: its structural sizing pins (shard count, node "
                + "fan-out), alias target, and per-tree runtime overrides (publish-events, projection-digest "
                + "maintenance, durable-history retention). An unregistered tree reports exists=false. Requires "
                + "whole-tree read authority. Read-only."),
            Read(services, TreeAdminLifecycleToolHandlers.GetShardMapAsync, "lattice_treeadmin_tree_get_shard_map",
                "Read a tree's persisted shard map",
                "Reads the registry-persisted shard map for a tree: whether a custom map has been persisted (versus "
                + "the default identity map) and, when it has, the persisted slot topology and map version. Distinct "
                + "from the live-routing shard-map inspection - this reflects durable registry truth. Requires "
                + "whole-tree read authority. Read-only."),
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

        if (enableLifecycle)
        {
            // ----- Tree lifecycle and registry config (destructive) -----
            tools.Add(Write(services, TreeAdminLifecycleToolHandlers.CreateTreeAsync, "lattice_treeadmin_tree_create",
                "Explicitly create a tree",
                "Explicitly creates (registers) a tree with an optional initial structural sizing (shard count, "
                + "leaf fan-out, internal fan-out), returning whether a new tree was registered and its effective "
                + "sizing. Idempotent: creating an existing tree preserves its configuration and reports "
                + "created=false. Rejected for a reserved system tree id. Admin-gated and destructive."));
            tools.Add(Write(services, TreeAdminLifecycleToolHandlers.SetTreeAliasAsync, "lattice_treeadmin_tree_set_alias",
                "Point a logical tree at a physical tree",
                "Points a logical tree at a physical tree so subsequent reads and writes routed through it target "
                + "the physical tree, returning the resulting alias state. Only a single level of indirection is "
                + "allowed - the physical target must not itself be aliased. Rejected for a reserved system tree id. "
                + "Admin-gated and destructive."));
            tools.Add(Write(services, TreeAdminLifecycleToolHandlers.SetTreeConfigAsync, "lattice_treeadmin_tree_set_config",
                "Update a tree's registry configuration",
                "Applies a partial update to a tree's per-tree runtime configuration (publish-events, "
                + "projection-digest maintenance, durable-history retention), returning the resulting config. Each "
                + "dimension is written only when its apply flag is set; a null value on an applied dimension clears "
                + "that override. Rejected for a reserved system tree id. Admin-gated and destructive."));
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
