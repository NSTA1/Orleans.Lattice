namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The remote-host <see cref="ILatticeApiMcpUnsupportedToolSource"/>: the fixed
/// set of tools whose backing gRPC method is not yet bound, so they cannot be
/// invoked over the wire and are deferred (omitted) from a remote session's tool
/// set rather than listed-then-erroring. The set is a property of the remote gRPC
/// surface, not of per-group configuration, so it is static.
/// </summary>
/// <remarks>
/// The three <c>lattice_state_*</c> summaries have no gRPC method
/// (<c>GetTreeSummary</c> / <c>GetShardSummaries</c> / <c>GetPhysicalShardCount</c>)
/// and <c>lattice_backup_inventory</c> has no <c>GetInventory</c> binding. The
/// tree-administration schema-control tools (<c>lattice_treeadmin_schema_*</c>) are
/// backed by the in-process <c>ILatticeSchemaControl</c> facade, whose gRPC client
/// (<c>Orleans.Lattice.Api.Schema.Grpc</c>) is not referenced by this MCP package,
/// so the remote host cannot wire the facade and every schema tool is deferred. When
/// those gRPC methods / clients are added and wired, remove the corresponding name
/// here and the tool becomes discoverable remotely with no other change.
/// </remarks>
internal sealed class LatticeApiMcpRemoteUnsupportedToolSource : ILatticeApiMcpUnsupportedToolSource
{
    /// <summary>The <c>lattice_state_get_tree_summary</c> tool - no gRPC <c>GetTreeSummary</c> binding.</summary>
    public const string StateGetTreeSummary = "lattice_state_get_tree_summary";

    /// <summary>The <c>lattice_state_get_shard_summaries</c> tool - no gRPC <c>GetShardSummaries</c> binding.</summary>
    public const string StateGetShardSummaries = "lattice_state_get_shard_summaries";

    /// <summary>The <c>lattice_state_get_physical_shard_count</c> tool - no gRPC <c>GetPhysicalShardCount</c> binding.</summary>
    public const string StateGetPhysicalShardCount = "lattice_state_get_physical_shard_count";

    /// <summary>The <c>lattice_backup_inventory</c> tool - no gRPC <c>GetInventory</c> binding.</summary>
    public const string BackupInventory = "lattice_backup_inventory";

    /// <summary>The tree-administration schema-control tools, deferred remotely: the schema gRPC client is not referenced here.</summary>
    private static readonly string[] TreeAdminSchemaTools =
    {
        "lattice_treeadmin_schema_get_policy",
        "lattice_treeadmin_schema_list_dead_letters",
        "lattice_treeadmin_schema_count_dead_letters",
        "lattice_treeadmin_schema_get_version_config",
        "lattice_treeadmin_schema_get_remediation_status",
        "lattice_treeadmin_schema_scan_compliance",
        "lattice_treeadmin_schema_probe_capabilities",
        "lattice_treeadmin_schema_set_policy",
        "lattice_treeadmin_schema_clear_policy",
        "lattice_treeadmin_schema_set_version_config",
        "lattice_treeadmin_schema_clear_version_config",
        "lattice_treeadmin_schema_advance_target_version",
        "lattice_treeadmin_schema_advance_and_migrate",
        "lattice_treeadmin_schema_migrate_to_target",
        "lattice_treeadmin_schema_remediate",
    };

    private static readonly HashSet<string> Deferred = BuildDeferred();

    /// <summary>The tool names deferred under the remote-host topology, in a stable order.</summary>
    public static IReadOnlyList<string> DeferredToolNames { get; } = BuildDeferredList();

    /// <inheritdoc />
    public bool IsUnsupported(string toolName)
    {
        ArgumentNullException.ThrowIfNull(toolName);
        return Deferred.Contains(toolName);
    }

    private static string[] BuildDeferredList()
    {
        var names = new List<string>(4 + TreeAdminSchemaTools.Length)
        {
            StateGetTreeSummary,
            StateGetShardSummaries,
            StateGetPhysicalShardCount,
            BackupInventory,
        };
        names.AddRange(TreeAdminSchemaTools);
        return names.ToArray();
    }

    private static HashSet<string> BuildDeferred()
        => new(BuildDeferredList(), StringComparer.Ordinal);
}
