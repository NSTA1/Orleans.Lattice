using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The state facade's per-facade MCP tool module. Implements
/// <see cref="ILatticeApiMcpToolGroup"/> for <see cref="LatticeApiMcpGroup.State"/>,
/// contributing the read-only tools that adapt <c>ILatticeStateQuery</c> so a
/// state-permitted caller can discover trees, views, and tag indexes and inspect
/// tree summaries, structure, entries, entry detail, and change history through
/// MCP. Every tool is annotated read-only and non-destructive; the module
/// exposes no mutation verb.
/// </summary>
/// <remarks>
/// <para>
/// The tools are built once in the constructor (the module is a DI singleton) so
/// the discovery core's per-session filtering selects from a prebuilt list and
/// never re-materialises a tool per <c>tools/list</c>. Each tool is a stateless
/// adapter: it resolves <c>ILatticeStateQuery</c> from the tool invocation's
/// request service provider (the credential the caller presented already flows on
/// the ambient context via the fail-closed credential bridge, so the facade's own
/// authorizer seam enforces per-tree / per-key visibility). The module adds no
/// authorization path of its own - the discovery core advertises these tools only
/// to a caller granted the state group.
/// </para>
/// <para>
/// The <see cref="IServiceProvider"/> supplied to the constructor is used only at
/// build time, so the SDK recognises <c>ILatticeStateQuery</c> as a
/// service-injected parameter and excludes it from every tool's input schema; the
/// instance itself is resolved per invocation from the request's service scope.
/// </para>
/// </remarks>
internal sealed class StateToolGroup : ILatticeApiMcpToolGroup
{
    /// <summary>
    /// Builds the read-only state tool set once from the supplied service
    /// provider, which the SDK consults to mark <c>ILatticeStateQuery</c> as a
    /// DI-injected (schema-excluded) tool parameter.
    /// </summary>
    /// <param name="services">
    /// The service provider whose <c>IServiceProviderIsService</c> recognises the
    /// registered <c>ILatticeStateQuery</c> facade.
    /// </param>
    public StateToolGroup(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);

        Tools = new McpServerTool[]
        {
            Create(
                services,
                StateToolHandlers.GetClusterInfoAsync,
                "lattice_state_get_cluster_info",
                "Get cluster info",
                "Returns the connected cluster's identity (Orleans cluster and service ids). Read-only."),
            Create(
                services,
                StateToolHandlers.ListTreesAsync,
                "lattice_state_list_trees",
                "List trees",
                "Enumerates the registered trees as a paged, id-ordered catalog with lifecycle state, shard "
                + "count, and effective configuration. Reserved system trees are hidden unless requested. Read-only."),
            Create(
                services,
                StateToolHandlers.ListViewsAsync,
                "lattice_state_list_views",
                "List views",
                "Enumerates the materialised views as a paged, name-ordered catalog, optionally with each view's "
                + "apply lag and materialised entry count. Read-only."),
            Create(
                services,
                StateToolHandlers.ListTagIndexesAsync,
                "lattice_state_list_tag_indexes",
                "List tag indexes",
                "Enumerates the tag-index membership trees as a paged catalog, optionally scoped to those covering "
                + "one source tree. Read-only."),
            Create(
                services,
                StateToolHandlers.ListTagValuesAsync,
                "lattice_state_list_tag_values",
                "List tag values",
                "Enumerates the distinct tag values one tag index carries over one subject tree, in ascending "
                + "ordinal order, as a paged catalog. Read-only."),
            Create(
                services,
                StateToolHandlers.ListCoveredTreesAsync,
                "lattice_state_list_covered_trees",
                "List covered trees",
                "Enumerates the subject trees a single tag index covers, in ascending ordinal order, as a paged "
                + "catalog. Read-only."),
            Create(
                services,
                StateToolHandlers.ListIndexTagsAsync,
                "lattice_state_list_index_tags",
                "List index tags",
                "Enumerates the distinct tag values a single tag index carries across every tree it covers, in "
                + "ascending ordinal order, as a paged catalog. Read-only."),
            Create(
                services,
                StateToolHandlers.ScanTagMembersAsync,
                "lattice_state_scan_tag_members",
                "Scan tag members",
                "Enumerates the live (tree, key) members of a single tag across every tree a tag index covers, in "
                + "ascending (tree id, key) order, as a paged catalog. Read-only."),
            Create(
                services,
                StateToolHandlers.GetTreeSummaryAsync,
                "lattice_state_get_tree_summary",
                "Get tree summary",
                "Returns a point-in-time summary of one tree, or a typed not-found when the tree does not exist. "
                + "Read-only."),
            Create(
                services,
                StateToolHandlers.GetShardSummariesAsync,
                "lattice_state_get_shard_summaries",
                "Get shard summaries",
                "Returns the per-shard summaries of one tree, ordered by shard index, or a typed not-found. This "
                + "fans a diagnostics read out to every shard. Read-only."),
            Create(
                services,
                StateToolHandlers.GetPhysicalShardCountAsync,
                "lattice_state_get_physical_shard_count",
                "Get physical shard count",
                "Returns the number of physical shards currently owning virtual slots for one tree via a single "
                + "fan-out-free routing read (safe against a saturated tree), or a typed not-found. Read-only."),
            Create(
                services,
                StateToolHandlers.GetTreeStructureAsync,
                "lattice_state_get_tree_structure",
                "Get tree structure",
                "Returns the bounded structural node graph of one tree (shard roots, internal nodes, leaves), depth- "
                + "and node-budget limited, optionally scoped to one shard or descended into a named node. Read-only."),
            Create(
                services,
                StateToolHandlers.ScanEntriesAsync,
                "lattice_state_scan_entries",
                "Scan entries",
                "Scans a key-ordered, paged page of one tree's live entries, optionally scoped to a key range or "
                + "filtered by a tag index, with a size-bounded per-entry value preview. Excludes tombstoned and "
                + "TTL-expired entries. Read-only."),
            Create(
                services,
                StateToolHandlers.GetEntryAsync,
                "lattice_state_get_entry",
                "Get entry",
                "Returns the full record for a single key with a larger value-preview budget than a scan, or a typed "
                + "not-found that distinguishes an unknown tree from a missing key. Read-only."),
            Create(
                services,
                StateToolHandlers.GetEntryHistoryAsync,
                "lattice_state_get_entry_history",
                "Get entry history",
                "Reads a single key's change-history timeline as a continuation-paged page of revision records, "
                + "reporting whether the timeline is durable-bounded or a truncated fallback. Read-only."),
            Create(
                services,
                StateToolHandlers.CancelScanAsync,
                "lattice_state_cancel_scan",
                "Cancel scan",
                "Releases the server-side snapshot cursor named by a scan continuation token, freeing its WAL "
                + "retention pin and per-shard baseline promptly. Best-effort and idempotent. Read-only."),
        };
    }

    /// <inheritdoc />
    public LatticeApiMcpGroup Group => LatticeApiMcpGroup.State;

    /// <inheritdoc />
    public IReadOnlyList<McpServerTool> Tools { get; }

    private static McpServerTool Create(
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
}
