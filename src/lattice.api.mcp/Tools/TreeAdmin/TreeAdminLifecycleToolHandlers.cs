using System.ComponentModel;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The thin adapter methods the tree-administration tool module exposes as MCP
/// tree-lifecycle tools: explicit tree creation, existence checks, alias
/// assignment / resolution, per-tree configuration read / update, and the
/// registry-persisted shard-map read. Every method is a stateless, static shim over
/// the transport-agnostic <see cref="ILatticeTreeAdmin"/> facade: it resolves the
/// facade from the tool invocation's request service provider (bound by the MCP SDK
/// from <c>RequestContext.Services</c>), marshals the tool-call arguments into the
/// facade's parameters, and returns the facade result verbatim. No authorization or
/// lifecycle logic lives here - the facade owns it, and its fail-closed access gate
/// (whole-tree read for the read verbs, whole-tree administration for the mutating
/// verbs) refuses an unauthorized caller even if one somehow reaches an invocation.
/// </summary>
/// <remarks>
/// The mutating verbs (create, set-alias, set-config) are contributed only when the
/// host opts in via <see cref="LatticeApiMcpOptions.EnableTreeAdminLifecycleTools"/>;
/// the read verbs (exists, resolve-alias, get-config, get-shard-map) are always
/// contributed. The methods are held as static method groups so the tool module
/// materialises each tool's delegate exactly once when it builds its tool list, never
/// per <c>tools/call</c>. The facade DTOs are reused verbatim as the tool result
/// shapes, so this surface adds no new serializable wire type.
/// </remarks>
internal static class TreeAdminLifecycleToolHandlers
{
    /// <summary>Explicitly creates (registers) a tree with an optional initial sizing. Idempotent.</summary>
    public static Task<TreeCreationResult> CreateTreeAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The tree to create. Must not be null, empty, or a reserved system tree id (the '_lattice_' namespace).")]
        string treeId,
        [Description("The initial physical shard count to pin, or null for the library default. Honoured only when the tree is created for the first time.")]
        int? shardCount = null,
        [Description("The initial maximum keys per leaf node to pin, or null for the library default. Honoured only on first creation.")]
        int? maxLeafKeys = null,
        [Description("The initial maximum children per internal node to pin, or null for the library default. Honoured only on first creation.")]
        int? maxInternalChildren = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        return treeAdmin.CreateTreeAsync(treeId, shardCount, maxLeafKeys, maxInternalChildren, cancellationToken);
    }

    /// <summary>Reports whether a tree is registered.</summary>
    public static Task<TreeExistenceResult> CheckTreeExistsAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The tree to check for registration. Must not be null or empty.")]
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        return treeAdmin.CheckTreeExistsAsync(treeId, cancellationToken);
    }

    /// <summary>Points a logical tree at a physical tree.</summary>
    public static Task<TreeAliasResolution> SetTreeAliasAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The logical tree to alias. Must not be null, empty, or a reserved system tree id.")]
        string treeId,
        [Description("The physical tree the logical tree should resolve to. Must not be null or empty, must differ from treeId, and must not itself be aliased.")]
        string physicalTreeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        return treeAdmin.SetTreeAliasAsync(treeId, physicalTreeId, cancellationToken);
    }

    /// <summary>Resolves the physical tree a logical tree maps to.</summary>
    public static Task<TreeAliasResolution> ResolveTreeAliasAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The logical tree to resolve to its physical target. Must not be null or empty.")]
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        return treeAdmin.ResolveTreeAliasAsync(treeId, cancellationToken);
    }

    /// <summary>Reads a tree's registry-backed configuration.</summary>
    public static Task<TreeConfigurationReport> GetTreeConfigAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The tree whose registry-backed configuration to read. Must not be null or empty.")]
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        return treeAdmin.GetTreeConfigAsync(treeId, cancellationToken);
    }

    /// <summary>Applies a partial per-tree configuration update.</summary>
    public static Task<TreeConfigurationReport> SetTreeConfigAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The tree to configure. Must not be null, empty, or a reserved system tree id.")]
        string treeId,
        [Description("When true, write the publishEvents override; when false, leave the tree's publish-events override unchanged.")]
        bool applyPublishEvents = false,
        [Description("The publish-events override to pin (true/false), or null to clear it (fall back to the silo-wide option). Honoured only when applyPublishEvents is true.")]
        bool? publishEvents = null,
        [Description("When true, write the maintainProjectionDigest override; when false, leave the tree's projection-digest override unchanged.")]
        bool applyMaintainProjectionDigest = false,
        [Description("The projection-digest-maintenance override to pin (true/false), or null to clear it. Honoured only when applyMaintainProjectionDigest is true. The permanent-disable latch, once set, supersedes a pinned true.")]
        bool? maintainProjectionDigest = null,
        [Description("When true, write the durable-history retention override (mode and window, each cleared independently by a null value); when false, leave the tree's history-retention override unchanged.")]
        bool applyHistoryRetention = false,
        [Description("The durable-history retention mode to pin, or null to clear it. Honoured only when applyHistoryRetention is true.")]
        HistoryRetentionMode? historyRetentionMode = null,
        [Description("The durable-history age bound in ticks to pin, or null to clear it. Must be strictly positive when supplied. Honoured only when applyHistoryRetention is true.")]
        long? historyRetentionWindowTicks = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        var update = new TreeConfigurationUpdate
        {
            ApplyPublishEvents = applyPublishEvents,
            PublishEvents = publishEvents,
            ApplyMaintainProjectionDigest = applyMaintainProjectionDigest,
            MaintainProjectionDigest = maintainProjectionDigest,
            ApplyHistoryRetention = applyHistoryRetention,
            HistoryRetentionMode = historyRetentionMode,
            HistoryRetentionWindowTicks = historyRetentionWindowTicks,
        };
        return treeAdmin.SetTreeConfigAsync(treeId, update, cancellationToken);
    }

    /// <summary>Reads a tree's registry-persisted shard map.</summary>
    public static Task<TreeShardMapView> GetShardMapAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The tree whose registry-persisted shard map to read. Must not be null or empty.")]
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        return treeAdmin.GetShardMapAsync(treeId, cancellationToken);
    }
}
