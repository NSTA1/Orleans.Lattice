using System.ComponentModel;
using Orleans.Lattice.Api.Data;
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

    /// <summary>Reads a tree's soft-deletion status.</summary>
    public static Task<TreeDeletionStatus> GetTreeDeletionStatusAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The tree whose soft-deletion status to read. Must not be null or empty.")]
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        return treeAdmin.GetTreeDeletionStatusAsync(treeId, cancellationToken);
    }

    /// <summary>Soft-deletes a tree, opening its recovery window.</summary>
    public static Task<TreeDeletionStatus> DeleteTreeAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The tree to soft-delete. Must not be null, empty, or a reserved system tree id.")]
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        return treeAdmin.DeleteTreeAsync(treeId, cancellationToken);
    }

    /// <summary>Recovers a soft-deleted tree within its recovery window.</summary>
    public static Task<TreeDeletionStatus> RecoverTreeAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The soft-deleted tree to recover. Must not be null, empty, or a reserved system tree id.")]
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        return treeAdmin.RecoverTreeAsync(treeId, cancellationToken);
    }

    /// <summary>Irreversibly hard-purges a soft-deleted tree.</summary>
    public static Task<TreeDeletionStatus> PurgeTreeAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The soft-deleted tree to hard-purge. Must not be null, empty, or a reserved system tree id.")]
        string treeId,
        [Description("Must be set to true to acknowledge the irreversible destruction of the tree's data. A false or omitted value is rejected.")]
        bool confirm = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        return treeAdmin.PurgeTreeAsync(treeId, confirm, cancellationToken);
    }

    /// <summary>Opens a bulk-load session over an empty tree under a stable, idempotent operation id.</summary>
    public static Task<TreeBulkLoadSession> BeginBulkLoadAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The empty tree to bulk-load. Must not be null, empty, or a reserved system tree id.")]
        string treeId,
        [Description("The caller's stable, idempotent bulk-load operation id. Must be non-empty and must not contain '/'. Reuse the same id across a resumed stream so re-driven chunks are deduplicated.")]
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        return treeAdmin.BeginBulkLoadAsync(treeId, operationId, cancellationToken);
    }

    /// <summary>Grafts one strictly-ascending chunk of entries onto an open bulk-load session.</summary>
    public static Task<TreeBulkLoadChunkAck> AppendBulkLoadAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The tree being bulk-loaded. Must not be null, empty, or a reserved system tree id.")]
        string treeId,
        [Description("The bulk-load operation id supplied to the begin call. Must be non-empty and must not contain '/'.")]
        string operationId,
        [Description("The zero-based, monotonically increasing chunk index. Re-sending the same index with the same operation id is idempotent, so a broken stream resumes from its last un-acknowledged chunk.")]
        long chunkIndex,
        [Description("The chunk's entries, in strictly ascending key order. Keys must not repeat within the chunk; the caller is responsible for keeping keys ascending across chunk boundaries too.")]
        IReadOnlyList<DataEntryDto>? entries = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        return treeAdmin.AppendBulkLoadAsync(treeId, operationId, chunkIndex, ToDataEntries(entries), cancellationToken);
    }

    /// <summary>Closes an open bulk-load session and reports its summary.</summary>
    public static Task<TreeBulkLoadResult> CommitBulkLoadAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The tree being bulk-loaded. Must not be null, empty, or a reserved system tree id.")]
        string treeId,
        [Description("The bulk-load operation id supplied to the begin call. Must be non-empty and must not contain '/'.")]
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        return treeAdmin.CommitBulkLoadAsync(treeId, operationId, cancellationToken);
    }

    /// <summary>Restores a captured backup into a tree via an online, reversible shadow-cutover.</summary>
    public static Task<TreeRestoreResult> RestoreTreeAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The tree to restore the backup into. Must not be null, empty, or a reserved system tree id.")]
        string treeId,
        [Description("The content-addressed id of the backup to restore. Must not be null or empty.")]
        string backupId,
        [Description("An optional idempotency key that makes a retried restore a no-op, or null to derive one from the request. Must be non-empty when supplied.")]
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        return treeAdmin.RestoreTreeAsync(treeId, backupId, operationId, cancellationToken);
    }

    /// <summary>Restores a captured backup set as a single all-or-nothing unit.</summary>
    public static async Task<TreeRestoreSetResult> RestoreTreeSetAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The content-addressed id of the backup set to restore. Must not be null or empty.")]
        string setId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        var results = await treeAdmin.RestoreTreeSetAsync(setId, cancellationToken).ConfigureAwait(false);
        return new TreeRestoreSetResult { Results = results };
    }

    /// <summary>Reverts a shadow-cutover restore, swapping the target tree's alias back to its pre-restore physical tree.</summary>
    public static async Task<TreeRestoreResult> RevertTreeRestoreAsync(
        ILatticeTreeAdmin treeAdmin,
        [Description("The tree whose restore is being reverted. Must match the restore result's target tree and must not be a reserved system tree id.")]
        string targetTreeId,
        [Description("The backup id that was restored, from the tree_restore result.")]
        string backupId,
        [Description("The idempotency key from the tree_restore result.")]
        string operationId,
        [Description("The physical tree the alias now points at, from the tree_restore result's shadowPhysicalTreeId. Required: a restore with no shadow physical tree is not revertible.")]
        string shadowPhysicalTreeId,
        [Description("The physical tree the alias pointed at before the restore, from the tree_restore result's previousPhysicalTreeId. Required to swap the alias back.")]
        string previousPhysicalTreeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        var restore = new TreeRestoreResult
        {
            BackupId = backupId,
            TargetTreeId = targetTreeId,
            Mode = TreeRestoreMode.ShadowCutover,
            OperationId = operationId,
            ManifestChain = [],
            EntriesApplied = 0,
            ShadowPhysicalTreeId = shadowPhysicalTreeId,
            PreviousPhysicalTreeId = previousPhysicalTreeId,
        };
        await treeAdmin.RevertTreeRestoreAsync(restore, cancellationToken).ConfigureAwait(false);
        return restore;
    }

    private static List<DataEntry> ToDataEntries(IReadOnlyList<DataEntryDto>? entries)
    {
        if (entries is null || entries.Count == 0)
        {
            return [];
        }

        var mapped = new List<DataEntry>(entries.Count);
        for (var i = 0; i < entries.Count; i++)
        {
            mapped.Add(new DataEntry { Key = entries[i].Key, Value = entries[i].Value });
        }

        return mapped;
    }
}
