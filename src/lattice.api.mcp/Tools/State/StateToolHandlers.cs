using System.ComponentModel;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The read-only adapter methods behind the state tool module. Each method is a
/// thin binding over a single <see cref="ILatticeStateQuery"/> call: it maps the
/// tool's scalar arguments onto the facade's request model and returns the
/// facade result unchanged, so the read semantics live in the facade and no
/// query logic is re-implemented here.
/// </summary>
/// <remarks>
/// <para>
/// The <see cref="ILatticeStateQuery"/> parameter on every method is resolved
/// from the tool invocation's request service provider by the MCP SDK (it is
/// excluded from each tool's input schema); the <see cref="CancellationToken"/>
/// is bound to the invocation's token. The remaining, schema-visible arguments
/// carry the caller's paging, key-range, filtering, and value-preview budgets.
/// </para>
/// <para>
/// Every method is non-mutating. Typed not-found outcomes (unknown tree, missing
/// key) are surfaced on the returned result's <c>Status</c> rather than thrown,
/// so an agent observes a structured result instead of a transport fault.
/// </para>
/// </remarks>
internal static class StateToolHandlers
{
    /// <summary>Returns identity and metadata for the connected cluster.</summary>
    public static Task<ClusterInfo> GetClusterInfoAsync(
        ILatticeStateQuery query,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.GetClusterInfoAsync(cancellationToken);
    }

    /// <summary>Enumerates the registered trees as a paged catalog.</summary>
    public static Task<TreeCatalogPage> ListTreesAsync(
        ILatticeStateQuery query,
        CancellationToken cancellationToken,
        [Description("Max entries per page (1-1000; values outside the range fall back to 100).")]
        int pageSize = CatalogRequest.DefaultPageSize,
        [Description("Exclusive continuation cursor: pass the previous page's nextPageToken. Null starts at the beginning.")]
        string? pageToken = null,
        [Description("Include reserved internal system trees (registry, WAL, queue, view backing). Defaults to false.")]
        bool includeSystemTrees = false)
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.ListTreesAsync(
            new CatalogRequest
            {
                PageSize = pageSize,
                PageToken = pageToken,
                IncludeSystemTrees = includeSystemTrees,
            },
            cancellationToken);
    }

    /// <summary>Enumerates the materialised views as a paged catalog.</summary>
    public static Task<ViewCatalogPage> ListViewsAsync(
        ILatticeStateQuery query,
        CancellationToken cancellationToken,
        [Description("Max entries per page (1-1000; values outside the range fall back to 100).")]
        int pageSize = CatalogRequest.DefaultPageSize,
        [Description("Exclusive continuation cursor: pass the previous page's nextPageToken. Null starts at the beginning.")]
        string? pageToken = null,
        [Description("Sample each view's apply lag and materialised entry count. More expensive; defaults to false.")]
        bool includeViewStats = false)
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.ListViewsAsync(
            new CatalogRequest
            {
                PageSize = pageSize,
                PageToken = pageToken,
                IncludeViewStats = includeViewStats,
            },
            cancellationToken);
    }

    /// <summary>Enumerates the tag-index membership trees as a paged catalog.</summary>
    public static Task<TagIndexCatalogPage> ListTagIndexesAsync(
        ILatticeStateQuery query,
        CancellationToken cancellationToken,
        [Description("Max entries per page (1-1000; values outside the range fall back to 100).")]
        int pageSize = CatalogRequest.DefaultPageSize,
        [Description("Exclusive continuation cursor: pass the previous page's nextPageToken. Null starts at the beginning.")]
        string? pageToken = null,
        [Description("When set, restrict the catalog to indexes covering this source tree. Null lists all tag indexes.")]
        string? sourceTreeId = null)
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.ListTagIndexesAsync(
            new CatalogRequest
            {
                PageSize = pageSize,
                PageToken = pageToken,
                SourceTreeId = sourceTreeId,
            },
            cancellationToken);
    }

    /// <summary>Enumerates the distinct tag values of one tag index over one subject tree.</summary>
    public static Task<TagValueCatalogPage> ListTagValuesAsync(
        ILatticeStateQuery query,
        CancellationToken cancellationToken,
        [Description("The tag-index name (the clean name from list_tag_indexes).")]
        string indexName,
        [Description("The subject tree id whose distinct tag values to list.")]
        string sourceTreeId,
        [Description("Max entries per page (1-1000; values outside the range fall back to 100).")]
        int pageSize = CatalogRequest.DefaultPageSize,
        [Description("Exclusive continuation cursor: pass the previous page's nextPageToken. Null starts at the beginning.")]
        string? pageToken = null)
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.ListTagValuesAsync(
            new CatalogRequest
            {
                IndexName = indexName,
                SourceTreeId = sourceTreeId,
                PageSize = pageSize,
                PageToken = pageToken,
            },
            cancellationToken);
    }

    /// <summary>Enumerates the subject trees a single tag index covers.</summary>
    public static Task<CoveredTreeCatalogPage> ListCoveredTreesAsync(
        ILatticeStateQuery query,
        CancellationToken cancellationToken,
        [Description("The tag-index name whose covered subject trees to list.")]
        string indexName,
        [Description("Max entries per page (1-1000; values outside the range fall back to 100).")]
        int pageSize = CatalogRequest.DefaultPageSize,
        [Description("Exclusive continuation cursor: pass the previous page's nextPageToken. Null starts at the beginning.")]
        string? pageToken = null)
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.ListCoveredTreesAsync(
            new CatalogRequest
            {
                IndexName = indexName,
                PageSize = pageSize,
                PageToken = pageToken,
            },
            cancellationToken);
    }

    /// <summary>Enumerates a tag index's distinct tag values across every covered tree.</summary>
    public static Task<TagValueCatalogPage> ListIndexTagsAsync(
        ILatticeStateQuery query,
        CancellationToken cancellationToken,
        [Description("The tag-index name whose index-wide distinct tag values to list.")]
        string indexName,
        [Description("Max entries per page (1-1000; values outside the range fall back to 100).")]
        int pageSize = CatalogRequest.DefaultPageSize,
        [Description("Exclusive continuation cursor: pass the previous page's nextPageToken. Null starts at the beginning.")]
        string? pageToken = null)
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.ListIndexTagsAsync(
            new CatalogRequest
            {
                IndexName = indexName,
                PageSize = pageSize,
                PageToken = pageToken,
            },
            cancellationToken);
    }

    /// <summary>Enumerates the live members of a single tag across a tag index.</summary>
    public static Task<TagMemberScanPage> ScanTagMembersAsync(
        ILatticeStateQuery query,
        CancellationToken cancellationToken,
        [Description("The tag-index name whose members to scan.")]
        string indexName,
        [Description("The tag whose live (tree, key) member pairs to return.")]
        string tag,
        [Description("Max entries per page (1-1000; values outside the range fall back to 100).")]
        int pageSize = TagMemberScanRequest.DefaultPageSize,
        [Description("Exclusive continuation cursor: pass the previous page's nextPageToken. Null starts at the beginning.")]
        string? pageToken = null)
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.ScanTagMembersAsync(
            new TagMemberScanRequest
            {
                IndexName = indexName,
                Tag = tag,
                PageSize = pageSize,
                PageToken = pageToken,
            },
            cancellationToken);
    }

    /// <summary>Returns a point-in-time summary of a single tree.</summary>
    public static Task<TreeSummaryResult> GetTreeSummaryAsync(
        ILatticeStateQuery query,
        CancellationToken cancellationToken,
        [Description("Logical tree identifier.")]
        string treeId,
        [Description("Include tombstone counts (a more expensive read). Defaults to true.")]
        bool deep = true)
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.GetTreeSummaryAsync(treeId, deep, cancellationToken);
    }

    /// <summary>Returns the per-shard summaries of a single tree, ordered by shard index.</summary>
    public static Task<ShardSummariesResult> GetShardSummariesAsync(
        ILatticeStateQuery query,
        CancellationToken cancellationToken,
        [Description("Logical tree identifier.")]
        string treeId,
        [Description("Include tombstone counts (a more expensive read). Defaults to true.")]
        bool deep = true)
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.GetShardSummariesAsync(treeId, deep, cancellationToken);
    }

    /// <summary>
    /// Returns the number of physical shards currently owning virtual slots for a
    /// tree. This is a single fan-out-free routing read, safe against a saturated
    /// tree; the result reports a typed not-found for an unknown tree.
    /// </summary>
    public static async Task<PhysicalShardCountResult> GetPhysicalShardCountAsync(
        ILatticeStateQuery query,
        CancellationToken cancellationToken,
        [Description("Logical tree identifier.")]
        string treeId)
    {
        ArgumentNullException.ThrowIfNull(query);
        var count = await query.GetPhysicalShardCountAsync(treeId, cancellationToken).ConfigureAwait(false);
        return new PhysicalShardCountResult { TreeId = treeId, PhysicalShardCount = count };
    }

    /// <summary>
    /// Returns the bounded structural node graph of a tree (shard roots, internal
    /// nodes, leaves), depth- and node-budget limited, optionally scoped to one
    /// shard or descended into a named internal node.
    /// </summary>
    public static Task<TreeStructureResult> GetTreeStructureAsync(
        ILatticeStateQuery query,
        CancellationToken cancellationToken,
        [Description("Logical tree identifier.")]
        string treeId,
        [Description("When set, restrict the response to the single shard with this index instead of every shard root.")]
        int? shardIndex = null,
        [Description("When set, descend into the internal node with this id (a nodeId from a prior response) and return only that subtree.")]
        string? subPathNodeId = null,
        [Description("Max depth of internal-node expansion (0-64; values outside the range fall back to 4).")]
        int depthLimit = StructureRequest.DefaultDepthLimit,
        [Description("Max nodes materialised across the whole response (1-100000; values outside the range fall back to 1000).")]
        int maxNodes = StructureRequest.DefaultMaxNodes)
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.GetTreeStructureAsync(
            new StructureRequest
            {
                TreeId = treeId,
                ShardIndex = shardIndex,
                SubPathNodeId = subPathNodeId,
                DepthLimit = depthLimit,
                MaxNodes = maxNodes,
            },
            cancellationToken);
    }

    /// <summary>
    /// Scans a key-ordered, paged page of a tree's live entries, optionally
    /// scoped to a key range or filtered by a tag index, with a size-bounded
    /// per-entry value preview. Tombstoned and TTL-expired entries are excluded.
    /// </summary>
    public static Task<EntryScanResult> ScanEntriesAsync(
        ILatticeStateQuery query,
        CancellationToken cancellationToken,
        [Description("Logical tree identifier to scan.")]
        string treeId,
        [Description("Inclusive lower key bound, or null to start at the first key.")]
        string? startInclusive = null,
        [Description("Exclusive upper key bound, or null to run to the last key.")]
        string? endExclusive = null,
        [Description("Scan in descending key order. Defaults to false.")]
        bool reverse = false,
        [Description("Max entries per page (values <= 0 use the configured default; larger values are clamped to the configured maximum).")]
        int pageSize = 0,
        [Description("Opaque continuation token from a prior page; null or empty opens a fresh scan. On a continuation, mode is ignored.")]
        string? continuationToken = null,
        [Description("Per-entry value-preview byte budget (values <= 0 use the configured default; larger values are clamped). The full value length is always reported.")]
        int valuePreviewBudget = 0,
        [Description("Optional tag-index name; with tag, restricts the scan to keys carrying that tag. Null for an unfiltered scan.")]
        string? indexName = null,
        [Description("Optional tag value; honoured only when indexName is also set. A tag-filtered scan ignores the key range.")]
        string? tag = null,
        [Description("Cursor isolation for a fresh scan: Snapshot (0, point-in-time, heaviest), Live (1, cheapest), or LivePointInTime (2). Defaults to Snapshot.")]
        EntryScanMode mode = EntryScanMode.Snapshot)
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.ScanEntriesAsync(
            new EntryScanRequest
            {
                TreeId = treeId,
                StartInclusive = startInclusive,
                EndExclusive = endExclusive,
                Reverse = reverse,
                PageSize = pageSize,
                ContinuationToken = continuationToken,
                ValuePreviewBudget = valuePreviewBudget,
                IndexName = indexName,
                Tag = tag,
                Mode = mode,
            },
            cancellationToken);
    }

    /// <summary>
    /// Returns the full record for a single key (with a larger value-preview
    /// budget than a scan), reporting a typed not-found that distinguishes an
    /// unknown tree from a missing key.
    /// </summary>
    public static Task<EntryDetailResult> GetEntryAsync(
        ILatticeStateQuery query,
        CancellationToken cancellationToken,
        [Description("Logical tree identifier.")]
        string treeId,
        [Description("The entry key to read.")]
        string key)
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.GetEntryAsync(treeId, key, cancellationToken);
    }

    /// <summary>
    /// Reads a single key's change-history timeline as a continuation-paged page
    /// of revision records. The result reports whether the timeline is
    /// durable-bounded or a truncated write-ahead-log fallback.
    /// </summary>
    public static Task<EntryHistoryResult> GetEntryHistoryAsync(
        ILatticeStateQuery query,
        CancellationToken cancellationToken,
        [Description("Logical tree identifier the key lives on.")]
        string treeId,
        [Description("The key whose revision timeline to read.")]
        string key,
        [Description("Max revisions per page (values <= 0 use the configured default; larger values are clamped to the configured maximum).")]
        int limit = 0,
        [Description("Opaque continuation token from a prior page; null or empty starts a fresh read. Paging advances oldest to newest.")]
        string? continuationToken = null,
        [Description("Per-revision value/delta preview byte budget (values <= 0 use the configured default; larger values are clamped). The full value length is always reported.")]
        int valuePreviewBudget = 0,
        [Description("Order revisions within each returned page newest-first. Defaults to false (oldest-first).")]
        bool reverse = false)
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.GetEntryHistoryAsync(
            new EntryHistoryRequest
            {
                TreeId = treeId,
                Key = key,
                Limit = limit,
                ContinuationToken = continuationToken,
                ValuePreviewBudget = valuePreviewBudget,
                Reverse = reverse,
            },
            cancellationToken);
    }

    /// <summary>
    /// Releases the server-side snapshot cursor named by a scan continuation
    /// token, freeing its WAL-retention pin and per-shard baseline promptly. The
    /// operation is best-effort and idempotent.
    /// </summary>
    public static async Task<ScanCancellationResult> CancelScanAsync(
        ILatticeStateQuery query,
        CancellationToken cancellationToken,
        [Description("Logical tree the cursor was opened against.")]
        string treeId,
        [Description("The scan continuation token to release. An empty or unknown token is a tolerated no-op.")]
        string? continuationToken = null)
    {
        ArgumentNullException.ThrowIfNull(query);
        await query.CancelScanAsync(treeId, continuationToken, cancellationToken).ConfigureAwait(false);
        return new ScanCancellationResult { TreeId = treeId };
    }
}
