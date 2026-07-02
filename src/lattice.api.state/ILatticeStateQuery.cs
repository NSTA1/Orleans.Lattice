namespace Orleans.Lattice.Api.State;

/// <summary>
/// Transport-agnostic read facade over a cluster's lattice state. Every
/// transport binding (the gRPC service now, a future MCP surface) is a thin
/// adapter over this single surface, so the read semantics are written and
/// tested once and no transport concern leaks into the query logic.
/// </summary>
/// <remarks>
/// The facade is read-only: it never mutates tree data or configuration. It
/// aggregates the metadata the core library already exposes (the tree
/// registry, <c>DiagnoseAsync</c>, per-shard digests) and performs a bounded
/// number of grain calls per request - O(1) for a tree-level summary.
/// </remarks>
internal interface ILatticeStateQuery
{
    /// <summary>
    /// Returns a point-in-time summary of the given tree, or a typed
    /// not-found result when the tree does not exist.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="deep">
    /// When <see langword="true"/>, includes tombstone counts (a more
    /// expensive read); when <see langword="false"/>, tombstone counts are
    /// reported as <c>0</c>.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<TreeSummaryResult> GetTreeSummaryAsync(
        string treeId,
        bool deep = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the per-shard summaries for the given tree, ordered by shard
    /// index, or a typed not-found result when the tree does not exist.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="deep">Whether to include tombstone counts.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<ShardSummariesResult> GetShardSummariesAsync(
        string treeId,
        bool deep = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the number of physical shards currently owning virtual slots for
    /// the given tree, read from the tree's routing map, or <see langword="null"/>
    /// when the tree does not exist. This is a single, fan-out-free grain call
    /// (it reads the shard-count from routing and never walks the per-shard leaf
    /// chains), so it is safe to call against a saturated tree whose shard roots
    /// are already contended - unlike <see cref="GetShardSummariesAsync"/>, which
    /// fans a diagnostics read out to every shard.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<int?> GetPhysicalShardCountAsync(
        string treeId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Enumerates the trees registered in the cluster as a paged, deterministic
    /// catalog ordered by tree id. Each entry carries the tree's lifecycle
    /// state, shard count, alias transparency, and effective configuration.
    /// Reserved internal system trees are hidden unless
    /// <see cref="CatalogRequest.IncludeSystemTrees"/> is set.
    /// </summary>
    /// <param name="request">Paging and filtering options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<TreeCatalogPage> ListTreesAsync(
        CatalogRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Enumerates the materialised views known to the cluster as a paged,
    /// deterministic catalog ordered by view name. Each entry carries the
    /// view's source tree and (when
    /// <see cref="CatalogRequest.IncludeViewStats"/> is set) its apply lag and
    /// materialised entry count.
    /// </summary>
    /// <param name="request">Paging and filtering options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<ViewCatalogPage> ListViewsAsync(
        CatalogRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns identity and high-level metadata for the cluster this query
    /// serves (the connected silo's Orleans cluster / service id). Intended for
    /// a client to display which cluster it is connected to. The result is
    /// generic and additive so further cluster metadata can be surfaced later
    /// without a new method.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<ClusterInfo> GetClusterInfoAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Enumerates the tag-index membership trees (those whose id carries the
    /// reserved <c>tag-</c> prefix) as a deterministic, paged catalog. These are
    /// listed separately from <see cref="ListTreesAsync"/> so a consumer can
    /// present tag indexes as their own category alongside trees and views.
    /// </summary>
    /// <param name="request">Paging request.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<TagIndexCatalogPage> ListTagIndexesAsync(
        CatalogRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Enumerates the distinct tag values carried by a single tag index over its
    /// subject tree, as a deterministic, paged catalog in ascending ordinal
    /// order. The index is named by <see cref="CatalogRequest.IndexName"/> and
    /// the subject tree by <see cref="CatalogRequest.SourceTreeId"/>; both are
    /// required. Intended to populate a tag-value picker for a tag-filtered
    /// entry scan. Returns an empty page when the subject tree is absent or
    /// reserved, when no tag-index factory is registered, or when the index has
    /// no members in that tree.
    /// </summary>
    /// <param name="request">
    /// Paging request carrying the subject tree
    /// (<see cref="CatalogRequest.SourceTreeId"/>) and index name
    /// (<see cref="CatalogRequest.IndexName"/>).
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<TagValueCatalogPage> ListTagValuesAsync(
        CatalogRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Enumerates the subject trees a single tag index covers, as a
    /// deterministic, paged catalog in ascending ordinal order. The index is
    /// named by <see cref="CatalogRequest.IndexName"/> (required);
    /// <see cref="CatalogRequest.SourceTreeId"/> is ignored. Returns an empty
    /// page when no tag-index factory is registered or when the index covers no
    /// trees.
    /// </summary>
    /// <param name="request">Paging request carrying the index name (<see cref="CatalogRequest.IndexName"/>).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<CoveredTreeCatalogPage> ListCoveredTreesAsync(
        CatalogRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Enumerates the distinct tag values a single tag index carries across
    /// <b>every</b> tree it covers, as a deterministic, paged catalog in
    /// ascending ordinal order. This is the index-wide analog of
    /// <see cref="ListTagValuesAsync"/> (which is scoped to one subject tree).
    /// The index is named by <see cref="CatalogRequest.IndexName"/> (required);
    /// <see cref="CatalogRequest.SourceTreeId"/> is ignored. Returns an empty
    /// page when no tag-index factory is registered or when the index has no
    /// members.
    /// </summary>
    /// <param name="request">Paging request carrying the index name (<see cref="CatalogRequest.IndexName"/>).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<TagValueCatalogPage> ListIndexTagsAsync(
        CatalogRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Enumerates the live members of a single tag - the <c>(tree, key)</c>
    /// pairs currently carrying it - across every tree a tag index covers, as a
    /// deterministic, paged catalog in ascending ordinal <c>(tree id, key)</c>
    /// order. Members whose primary key no longer exists in its subject tree (a
    /// membership row that outlived its key, pending reconcile) are skipped, so
    /// only live rows are returned. Returns an empty page when no tag-index
    /// factory is registered or the tag has no live members.
    /// </summary>
    /// <param name="request">
    /// Paging request carrying the index name
    /// (<see cref="TagMemberScanRequest.IndexName"/>) and tag
    /// (<see cref="TagMemberScanRequest.Tag"/>).
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<TagMemberScanPage> ScanTagMembersAsync(
        TagMemberScanRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the structural node graph of the given tree - shard roots,
    /// internal nodes, and leaves - each annotated with node kind, key-range
    /// bounds, live/tombstone counts, fan-out, and depth, or a typed
    /// not-found result when the tree does not exist. Built from the pushed-up
    /// per-shard topology snapshots: a whole-tree read issues one structural
    /// read per shard root (O(shards)) and never fans out to leaves. The
    /// response is bounded by the request's depth and node-count budget, with
    /// <see cref="NodeStateSummary.HasMoreChildren"/> markers so a client can
    /// lazily expand truncated subtrees via
    /// <see cref="StructureRequest.SubPathNodeId"/>.
    /// </summary>
    /// <param name="request">Scope (tree, optional shard / sub-path) and depth / node budget.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<TreeStructureResult> GetTreeStructureAsync(
        StructureRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Scans the actual entries of a tree as a key-ordered, paged read,
    /// optionally scoped to a key range and filtered by a server-side predicate.
    /// The <see cref="EntryScanRequest.Mode"/> selects the cursor isolation for a
    /// fresh scan (no <see cref="EntryScanRequest.ContinuationToken"/>): the
    /// default <see cref="EntryScanMode.Snapshot"/> opens a point-in-time cursor
    /// whose every continuation pages against the same frozen view, so the scan
    /// never observes a torn write and is resilient to concurrent writes,
    /// splits, and reshards, at the cost of an all-shard baseline capture at
    /// open; <see cref="EntryScanMode.Live"/> and
    /// <see cref="EntryScanMode.LivePointInTime"/> page a baseline-free live
    /// cursor whose continuation is keyed on the last yielded key, so later
    /// pages can reflect writes committed after the open. Values are returned as
    /// size-bounded previews (the full length is always reported) so whole
    /// values do not cross the wire unnecessarily. Tombstoned and TTL-expired
    /// entries are excluded from the scan rather than surfaced as live.
    /// </summary>
    /// <param name="request">Scope, paging, preview budget, and optional predicate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<EntryScanResult> ScanEntriesAsync(
        EntryScanRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the full record for a single key (with a larger value-preview
    /// budget than a scan), or a typed not-found that distinguishes an unknown
    /// tree from a missing key. Intended for the explorer's detail pane.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The entry key to read.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<EntryDetailResult> GetEntryAsync(
        string treeId,
        string key,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads a single key's change-history timeline as a continuation-paged page
    /// of revision records, sourced from the tree's durable per-key history view
    /// when one is enabled (a clean, age-bounded timeline) or, as a best-effort
    /// fallback, from the retained source write-ahead-log window (which reports
    /// truncation honestly once garbage collection has trimmed its oldest
    /// entries). Each revision carries its hybrid-logical-clock, kind, origin,
    /// category, a value-or-metadata view bounded by the tree's retention mode,
    /// the per-revision retention descriptor, and - for a CRDT revision whose
    /// bytes were retained in full - the decoded element-level member changes.
    /// The result's <see cref="EntryHistoryResult.Bound"/> tells the consumer
    /// whether the timeline is durable-bounded or a truncated fallback. Returns a
    /// typed not-found when the tree does not exist.
    /// </summary>
    /// <param name="request">Scope (tree, key), optional HLC bounds, paging, preview budget, and in-page order.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<EntryHistoryResult> GetEntryHistoryAsync(
        EntryHistoryRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Releases the server-side snapshot cursor named by a scan continuation
    /// token, freeing its WAL-retention pin and per-shard baseline promptly
    /// instead of waiting for the cursor's idle TTL. Intended for a client that
    /// abandons a multi-page scan before draining it (e.g. the explorer
    /// refreshing, re-filtering, or navigating away). The operation is
    /// best-effort and idempotent: an empty token, or one that names an unknown,
    /// already-drained, or already-closed cursor, is a tolerated no-op rather
    /// than a fault.
    /// </summary>
    /// <param name="treeId">Logical tree the cursor was opened against.</param>
    /// <param name="continuationToken">The cursor's continuation token.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task CancelScanAsync(
        string treeId,
        string? continuationToken,
        CancellationToken cancellationToken = default);
}
