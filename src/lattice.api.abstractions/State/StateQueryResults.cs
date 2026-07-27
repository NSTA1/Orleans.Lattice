namespace Orleans.Lattice.Api.State;

/// <summary>
/// Outcome of a state-query lookup that can fail to resolve its target.
/// </summary>
public enum StateQueryStatus
{
    /// <summary>The target was found and the payload is populated.</summary>
    Found = 0,

    /// <summary>The requested tree does not exist.</summary>
    TreeNotFound = 1,

    /// <summary>The tree exists but the requested key was not found.</summary>
    KeyNotFound = 2,

    /// <summary>
    /// The named tag index does not exist (no membership tree has been
    /// materialised for it). Distinguishes a mistyped index name from a
    /// real-but-empty index, which returns <see cref="Found"/> with zero
    /// entries.
    /// </summary>
    IndexNotFound = 3,
}

/// <summary>
/// Result of <see cref="ILatticeStateQuery.GetTreeSummaryAsync"/>: either a
/// populated <see cref="TreeStateSummary"/> or a typed not-found, so an
/// unknown tree never surfaces an Orleans-internal exception to callers.
/// </summary>
public sealed record TreeSummaryResult
{
    /// <summary>Lookup outcome.</summary>
    public StateQueryStatus Status { get; init; }

    /// <summary>The tree id that was queried.</summary>
    public required string TreeId { get; init; }

    /// <summary>The summary when <see cref="Status"/> is <see cref="StateQueryStatus.Found"/>.</summary>
    public TreeStateSummary? Summary { get; init; }

    /// <summary>Builds a found result.</summary>
    public static TreeSummaryResult Found(TreeStateSummary summary)
    {
        ArgumentNullException.ThrowIfNull(summary);
        return new TreeSummaryResult { Status = StateQueryStatus.Found, TreeId = summary.TreeId, Summary = summary };
    }

    /// <summary>Builds a not-found result for the given tree id.</summary>
    public static TreeSummaryResult NotFound(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return new TreeSummaryResult { Status = StateQueryStatus.TreeNotFound, TreeId = treeId };
    }
}

/// <summary>
/// Result of <see cref="ILatticeStateQuery.GetShardSummariesAsync"/>: either
/// the per-shard summaries or a typed not-found.
/// </summary>
public sealed record ShardSummariesResult
{
    /// <summary>Lookup outcome.</summary>
    public StateQueryStatus Status { get; init; }

    /// <summary>The tree id that was queried.</summary>
    public required string TreeId { get; init; }

    /// <summary>The per-shard summaries (empty when not found), ordered by shard index.</summary>
    public IReadOnlyList<ShardStateSummary> Shards { get; init; } = Array.Empty<ShardStateSummary>();

    /// <summary>Builds a found result.</summary>
    public static ShardSummariesResult Found(string treeId, IReadOnlyList<ShardStateSummary> shards)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(shards);
        return new ShardSummariesResult { Status = StateQueryStatus.Found, TreeId = treeId, Shards = shards };
    }

    /// <summary>Builds a not-found result for the given tree id.</summary>
    public static ShardSummariesResult NotFound(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return new ShardSummariesResult { Status = StateQueryStatus.TreeNotFound, TreeId = treeId };
    }
}

/// <summary>
/// Result of <see cref="ILatticeStateQuery.GetTreeStructureAsync"/>: either a
/// bounded, depth-limited node graph (one entry per shard root for a
/// whole-tree read, or a single subtree for a sub-path descent) or a typed
/// not-found.
/// </summary>
public sealed record TreeStructureResult
{
    /// <summary>Lookup outcome.</summary>
    public StateQueryStatus Status { get; init; }

    /// <summary>The tree id that was queried.</summary>
    public required string TreeId { get; init; }

    /// <summary>
    /// The root nodes of the response, in deterministic key-range order. For a
    /// whole-tree read this is the per-shard root nodes; for a sub-path descent
    /// it is the single requested subtree. Empty when not found.
    /// </summary>
    public IReadOnlyList<NodeStateSummary> Roots { get; init; } = Array.Empty<NodeStateSummary>();

    /// <summary>
    /// Whether the node-count budget was exhausted, so some subtrees were
    /// truncated and can be re-read with a sub-path descent. The per-node
    /// <see cref="NodeStateSummary.HasMoreChildren"/> flags identify exactly
    /// which nodes were truncated.
    /// </summary>
    public bool Truncated { get; init; }

    /// <summary>Builds a found result.</summary>
    public static TreeStructureResult Found(string treeId, IReadOnlyList<NodeStateSummary> roots, bool truncated)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(roots);
        return new TreeStructureResult
        {
            Status = StateQueryStatus.Found,
            TreeId = treeId,
            Roots = roots,
            Truncated = truncated,
        };
    }

    /// <summary>Builds a not-found result for the given tree id.</summary>
    public static TreeStructureResult NotFound(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return new TreeStructureResult { Status = StateQueryStatus.TreeNotFound, TreeId = treeId };
    }
}

/// <summary>
/// Result of <see cref="ILatticeStateQuery.ScanEntriesAsync"/>: a
/// snapshot-isolated, key-ordered page of entries plus an opaque continuation
/// token, or a typed not-found when the tree does not exist.
/// </summary>
public sealed record EntryScanResult
{
    /// <summary>Lookup outcome.</summary>
    public StateQueryStatus Status { get; init; }

    /// <summary>The tree id that was scanned.</summary>
    public required string TreeId { get; init; }

    /// <summary>
    /// The entries in this page, in the scan's key order (empty when not
    /// found or when the scan is drained).
    /// </summary>
    public IReadOnlyList<EntryRecord> Entries { get; init; } = Array.Empty<EntryRecord>();

    /// <summary>
    /// Opaque token to pass as <see cref="EntryScanRequest.ContinuationToken"/>
    /// to fetch the next page against the same snapshot, or
    /// <see langword="null"/> when the scan is fully drained.
    /// </summary>
    public string? ContinuationToken { get; init; }

    /// <summary>Builds a found page.</summary>
    public static EntryScanResult Found(string treeId, IReadOnlyList<EntryRecord> entries, string? continuationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(entries);
        return new EntryScanResult
        {
            Status = StateQueryStatus.Found,
            TreeId = treeId,
            Entries = entries,
            ContinuationToken = continuationToken,
        };
    }

    /// <summary>Builds a not-found result for the given tree id.</summary>
    public static EntryScanResult NotFound(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return new EntryScanResult { Status = StateQueryStatus.TreeNotFound, TreeId = treeId };
    }

    /// <summary>
    /// Builds an index-not-found result for a tag-filtered scan whose
    /// <see cref="EntryScanRequest.IndexName"/> names no materialised tag
    /// index. Distinguishes a mistyped index name from a real-but-empty index.
    /// </summary>
    public static EntryScanResult IndexNotFound(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return new EntryScanResult { Status = StateQueryStatus.IndexNotFound, TreeId = treeId };
    }
}

/// <summary>
/// Result of <see cref="ILatticeStateQuery.GetEntryAsync"/>: the full record
/// for a single existing key, or a typed not-found that distinguishes an
/// unknown tree from a missing key.
/// </summary>
public sealed record EntryDetailResult
{
    /// <summary>Lookup outcome.</summary>
    public StateQueryStatus Status { get; init; }

    /// <summary>The tree id that was queried.</summary>
    public required string TreeId { get; init; }

    /// <summary>The key that was queried.</summary>
    public required string Key { get; init; }

    /// <summary>The record when <see cref="Status"/> is <see cref="StateQueryStatus.Found"/>.</summary>
    public EntryRecord? Entry { get; init; }

    /// <summary>Builds a found result for the given key.</summary>
    public static EntryDetailResult Found(string treeId, EntryRecord entry)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(entry);
        return new EntryDetailResult
        {
            Status = StateQueryStatus.Found,
            TreeId = treeId,
            Key = entry.Key,
            Entry = entry,
        };
    }

    /// <summary>Builds a tree-not-found result.</summary>
    public static EntryDetailResult TreeNotFound(string treeId, string key)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(key);
        return new EntryDetailResult { Status = StateQueryStatus.TreeNotFound, TreeId = treeId, Key = key };
    }

    /// <summary>Builds a key-not-found result.</summary>
    public static EntryDetailResult KeyNotFound(string treeId, string key)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(key);
        return new EntryDetailResult { Status = StateQueryStatus.KeyNotFound, TreeId = treeId, Key = key };
    }
}

/// <summary>
/// Result of <see cref="ILatticeStateQuery.GetEntryHistoryAsync"/>: a
/// continuation-paged page of a key's revision timeline plus the history
/// metadata (how the timeline is bounded and, when truncated, the oldest
/// still-readable revision), or a typed not-found when the tree does not exist.
/// </summary>
public sealed record EntryHistoryResult
{
    /// <summary>Lookup outcome.</summary>
    public StateQueryStatus Status { get; init; }

    /// <summary>The tree id that was queried.</summary>
    public required string TreeId { get; init; }

    /// <summary>The key whose history was queried.</summary>
    public required string Key { get; init; }

    /// <summary>
    /// The revisions in this page, ordered per the request's
    /// <see cref="EntryHistoryRequest.Reverse"/> flag (oldest-first by default).
    /// Empty when not found or when the timeline is drained.
    /// </summary>
    public IReadOnlyList<EntryRevisionRecord> Revisions { get; init; } = Array.Empty<EntryRevisionRecord>();

    /// <summary>
    /// Opaque token to pass as
    /// <see cref="EntryHistoryRequest.ContinuationToken"/> to fetch the next
    /// page, or <see langword="null"/> when the timeline is fully drained.
    /// </summary>
    public string? ContinuationToken { get; init; }

    /// <summary>How the returned timeline is bounded below.</summary>
    public EntryHistoryBound Bound { get; init; }

    /// <summary>
    /// On a <see cref="EntryHistoryBound.Truncated"/> page, the
    /// hybrid-logical-clock timestamp of the oldest still-readable revision;
    /// <see cref="HybridLogicalClock.Zero"/> otherwise.
    /// </summary>
    public HybridLogicalClock EarliestAvailable { get; init; }

    /// <summary>Builds a found page.</summary>
    public static EntryHistoryResult Found(
        string treeId,
        string key,
        IReadOnlyList<EntryRevisionRecord> revisions,
        string? continuationToken,
        EntryHistoryBound bound,
        HybridLogicalClock earliestAvailable)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(revisions);
        return new EntryHistoryResult
        {
            Status = StateQueryStatus.Found,
            TreeId = treeId,
            Key = key,
            Revisions = revisions,
            ContinuationToken = continuationToken,
            Bound = bound,
            EarliestAvailable = earliestAvailable,
        };
    }

    /// <summary>Builds a tree-not-found result.</summary>
    public static EntryHistoryResult TreeNotFound(string treeId, string key)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(key);
        return new EntryHistoryResult { Status = StateQueryStatus.TreeNotFound, TreeId = treeId, Key = key };
    }

    /// <summary>
    /// Builds a key-not-found result: the tree exists and is readable, but the
    /// requested key is not readable by (or not present for) the caller.
    /// </summary>
    public static EntryHistoryResult KeyNotFound(string treeId, string key)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(key);
        return new EntryHistoryResult { Status = StateQueryStatus.KeyNotFound, TreeId = treeId, Key = key };
    }
}
