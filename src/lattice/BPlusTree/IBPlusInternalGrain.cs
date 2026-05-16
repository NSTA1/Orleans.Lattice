namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// An internal (non-leaf) node grain in the B+ tree. Stores separator keys
/// and references to child grains (which may be internal or leaf nodes).
/// </summary>
[Alias(TypeAliases.IBPlusInternalGrain)]
internal interface IBPlusInternalGrain : IGrainWithGuidKey
{
    /// <summary>Initialises this internal node with the result of a root split.</summary>
    Task InitializeAsync(string separatorKey, GrainId leftChild, GrainId rightChild, bool childrenAreLeaves);

    /// <summary>
    /// Routes a key down to the appropriate child grain and returns whether
    /// this node's children are leaves, in a single call. Avoids two
    /// sequential RPCs during tree traversal.
    /// </summary>
    Task<(GrainId ChildId, bool ChildrenAreLeaves)> RouteWithMetadataAsync(string key);

    /// <summary>
    /// Returns a point-in-time snapshot of the full routing table -
    /// separator keys, child identities, and the
    /// <c>ChildrenAreLeaves</c> flag. The shard root caches this snapshot
    /// per activation and uses it to perform key-to-child routing locally,
    /// eliminating the per-traversal-step <see cref="RouteWithMetadataAsync"/>
    /// cross-grain RPC. The snapshot becomes stale on the next
    /// <see cref="AcceptSplitAsync"/> against this node; the shard root
    /// invalidates its cache entry on every such call.
    /// </summary>
    Task<RoutingTableSnapshot> GetRoutingTableAsync();

    /// <summary>Returns the grain identity of the leftmost child.</summary>
    Task<GrainId> GetLeftmostChildAsync();

    /// <summary>Returns the grain identity of the rightmost child.</summary>
    Task<GrainId> GetRightmostChildAsync();

    /// <summary>
    /// Returns the leftmost child and whether this node's children are leaves, in a single call.
    /// </summary>
    Task<(GrainId ChildId, bool ChildrenAreLeaves)> GetLeftmostChildWithMetadataAsync();

    /// <summary>
    /// Returns the rightmost child and whether this node's children are leaves, in a single call.
    /// </summary>
    Task<(GrainId ChildId, bool ChildrenAreLeaves)> GetRightmostChildWithMetadataAsync();

    /// <summary>Returns whether this node's children are leaf grains.</summary>
    Task<bool> AreChildrenLeavesAsync();

    /// <summary>Accepts a promoted split from a child node.</summary>
    /// <returns>A <see cref="SplitResult"/> if this node itself needed to split, otherwise <c>null</c>.</returns>
    Task<SplitResult?> AcceptSplitAsync(string promotedKey, GrainId newChild);

    /// <summary>
    /// Associates this node with a tree, enabling named options resolution.
    /// Called once by the shard root after creating the grain. Idempotent.
    /// </summary>
    Task SetTreeIdAsync(string treeId);

    /// <summary>
    /// Initialises this internal node with a pre-built list of children.
    /// Used by bulk load to construct internal nodes in a single call.
    /// <paramref name="separatorKeys"/> and <paramref name="childIds"/> must have equal length.
    /// The first separator key must be <c>null</c> (leftmost catch-all).
    /// </summary>
    Task InitializeWithChildrenAsync(List<string?> separatorKeys, List<GrainId> childIds, bool childrenAreLeaves);

    /// <summary>
    /// Returns the grain identities of all children of this internal node.
    /// Used during tree purge to enumerate the tree structure.
    /// </summary>
    Task<List<GrainId>> GetChildIdsAsync();

    /// <summary>
    /// Clears all persistent state for this grain and deactivates it.
    /// Used during tree purge to permanently remove internal node data.
    /// </summary>
    Task ClearGrainStateAsync();

    /// <summary>
    /// Stores a grain reference to the parent internal node so this node
    /// can propagate its <see cref="ChildDigestSnapshot"/> upward when its
    /// own subtree fold changes. Called once by the shard root after
    /// creating the grain (or after a root-promotion that grafts this
    /// node beneath a new parent). A <see langword="null"/> parent marks
    /// this node as the shard root, so digest propagation stops here.
    /// Idempotent: a re-call with the same id is a no-op; a re-call with
    /// a different id (root rotation) overwrites the slot.
    /// </summary>
    Task SetParentAsync(GrainId? parentId);

    /// <summary>
    /// Hook invoked by a child grain (leaf or internal) when its
    /// published <see cref="ChildDigestSnapshot"/> changes. This node
    /// XOR-folds the delta between its persisted prior snapshot for
    /// <paramref name="childId"/> (or 16 zero bytes when no prior
    /// snapshot is recorded) and <paramref name="newSnapshot"/> into
    /// its <c>SubtreeProjectionHash</c>, updates its
    /// <c>SubtreeEntryCount</c> and <c>SubtreeHighestCheckpointOffset</c>
    /// aggregates, persists the change, and (if its own subtree fold
    /// changed) propagates a fresh snapshot to its own parent. The
    /// <paramref name="childId"/> argument identifies the calling child
    /// so the parent can record which slot supplied which snapshot.
    /// </summary>
    Task OnChildDigestPublishedAsync(GrainId childId, ChildDigestSnapshot newSnapshot);

    /// <summary>
    /// Returns this internal node's current subtree fold as a
    /// <see cref="LeafProjectionDigest"/>: the <c>SubtreeProjectionHash</c>
    /// chained with the subtree entry count and highest checkpoint
    /// offset via XxHash128, in the same shape as the leaf-level
    /// digest. Used by the shard root to satisfy
    /// <c>GetShardProjectionDigestAsync</c> in one grain call when this
    /// node is the shard's root internal node.
    /// </summary>
    Task<LeafProjectionDigest> GetSubtreeProjectionDigestAsync();

    /// <summary>
    /// Returns this internal node's current contribution to its own
    /// parent's subtree fold: the raw 16-byte
    /// <c>SubtreeProjectionHash</c>, the descendant entry count, and the
    /// max-reduced checkpoint offset. Distinct from
    /// <see cref="GetSubtreeProjectionDigestAsync"/>, which folds those
    /// three fields into a single XxHash128 fingerprint for public
    /// consumption. Used by the parent's lazy-backfill path when no
    /// prior snapshot has been recorded for this node (e.g. an internal
    /// node activating with legacy state, or a crash-recovery rebuild).
    /// </summary>
    Task<ChildDigestSnapshot> GetChildDigestSnapshotAsync();
}
