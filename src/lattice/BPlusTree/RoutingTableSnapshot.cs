namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// An immutable, point-in-time snapshot of an internal node's routing
/// table — the parallel <see cref="SeparatorKeys"/> / <see cref="ChildIds"/>
/// arrays plus the <see cref="ChildrenAreLeaves"/> flag — produced by
/// <c>IBPlusInternalGrain.GetRoutingTableAsync</c> and cached per
/// <c>ShardRootGrain</c> activation. The snapshot enables the shard root
/// to perform key-to-child routing locally instead of paying a cross-grain
/// <c>RouteWithMetadataAsync</c> RPC for every traversal step.
/// </summary>
/// <remarks>
/// The snapshot is invalidated by the shard root after every
/// <c>AcceptSplitAsync</c> call — the only path that mutates an internal
/// node's children list outside of fresh-construction (which has no prior
/// cache entry to invalidate). Cache miss is always safe: the snapshot
/// is simply re-fetched on demand.
///
/// Index 0 always carries <c>SeparatorKey = null</c> (leftmost catch-all);
/// indices 1..N carry "keys ≥ separator route to this child" semantics.
/// The local <see cref="Route"/> implementation mirrors
/// <c>InternalNodeState.Route</c> exactly.
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.RoutingTableSnapshot)]
[Immutable]
internal readonly record struct RoutingTableSnapshot
{
    /// <summary>Separator keys, parallel to <see cref="ChildIds"/>; index 0 is always <c>null</c>.</summary>
    [Id(0)] public string?[] SeparatorKeys { get; init; }

    /// <summary>Child grain identities, parallel to <see cref="SeparatorKeys"/>.</summary>
    [Id(1)] public GrainId[] ChildIds { get; init; }

    /// <summary>Whether this node's children are leaves (<c>true</c>) or internal nodes (<c>false</c>).</summary>
    [Id(2)] public bool ChildrenAreLeaves { get; init; }

    /// <summary>
    /// Routes a key to its child grain by finding the rightmost separator ≤ key.
    /// Mirrors <c>InternalNodeState.Route</c>; the two implementations must
    /// stay in lockstep — any change to the routing semantics on the server
    /// side requires the same change here, otherwise the local cache lookup
    /// and the cross-grain fallback diverge.
    /// </summary>
    public (GrainId ChildId, bool ChildrenAreLeaves) Route(string key)
    {
        var seps = SeparatorKeys;
        for (int i = seps.Length - 1; i >= 0; i--)
        {
            var sep = seps[i];
            if (sep is null || string.Compare(key, sep, StringComparison.Ordinal) >= 0)
            {
                return (ChildIds[i], ChildrenAreLeaves);
            }
        }
        return (ChildIds[0], ChildrenAreLeaves);
    }
}