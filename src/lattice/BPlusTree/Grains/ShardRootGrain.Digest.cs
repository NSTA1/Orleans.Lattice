using System.IO.Hashing;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Shard-side projection-digest implementation.
/// <para>
/// When the shard's root is an internal node, the digest is satisfied
/// by a single grain call to <see cref="IBPlusInternalGrain.GetSubtreeProjectionDigestAsync"/>:
/// every internal node maintains an XOR-folded
/// <c>SubtreeProjectionHash</c> plus aggregated entry-count and
/// max-reduced checkpoint offset over its descendant leaves, updated
/// incrementally as each leaf's
/// <see cref="ChildDigestSnapshot"/> propagates upward through
/// <see cref="IBPlusInternalGrain.OnChildDigestPublishedAsync"/>. A
/// whole-tree poll therefore costs O(shardCount) grain hops rather
/// than O(shardCount x leafCount).
/// </para>
/// <para>
/// When the shard's root is a single leaf (flat-tree case), the
/// digest is read directly from that leaf via
/// <see cref="IBPlusLeafGrain.GetProjectionDigestAsync"/>. When the
/// shard has no root (empty shard), an empty digest is returned.
/// </para>
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <inheritdoc />
    public async Task<LeafProjectionDigest> GetShardProjectionDigestAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await PrepareForOperationAsync();
        cancellationToken.ThrowIfCancellationRequested();

        // One counter increment per call, regardless of whether the shard is
        // empty, flat, or fully-internal. Lets the integration oracle assert
        // the chained-fold's headline invariant ("a whole-tree poll issues
        // exactly one grain call per shard") via OpenTelemetry instead of a
        // bespoke counting harness.
        LatticeMetrics.ShardDigestReads.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagShard, ShardIndex));

        if (state.State.RootNodeId is null)
        {
            return new LeafProjectionDigest
            {
                Hash = new XxHash128().GetHashAndReset(),
                EntryCount = 0,
                CheckpointOffset = 0,
                Version = LeafProjectionDigest.CurrentVersion,
            };
        }

        if (state.State.RootIsLeaf)
        {
            // Flat-tree fallback: no internal node exists to host the
            // chained fold, so read the single root leaf directly.
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(state.State.RootNodeId!.Value);
            return await leaf.GetProjectionDigestAsync();
        }

        // Root is an internal node: read its pre-folded subtree digest
        // in a single grain call. The internal node's
        // SubtreeProjectionHash has been maintained incrementally by
        // every leaf-mutation's PublishDigestUpwardAsync chain, so the
        // returned value is bit-identical to a fresh walk over every
        // descendant leaf.
        var root = grainFactory.GetGrain<IBPlusInternalGrain>(state.State.RootNodeId!.Value);
        return await root.GetSubtreeProjectionDigestAsync();
    }

    /// <inheritdoc />
    public Task<ShardRootNodeRef?> GetRootNodeRefAsync()
    {
        // Synchronous best-effort read of the in-memory routing slots. No
        // PrepareForOperationAsync / re-read: this is a read-only diagnostic
        // accessor for the anti-entropy drift-localisation walk, which only
        // probes shards the digest-probe scheduler has already activated and
        // read. An empty shard (no root yet) returns null.
        if (state.State.RootNodeId is null)
        {
            return Task.FromResult<ShardRootNodeRef?>(null);
        }

        return Task.FromResult<ShardRootNodeRef?>(new ShardRootNodeRef
        {
            NodeId = state.State.RootNodeId.Value,
            IsLeaf = state.State.RootIsLeaf,
        });
    }
}
