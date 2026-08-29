using System.Buffers.Binary;
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
/// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.GetProjectionDigestAsync"/>. When the
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
            new KeyValuePair<string, object?>(LatticeMetrics.TagShard, ShardIndex),
            LatticeTenantLabel.ForTree(TreeId));

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

        if (RootIsLeafTyped)
        {
            // Flat-tree fallback: no internal node exists to host the
            // chained fold, so read the single root leaf directly. Decided by
            // node TYPE: a corrupt RootIsLeaf flag over an internal root
            // (issue 899) reads the internal subtree digest below instead.
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
    public async Task<LeafProjectionDigest> GetShardProjectionDigestForRangeAsync(
        string? startInclusive,
        string? endExclusive,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await PrepareForOperationAsync();
        cancellationToken.ThrowIfCancellationRequested();

        LatticeMetrics.ShardDigestReads.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagShard, ShardIndex),
            LatticeTenantLabel.ForTree(TreeId));

        if (state.State.RootNodeId is null)
        {
            // Empty shard: return the exact same bare empty digest as
            // GetShardProjectionDigestAsync so a full-range probe of an empty
            // shard is byte-identical to its whole-shard digest.
            return new LeafProjectionDigest
            {
                Hash = new XxHash128().GetHashAndReset(),
                EntryCount = 0,
                CheckpointOffset = 0,
                Version = LeafProjectionDigest.CurrentVersion,
            };
        }

        if (RootIsLeafTyped)
        {
            // Flat-tree case: fold the single root leaf's in-range snapshot
            // and wrap it with the leaf's own checkpoint offset. For a full
            // range this is byte-identical to the leaf's whole-tree digest
            // (GetProjectionDigestAsync), which the flat-tree branch of
            // GetShardProjectionDigestAsync returns verbatim.
            var leaf = ResolveLeafGrain(state.State.RootNodeId!.Value);
            var snapshot = await leaf.GetProjectionDigestForRangeAsync(startInclusive, endExclusive);
            return WrapRangeDigest(snapshot.Hash, snapshot.EntryCount, snapshot.CheckpointOffset);
        }

        // Internal-rooted tree: descend by separator-key range, folding only
        // the leaves (and whole subtrees) that overlap [start, end). The
        // accumulator mirrors the internal-node fold algebra exactly: XOR the
        // raw child hashes, sum the entry counts, and max-reduce the
        // checkpoint offsets starting from 0 (so a negative pre-applied
        // checkpoint clamps to 0, matching ApplyChildSnapshotAsync).
        var accumulator = new RangeFoldAccumulator();
        await FoldRangeAsync(
            state.State.RootNodeId!.Value,
            nodeLow: null,
            nodeHigh: null,
            startInclusive,
            endExclusive,
            accumulator,
            cancellationToken);

        return WrapRangeDigest(accumulator.Hash, accumulator.EntryCount, accumulator.MaxCheckpoint);
    }

    /// <summary>
    /// Mutable per-call accumulator for the range fold. Holds the 16-byte XOR
    /// running hash, the summed entry count, and the max-reduced checkpoint
    /// offset. Allocated once per <see cref="GetShardProjectionDigestForRangeAsync"/>
    /// call (a single small object) and threaded through the recursion by
    /// reference to keep the descent allocation-lean.
    /// </summary>
    private sealed class RangeFoldAccumulator
    {
        public readonly byte[] Hash = new byte[RangeFoldHashSize];
        public long EntryCount;
        public long MaxCheckpoint;
    }

    private const int RangeFoldHashSize = 16;

    /// <summary>
    /// Recursively folds the in-range portion of the internal-node subtree
    /// rooted at <paramref name="nodeId"/> into <paramref name="accumulator"/>.
    /// <paramref name="nodeLow"/> / <paramref name="nodeHigh"/> are the node's
    /// own key-range bounds (null = unbounded). Only descends into children
    /// whose separator-key range overlaps [<paramref name="start"/>,
    /// <paramref name="end"/>); a child whose range is fully inside the query
    /// range is folded from its pre-computed subtree snapshot without further
    /// descent. Only ever invoked for internal nodes - the flat-tree (root is
    /// leaf) case is handled directly by the caller.
    /// </summary>
    private async Task FoldRangeAsync(
        GrainId nodeId,
        string? nodeLow,
        string? nodeHigh,
        string? start,
        string? end,
        RangeFoldAccumulator accumulator,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var routing = await GetRoutingTableSnapshotAsync(nodeId);
        var separators = routing.SeparatorKeys;
        var childIds = routing.ChildIds;
        var n = Math.Min(separators.Length, childIds.Length);

        for (var i = 0; i < n; i++)
        {
            // Index 0's separator is always null (leftmost catch-all); inherit
            // the parent's low bound there. The child's exclusive high bound is
            // the next sibling's separator, or the parent's high bound for the
            // last child.
            var childLow = separators[i] ?? nodeLow;
            var childHigh = (i + 1 < n) ? separators[i + 1] : nodeHigh;

            if (!RangesOverlap(childLow, childHigh, start, end))
            {
                continue;
            }

            if (routing.ChildrenAreLeaves)
            {
                // The leaf clamps to the in-range subset itself, so a boundary
                // leaf that straddles start/end still folds only its in-range
                // entries.
                var leaf = ResolveLeafGrain(childIds[i]);
                var snapshot = await leaf.GetProjectionDigestForRangeAsync(start, end);
                FoldSnapshot(accumulator, snapshot);
            }
            else if (RangeFullyInside(childLow, childHigh, start, end))
            {
                // The whole internal subtree is inside the query range: fold its
                // pre-computed snapshot (raw XOR hash + entry count + max
                // checkpoint) in a single grain call instead of descending. This
                // is algebraically exact - the snapshot equals the XOR of every
                // descendant leaf's raw hash, the sum of their counts, and the
                // max of their checkpoints.
                var internalNode = ResolveInternalGrain(childIds[i]);
                var snapshot = await internalNode.GetChildDigestSnapshotAsync();
                FoldSnapshot(accumulator, snapshot);
            }
            else
            {
                await FoldRangeAsync(
                    childIds[i],
                    childLow,
                    childHigh,
                    start,
                    end,
                    accumulator,
                    cancellationToken);
            }
        }
    }

    /// <summary>
    /// XOR-folds a child snapshot into the accumulator: XOR the raw hash, sum
    /// the entry count, and raise the max-reduced checkpoint. The checkpoint
    /// max starts from the accumulator's running value (initialised to 0), so
    /// a negative pre-applied checkpoint never lowers the aggregate - matching
    /// the internal-node fold in <c>BPlusInternalGrain.ApplyChildSnapshotAsync</c>.
    /// </summary>
    private static void FoldSnapshot(RangeFoldAccumulator accumulator, ChildDigestSnapshot snapshot)
    {
        if (snapshot.Hash is { Length: RangeFoldHashSize } childHash)
        {
            var hash = accumulator.Hash;
            for (var i = 0; i < RangeFoldHashSize; i++) hash[i] ^= childHash[i];
        }
        accumulator.EntryCount += snapshot.EntryCount;
        if (snapshot.CheckpointOffset > accumulator.MaxCheckpoint)
        {
            accumulator.MaxCheckpoint = snapshot.CheckpointOffset;
        }
    }

    /// <summary>
    /// Wraps a raw fold (16-byte XOR hash, entry count, checkpoint offset) in
    /// the same XxHash128(rawHash || int64LE(entryCount) || int64LE(checkpoint))
    /// shape that <c>BPlusInternalGrain.ComputePublishedDigest</c> and
    /// <c>BPlusLeafGrain.GetProjectionDigestAsync</c> publish, so a full-range
    /// fold is byte-identical to the whole-shard digest.
    /// </summary>
    private static LeafProjectionDigest WrapRangeDigest(byte[]? rawHash, long entryCount, long checkpointOffset)
    {
        var hasher = new XxHash128();
        Span<byte> scratch = stackalloc byte[8];

        if (rawHash is { Length: RangeFoldHashSize })
        {
            hasher.Append(rawHash);
        }
        else
        {
            Span<byte> zero = stackalloc byte[RangeFoldHashSize];
            hasher.Append(zero);
        }

        BinaryPrimitives.WriteInt64LittleEndian(scratch, entryCount);
        hasher.Append(scratch[..8]);
        BinaryPrimitives.WriteInt64LittleEndian(scratch, checkpointOffset);
        hasher.Append(scratch[..8]);

        return new LeafProjectionDigest
        {
            Hash = hasher.GetHashAndReset(),
            EntryCount = entryCount,
            CheckpointOffset = checkpointOffset,
            Version = LeafProjectionDigest.CurrentVersion,
        };
    }

    /// <summary>
    /// Returns <c>true</c> when the half-open child range [cl, ch) overlaps
    /// the half-open query range [s, e). A null bound denotes -infinity (low)
    /// or +infinity (high). Overlap holds iff <c>cl &lt; e</c> and
    /// <c>s &lt; ch</c>.
    /// </summary>
    private static bool RangesOverlap(string? cl, string? ch, string? s, string? e)
    {
        if (e is not null && cl is not null && string.CompareOrdinal(cl, e) >= 0)
        {
            return false;
        }
        if (s is not null && ch is not null && string.CompareOrdinal(ch, s) <= 0)
        {
            return false;
        }
        return true;
    }

    /// <summary>
    /// Returns <c>true</c> when the half-open child range [cl, ch) is fully
    /// contained in the half-open query range [s, e): <c>s &lt;= cl</c> and
    /// <c>ch &lt;= e</c>. A null bound denotes -infinity (low) or +infinity
    /// (high).
    /// </summary>
    private static bool RangeFullyInside(string? cl, string? ch, string? s, string? e)
    {
        if (s is not null && (cl is null || string.CompareOrdinal(cl, s) < 0))
        {
            return false;
        }
        if (e is not null && (ch is null || string.CompareOrdinal(ch, e) > 0))
        {
            return false;
        }
        return true;
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

    /// <inheritdoc />
    public async Task<ShardTopologyNode?> GetTopologySnapshotAsync(int depthLimit, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await PrepareForOperationAsync();
        cancellationToken.ThrowIfCancellationRequested();

        // Empty shard (no root yet): nothing to describe.
        if (state.State.RootNodeId is null)
        {
            return null;
        }

        if (RootIsLeafTyped)
        {
            // Flat-tree case: the single root leaf is the whole topology, so
            // the one unavoidable leaf call here is O(1) - it never fans out.
            // Decided by node TYPE: a corrupt RootIsLeaf flag over an internal
            // root (issue 899) takes the internal-rooted path below instead of
            // blind-casting the internal root to IBPlusLeafGrain.
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(state.State.RootNodeId!.Value);
            var leafNode = await leaf.GetTopologyNodeAsync();
            return leafNode with { ShardIndex = ShardIndex };
        }

        // Internal-rooted tree: the root internal node reconstructs the
        // structure from the per-child snapshot tables that mutations have
        // already propagated upward, so leaves are summarised in-place and
        // never called. Cost is bounded by the internal nodes visited under
        // depthLimit, not by leaf count.
        var root = grainFactory.GetGrain<IBPlusInternalGrain>(state.State.RootNodeId!.Value);
        var topology = await root.GetTopologyAsync(depthLimit);
        return topology with { ShardIndex = ShardIndex };
    }
}
