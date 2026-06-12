using System.Globalization;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The read-only Merkle-walk drift-localisation engine. Given a shard whose
/// content digest mismatched a peer, it descends the local cluster's
/// internal-node tree top-down by cluster-stable separator-key ranges, asking
/// the peer for its subtree digest over the same key-range at each node, and
/// narrows the divergence to a leaf (or small leaf set). It caps recursion
/// depth and total digest bytes inspected, emits the localised and aborted
/// counters, and never mutates data or any replication cursor.
/// </summary>
internal static class MerkleWalkLocaliser
{
    /// <summary>
    /// Walks the shard's local internal-node tree to localise a digest mismatch
    /// against a peer.
    /// </summary>
    /// <param name="treeName">The logical replicated-tree name.</param>
    /// <param name="shardIndex">The shard index whose mismatch is being localised.</param>
    /// <param name="peer">The peer cluster identifier to probe.</param>
    /// <param name="localTree">A read-only view over the local shard tree.</param>
    /// <param name="transport">The read-only probe transport used to ask the peer for range digests.</param>
    /// <param name="maxDepth">The recursion-depth cap; the walk aborts when an internal node at this depth still diverges.</param>
    /// <param name="maxBytes">The per-walk budget of digest hash bytes inspected.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The outcome of the walk.</returns>
    public static async Task<MerkleWalkOutcome> WalkAsync(
        string treeName,
        int shardIndex,
        string peer,
        IMerkleWalkLocalTree localTree,
        IReplicationDigestProbeTransport transport,
        int maxDepth,
        long maxBytes,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeName);
        ArgumentNullException.ThrowIfNull(peer);
        ArgumentNullException.ThrowIfNull(localTree);
        ArgumentNullException.ThrowIfNull(transport);

        var root = await localTree.GetRootAsync(cancellationToken).ConfigureAwait(false);
        if (root is null)
        {
            // Empty shard - nothing to localise.
            return MerkleWalkOutcome.NotLocalised;
        }

        var state = new WalkState();
        await DescendAsync(root.Value, rangeStart: null, rangeEnd: null, depth: 0).ConfigureAwait(false);

        if (state.Abort != MerkleWalkAbortReason.None)
        {
            RecordAborted(state.Abort);
            return new MerkleWalkOutcome
            {
                Localised = false,
                LeavesLocalised = 0,
                DepthReached = state.DepthReached,
                AbortReason = state.Abort,
                BytesInspected = state.BytesInspected,
            };
        }

        if (state.LeavesLocalised > 0)
        {
            RecordLocalised(treeName, state.DepthReached, state.LeavesLocalised);
            return new MerkleWalkOutcome
            {
                Localised = true,
                LeavesLocalised = state.LeavesLocalised,
                DepthReached = state.DepthReached,
                AbortReason = MerkleWalkAbortReason.None,
                BytesInspected = state.BytesInspected,
            };
        }

        // No divergence reproduced below the shard root (e.g. a transient race
        // that has since converged) - nothing localised, no abort.
        return new MerkleWalkOutcome
        {
            Localised = false,
            LeavesLocalised = 0,
            DepthReached = state.DepthReached,
            AbortReason = MerkleWalkAbortReason.None,
            BytesInspected = state.BytesInspected,
        };

        async Task DescendAsync(MerkleWalkLocalNode node, string? rangeStart, string? rangeEnd, int depth)
        {
            if (state.Abort != MerkleWalkAbortReason.None)
            {
                return;
            }

            MerkleWalkProbeResponse remote;
            try
            {
                remote = await transport.ProbeMerkleWalkAsync(
                    peer,
                    new MerkleWalkProbeRequest
                    {
                        TreeName = treeName,
                        ShardIndex = shardIndex,
                        RangeStartKey = rangeStart,
                        RangeEndKey = rangeEnd,
                        Depth = depth,
                    },
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception) when (!cancellationToken.IsCancellationRequested)
            {
                state.Abort = MerkleWalkAbortReason.RemoteUnavailable;
                return;
            }

            if (!remote.Available)
            {
                state.Abort = MerkleWalkAbortReason.RemoteUnavailable;
                return;
            }

            var localHashLen = node.Digest.Hash?.Length ?? 0;
            var remoteHashLen = remote.Digest.Hash?.Length ?? 0;
            state.BytesInspected += localHashLen + remoteHashLen;
            if (state.BytesInspected > maxBytes)
            {
                state.Abort = MerkleWalkAbortReason.ByteBudgetExceeded;
                return;
            }

            if (node.Digest.Version != remote.Digest.Version)
            {
                state.Abort = MerkleWalkAbortReason.VersionSkew;
                return;
            }

            if (HashesEqual(node.Digest.Hash, remote.Digest.Hash))
            {
                // Subtree matches remotely - prune.
                return;
            }

            if (node.IsLeaf)
            {
                state.LeavesLocalised++;
                if (depth > state.DepthReached)
                {
                    state.DepthReached = depth;
                }

                return;
            }

            if (depth >= maxDepth)
            {
                state.Abort = MerkleWalkAbortReason.DepthCapExceeded;
                return;
            }

            var children = node.Children;
            var childCount = children?.Count ?? 0;
            for (var i = 0; i < childCount; i++)
            {
                if (state.Abort != MerkleWalkAbortReason.None)
                {
                    break;
                }

                var child = children![i];
                var childStart = child.SeparatorKey ?? rangeStart;
                var childEnd = (i + 1 < childCount) ? children[i + 1].SeparatorKey : rangeEnd;

                var childNode = await localTree
                    .ResolveAsync(child.NodeId, child.ChildIsLeaf, cancellationToken)
                    .ConfigureAwait(false);
                await DescendAsync(childNode, childStart, childEnd, depth + 1).ConfigureAwait(false);
            }
        }
    }

    private static void RecordLocalised(string tree, int depth, int leaves)
    {
        LatticeReplicationMetrics.MerkleWalkLocalised.Add(
            leaves,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, tree),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagDepth, depth.ToString(CultureInfo.InvariantCulture)));
    }

    private static void RecordAborted(MerkleWalkAbortReason reason)
    {
        LatticeReplicationMetrics.MerkleWalkAborted.Add(
            1,
            new KeyValuePair<string, object?>(
                LatticeReplicationMetrics.TagReason,
                LatticeReplicationMetrics.MerkleWalkAbortReasonTag(reason)));
    }

    private static bool HashesEqual(byte[]? a, byte[]? b)
    {
        if (a is null || b is null)
        {
            return ReferenceEquals(a, b);
        }

        return ((ReadOnlySpan<byte>)a).SequenceEqual(b);
    }

    private sealed class WalkState
    {
        public MerkleWalkAbortReason Abort;
        public int LeavesLocalised;
        public int DepthReached;
        public long BytesInspected;
    }
}
