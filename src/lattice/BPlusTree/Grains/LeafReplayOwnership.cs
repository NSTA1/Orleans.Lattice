using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The ownership a leaf replays a WAL window under, captured once per replay:
/// the judge that decides which records the leaf applies
/// (<see cref="BPlusLeafGrain.ShouldApplyDuringReplay"/>) together with the
/// <see cref="WalKeyFilter"/> that pushes the same decision down to storage
/// (issue #3565).
/// <para>
/// <b>Why the two are one value.</b> Pushing the filter down is safe only
/// because every record it drops is one the judge would reject. That holds when
/// both are evaluated over the same shard index, key bounds and shard map, and it
/// can fail the moment they are read separately: a range that narrowed between
/// building the filter and judging a slice would let the filter drop a record the
/// judge now owns. Capturing both from one read of the leaf's state makes "a
/// slice is judged by the ownership it was filtered with" true by construction
/// rather than by care at each call site.
/// </para>
/// <para>
/// The filter carries the shard axis only when the judge resolves shard
/// ownership through a shard map. Without one the judge falls back to the
/// record's stamped shard index, which is not part of the routing prefix
/// storage reads, so that axis stays with the judge and the filter constrains
/// the key range alone - a weaker filter, never a wrong one.
/// </para>
/// </summary>
internal readonly struct LeafReplayOwnership
{
    private readonly int? _shardIndex;
    private readonly string? _lowKeyInclusive;
    private readonly string? _highKeyExclusive;
    private readonly ShardMap? _shardMap;

    private LeafReplayOwnership(int? shardIndex, string? lowKeyInclusive, string? highKeyExclusive, ShardMap? shardMap)
    {
        _shardIndex = shardIndex;
        _lowKeyInclusive = lowKeyInclusive;
        _highKeyExclusive = highKeyExclusive;
        _shardMap = shardMap;
        Filter = shardIndex is int shard && shardMap is { Slots.Length: > 0 }
            ? new WalKeyFilter(lowKeyInclusive, highKeyExclusive, shardMap, shard)
            : new WalKeyFilter(lowKeyInclusive, highKeyExclusive);
    }

    /// <summary>
    /// The filter pushed down with every slice this replay reads. Excludes
    /// exactly the key-scoped records <see cref="ShouldApply"/> rejects on
    /// ownership grounds, or a subset of them.
    /// </summary>
    public WalKeyFilter Filter { get; }

    /// <summary>
    /// Captures the leaf's current ownership from its persisted state and the
    /// replay's resolved shard map.
    /// </summary>
    /// <param name="state">The leaf's persisted state.</param>
    /// <param name="replayShardMap">The routing map the replay resolved, or <see langword="null"/> for the stamped-index fallback.</param>
    public static LeafReplayOwnership Capture(LeafNodeState state, ShardMap? replayShardMap)
    {
        ArgumentNullException.ThrowIfNull(state);
        return new LeafReplayOwnership(
            state.ShardIndex,
            state.LowKeyInclusive,
            state.HighKeyExclusive,
            replayShardMap);
    }

    /// <summary>
    /// The replay judgement for <paramref name="mutation"/>, over the captured
    /// ownership: <see cref="BPlusLeafGrain.ShouldApplyDuringReplay"/>.
    /// </summary>
    public bool ShouldApply(in LatticeMutation mutation) =>
        BPlusLeafGrain.ShouldApplyDuringReplay(
            mutation, _shardIndex, _lowKeyInclusive, _highKeyExclusive, _shardMap);
}
