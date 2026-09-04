namespace Orleans.Lattice;

/// <summary>
/// Groups a batch by the physical shard that owns each key, for the
/// per-shard fan-out the batch read and write paths perform.
/// </summary>
/// <remarks>
/// <para>
/// The grouping key is a <em>physical</em> shard index, which lives in a
/// tiny dense domain (one entry per shard root, typically 1-16) even though
/// the virtual slot space it is projected from is 4096 wide. Hashing that
/// domain once per batch entry is pure overhead: a dense
/// <c>List&lt;T&gt;?[]</c> indexed directly by the physical index answers
/// the same question with a bounds-checked array read.
/// </para>
/// <para>
/// <see cref="ShardMap.GetPhysicalShardIndices"/> returns <em>distinct
/// ascending</em> indices and every value <see cref="ShardMap.Resolve"/> can
/// return is drawn from that set, so the last element is an exact upper
/// bound on the dense array length. A hand-constructed map carrying a
/// negative or pathologically large physical index falls back to the hash
/// map, mirroring the guard <c>ShardMap.GetPhysicalShardIndices</c> and
/// <c>LatticeGrain.BuildOwnedSlotMap</c> already keep for the same reason.
/// </para>
/// </remarks>
internal static class ShardFanout
{
    /// <summary>
    /// Upper bound on the dense array length, mirroring
    /// <c>ShardMap</c>'s own heap-bitmap threshold.
    /// </summary>
    internal const int DenseOwnerLimit = 1 << 20;

    /// <summary>
    /// Computes the dense bucket-array length for <paramref name="physicalShards"/>,
    /// or reports that the index domain is unsuitable for a dense array.
    /// </summary>
    internal static bool TryGetDenseLength(IReadOnlyList<int> physicalShards, out int length)
    {
        length = 0;
        var count = physicalShards.Count;
        if (count == 0)
            return false;

        // Distinct ascending: the first element is the min and the last is the max.
        if (physicalShards[0] < 0)
            return false;

        var max = physicalShards[count - 1];
        if (max >= DenseOwnerLimit)
            return false;

        length = max + 1;
        return true;
    }

    /// <summary>
    /// Buckets <paramref name="keys"/> by owning physical shard.
    /// </summary>
    internal static ShardFanoutBuckets<string> BucketKeys(
        IReadOnlyList<string> keys,
        ShardMap shardMap,
        IReadOnlyList<int> physicalShards,
        int bucketCapacity)
    {
        if (TryGetDenseLength(physicalShards, out var length))
        {
            var dense = new List<string>?[length];
            var distinct = 0;
            for (var i = 0; i < keys.Count; i++)
            {
                var key = keys[i];
                var idx = shardMap.Resolve(key);
                var bucket = dense[idx];
                if (bucket is null)
                {
                    bucket = new List<string>(bucketCapacity);
                    dense[idx] = bucket;
                    distinct++;
                }

                bucket.Add(key);
            }

            return new ShardFanoutBuckets<string>(dense, distinct);
        }

        var sparse = new Dictionary<int, List<string>>(physicalShards.Count);
        for (var i = 0; i < keys.Count; i++)
        {
            var key = keys[i];
            var idx = shardMap.Resolve(key);
            if (!sparse.TryGetValue(idx, out var bucket))
            {
                bucket = new List<string>(bucketCapacity);
                sparse[idx] = bucket;
            }

            bucket.Add(key);
        }

        return new ShardFanoutBuckets<string>(sparse);
    }

    /// <summary>
    /// Buckets <paramref name="entries"/> by the physical shard owning each entry's key.
    /// </summary>
    internal static ShardFanoutBuckets<KeyValuePair<string, byte[]>> BucketEntries(
        IReadOnlyList<KeyValuePair<string, byte[]>> entries,
        ShardMap shardMap,
        IReadOnlyList<int> physicalShards,
        int bucketCapacity)
    {
        if (TryGetDenseLength(physicalShards, out var length))
        {
            var dense = new List<KeyValuePair<string, byte[]>>?[length];
            var distinct = 0;
            for (var i = 0; i < entries.Count; i++)
            {
                var entry = entries[i];
                var idx = shardMap.Resolve(entry.Key);
                var bucket = dense[idx];
                if (bucket is null)
                {
                    bucket = new List<KeyValuePair<string, byte[]>>(bucketCapacity);
                    dense[idx] = bucket;
                    distinct++;
                }

                bucket.Add(entry);
            }

            return new ShardFanoutBuckets<KeyValuePair<string, byte[]>>(dense, distinct);
        }

        var sparse = new Dictionary<int, List<KeyValuePair<string, byte[]>>>(physicalShards.Count);
        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            var idx = shardMap.Resolve(entry.Key);
            if (!sparse.TryGetValue(idx, out var bucket))
            {
                bucket = new List<KeyValuePair<string, byte[]>>(bucketCapacity);
                sparse[idx] = bucket;
            }

            bucket.Add(entry);
        }

        return new ShardFanoutBuckets<KeyValuePair<string, byte[]>>(sparse);
    }

    /// <summary>
    /// Computes the shard-fair per-bucket capacity for a batch of
    /// <paramref name="itemCount"/> items spread over
    /// <paramref name="physicalShardCount"/> shards, floored at 4 and capped
    /// at 256 so a wide batch does not pre-allocate the whole batch per shard.
    /// </summary>
    internal static int BucketCapacity(int itemCount, int physicalShardCount)
        => Math.Min(Math.Max(4, itemCount / Math.Max(1, physicalShardCount)), 256);
}

/// <summary>
/// The result of a <see cref="ShardFanout"/> bucketing pass: the non-empty
/// per-shard buckets, enumerable as <c>(shardIndex, bucket)</c> pairs in
/// ascending shard-index order on the dense path.
/// </summary>
/// <typeparam name="T">The bucketed item type.</typeparam>
internal readonly struct ShardFanoutBuckets<T>
{
    private readonly List<T>?[]? _dense;
    private readonly Dictionary<int, List<T>>? _sparse;

    internal ShardFanoutBuckets(List<T>?[] dense, int count)
    {
        _dense = dense;
        _sparse = null;
        Count = count;
    }

    internal ShardFanoutBuckets(Dictionary<int, List<T>> sparse)
    {
        _dense = null;
        _sparse = sparse;
        Count = sparse.Count;
    }

    /// <summary>Gets the number of non-empty buckets.</summary>
    public int Count { get; }

    /// <summary>Gets an enumerator over the non-empty buckets.</summary>
    public Enumerator GetEnumerator() => new(_dense, _sparse);

    /// <summary>Enumerates the non-empty <c>(shardIndex, bucket)</c> pairs.</summary>
    public struct Enumerator
    {
        private readonly List<T>?[]? _dense;
        private Dictionary<int, List<T>>.Enumerator _sparse;
        private int _index;

        internal Enumerator(List<T>?[]? dense, Dictionary<int, List<T>>? sparse)
        {
            // A default-constructed ShardFanoutBuckets<T> has neither backing
            // store; enumerate an empty dense array so MoveNext stays total.
            _dense = dense ?? (sparse is null ? Array.Empty<List<T>?>() : null);
            _sparse = _dense is null ? sparse!.GetEnumerator() : default;
            _index = -1;
            Current = default;
        }

        /// <summary>Gets the current bucket and the physical shard index owning it.</summary>
        public (int ShardIndex, List<T> Bucket) Current { get; private set; }

        /// <summary>Advances to the next non-empty bucket.</summary>
        public bool MoveNext()
        {
            var dense = _dense;
            if (dense is not null)
            {
                for (var i = _index + 1; i < dense.Length; i++)
                {
                    var bucket = dense[i];
                    if (bucket is null)
                        continue;

                    _index = i;
                    Current = (i, bucket);
                    return true;
                }

                _index = dense.Length;
                return false;
            }

            if (_sparse.MoveNext())
            {
                var pair = _sparse.Current;
                Current = (pair.Key, pair.Value);
                return true;
            }

            return false;
        }
    }
}
