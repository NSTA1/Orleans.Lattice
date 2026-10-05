namespace Orleans.Lattice.Replication;

/// <summary>
/// Receiver-side, in-process index mapping a replicated key to the write the
/// receiver most recently merged for it: the
/// <see cref="ReplicationContentHash"/> of its value together with its source
/// identity (origin cluster id and source HLC). Partitioned per logical tree and
/// bounded by a least-recently-used eviction policy within each tree. It answers
/// the content-manifest exchange handler's first question - "did I already merge
/// exactly this write?" - so the receiver can report the subset of a sender's
/// manifest it is missing and let the sender elide the rest.
/// <para>
/// A hit is a candidate, not a verdict (#4585). Bytes alone say nothing about
/// last-writer-wins order: the same value at a newer version, from another origin,
/// or recorded from a merge that lost at the leaf must still ship. The handler
/// therefore elides only an entry whose exact source identity was recorded here
/// <em>and</em> whose key the leaf still holds at that version or a newer one, so
/// a record left stale by anything that lowers the leaf (a restore or its revert,
/// a purge and recreate, an alias rebind, a clearing bootstrap) never elides.
/// </para>
/// <para>
/// The index is a <b>best-effort cache</b>, never a correctness oracle. A
/// key that is absent (cold start, never applied, or evicted) is simply
/// reported as not held, so the manifest handler treats it as missing and
/// the sender ships the payload verbatim - always safe. Receiver-side
/// apply still relies on the shadow-forward identity cache and
/// idempotent leaf merge; this index only decides
/// whether a payload can be
/// elided, never whether a mutation is applied. It is process-local change
/// state and is never serialized.
/// </para>
/// <para>
/// The content hash is computed with the same FNV-1a digest the sender
/// manifests (<see cref="ReplicationContentHash"/>), so a hit means the
/// receiver holds byte-identical value bytes. The index is maintained only
/// while the content-hash dedup master switch is enabled; when it is off
/// the apply path never records into it, so the feature stays cost-free by
/// default. A single lock guards the whole structure because it is a
/// process-wide singleton read by inbound-apply turns and written by the
/// gRPC exchange handler concurrently.
/// </para>
/// </summary>
internal sealed class ReceiverAppliedContentIndex
{
    private readonly object _gate = new();
    private readonly Dictionary<string, TreePartition> _trees = new(StringComparer.Ordinal);

    /// <summary>
    /// Records that the receiver has merged the write of
    /// <paramref name="originClusterId"/> at <paramref name="hlc"/>, whose value
    /// has digest <paramref name="contentHash"/>, for <paramref name="key"/> on the
    /// tree named <paramref name="treeId"/>, promoting the key to
    /// most-recently-used within that tree's partition and evicting the
    /// least-recently-used key when the partition exceeds
    /// <paramref name="capacity"/>. An existing entry for the key is
    /// overwritten with the new digest. <paramref name="capacity"/> is
    /// floored at <c>1</c> so a degenerate option value still retains the
    /// hottest key.
    /// </summary>
    /// <param name="treeId">The logical tree the applied entry belongs to. Must be non-null.</param>
    /// <param name="key">The applied key. Must be non-null.</param>
    /// <param name="contentHash">The FNV-1a digest of the applied value bytes.</param>
    /// <param name="originClusterId">The applied write's origin cluster id.</param>
    /// <param name="hlc">The applied write's source HLC.</param>
    /// <param name="capacity">
    /// Maximum number of distinct keys this tree's partition retains.
    /// Values below <c>1</c> are treated as <c>1</c>.
    /// </param>
    public void RecordSet(string treeId, string key, ulong contentHash, string? originClusterId, HybridLogicalClock hlc, int capacity)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(key);

        var bounded = capacity < 1 ? 1 : capacity;
        lock (_gate)
        {
            if (!_trees.TryGetValue(treeId, out var partition))
            {
                partition = new TreePartition();
                _trees[treeId] = partition;
            }
            partition.Set(key, new ReceiverHeldContent(contentHash, originClusterId, hlc), bounded);
        }
    }

    /// <summary>
    /// Removes any recorded content for <paramref name="key"/> on the tree
    /// named <paramref name="treeId"/>, reflecting a delete the receiver
    /// applied so a subsequent manifest of the same key is reported as
    /// missing rather than elided against stale content. A no-op when the
    /// key (or tree) is not tracked.
    /// </summary>
    /// <param name="treeId">The logical tree the deleted entry belonged to. Must be non-null.</param>
    /// <param name="key">The deleted key. Must be non-null.</param>
    public void RecordDelete(string treeId, string key)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(key);

        lock (_gate)
        {
            if (_trees.TryGetValue(treeId, out var partition))
            {
                partition.Remove(key);
            }
        }
    }

    /// <summary>
    /// Drops the entire content index for the tree named
    /// <paramref name="treeId"/>. Used after a range delete, whose
    /// per-key footprint the index does not enumerate: clearing the
    /// partition conservatively forces every key on the tree to be
    /// reported as missing until it re-populates, which is always safe.
    /// A no-op when the tree is not tracked.
    /// </summary>
    /// <param name="treeId">The logical tree to invalidate. Must be non-null.</param>
    public void InvalidateTree(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        lock (_gate)
        {
            _trees.Remove(treeId);
        }
    }

    /// <summary>
    /// Looks up the write the receiver most recently merged for
    /// <paramref name="key"/> on the tree named <paramref name="treeName"/>.
    /// Returns <see langword="true"/> and its digest and source identity when
    /// the key is recorded, promoting it to most-recently-used; returns
    /// <see langword="false"/> (with <paramref name="held"/> set to its default)
    /// when the key is cold, evicted, or deleted.
    /// </summary>
    /// <param name="treeName">The logical tree to query. Must be non-null.</param>
    /// <param name="key">The key to look up. Must be non-null.</param>
    /// <param name="held">The recorded digest and source identity when recorded.</param>
    public bool TryGetContent(string treeName, string key, out ReceiverHeldContent held)
    {
        ArgumentNullException.ThrowIfNull(treeName);
        ArgumentNullException.ThrowIfNull(key);

        lock (_gate)
        {
            if (_trees.TryGetValue(treeName, out var partition)
                && partition.TryGet(key, out held))
            {
                return true;
            }
        }
        held = default;
        return false;
    }

    /// <summary>
    /// The number of distinct keys currently retained for the tree named
    /// <paramref name="treeId"/>, or <c>0</c> when the tree is not tracked.
    /// Intended for tests and diagnostics.
    /// </summary>
    /// <param name="treeId">The logical tree to count. Must be non-null.</param>
    public int CountForTree(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        lock (_gate)
        {
            return _trees.TryGetValue(treeId, out var partition) ? partition.Count : 0;
        }
    }

    /// <summary>
    /// Per-tree bounded LRU map of key to recorded write. Not thread-safe
    /// on its own; every access is serialized by the owning index's lock.
    /// </summary>
    private sealed class TreePartition
    {
        private readonly LinkedList<KeyHash> _order = new();
        private readonly Dictionary<string, LinkedListNode<KeyHash>> _index = new(StringComparer.Ordinal);

        public int Count => _order.Count;

        public void Set(string key, ReceiverHeldContent content, int capacity)
        {
            if (_index.TryGetValue(key, out var existing))
            {
                existing.Value = new KeyHash(key, content);
                _order.Remove(existing);
                _order.AddLast(existing);
                // A hit never grows the partition, so only a capacity that
                // shrank between calls can leave it over its bound.
                Trim(capacity);
                return;
            }

            // Miss. Once the partition is at its bound, admitting a key must
            // evict one - so detach the least-recently-used node and re-file it
            // under the incoming key rather than allocating a replacement and
            // letting the evicted node become garbage. On a warm partition
            // sitting at capacity (the steady state on the receiver apply path)
            // this removes the per-miss LinkedListNode allocation outright. The
            // survivor set is unchanged: the recycled node is re-attached at
            // most-recently-used, exactly where a freshly allocated one landed,
            // and Trim still evicts from the least-recently-used end.
            if (_order.Count >= capacity && _order.First is { } lru)
            {
                _index.Remove(lru.Value.Key);
                _order.Remove(lru);
                lru.Value = new KeyHash(key, content);
                _order.AddLast(lru);
                _index[key] = lru;
            }
            else
            {
                _index[key] = _order.AddLast(new KeyHash(key, content));
            }

            Trim(capacity);
        }

        /// <summary>
        /// Evicts from the least-recently-used end until the partition is
        /// within <paramref name="capacity"/>. A no-op in the steady state;
        /// it does real work only when the configured capacity shrank between
        /// calls.
        /// </summary>
        private void Trim(int capacity)
        {
            while (_order.Count > capacity)
            {
                var lru = _order.First!;
                _index.Remove(lru.Value.Key);
                _order.RemoveFirst();
            }
        }

        public bool TryGet(string key, out ReceiverHeldContent content)
        {
            if (_index.TryGetValue(key, out var node))
            {
                content = node.Value.Content;
                _order.Remove(node);
                _order.AddLast(node);
                return true;
            }
            content = default;
            return false;
        }

        public void Remove(string key)
        {
            if (_index.Remove(key, out var node))
            {
                _order.Remove(node);
            }
        }

        private readonly record struct KeyHash(string Key, ReceiverHeldContent Content);
    }
}
